# ARCHITECTURE.md — vmdk2kvm Internal Architecture

## Purpose
This document describes the **module-level architecture**, execution flow, and hard invariants inside `vmdk2kvm`.

It is written for contributors/reviewers who need to understand:
- **where logic lives**
- **how data and state move**
- **why the boundaries exist**

The repo is intentionally structured so we can fix “successful” conversions that still don’t boot, don’t network, or don’t behave deterministically.

---

## The canonical pipeline

Everything maps to one conceptual pipeline:

**FETCH → FLATTEN → INSPECT → PLAN → FIX → CONVERT → VALIDATE/TEST**

Not every command runs every stage, but the **ordering is never violated**.
Stages can be skipped, but they’re never permuted.

### What each stage means (in this repo)

- **FETCH**: Acquire the source disks/metadata from somewhere (vSphere, ESXi over SSH, local).
- **FLATTEN**: Normalize snapshot chains and weird disk formats into stable, single-image inputs.
- **INSPECT**: Use libguestfs (offline) to detect OS + mount layout + key files.
- **PLAN**: Decide *what should be done* (inventory + plan modes) before doing it.
- **FIX**: Apply offline (or explicitly live) mutations to make the guest boot/network sane.
- **CONVERT**: Perform the actual image format conversion (qemu-img style operations).
- **VALIDATE/TEST**: Run deterministic checks; optionally boot-test via libvirt or qemu harness.

---

## Actual repo layout (authoritative)

This matches your real `tree vmdk2kvm`:

```

vmdk2kvm/
├── **main**.py                  # python -m vmdk2kvm entry (dispatches CLI)
├── **init**.py

├── cli/                         # argument parsing + help UX
│   ├── argument_parser.py       # argparse + config merge entry
│   └── help_texts.py            # long YAML/CLI examples / epilog text

├── config/                      # config loading + defaults
│   ├── config_loader.py         # YAML/JSON load + merge + validation surface
│   └── systemd_template.py      # templates injected into guest (firstboot / services)

├── core/                        # shared primitives used everywhere
│   ├── cred.py                  # unified credential resolver (env + aliases)
│   ├── exceptions.py            # typed errors (VMwareError etc.)
│   ├── logger.py                # structured logging (emoji-safe), verbosity control
│   ├── recovery_manager.py      # checkpoint/resume primitives (idempotent orchestration)
│   ├── sanity_checker.py        # host/tool sanity (qemu-img, guestfs, etc.)
│   ├── utils.py                 # subprocess helpers + FS helpers + guest probing helpers
│   └── validation_suite.py      # post-fix assertions + safety checks

├── orchestrator/                # “pipeline runner”
│   └── orchestrator.py          # central conductor: stage ordering + recovery + reporting

├── converters/                  # conversions + extractions + disk operations
│   ├── fetch.py                 # conversion-oriented fetch wrapper (non-vSphere too)
│   ├── flatten.py               # snapshot flatten wrapper (qemu-img orchestration)
│   ├── qemu_converter.py        # qemu-img convert wrapper (format, compression, etc.)
│   ├── disk_resizer.py          # optional growth/resize logic (safe expansion)
│   ├── ovf_extractor.py         # OVA/OVF unpack + mapping
│   ├── ami_extractor.py         # AMI-ish extraction (where applicable)
│   └── vhd_extractor.py         # VHD/VHDX extraction/conversion helper

├── fixers/                      # offline & live guest mutation logic
│   ├── base_fixer.py            # base class contracts + shared fixer helpers
│   ├── offline_fixer.py         # main offline fixer orchestrator (calls sub-fixers)
│   ├── live_fixer.py            # live-fix entry (runs in a booted guest context)
│   ├── fstab_rewriter.py        # by-path → UUID/PARTUUID rewrite + mount invariants
│   ├── grub_fixer.py            # kernel cmdline + GRUB/BLS stabilization + initramfs regen
│   ├── bootloader_fixer.py      # higher-level bootloader layout handling (GRUB/BLS/etc.)
│   ├── network_fixer.py         # net config normalization (netplan/ifcfg/NetworkManager)
│   ├── windows_fixer.py         # Windows-only logic (BCD/registry/drivers) isolated
│   ├── cloud_init_injector.py   # cloud-init seed/injection logic (if requested)
│   ├── offline_vmware_tools_remover.py  # VMware tools removal (offline)
│   ├── live_grub_fixer.py       # live-mode bootloader touches (separate from offline)
│   └── report_writer.py         # emits conversion/fix report artifacts

├── modes/                       # “read-only” planning/introspection modes
│   ├── inventory_mode.py        # inspect + summarize (no mutation)
│   └── plan_mode.py             # compute fix plan (what would be changed)

├── testers/                     # boot verification harnesses
│   ├── qemu_tester.py           # direct qemu runner (serial / debug)
│   └── libvirt_tester.py        # libvirt XML + virt-install based validation

├── ssh/                         # SSH transport used by some fetch paths
│   ├── ssh_client.py            # paramiko/openssh wrapper
│   └── ssh_config.py            # ssh options + host profiles

└── vmware/                      # VMware/vSphere control plane + VDDK data plane
├── vsphere_mode.py          # CLI “vsphere …” mode glue / dispatch
├── vsphere_command.py       # subcommand implementations
├── vsphere_mode.py          # orchestration glue for vSphere paths
├── vmware_client.py         # vCenter/ESXi API client (pyvmomi control-plane)
├── vddk_client.py           # VDDK-based data-plane (disk reads/exports)
└── vmdk_parser.py           # VMDK chain parsing / metadata interpretation

```

---

## Control-plane vs data-plane (VMware paths)

`vmdk2kvm` splits VMware integration into two mental halves:

### Control-plane: “what exists, where is it, what should we do?”
- `vmware/vmware_client.py` (pyvmomi API session, inventory queries, VM metadata)
- `vmware/vsphere_mode.py` + `vmware/vsphere_command.py` (CLI glue, user intent → actions)
- `core/cred.py` (credential aliasing so the same config works across call sites)

### Data-plane: “move the bytes”
- `vmware/vddk_client.py` (high-throughput disk export/read path when VDDK is used)
- `ssh/` (when you fetch over SSH/SCP rather than VDDK)
- `converters/fetch.py` (non-VMware fetch unification + local copy handling)
- `converters/flatten.py` (normalize the disk chain once bytes land locally)

The orchestrator treats both as interchangeable fetchers: a VM is a VM; the byte path is just plumbing.

---

## Where the pipeline actually “runs”

### The orchestrator is the boss
`orchestrator/orchestrator.py` is the conductor:
- enforces stage ordering
- coordinates recovery/resume (`core/recovery_manager.py`)
- calls sanity checks (`core/sanity_checker.py`)
- calls converters/fixers/testers as needed
- writes reports (`fixers/report_writer.py`)

### Fix orchestration splits by *offline vs live*
- `fixers/offline_fixer.py`: **default** for conversions (libguestfs, no boot required)
- `fixers/live_fixer.py`: only when explicitly requested (guest is running)

That separation is not aesthetic — it keeps “guest state assumptions” from leaking everywhere.

---

## Key architectural invariants (the laws of physics here)

### 1) Fixers are offline by default
Unless explicitly in `live-fix`, every fixer must assume:
- no systemd runtime
- no efivars
- no proc/sys guarantees
- only the disk image + libguestfs

### 2) libguestfs is the authority
OS type, mount layout, and partition reality come from inspection, not vibes.
If a fixer needs to know “where root is” or “does this use BLS?”, it must derive it from the inspected guest.

### 3) `/dev/disk/by-path` is radioactive
Anything touching:
- `/etc/fstab`
- bootloader cmdline (GRUB/BLS/kernel-install)
- crypttab / initramfs inputs

…must eliminate by-path and prefer stable identifiers (UUID/PARTUUID/LABEL) based on the real devices.

### 4) Windows stays in its lane
Windows logic lives in `fixers/windows_fixer.py` and nowhere else.
Linux fixers are allowed to *detect* Windows and then bail out, but never mix behaviors.

### 5) Best-effort + idempotent-ish behavior
Fixers should tolerate being run multiple times.
A failure should be **contained** (warn + continue) unless it violates a hard prerequisite of the selected command.

---

## Module responsibilities (who owns what)

### `cli/`
Owns:
- CLI surface area
- YAML examples and help text formatting
- config merge entrypoint (multiple `--config` files)

Does *not* own:
- business logic
- fixer behavior
- conversion implementation

### `config/`
Owns:
- config schema surface (as implemented)
- defaults + template blobs (systemd unit templates)
- config merging rules

### `core/`
Owns:
- logging
- exceptions
- subprocess helpers
- sanity checks
- recovery/resume primitives
- validation primitives

### `vmware/` and `ssh/`
Own:
- remote access and disk acquisition mechanisms

### `converters/`
Own:
- qemu-img operations
- flattening
- conversion steps (format outputs)
- extraction logic (OVF/OVA/VHD/AMI-like)

### `fixers/`
Own:
- guest mutation (bootloader, fstab, network, cleanup, cloud-init)
- offline vs live split
- reporting output for fix phase

### `modes/`
Own:
- “read-only” workflows: inventory and planning

### `testers/`
Own:
- actual boot smoke tests (libvirt/qemu)
- VM launch scaffolding for validation (not conversion)

---

## Why this structure works

Because conversions fail in boring, repeatable ways:
- fstab points to by-path, disks reorder, boot dies
- root= missing or wrong, GRUB/BLS diverges
- initramfs missing storage/network drivers for the new virtual hardware
- network config expects old NIC names/MACs
- VMware tools/hardware assumptions linger
- Windows needs driver/BCD/registry surgery, and it must not contaminate Linux logic

This architecture gives:
- **determinism** (inspection-driven decisions)
- **retries** (recovery manager + idempotent fixers)
- **partial reuse** (modes/testers can run without converting)
- **controlled blast radius** (Windows isolated, live-fix isolated)

---

## Adding a new feature (design rule)

New features must fit one of these buckets:

1) **Fetch path** (new source: protocol/platform) → `vmware/` or `ssh/` or `converters/fetch.py`  
2) **Flatten/convert** (new disk format behavior) → `converters/`  
3) **Inspect/plan** (better detection + decision logic) → `modes/` + `core/` + inspection helpers  
4) **Fix** (guest mutation) → `fixers/` (offline first; live only when unavoidable)  
5) **Validate/test** (new boot test harness or checks) → `testers/` + `core/validation_suite.py`

If it doesn’t fit any stage, it probably belongs in the orchestrator only if it is stage coordination, not stage behavior.

