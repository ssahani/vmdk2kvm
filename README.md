# vmdk2kvm

**VMware → KVM/QEMU conversion, repair, and automation toolkit**

`vmdk2kvm` is a production-oriented toolkit for migrating VMware VMs (VMDK / OVA / OVF / ESXi / vCenter) into KVM/QEMU-bootable images **without relying on boot-time luck**.

It exists to solve the problems that appear *after* a “successful” conversion:
broken boots, unstable device naming, missing drivers, corrupted snapshot chains,
and Windows guests that blue-screen on first KVM boot.

This repo is intentionally **not** “click migrate and pray”.

---

## Table of contents

1. [Scope and non-goals](#1-scope-and-non-goals)
2. [Design principles](#2-design-principles)
3. [Supported inputs and execution modes](#3-supported-inputs-and-execution-modes)
4. [Pipeline model](#4-pipeline-model)
5. [Control-plane vs data-plane (vSphere, VDDK, HTTP)](#5-control-plane-vs-data-plane-vsphere-vddk-http)
6. [Linux fixes](#6-linux-fixes)
7. [Windows handling](#7-windows-handling)
8. [Snapshots and flattening](#8-snapshots-and-flattening)
9. [Output formats and validation](#9-output-formats-and-validation)
10. [YAML configuration model](#10-yaml-configuration-model)
11. [Multi-VM and batch processing](#11-multi-vm-and-batch-processing)
12. [Live-fix mode (SSH)](#12-live-fix-mode-ssh)
13. [ESXi and vSphere integration](#13-esxi-and-vsphere-integration)
14. [virt-v2v integration strategy](#14-virt-v2v-integration-strategy)
15. [Safety mechanisms](#15-safety-mechanisms)
16. [Daemon mode and automation](#16-daemon-mode-and-automation)
17. [Testing and verification](#17-testing-and-verification)
18. [Failure modes and troubleshooting](#18-failure-modes-and-troubleshooting)
19. [When not to use this tool](#19-when-not-to-use-this-tool)
20. [Documentation index](#documentation-index)

---

## 1. Scope and non-goals

### What this tool **does**

- Converts VMware disks into KVM-usable formats
- Repairs Linux and Windows guests **offline** (and some Linux fixes live via SSH)
- Makes storage identifiers stable and deterministic
- Injects Windows VirtIO drivers safely (storage first, always)
- Flattens snapshot chains correctly
- Supports repeatable, automatable migrations via mergeable YAML
- Validates results via libvirt / qemu smoke tests

### What this tool **does not**

- Not a GUI wizard
- Not a cloud importer
- Not a “thin wrapper around `virt-v2v`”
- Not a promise of zero-touch Windows fixes without drivers
- Not a complexity-hider

If you want “fast over correct”, this repo will annoy you (politely, with logs).

---

## 2. Design principles

These are the rules that keep migrations from becoming folklore:

1. **Boot failures are caused by configuration, not conversion**  
   Disk bytes can be correct and the system can still not boot. Fix the config deterministically.

2. **Device naming must be stable across hypervisors**  
   `/dev/sdX`, `/dev/vdX`, and `/dev/disk/by-path/*` are portability traps.

3. **Snapshot chains lie unless flattened**  
   VMware snapshot metadata and assumptions do not carry cleanly into KVM.

4. **Windows storage must be BOOT_START before first KVM boot**  
   If you don’t pre-stage storage drivers + CDD entries, Windows will punish you with `INACCESSIBLE_BOOT_DEVICE`.

5. **Every destructive step must have a safe mode**  
   Dry-run, backups, atomic writes, and checkpoints are not optional.

6. **Configs must be replayable**  
   YAML is treated as an artifact: mergeable, reviewable, and rerunnable.

7. **Control-plane and data-plane must not be mixed**  
   Inventory/API logic (pyvmomi) should not be entangled with “copy bytes” logic (VDDK/HTTP/virt-v2v).

---

## 3. Supported inputs and execution modes

### Offline / local inputs
- Descriptor VMDK
- Monolithic VMDK
- Multi-extent snapshot chains

### Remote inputs
- ESXi over SSH/SCP
- Recursive fetch of full snapshot chain

### Archive formats
- OVA (tar + OVF + VMDK(s))
- OVF with extracted disks

### Live systems
- SSH access to running Linux guests (**live-fix**)

### API-based (vSphere)
- vCenter / ESXi via **pyvmomi** (VM discovery, disk inspection, snapshots, CBT queries, datastore browsing/downloads)

---

## 4. Pipeline model

Every mode maps to the same internal pipeline:

```

FETCH → FLATTEN → INSPECT → FIX → CONVERT → VALIDATE

````

Not every mode runs all stages.

| Stage    | Description |
|---------:|-------------|
| FETCH    | Obtain VMDK(s) from local, ESXi, OVA, OVF, or vSphere |
| FLATTEN  | Collapse snapshot chains into a single coherent disk |
| INSPECT  | Detect OS, filesystems, boot method, layout |
| FIX      | Apply OS-specific fixes offline or live |
| CONVERT  | Produce qcow2/raw/vdi output |
| VALIDATE | Boot via libvirt or qemu (optional but recommended) |

This pipeline is **explicit**, not implicit.

---

## 5. Control-plane vs data-plane (vSphere, VDDK, HTTP)

`vmdk2kvm` splits vSphere-related work into two planes:

### Control-plane (pyvmomi / pyVim)
Inventory + orchestration (fast API operations):
- list VMs, inspect VM config, locate disks
- create snapshots, enable CBT
- query changed block maps (CBT ranges)
- browse datastore directories (metadata/listing)

### Data-plane (moving bytes)
Choose the least invasive transport that fits the goal:

1. **virt-v2v** (conversion-grade)
   - Best when you want qcow2/raw output and guest conversion logic
   - Can use VDDK or SSH transport depending on environment

2. **HTTP `/folder`** (byte-for-byte datastore pulls)
   - Uses vCenter session cookie to download files from datastore paths
   - Great for “download-only VM folder” workflows, artifacts (vmx/nvram/vmdk), and ranged reads

3. **VDDK mode** (high-performance disk reads)
   - Best when you need raw disk throughput from VMware stacks
   - Commonly used for export or “download disk once” phases

4. **CBT delta sync** (incremental)
   - Control-plane gets changed ranges via CBT
   - Data-plane applies HTTP Range reads to patch a local base disk

**Why this split exists:** it keeps the repo sane. Control-plane code stays deterministic and testable. Data-plane is swappable based on environment constraints (TLS, access, VDDK availability, etc.).

---

## 6. Linux fixes

### `/etc/fstab` rewriting

Default behavior: `fstab_mode: stabilize-all`

Rewrites hostile identifiers:
- `/dev/disk/by-path/*`
- `/dev/sdX`, `/dev/vdX`, etc.

Into stable IDs, preferring:
1. `UUID=`
2. `PARTUUID=`
3. `LABEL=` / `PARTLABEL=`

This is done **offline**, not at boot.

### BTRFS handling
- Canonicalizes subvolume entries
- Removes `btrfsvol:` pseudo-specs when present
- Preserves mount options

### GRUB handling
- Stabilizes `root=` kernel arguments
- Cleans stale `device.map`
- Supports BIOS and UEFI layouts
- Optional skip via `no_grub: true`

### initramfs
- Best-effort regeneration
- Distro-aware tool detection
- Explicit opt-out supported

---

## 7. Windows handling

Windows is treated as a **first-class migration target**.

### Driver injection
- VirtIO storage (viostor / vioscsi) **boot-critical**
- Network (netkvm)
- Balloon / input / fs / GPU (optional)
- Architecture-aware (amd64 / x86 / arm64 where applicable)

### Registry edits (offline)
- SYSTEM hive modification via offline tooling (e.g., hivex path)
- Services created with correct:
  - StartType (BOOT / SYSTEM / AUTO)
  - Group
  - ImagePath
- CriticalDeviceDatabase entries populated

### BCD handling
- Detects BIOS and UEFI BCD stores
- Creates timestamped backups
- Avoids unsafe binary patching
- Provides recovery guidance

**Why this matters:** without BOOT_START storage + CDD entries, Windows fails with:
`INACCESSIBLE_BOOT_DEVICE` on first KVM boot.

---

## 8. Snapshots and flattening

### VMware snapshot chains
- Descriptor recursion supported
- Parent chains fetched automatically (where applicable)
- Flattening performed via safe conversion steps
- Atomic `.part → final` semantics where possible

### Why flattening is recommended
- VMware snapshot chains encode VMware-specific assumptions
- KVM does not interpret VMware snapshot metadata
- Flattening removes ambiguity and corruption risk

---

## 9. Output formats and validation

### Output formats
- `qcow2` (recommended)
- `raw`
- `vdi`

### Compression
- qcow2 compression supported
- Adjustable compression levels (1–9)

### Validation
- SHA256 checksum
- libvirt define + boot
- qemu direct boot
- BIOS or UEFI
- Headless mode supported

---

## 10. YAML configuration model

### Why YAML
- Repeatable
- Mergeable
- Reviewable
- Automatable

### Config layering
```bash
--config base.yaml --config vm.yaml --config overrides.yaml
````

Later configs override earlier ones.

### Multi-VM configs

```yaml
vms:
  - vmdk: vm1.vmdk
    to_output: vm1.qcow2
  - vmdk: vm2.vmdk
    to_output: vm2.qcow2
    compress: false
```

Top-level keys act as defaults.

> See: `docs/yaml-examples.md` (the copy-paste cookbook).

---

## 11. Multi-VM and batch processing

* Parallel disk conversion
* Shared defaults + per-VM overrides
* Recovery checkpoints
* Deterministic output paths

Designed for:

* fleet migrations
* CI pipelines
* drop-folder automation

---

## 12. Live-fix mode (SSH)

Used **after** migration, not instead of offline fixes.

Capabilities:

* rewrite fstab
* regenerate initramfs
* remove VMware tools
* safe sudo execution
* dry-run preview

Not suitable for:

* snapshot fixes
* Windows driver injection
* deep bootloader repair

---

## 13. ESXi and vSphere integration

### SSH-based ESXi fetch

* Descriptor and extent download
* Snapshot recursion
* No API dependency

### vSphere / pyvmomi mode

Control-plane actions:

* VM listing / inspection
* disk selection
* snapshot creation
* CBT enablement
* changed block queries

Data-plane actions:

* datastore downloads via HTTP `/folder`
* download-only VM folder pulls (vmx/vmdk/nvram/etc.)
* CBT delta sync via HTTP Range reads

CBT workflows are **explicit**, not magical.

---

## 14. virt-v2v integration strategy

Two patterns:

* `use_v2v`: virt-v2v does primary conversion, `vmdk2kvm` does deterministic post-repair
* `post_v2v`: `vmdk2kvm` prepares/fixes inputs, virt-v2v runs after

Rationale:

* virt-v2v solves some problems
* it does not solve all problems
* `vmdk2kvm` fills the gaps deliberately and reproducibly

---

## 15. Safety mechanisms

* `dry_run`
* internal backups
* atomic file replacement
* recovery checkpoints
* resume-safe operations
* explicit opt-out for destructive steps

Default posture: **safe, verbose, conservative**

---

## 16. Daemon mode and automation

* Watch directory for new VMDKs
* Automatic conversion
* systemd integration
* Logging + reports
* No interactive state

Designed for:

* ingestion pipelines
* CI/CD
* migration factories

---

## 17. Testing and verification

Testing is optional but strongly recommended:

* libvirt smoke test
* qemu direct boot
* BIOS / UEFI
* headless server compatibility
* timeout enforcement

Testing failures should stop the pipeline by default.

---

## 18. Failure modes and troubleshooting

### Common issues

* Missing VirtIO storage → Windows boot failure
* by-path fstab → random disk renumbering
* unflattened snapshots → silent corruption
* GUI devices on servers → libvirt test failures

### Tooling provided

* `--print-fstab`
* `--dump-config` (merged view)
* `--dump-args` (final argparse state)
* detailed logs + Markdown reports

---

## 19. When not to use this tool

* You need a GUI wizard
* You expect Windows to fix itself magically
* You can’t tolerate explicit configuration
* You want “fast” over “correct”

---

## Documentation index

* `README.md` — this file
* `docs/yaml-examples.md` — runnable configs + cookbook (including vSphere/CBT flows)
* `--help` — CLI reference
* `--dump-config` — merged config view
* `--dump-args` — final argparse state
