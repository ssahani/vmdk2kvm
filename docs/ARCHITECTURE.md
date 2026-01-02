Alright, let’s tighten the bolts and add **govc** properly—not as a bolt-on, but as a *first-class control-plane primitive* that coexists cleanly with pyvmomi and VDDK.

Below is an **enhanced ARCHITECTURE.md**, written as if it always knew govc would exist. I’ve **not changed your philosophy**, only sharpened the boundaries and made the invariants explicit.

You can drop this in as a replacement or cherry-pick sections.

---

# ARCHITECTURE.md — vmdk2kvm Internal Architecture

## Purpose

This document describes the **module-level architecture**, execution flow, and hard invariants inside `vmdk2kvm`.

It is written for contributors and reviewers who need to understand:

* **where logic lives**
* **how data and state move**
* **why the boundaries exist**

The repository is intentionally structured to fix **“successful” conversions** that still fail to boot, network, or behave deterministically after migration.

This is not accidental complexity.
It is containment.

---

## The canonical pipeline

Everything maps to one conceptual pipeline:

**FETCH → FLATTEN → INSPECT → PLAN → FIX → CONVERT → VALIDATE / TEST**

Not every command runs every stage, but the **ordering is never violated**.

Stages may be skipped.
They are never permuted.

### What each stage means (in this repo)

* **FETCH**
  Acquire source disks *and* metadata from somewhere (vSphere, ESXi over SSH, local filesystem).

* **FLATTEN**
  Normalize snapshot chains, delta extents, and odd formats into stable, single-image inputs.

* **INSPECT**
  Offline inspection via libguestfs to determine:
  OS family, firmware mode, mount layout, bootloader model, and critical files.

* **PLAN**
  Decide *what should be done* before doing it.
  Inventory, dry-run, and plan modes live here.

* **FIX**
  Apply deterministic mutations (offline by default) to make the guest bootable and sane.

* **CONVERT**
  Perform actual image format conversion and resizing (qemu-img–class operations).

* **VALIDATE / TEST**
  Deterministic verification and optional boot tests (libvirt or direct qemu).

---

## Actual repo layout (authoritative)

This reflects the real project structure and is the source of truth:

```
vmdk2kvm/
├── main.py
├── __init__.py

├── cli/
│   ├── argument_parser.py
│   └── help_texts.py

├── config/
│   ├── config_loader.py
│   └── systemd_template.py

├── core/
│   ├── cred.py
│   ├── exceptions.py
│   ├── logger.py
│   ├── recovery_manager.py
│   ├── sanity_checker.py
│   ├── utils.py
│   └── validation_suite.py

├── orchestrator/
│   └── orchestrator.py

├── converters/
│   ├── fetch.py
│   ├── flatten.py
│   ├── qemu_converter.py
│   ├── disk_resizer.py
│   ├── ovf_extractor.py
│   ├── ami_extractor.py
│   └── vhd_extractor.py

├── fixers/
│   ├── base_fixer.py
│   ├── offline_fixer.py
│   ├── live_fixer.py
│   ├── fstab_rewriter.py
│   ├── grub_fixer.py
│   ├── bootloader_fixer.py
│   ├── network_fixer.py
│   ├── windows_fixer.py
│   ├── cloud_init_injector.py
│   ├── offline_vmware_tools_remover.py
│   ├── live_grub_fixer.py
│   └── report_writer.py

├── modes/
│   ├── inventory_mode.py
│   └── plan_mode.py

├── testers/
│   ├── qemu_tester.py
│   └── libvirt_tester.py

├── ssh/
│   ├── ssh_client.py
│   └── ssh_config.py

└── vmware/
    ├── vsphere_mode.py
    ├── vsphere_command.py
    ├── vmware_client.py
    ├── vddk_client.py
    └── vmdk_parser.py
```

---

## Control-plane vs data-plane (VMware paths)

VMware support is **intentionally split** into two mental halves.

This split is not cosmetic.
It is what allows correctness.

---

### Control-plane: inventory, intent, planning

Control-plane answers:

> *What exists? Where is it? What should we do?*

Control-plane never moves large data.

#### Control-plane implementations

**Primary (preferred): govc**

* Used for:

  * VM discovery (name, UUID, MoRef)
  * Snapshot tree inspection
  * Disk backing path resolution
  * Firmware detection (BIOS vs UEFI)
  * CBT enablement and range queries
  * Datastore and folder listing
* Characteristics:

  * Stable CLI semantics
  * Predictable, scriptable output
  * Minimal SDK state leakage
  * Excellent coverage of real vSphere behavior

**Secondary / fallback: pyvmomi**

* Used when:

  * govc coverage is insufficient
  * deep object graph traversal is required
  * API-only attributes are needed
* Lives in:

  * `vmware/vmware_client.py`

**CLI glue**

* `vmware/vsphere_mode.py`
* `vmware/vsphere_command.py`

These map user intent (`vsphere inventory`, `vsphere download`, etc.) into **plans**, not data movement.

---

### Data-plane: moving bytes safely

Data-plane answers only one question:

> *How do we move bytes without lying to ourselves?*

Data-plane never makes inventory decisions.

#### Data-plane implementations

* **VDDK**

  * `vmware/vddk_client.py`
  * High-throughput disk reads
  * Used when performance matters

* **HTTP `/folder`**

  * Datastore file downloads
  * Supports ranged reads for CBT
  * Stateless and resumable

* **SSH / SCP**

  * `ssh/`
  * Used when APIs are unavailable
  * Lowest common denominator path

* **Local copy**

  * Unified through `converters/fetch.py`

Once bytes land locally, **all VMware-ness stops**.
From that point forward, disks are just disks.

---

## Where the pipeline actually runs

### The orchestrator is the authority

`orchestrator/orchestrator.py` is the conductor.

It:

* enforces pipeline ordering
* coordinates resume/recovery
* invokes sanity checks
* dispatches converters, fixers, and testers
* aggregates reports

It owns **when**, not **how**.

---

### Fix orchestration: offline vs live is a hard boundary

* **Offline (default)**
  `fixers/offline_fixer.py`
  Uses libguestfs. No boot assumptions. No runtime services.

* **Live (explicit opt-in)**
  `fixers/live_fixer.py`
  Assumes a running Linux guest over SSH.

This separation prevents runtime assumptions from infecting offline logic.

---

## Key architectural invariants (laws of physics)

### 1) Offline is the default truth

Unless explicitly in `live-fix` mode, fixers must assume:

* no systemd runtime
* no efivars
* no kernel APIs
* only disk images + libguestfs

If it requires a running system, it does not belong in offline fixers.

---

### 2) Inspection beats assumptions

libguestfs inspection is authoritative.

OS type, mount layout, firmware mode, bootloader style, and root device
**must be derived**, never guessed.

---

### 3) `/dev/disk/by-path` is radioactive

Any code touching:

* `/etc/fstab`
* bootloader cmdlines
* initramfs inputs
* crypttab

**must eliminate by-path usage** and replace it with
`UUID=`, `PARTUUID=`, or label-based identifiers derived from the real disk.

---

### 4) Windows logic is hermetically sealed

Windows behavior lives **only** in:

```
fixers/windows_fixer.py
```

Linux fixers may detect Windows and bail out.
They may not mutate Windows.

Cross-contamination is forbidden.

---

### 5) Best-effort, idempotent-ish behavior

* Fixers should tolerate re-runs
* Failures should be contained and reported
* Only hard prerequisites abort the pipeline

This is a repair tool, not a one-shot installer.

---

## Module responsibilities (ownership map)

### `cli/`

Owns:

* CLI surface
* help text
* YAML example presentation

Does not own logic.

---

### `config/`

Owns:

* config merging rules
* defaults
* template blobs injected into guests

---

### `core/`

Owns:

* logging
* exceptions
* subprocess helpers
* sanity checks
* recovery / resume
* validation primitives

---

### `vmware/` and `ssh/`

Own:

* remote access
* inventory queries
* disk acquisition mechanisms

No guest mutation allowed here.

---

### `converters/`

Own:

* qemu-img orchestration
* snapshot flattening
* format conversion
* extraction from foreign containers

---

### `fixers/`

Own:

* guest mutation
* offline vs live split
* reporting artifacts

---

### `modes/`

Own:

* read-only workflows
* inventory and planning

---

### `testers/`

Own:

* boot smoke tests
* libvirt and qemu harnesses
* validation execution

---

## Why this architecture holds up

Because failures are boring and repeatable:

* unstable disk identifiers
* broken root=
* missing initramfs drivers
* stale NIC naming
* VMware assumptions leaking through
* Windows needing *surgical*, ordered fixes

This design gives you:

* **determinism** (inspection-driven)
* **repeatability** (plans + recovery)
* **containment** (Windows isolated, live-fix isolated)
* **composability** (inventory, fix, test can run independently)

---

## Adding a new feature (design rule)

New features must land in exactly one bucket:

1. **Fetch path** → `vmware/`, `ssh/`, or `converters/fetch.py`
2. **Flatten / convert** → `converters/`
3. **Inspect / plan** → `modes/` + inspection helpers
4. **Fix** → `fixers/` (offline first)
5. **Validate / test** → `testers/` + `core/validation_suite.py`

If it doesn’t fit, it probably belongs in the orchestrator **only** as coordination logic.

---

### Final note

`govc` gives `vmdk2kvm` a **clean, auditable control-plane**.
libguestfs gives it **ground truth**.
The rest is plumbing, discipline, and refusal to guess.

This is how you make migrations boring—and boring is success.
