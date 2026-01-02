# vmdk2kvm

**VMware → KVM/QEMU conversion, repair, and automation toolkit**

`vmdk2kvm` is a production-oriented toolkit for migrating VMware virtual machines
(VMDK / OVA / OVF / ESXi / vCenter)
into **KVM/QEMU-bootable images**
**without relying on boot-time luck**.

It exists to solve the problems that appear *after* a “successful” conversion:

* broken boots
* unstable device naming
* missing or misordered drivers
* corrupted snapshot chains
* Windows guests that blue-screen on first KVM boot

This repository is intentionally **not** “click migrate and pray”.

---

## Table of contents

1. Scope and non-goals
2. Design principles
3. Supported inputs and execution modes
4. Pipeline model
5. Control-plane vs data-plane (vSphere, VDDK, HTTP, SSH)
6. Linux fixes
7. Windows handling
8. Snapshots and flattening
9. Output formats and validation
10. YAML configuration model
11. Multi-VM and batch processing
12. Live-fix mode (SSH)
13. ESXi and vSphere integration
14. virt-v2v integration strategy
15. Safety mechanisms
16. Daemon mode and automation
17. Testing and verification
18. Failure modes and troubleshooting
19. When not to use this tool
20. Documentation index

---

## 1. Scope and non-goals

### What this tool **does**

* Converts VMware disks into KVM-usable formats
* Repairs Linux and Windows guests **offline**
* Applies some Linux fixes **live over SSH**
* Stabilizes storage and network identifiers
* Injects Windows VirtIO drivers safely (storage first, always)
* Flattens VMware snapshot chains deterministically
* Enables repeatable, automatable migrations via mergeable YAML
* Validates results using libvirt / qemu smoke tests

### What this tool **does not**

* Not a GUI wizard
* Not a cloud importer
* Not a “thin wrapper around virt-v2v”
* Not a promise of zero-touch Windows fixes
* Not a complexity hider

If you want *fast over correct*, this repo will argue with you (politely, with logs).

---

## 2. Design principles

These principles are why this tool exists.

1. **Boot failures are configuration problems, not copy problems**
   Disk bytes can be perfect and the VM can still fail to boot.

2. **Device naming must survive hypervisor changes**
   `/dev/sdX`, `/dev/vdX`, and `/dev/disk/by-path/*` are portability traps.

3. **Snapshot chains lie unless flattened**
   VMware snapshot metadata does not carry cleanly into KVM.

4. **Windows storage must be BOOT_START before first KVM boot**
   Otherwise you get `INACCESSIBLE_BOOT_DEVICE`. Always.

5. **Every destructive step needs a safe mode**
   Dry-run, backups, atomic writes, and checkpoints are mandatory.

6. **Configurations must be replayable**
   YAML is an artifact: mergeable, reviewable, rerunnable.

7. **Control-plane and data-plane must not be mixed**
   Inventory logic must not be entangled with byte-moving logic.

---

## 3. Supported inputs and execution modes

### Offline / local

* Descriptor VMDK
* Monolithic VMDK
* Multi-extent snapshot chains

### Remote

* ESXi over SSH/SCP
* Recursive snapshot fetch

### Archives

* OVA
* OVF + extracted disks

### Live systems

* SSH access to running Linux guests (**live-fix mode**)

### API-based (vSphere)

* vCenter / ESXi via **pyvmomi**

  * inventory
  * snapshot inspection
  * CBT queries
  * datastore browsing

---

## 4. Pipeline model

All execution modes map to a **single internal pipeline**.

```
FETCH → FLATTEN → INSPECT → FIX → CONVERT → VALIDATE
```

Not every mode runs every stage — but **order is never violated**.

|    Stage | Meaning                     |
| -------: | --------------------------- |
|    FETCH | Obtain disks/artifacts      |
|  FLATTEN | Collapse snapshot chains    |
|  INSPECT | Detect OS, layout, firmware |
|      FIX | Apply deterministic repairs |
|  CONVERT | Produce qcow2/raw/etc       |
| VALIDATE | Boot-test and verify        |

This pipeline is **explicit**, not emergent.

---

## 5. Control-plane vs data-plane

This section is the *mental model* for the whole repo.

### High-level view

```
            ┌──────────────────────────┐
            │       CONTROL PLANE       │
            │  (what exists, what to do)│
            │                            │
            │  pyvmomi / pyVim           │
            │  VM inventory              │
            │  snapshot planning         │
            │  CBT range discovery       │
            │  datastore listing         │
            └─────────────┬────────────┘
                          │
                          │ plans, ranges, metadata
                          │
            ┌─────────────▼────────────┐
            │        DATA PLANE         │
            │   (move bytes reliably)   │
            │                            │
            │  virt-v2v                  │
            │  VDDK reads                │
            │  HTTP /folder downloads    │
            │  SSH/SCP                   │
            │  resume + verify           │
            └──────────────────────────┘
```

Control-plane **never** moves large data.
Data-plane **never** makes inventory decisions.

---

### 5.1 Control-plane responsibilities

Control-plane uses **vSphere APIs** to answer questions:

* What VMs exist?
* Where are their disks?
* What snapshots exist?
* What changed since last time?
* What *should* be fetched?

Responsibilities:

* VM discovery (name / UUID / MoRef)
* Disk enumeration and backing paths
* Snapshot chain analysis
* Firmware detection (BIOS vs UEFI)
* CBT enablement and range queries
* Datastore directory listing

Control-plane code is designed to be:

* deterministic
* testable
* side-effect minimal

---

### 5.2 Data-plane transports

Data-plane answers one question: **how do bytes move safely?**

Supported transports:

#### virt-v2v

Conversion-grade, semantic-aware.

Use when:

* you want qcow2/raw output
* you want guest conversion logic
* you want fewer moving parts

#### HTTP `/folder`

Datastore file downloads via vCenter session auth.

Use when:

* you want *download-only*
* you want VM folder artifacts
* you want ranged reads (CBT)

#### VDDK

High-throughput disk reads.

Use when:

* performance matters
* VDDK is allowed/available

#### SSH/SCP

Minimal dependency path.

Use when:

* no API access
* locked-down networks
* “just ESXi access”

---

### 5.3 Decision matrix

| Goal                  | Recommended      |
| --------------------- | ---------------- |
| Convert VM and boot   | virt-v2v         |
| Download VM artifacts | HTTP `/folder`   |
| Fast disk extraction  | VDDK             |
| No vCenter API        | SSH/SCP          |
| Incremental sync      | CBT + HTTP Range |

---

### 5.4 Incremental migration (CBT)

CBT is **explicit**, not magical.

```
CONTROL PLANE:
  query changed block ranges
  ↓
DATA PLANE:
  HTTP Range GETs
  ↓
LOCAL DISK PATCH
```

Used for:

* warm migrations
* large disks
* predictable cutover windows

If CBT lies, the tool tells you — it does not pretend.

---

### 5.5 Resume, integrity, and checkpoints

Every data-plane operation is built around failure tolerance:

* download plans
* `.part → final` promotion
* resumable transfers
* size verification
* optional SHA256
* rerun safety

Rerunning the same config should produce the same result.

---

## 6. Linux fixes

### fstab stabilization

Rewrites:

* `/dev/sdX`
* `/dev/vdX`
* `/dev/disk/by-path/*`

Into:

* `UUID=`
* `PARTUUID=`
* `LABEL=` / `PARTLABEL=`

### GRUB

* stabilizes `root=`
* cleans `device.map`
* BIOS and UEFI supported

### initramfs

* distro-aware regeneration
* safe skip supported

### Network

* MAC pinning removal
* VMware naming cleanup
* bond / bridge / VLAN preservation
* topology-aware DHCP enablement

---

## 7. Windows handling

Windows is a **first-class citizen**.

### Driver injection

* VirtIO storage (BOOT_START)
* network, balloon, input, fs
* arch-aware

### Registry (offline)

* SYSTEM hive edits
* CriticalDeviceDatabase
* correct StartType + Group

### BCD

* BIOS + UEFI
* backups created
* no blind binary patching

---

## 8. Snapshots and flattening

* descriptor recursion
* parent chain resolution
* flatten before conversion
* atomic outputs

Flattening is recommended because VMware metadata does not translate.

---

## 9. Output formats and validation

Formats:

* qcow2 (recommended)
* raw
* vdi

Validation:

* checksum
* libvirt boot
* qemu direct boot
* BIOS / UEFI
* headless mode supported

---

## 10. YAML configuration model

YAML is treated as **code**.

* mergeable
* reviewable
* rerunnable

```bash
--config base.yaml --config vm.yaml --config overrides.yaml
```

Multi-VM example:

```yaml
vms:
  - vmdk: vm1.vmdk
    to_output: vm1.qcow2
  - vmdk: vm2.vmdk
    to_output: vm2.qcow2
    compress: false
```

---

## 11. Multi-VM and batch processing

* parallel conversion
* shared defaults
* recovery checkpoints
* deterministic paths

Designed for fleets and factories.

---

## 12. Live-fix mode (SSH)

Used **after** migration.

* fstab rewrite
* initramfs regen
* remove VMware tools
* dry-run preview

Not for Windows or snapshot repair.

---

## 13. ESXi and vSphere integration

SSH mode:

* no API dependency
* recursive snapshot fetch

vSphere mode:

* inventory
* CBT
* datastore browsing
* HTTP `/folder` pulls

---

## 14. virt-v2v integration strategy

Two patterns:

* `use_v2v`: v2v converts, vmdk2kvm repairs
* `post_v2v`: vmdk2kvm prepares, v2v converts

virt-v2v solves *some* problems.
`vmdk2kvm` solves the rest deterministically.

---

## 15. Safety mechanisms

* dry-run
* backups
* atomic writes
* checkpoints
* resume support

Default posture: **safe and verbose**.

---

## 16. Daemon mode and automation

* directory watcher
* systemd-friendly
* non-interactive
* CI/CD friendly

---

## 17. Testing and verification

Strongly recommended:

* libvirt smoke boot
* qemu direct boot
* timeout enforcement

Failures stop the pipeline by default.

---

## 18. Failure modes

Common:

* missing VirtIO storage
* unstable fstab
* snapshot corruption
* GUI devices on servers

Tooling:

* `--dump-config`
* `--dump-args`
* detailed reports

---

## 19. When not to use this tool

* you want a wizard
* you expect Windows magic
* you dislike explicit config
* you prefer folklore over determinism

---

## Documentation index

* `README.md` — this file
* `docs/yaml-examples.md` — runnable cookbook
* `--help` — CLI reference
* `--dump-config` — merged YAML
* `--dump-args` — final argparse state
