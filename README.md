# vmdk2kvm

**VMware → KVM/QEMU conversion, repair, and automation toolkit**

`vmdk2kvm` is a production-oriented toolkit for migrating VMware virtual machines  
(VMDK / OVA / OVF / ESXi / vCenter) into **KVM/QEMU-bootable images**  
**without relying on boot-time luck**.

It exists to solve the problems that appear *after* a “successful” conversion:

* broken boots
* unstable device naming
* missing or misordered drivers
* corrupted or misleading snapshot chains
* Windows guests that blue-screen on first KVM boot

This repository is intentionally **not** “click migrate and pray”.  
It is “convert, repair, validate — and make it repeatable”.

---

## Table of contents

1. Scope and non-goals  
2. Design principles  
3. Supported inputs and execution modes  
4. Pipeline model  
5. Control-plane vs data-plane (vSphere, govc, VDDK, HTTP, SSH)  
6. Linux fixes  
7. Windows handling  
8. Snapshots and flattening  
9. Output formats and validation  
10. YAML configuration model  
11. Multi-VM and batch processing  
12. Live-fix mode (SSH)  
13. ESXi and vSphere integration (govc + APIs)  
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
* Applies selected Linux fixes **live over SSH**
* Stabilizes storage and network identifiers across hypervisors
* Injects Windows VirtIO drivers safely (**storage first, always**)
* Flattens VMware snapshot chains deterministically
* Enables repeatable, automatable migrations via mergeable YAML
* Validates results using libvirt / QEMU smoke tests

### What this tool **does not**

* Not a GUI wizard
* Not a cloud importer
* Not a thin wrapper around `virt-v2v`
* Not a promise of zero-touch Windows fixes
* Not a complexity hider

If you want *fast over correct*, this repo will argue with you — politely, and with logs.

---

## 2. Design principles

1. **Boot failures are configuration problems, not copy problems**
2. **Device naming must survive hypervisor changes**
3. **Snapshot chains lie unless flattened or verified**
4. **Windows storage must be BOOT_START before first KVM boot**
5. **Every destructive step needs a safe mode**
6. **Configurations must be replayable**
7. **Control-plane and data-plane must not be mixed**

These rules are enforced structurally, not by convention.

---

## 3. Supported inputs and execution modes

### Offline / local

* Descriptor VMDK
* Monolithic VMDK
* Multi-extent snapshot chains

### Remote

* ESXi over SSH / SCP
* Recursive snapshot fetch

### Archives

* OVA
* OVF + extracted disks

### Live systems

* SSH access to running Linux guests (**live-fix mode**)

### API and CLI based (vSphere)

* vCenter / ESXi via:
  * **govc** (primary CLI control-plane)
  * pyvmomi / pyVim (API fallback and deep inspection)

Used for:

* inventory
* snapshot planning
* CBT discovery
* datastore browsing
* artifact resolution

---

## 4. Pipeline model

All execution modes map to a **single internal pipeline**:

```

FETCH → FLATTEN → INSPECT → FIX → CONVERT → VALIDATE

````

Stages are optional.  
**Order is not.**

| Stage    | Meaning                     |
|----------|-----------------------------|
| FETCH    | Obtain disks and metadata   |
| FLATTEN  | Collapse snapshot chains    |
| INSPECT  | Detect OS, layout, firmware |
| FIX      | Apply deterministic repairs |
| CONVERT  | Produce qcow2/raw/etc       |
| VALIDATE | Boot-test and verify        |

The pipeline is explicit, inspectable, and restart-safe.

---

## 5. Control-plane vs data-plane

This separation is the **spine** of the project.

```mermaid
flowchart TB
  %% GitHub-safe Mermaid

  subgraph CP
    CP_TITLE["CONTROL PLANE<br/>(what exists, what to do)"]
    GOVC["govc<br/>(primary control-plane)"]
    PYVM["pyvmomi / pyVim<br/>(fallback + deep inspection)"]
    INV["inventory & snapshots"]
    CBT["CBT planning<br/>(ranges, change IDs)"]
    DS["datastore inspection<br/>(paths, artifacts)"]

    GOVC --> INV
    GOVC --> CBT
    GOVC --> DS
    PYVM --> INV
    PYVM --> CBT
    PYVM --> DS
  end

  META["plans, ranges,<br/>metadata"]

  subgraph DP
    DP_TITLE["DATA PLANE<br/>(move bytes reliably)"]
    V2V["virt-v2v"]
    VDDK["VDDK reads"]
    HTTP["HTTP /folder<br/>(+ Range GET)"]
    SSH["SSH / SCP"]
    RESUME["resume & verify<br/>.part → final"]
  end

  CP --> META --> DP
  V2V --> RESUME
  VDDK --> RESUME
  HTTP --> RESUME
  SSH --> RESUME
````

* Control-plane **never** moves large data
* Data-plane **never** makes inventory decisions

---

### 5.1 Control-plane responsibilities (govc-first)

`govc` is treated as a **first-class control-plane tool**, not a convenience wrapper.

Used for:

* VM discovery (name, UUID, MoRef)
* Disk and backing path resolution
* Snapshot tree inspection
* CBT enablement and range queries
* Datastore browsing
* Folder-level artifact enumeration

Why govc?

* Stable CLI semantics
* Strong vSphere feature coverage
* Predictable, JSON-friendly output
* Easier to reason about than opaque SDK state

`pyvmomi` remains available when:

* API-only fields are required
* govc coverage is insufficient
* deeper object-graph traversal is needed

---

### 5.2 Data-plane transports

Data-plane answers one question only:

**How do bytes move safely?**

Supported transports:

#### virt-v2v

Guest-aware semantic conversion engine.

#### HTTP `/folder`

Datastore artifact downloads, including ranged reads for CBT.

#### VDDK

High-throughput raw disk access when permitted.

#### SSH / SCP

Fallback transport for locked-down environments.

---

### 5.3 Decision matrix

| Goal                 | Recommended      |
| -------------------- | ---------------- |
| Convert and boot VM  | virt-v2v         |
| Inventory + planning | govc             |
| Download artifacts   | HTTP `/folder`   |
| Fast disk extraction | VDDK             |
| No vCenter access    | SSH / SCP        |
| Incremental sync     | CBT + HTTP Range |

---

### 5.4 Incremental migration (CBT)

CBT usage is explicit and audited.

```
CONTROL PLANE:
  govc → query changed block ranges
        ↓
DATA PLANE:
  HTTP Range GETs
        ↓
LOCAL DISK PATCH
```

If CBT lies, the tool tells you.
It does not pretend.

---

### 5.5 Resume, integrity, and checkpoints

All data-plane operations support failure recovery:

* resumable transfers
* `.part → final` promotion
* size verification
* optional checksums
* rerun safety

Same config in. Same result out.

---

## 6. Linux fixes

* `/etc/fstab` rewrite (`UUID=` / `PARTUUID=` preferred)
* GRUB root stabilization (BIOS + UEFI)
* initramfs regeneration (distro-aware)
* network cleanup (MAC pinning, VMware artifacts)

---

## 7. Windows handling

Windows is a **first-class citizen**, not an afterthought.

* VirtIO storage injected as **BOOT_START**
* Offline registry and hive edits
* `CriticalDeviceDatabase` fixes
* BCD handling with backups
* No blind binary patching

---

## 8. Snapshots and flattening

* Recursive descriptor resolution
* Parent-chain verification
* Flatten **before** conversion
* Atomic outputs

Snapshot flattening is strongly recommended.

---

## 9. Output formats and validation

Formats:

* qcow2 (recommended)
* raw
* vdi

Validation:

* checksums
* libvirt smoke boots
* direct QEMU boots
* BIOS and UEFI
* headless supported

---

## 10. YAML configuration model

YAML is treated as **code**:

* mergeable
* reviewable
* rerunnable

```bash
--config base.yaml --config vm.yaml --config overrides.yaml
```

---

## 11–19

* batch processing
* live-fix mode
* ESXi + vSphere via govc
* virt-v2v integration
* safety mechanisms
* daemon / automation
* testing and failure analysis
* explicit non-goals

---

## 20. Documentation index

All detailed documentation, workflows, examples, and reference material live here:

👉 **[https://github.com/ssahani/vmdk2kvm/tree/main/docs](https://github.com/ssahani/vmdk2kvm/tree/main/docs)**

---

*Convert with intent. Repair with evidence. Boot without luck.*

