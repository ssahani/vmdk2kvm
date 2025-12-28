# vmdk2kvm

**VMware → KVM/QEMU conversion, repair, and automation toolkit**

`vmdk2kvm` is a production-oriented tool for converting VMware virtual machines (VMDK / OVA / OVF / ESXi / vCenter) into KVM/QEMU-bootable images **without relying on boot-time luck**.

It exists to solve the problems that appear *after* a successful conversion:
broken boots, unstable device naming, missing drivers, corrupted snapshot chains,
and Windows guests that blue-screen despite “successful” migration.

---

## Table of contents

1. Scope and non-goals
2. Design assumptions
3. Supported inputs and execution modes
4. Conversion pipeline (internal model)
5. Filesystem and boot fixes (Linux)
6. Windows handling (drivers, registry, BCD)
7. Snapshot handling and flattening
8. Output formats and validation
9. YAML configuration model
10. Multi-VM and batch processing
11. Live-fix mode (SSH)
12. ESXi and vSphere integration
13. virt-v2v integration strategy
14. Safety mechanisms
15. Daemon mode and automation
16. Testing and verification
17. Failure modes and troubleshooting
18. When **not** to use this tool

---

## 1. Scope and non-goals

### What this tool **does**

* Converts VMware disks into KVM-usable formats
* Repairs Linux and Windows guests **offline**
* Makes storage identifiers stable and deterministic
* Injects Windows VirtIO drivers safely
* Flattens snapshot chains correctly
* Produces repeatable, automatable migrations
* Validates results via libvirt / qemu smoke tests

### What this tool **does not**

* It is **not** a GUI
* It is **not** a cloud importer
* It is **not** a thin wrapper around `virt-v2v`
* It does **not** promise zero-touch Windows fixes without drivers
* It does **not** hide complexity

If you want “click migrate and pray”, this tool is not for you.

---

## 2. Design assumptions

These assumptions are baked into the codebase:

1. **Boot failures are caused by configuration, not conversion**
2. `/dev/disk/by-path` is hostile to portability
3. Snapshot chains lie unless flattened
4. Windows storage drivers must be BOOT_START *before* first KVM boot
5. Dry-run and backups must exist for every destructive step
6. YAML configs must be mergeable and replayable
7. Conversion must survive heterogeneous qemu/libguestfs versions

---

## 3. Supported inputs and execution modes

### Offline / local

* Descriptor VMDK
* Monolithic VMDK
* Multi-extent snapshot chains

### Remote

* ESXi over SSH/SCP
* Full snapshot chain recursive fetch

### Archive formats

* OVA (tar + OVF + VMDK)
* OVF with extracted disks

### Live systems

* SSH access to running Linux guests (live-fix)

### API-based

* vCenter / ESXi via pyvmomi

  * VM discovery
  * Disk inspection
  * Snapshot creation
  * CBT queries
  * Datastore downloads

---

## 4. Conversion pipeline (internal model)

Every mode maps to the same internal pipeline:

```
FETCH → FLATTEN → INSPECT → FIX → CONVERT → VALIDATE
```

Not every mode runs all stages.

| Stage    | Description                                           |
| -------- | ----------------------------------------------------- |
| FETCH    | Obtain VMDK(s) from local, ESXi, OVA, OVF, or vSphere |
| FLATTEN  | Collapse snapshot chains into a single coherent disk  |
| INSPECT  | Detect OS, filesystems, boot method, layout           |
| FIX      | Apply OS-specific fixes offline or live               |
| CONVERT  | Produce qcow2/raw/vdi output                          |
| VALIDATE | Boot via libvirt or qemu (optional but recommended)   |

This pipeline is **explicit**, not implicit.

---

## 5. Filesystem and boot fixes (Linux)

### `/etc/fstab` rewriting

Default behavior: `fstab_mode: stabilize-all`

Rewrites:

* `/dev/disk/by-path/*`
* `/dev/sdX`
* `/dev/vdX`

Into stable identifiers, preferring:

1. `UUID=`
2. `PARTUUID=`
3. `LABEL=` / `PARTLABEL=`

This is done **offline**, not during boot.

### BTRFS handling

* Canonicalizes subvolume entries
* Removes `btrfsvol:` pseudo-specs
* Preserves mount options

### GRUB handling

* Rewrites `root=` kernel arguments
* Cleans stale `device.map`
* Supports BIOS and UEFI layouts
* Optional skip via `no_grub: true`

### initramfs

* Best-effort regeneration
* Distribution-aware
* Can be disabled explicitly

---

## 6. Windows handling (drivers, registry, BCD)

Windows is treated as a **first-class migration target**, not an afterthought.

### Driver injection

* VirtIO storage (viostor / vioscsi)
* Network (netkvm)
* Balloon, input, fs, GPU (optional)
* Architecture-aware (amd64 / x86 / arm64)

### Registry edits (offline)

* SYSTEM hive modification via `hivex`
* Services added with correct:

  * StartType (BOOT / SYSTEM / AUTO)
  * Group
  * ImagePath
* CriticalDeviceDatabase entries populated

### BCD handling

* Detects BIOS and UEFI BCD stores
* Creates timestamped backups
* Does **not** attempt unsafe binary patching
* Provides recovery guidance

### Why this matters

Without BOOT_START drivers and CDD entries, Windows will fail with:
`INACCESSIBLE_BOOT_DEVICE`

This tool avoids that *before first KVM boot*.

---

## 7. Snapshot handling and flattening

### VMware snapshot chains

* Descriptor recursion supported
* Parent chains fetched automatically
* Flattening performed via qemu-img convert
* Atomic `.part → final` semantics

### Why flattening is recommended

* Snapshot chains encode VMware-specific assumptions
* KVM does not interpret VMware snapshot metadata
* Flattening removes ambiguity and corruption risk

---

## 8. Output formats and validation

### Output formats

* `qcow2` (recommended)
* `raw`
* `vdi`

### Compression

* qcow2 compression supported
* Adjustable compression levels (1–9)

### Validation

* SHA256 checksum
* libvirt define + boot
* qemu direct boot
* BIOS or UEFI
* Headless mode supported

---

## 9. YAML configuration model

### Why YAML

* Repeatable
* Mergeable
* Reviewable
* Automatable

### Config layering

```bash
--config base.yaml --config vm.yaml --config overrides.yaml
```

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

---

## 10. Multi-VM and batch processing

* Parallel disk conversion
* Shared defaults
* Per-VM overrides
* Recovery checkpoints
* Deterministic output paths

Designed for:

* Fleet migrations
* CI pipelines
* Drop-folder automation

---

## 11. Live-fix mode (SSH)

Used **after** migration, not instead of offline fixes.

Capabilities:

* Rewrite fstab
* Regenerate initramfs
* Remove VMware tools
* Safe sudo execution
* Dry-run preview

Not suitable for:

* Snapshot fixes
* Windows driver injection
* Deep bootloader repair

---

## 12. ESXi and vSphere integration

### SSH-based ESXi fetch

* Descriptor and extent download
* Snapshot recursion
* No API dependency

### vSphere / pyvmomi

* VM listing
* Disk inspection
* Snapshot creation
* CBT enablement
* Changed block queries
* Datastore downloads

CBT workflows are **explicit**, not magical.

---

## 13. virt-v2v integration strategy

Two modes:

* `use_v2v`: virt-v2v does primary conversion
* `post_v2v`: virt-v2v runs *after* offline fixes

Rationale:

* virt-v2v solves some problems
* it does not solve all problems
* this tool fills the gaps deliberately

---

## 14. Safety mechanisms

* `dry_run`
* Internal backups
* Atomic file replacement
* Recovery checkpoints
* Resume-safe operations
* Explicit opt-out for destructive steps

Default posture: **safe, verbose, conservative**

---

## 15. Daemon mode and automation

* Watch directory for new VMDKs
* Automatic conversion
* systemd integration
* Logging and reports
* No interactive state

Designed for:

* Ingestion pipelines
* CI/CD
* Migration factories

---

## 16. Testing and verification

Testing is optional but strongly recommended.

* libvirt smoke test
* qemu direct boot
* BIOS / UEFI
* Headless server compatibility
* Timeout enforcement

Testing failures stop the pipeline by default.

---

## 17. Failure modes and troubleshooting

### Common issues

* Missing VirtIO drivers → Windows boot failure
* by-path fstab → random disk renumbering
* Unflattened snapshots → silent corruption
* GUI devices on servers → libvirt failures

### Tooling provided

* `--print-fstab`
* `--dump-config`
* `--dump-args`
* Detailed logs
* Markdown reports

---

## 18. When **not** to use this tool

* You need a GUI wizard
* You expect Windows to fix itself magically
* You cannot tolerate explicit configuration
* You want “fast” over “correct”

---

## Documentation index

* `README.md` — this file
* `docs/yaml-examples.md` — full runnable configs
* `--help` — CLI reference
* `--dump-config` — merged config view
* `--dump-args` — final argparse state

---

### Final note

This tool exists because **bootability is not optional**.

If a VM does not boot reliably after migration, the migration failed —
even if the conversion “succeeded”.

`vmdk2kvm` is built to prevent that class of failure.
