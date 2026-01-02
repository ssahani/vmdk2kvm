### Comprehensive CLI Reference 

`vmdk2kvm.py` is a production-minded tool for converting VMware workloads into KVM/QEMU-friendly disk images (qcow2/raw/vdi) while applying the fixes that actually make migrations succeed: stable `/etc/fstab`, bootloader/root= alignment, initramfs regeneration, VMware tools cleanup, Windows virtio enablement, plus “prove it boots” smoke tests.

This document is the **interface contract** for the CLI as implemented by your **current `build_parser()` + YAML-driven validator**. It **does not add new flags**. It’s written fresh for the new model: **config selects the operation**, CLI optionally overrides.

---

## Design Principles

### Config-first, automation-friendly

* **Operation selection lives in config**: `cmd` determines what the run does.
* **Two-phase parse**: configs are loaded/merged first, applied as argparse defaults, then the final parse happens.
* **Repeatable `--config` merge**: later files override earlier files cleanly (base + overrides).

### Safety is a feature

* Dry-run support.
* Backups (unless explicitly disabled).
* Recovery checkpoints for long operations.
* Reports for auditability.

### Works in the mess

Designed for real VMware → KVM pain: snapshot chains, unstable by-path naming, boot plumbing, mixed distros, Windows storage enablement, and verification.

---

## How to Run (New Model)

### Standard run (config chooses operation)

```bash
sudo python vmdk2kvm.py --config job.yaml
```

### Merge configs (base + overrides)

```bash
sudo python vmdk2kvm.py --config base.yaml --config override.yaml
```

### Override a knob from CLI (config still drives the run)

```bash
sudo python vmdk2kvm.py --config job.yaml --output-dir /var/tmp/out -vv
```

### Inspect merged config (does not require cmd)

```bash
sudo python vmdk2kvm.py --config job.yaml --dump-config
```

### Inspect final parsed args (requires cmd because validation runs)

```bash
sudo python vmdk2kvm.py --config job.yaml --dump-args
```

---

## Required Config Keys

### `cmd`

Every run must specify a command via config (or via CLI `--cmd` override):

* **Config key:** `cmd`
* **CLI override:** `--cmd`

Supported `cmd` values enforced by `validate_args()`:

* `local`
* `fetch-and-fix`
* `ova`
* `ovf`
* `vhd`
* `ami`
* `live-fix`
* `vsphere`
* `daemon`
* `generate-systemd`

### `vs_action` (only when `cmd: vsphere`)

vSphere mode requires:

* **Config key:** `vs_action`
* **CLI override:** `--vs-action`

---

## Quick Start Examples (Config-Driven)

### 1) Local VMDK → qcow2 with fixes + compression (recommended)

**job.yaml**

```yaml
cmd: local
vmdk: /path/to/vm.vmdk

output_dir: ./out
flatten: true
to_output: vm-fixed.qcow2
out_format: qcow2
compress: true
compress_level: 6
checksum: true

fstab_mode: stabilize-all
regen_initramfs: true
remove_vmware_tools: true
print_fstab: true

verbose: 1
report: ./out/report.md
```

Run:

```bash
sudo python vmdk2kvm.py --config job.yaml
```

### 2) Dry-run preview (no writes)

```bash
sudo python vmdk2kvm.py --config job.yaml --dry-run -vv
```

### 3) Base + override

```bash
sudo python vmdk2kvm.py --config base.yaml --config rhel10.yaml
```

---

## Global Options (CLI flags)

These flags exist on the CLI and can also be supplied via config using their argparse dest names (same spelling with underscores).

### Configuration & introspection

* `--config` *(repeatable, default `[]`)*
  One or more YAML/JSON config files. Later overrides earlier.

* `--dump-config` *(store_true)*
  Print merged normalized config and exit.

* `--dump-args` *(store_true)*
  Print parsed args and exit.

* `--version`
  Print version and exit.

### Logging & verbosity

* `-v, --verbose` *(count, default 0)*
  Increase verbosity (`-v`, `-vv`).

* `--log-file` *(default None)*
  Write logs to a file.

### Operation selection (new project control)

* `--cmd` *(default None)*
  Operation mode. Normally comes from config `cmd`.

* `--vs-action` *(default None)*
  vSphere action. Normally comes from config `vs_action`.

### Paths & behavior

* `--output-dir` *(default `./out`)*
  Root output directory.

* `--workdir` *(default None)*
  Working directory (implementation uses `<output-dir>/work` if unset).

* `--dry-run` *(store_true)*
  Do not modify guest/convert output.

* `--no-backup` *(store_true)*
  Skip backups inside guest.

* `--print-fstab` *(store_true)*
  Print `/etc/fstab` before+after.

---

## Flatten & Conversion Outputs

* `--flatten` *(store_true)*
  Flatten snapshot chain into a single working image first.

* `--flatten-format` *(default `qcow2`, choices: `qcow2|raw`)*
  Flatten output format.

* `--to-output` *(default None)*
  Convert final working image to this path (relative to output-dir if not absolute).

* `--out-format` *(default `qcow2`, choices: `qcow2|raw|vdi`)*
  Final output format.

* `--compress` *(store_true)*
  Enable qcow2 compression.

* `--compress-level` *(int 1–9, default None)*
  Compression level.

* `--checksum` *(store_true)*
  Compute SHA256 checksum of output.

---

## Fixing Behavior

* `--fstab-mode` *(choices from `FstabMode`, default `stabilize-all`)*
  How `/etc/fstab` is rewritten.

* `--no-grub` *(store_true)*
  Skip GRUB root= update and device.map cleanup.

* `--regen-initramfs` / `--no-regen-initramfs` *(default True)*
  Enable/disable initramfs + grub regen (best effort).

* `--remove-vmware-tools` *(store_true)*
  Remove VMware tools (Linux only).

* `--cloud-init-config` *(default None)*
  Cloud-init config (YAML/JSON) to inject.

* `--virtio-drivers-dir` *(default None)*
  Path to virtio-win drivers directory for Windows injection.

* `--resize` *(default None)*
  Resize root filesystem (enlarge only, e.g. `+10G` or `50G`).

* `--report` *(default None)*
  Write Markdown report.

---

## Recovery, Performance, virt-v2v Knobs

* `--enable-recovery` *(store_true)*
  Enable checkpoint recovery.

* `--parallel-processing` *(store_true)*
  Process multiple disks in parallel.

* `--use-v2v` *(store_true)*
  Use virt-v2v for conversion if available.

* `--post-v2v` *(store_true)*
  Run virt-v2v after internal fixes.

* `--v2v-parallel` *(store_true)*
  Run multiple virt-v2v jobs in parallel (experimental).

* `--v2v-concurrency` *(int, default 2)*
  Max concurrent virt-v2v jobs when `--v2v-parallel` is set.

---

## LUKS Knobs

* `--luks-passphrase` *(default: env `VMDK2KVM_LUKS_PASSPHRASE`)*
* `--luks-passphrase-env` *(default None)*
* `--luks-keyfile` *(default None)*
* `--luks-mapper-prefix` *(default `vmdk2kvm-crypt`)*
* `--luks-enable` *(store_true)*

---

## Smoke Tests

* `--libvirt-test` *(store_true)*
* `--qemu-test` *(store_true)*
* `--vm-name` *(default `converted-vm`)*
* `--memory` *(int, default 2048 MiB)*
* `--vcpus` *(int, default 2)*
* `--uefi` *(store_true)*
* `--timeout` *(int, default 60)*
* `--keep-domain` *(store_true)*
* `--headless` *(store_true)*

---

## Daemon Mode

* `--daemon` *(store_true)*
* `--watch-dir` *(default None)*

(When running daemon-style, `cmd: daemon` should be set in config.)

---

## OVA/OVF Helper Knobs

* `--log-virt-filesystems` *(store_true)*
* `--ova-convert-to-qcow2` *(store_true)*
* `--ova-qcow2-dir` *(default None)*
* `--ova-convert-compress` *(store_true)*
* `--ova-convert-compress-level` *(int 1–9, default None)*

---

## AMI / Cloud Tarball Helper Knobs

* `--extract-nested-tar` / `--no-extract-nested-tar` *(default True)*
* `--convert-payload-to-qcow2` *(store_true)*
* `--payload-qcow2-dir` *(default None)*
* `--payload-convert-compress` *(store_true)*
* `--payload-convert-compress-level` *(int 1–9, default None)*

---

## Inputs (Selected by `cmd`)

These are available globally and expected depending on `cmd`:

### `cmd: local`

* `--vmdk` *(required via config or CLI)*

Example:

```bash
sudo python vmdk2kvm.py --config job.yaml --vmdk /path/to/vm.vmdk
```

### `cmd: ova`

* `--ova` *(required)*

### `cmd: ovf`

* `--ovf` *(required)*

### `cmd: vhd`

* `--vhd` *(required)*

### `cmd: ami`

* `--ami` *(required)*

### `cmd: fetch-and-fix`

Requires SSH/SCP source:

* `--host` *(required)*
* `--remote` *(required)*

Additional knobs:

* `--user` *(default root)*
* `--port` *(default 22)*
* `--identity`
* `--ssh-opt` *(repeatable)*
* `--fetch-dir`
* `--fetch-all`

### `cmd: live-fix`

* `--host` *(required)*

Additional knobs:

* `--user` *(default root)*
* `--port` *(default 22)*
* `--identity`
* `--ssh-opt` *(repeatable)*
* `--sudo` *(store_true)*

---

## `cmd: generate-systemd`

* `--systemd-output` *(default None)*
  Write unit file to path instead of stdout.

---

## vSphere / vCenter (`cmd: vsphere`)

### Connection flags (required)

* `--vcenter` *(required)*
* `--vc-user` *(required)*

Credentials:

* `--vc-password` *(optional)*
* `--vc-password-env` *(optional)*

TLS / URL:

* `--vc-port` *(default 443)*
* `--vc-insecure` *(store_true)*
* `--dc-name` *(default `ha-datacenter`)*

### Action selection

In the new model, `vs_action` comes from config (or `--vs-action` override).

Supported `vs_action` values in your parser:

* `list_vm_names`
* `get_vm_by_name`
* `vm_disks`
* `select_disk`
* `download_datastore_file`
* `download_vm_disk`
* `create_snapshot`
* `enable_cbt`
* `query_changed_disk_areas`
* `cbt_sync`
* `download_only_vm`
* `vddk_download_disk`

### Action-scoped flags (available globally)

* `--json` *(store_true)*
* `--vm_name` → **mapped to dest `vm_name_vsphere`** in parser
* `--name` → dest `name_vsphere`
* `--label_or_index`
* `--datastore`
* `--ds_path`
* `--local_path`
* `--chunk_size`

Snapshot/CBT:

* `--snapshot_name`
* `--quiesce` / `--no_quiesce`
* `--snapshot_memory`
* `--description` → dest `snapshot_description`
* `--enable_cbt`
* `--device_key`
* `--disk`
* `--start_offset`
* `--change_id`

Download-only:

* `--vs_output_dir`

### Required fields by action (enforced by validator)

* Actions requiring a VM name:
  `vm_disks`, `select_disk`, `download_vm_disk`, `cbt_sync`, `create_snapshot`,
  `enable_cbt`, `query_changed_disk_areas`, `download_only_vm`, `vddk_download_disk`

  * require `vm_name` (config or CLI `--vm_name`)

* `get_vm_by_name`

  * requires `name`

* `select_disk`

  * requires `label_or_index`

* `download_datastore_file`

  * requires `datastore`, `ds_path`, `local_path`

* `download_vm_disk` / `vddk_download_disk` / `cbt_sync`

  * requires `local_path`

* `query_changed_disk_areas`

  * requires `device_key` **or** `disk`

### Example: list VM names (config-driven)

**vs-list.yaml**

```yaml
cmd: vsphere
vs_action: list_vm_names

vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true

json: true
verbose: 1
```

Run:

```bash
sudo python vmdk2kvm.py --config vs-list.yaml
```

### Example: download a VM disk

**vs-dl.yaml**

```yaml
cmd: vsphere
vs_action: download_vm_disk

vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true

vm_name: myVM
disk: "0"
local_path: ./downloads/myVM-disk0.vmdk
chunk_size: 1048576
json: true
```

Run:

```bash
sudo python vmdk2kvm.py --config vs-dl.yaml
```

---

## Dependency Notes (practical)

* Needs Python 3
* Conversion pipeline typically expects: `qemu-img`, `libguestfs`
* YAML configs require PyYAML
* vSphere mode requires pyvmomi (and any HTTP libs your implementation uses)

---
