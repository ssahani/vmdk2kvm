# YAML Configuration Examples for vmdk2kvm.py

This page is a **copy‑paste cookbook** for running `vmdk2kvm.py` using YAML configs.
It covers the main modes you already use (**local**, **live-fix**, **fetch-and-fix**) and adds the “missing” ones
that end up in real migrations (**ova**, **ovf**, **daemon**, **vsphere/pyvmomi**), plus practical patterns for:

- **single VM** and **multi‑VM batch** configs
- **config layering** (base + overrides)
- **Windows virtio injection** and **BCD safety backups**
- **recovery checkpoints** and **parallel processing**
- **smoke tests** (libvirt / qemu, BIOS/UEFI, headless)
- **virt‑v2v hybrid flows** (`use_v2v` / `post_v2v`)

> Tip: keep one “base.yaml” with your defaults, and override per‑customer / per‑VM in a tiny overlay file.

## Table of contents

- [Running configs](#running-configs)
- [Common keys](#common-keys)
- [1. Local mode](#1-local-mode-offline-conversion-from-local-vmdk)
- [2. Live-fix mode](#2-live-fix-mode-live-fixes-on-running-vm-via-ssh)
- [3. Fetch-and-fix mode](#3-fetch-and-fix-mode-fetch-from-esxi-and-fix-offline)
- [4. OVA mode](#4-ova-mode-extract-from-ova-and-convert)
- [5. OVF mode](#5-ovf-mode-parse-ovf-and-convert)
- [6. Daemon mode](#6-daemon-mode-watch-a-directory-and-auto-convert)
- [7. vSphere (pyvmomi) mode](#7-vsphere-pyvmomi-mode-discovery-download-cbt)
- [Base + overrides pattern](#base--overrides-pattern)
- [Troubleshooting patterns](#troubleshooting-patterns)

## Running configs

Configs can be run with:

```bash
sudo ./vmdk2kvm.py --config example.yaml local
```

Or merge multiple configs (later overrides earlier):

```bash
sudo ./vmdk2kvm.py --config base.yaml --config overrides.yaml local
```

### Multi-VM configs
If you use a `vms:` list, the tool treats **top-level keys as defaults** and allows per‑VM overrides.

```yaml
vms:
  - vmdk: /path/to/vm1.vmdk
    to_output: vm1.qcow2
  - vmdk: /path/to/vm2.vmdk
    to_output: vm2.qcow2
    compress: false   # override only for vm2
compress: true        # default for all
out_format: qcow2
```

## Common keys

These keys are frequently used across multiple modes:

### Logging / diagnostics
- `verbose`: `0|1|2` (or `-v/-vv`)
- `log_file`: write logs to a file
- `report`: write a Markdown report (recommended for long conversions)
- `checksum`: compute SHA256 of final output

### Safety
- `dry_run`: preview changes (does not modify the guest image / output)
- `no_backup`: skip backups inside guest (**dangerous**)
- `enable_recovery`: checkpoints for long ops (recommended)
- `parallel_processing`: parallelize batch conversion

### Conversion
- `flatten`: flatten snapshot chain first (recommended if snapshots exist)
- `flatten_format`: `qcow2` or `raw`
- `to_output`: final output file name/path
- `out_format`: `qcow2|raw|vdi`
- `compress`: qcow2 compression
- `compress_level`: `1..9`
- `resize`: `+10G` (grow by) or `50G` (set total)

### Fixes
- `fstab_mode`: `stabilize-all` (recommended) | `bypath-only` | `noop`
- `print_fstab`: print fstab before/after
- `no_grub`: skip grub root= changes and device.map cleanup
- `regen_initramfs`: regenerate initramfs + grub config (best-effort)
- `remove_vmware_tools`: remove VMware tools (Linux guests)
- `cloud_init_config`: inject cloud-init config (Linux guests)

### Tests (optional)
- `libvirt_test`: define + boot the VM and verify it reaches RUNNING
- `qemu_test`: basic qemu launch smoke
- `uefi`: use UEFI (OVMF) for test VM
- `headless`: no graphics device (important for headless servers)

Below are detailed YAML configuration examples for the main modes in `vmdk2kvm.py`: **local** (offline local VMDK conversion), **live-fix** (live fixes via SSH), and **fetch-and-fix** (fetch from ESXi and fix offline). These examples demonstrate how to use configs for single or multi-VM setups, with common options like flattening, output conversion, fixes, and safety features.

Configs can be run with:
```
sudo ./vmdk2kvm.py --config example.yaml
```
Or merge multiple:
```
sudo ./vmdk2kvm.py --config base.yaml --config overrides.yaml
```

## 1. Local Mode (Offline Conversion from Local VMDK)
This mode processes local VMDK files offline. Use for fixing and converting standalone VMs.

### Basic Single VM Example
```yaml
# Basic local mode config: Fix and convert a single VMDK to qcow2
command: local
vmdk: /path/to/vm.vmdk  # Path to VMDK (descriptor or monolithic)
output_dir: ./out       # Output directory
flatten: true           # Flatten snapshots if present
to_output: vm-fixed.qcow2  # Final output file
out_format: qcow2       # Output format
compress: true          # Enable compression
regen_initramfs: true   # Regenerate initramfs and GRUB
remove_vmware_tools: true  # Remove VMware tools
verbose: 1              # Verbosity level
```

### Advanced Single VM with Windows and Cloud-Init
```yaml
# Advanced local: Windows virtio injection, resize, cloud-init, report
command: local
vmdk: /path/to/windows-vm.vmdk
virtio_drivers_dir: /path/to/virtio-win  # For Windows virtio drivers
resize: +20G            # Enlarge disk by 20G
cloud_init_config: /path/to/cloud-config.yaml  # Inject cloud-init
fstab_mode: stabilize-all  # Rewrite fstab to stable IDs
no_grub: false          # Update GRUB (default: true)
report: windows-report.md  # Generate Markdown report
checksum: true          # Compute SHA256 of output
dry_run: false         # Actually apply changes
```

### Multi-VM Batch in One Config
```yaml
# Multi-VM local: Process multiple VMs with shared settings
vms:
  - vmdk: /path/to/vm1.vmdk
    to_output: vm1-fixed.qcow2
    resize: +10G
  - vmdk: /path/to/vm2.vmdk
    to_output: vm2-fixed.qcow2
    remove_vmware_tools: false  # Override for this VM
flatten: true           # Shared: Flatten for all
out_format: qcow2       # Shared: qcow2 output
compress_level: 6       # Shared: Compression level
parallel_processing: true  # Process VMs in parallel
enable_recovery: true   # Checkpointing for recovery
```

## 2. Live-Fix Mode (Live Fixes on Running VM via SSH)
This mode applies fixes live over SSH without shutting down the VM. Ideal for post-migration tweaks.

### Basic Live-Fix Example
```yaml
# Basic live-fix: Rewrite fstab and regenerate initramfs via SSH
command: live-fix
host: 192.168.1.100      # VM host IP or hostname
user: root              # SSH user (default: root)
port: 22                # SSH port (default: 22)
sudo: true              # Use sudo for commands
print_fstab: true       # Print fstab before/after
regen_initramfs: true   # Regenerate initramfs/GRUB
remove_vmware_tools: true  # Remove VMware tools live
no_backup: false        # Backup files (default)
verbose: 2              # High verbosity
```

### Advanced Live-Fix with Custom SSH Options
```yaml
# Advanced live-fix: Custom identity, options, no GRUB update
command: live-fix
host: vm.example.com
user: admin
identity: ~/.ssh/custom_key  # SSH private key
ssh_opt:                # Extra SSH options (list)
  - "-o StrictHostKeyChecking=no"
  - "-o ConnectTimeout=30"
no_grub: true           # Skip GRUB updates
fstab_mode: bypath-only  # Only fix by-path in fstab
dry_run: true          # Preview changes only
log_file: live-fix.log  # Log to file
```

### Multi-VM Live-Fix (Sequential Processing)
```yaml
# Multi-VM live-fix: Fix multiple running VMs
vms:
  - host: vm1.example.com
    user: root
    sudo: true
    regen_initramfs: true
  - host: vm2.example.com
    user: admin
    identity: ~/.ssh/key
    remove_vmware_tools: false  # Override per VM
print_fstab: true       # Shared: Print fstab for all
```

## 3. Fetch-and-Fix Mode (Fetch from ESXi and Fix Offline)
This mode fetches VMDKs from ESXi via SSH, then applies offline fixes.

### Basic ESXi Fetch Example
```yaml
# Basic fetch-and-fix: Fetch descriptor and fix
command: fetch-and-fix
host: esxi.example.com  # ESXi host
user: root              # SSH user
remote: /vmfs/volumes/datastore1/vm/vm.vmdk  # Remote VMDK path
fetch_dir: ./downloads  # Where to store fetched files
to_output: esxi-vm-fixed.qcow2  # Output after fix
out_format: qcow2
compress: true
```

### Advanced ESXi with Full Snapshot Chain
```yaml
# Advanced: Fetch full chain, flatten, resize, test
command: fetch-and-fix
host: esxi-host
port: 22
identity: ~/.ssh/esxi_key
remote: /path/to/snapshot-vm.vmdk
fetch_all: true         # Fetch entire snapshot chain
flatten: true           # Flatten after fetch
resize: 50G             # Set disk to 50G total
regen_initramfs: true
libvirt_test: true      # Libvirt smoke test
vm_name: esxi-test-vm   # Test VM name
uefi: true              # Use UEFI for test
timeout: 90             # Test timeout seconds
report: esxi-report.md  # Generate report
```

### Multi-VM ESXi Fetch (Batch Fetch and Fix)
```yaml
# Multi-VM fetch-and-fix: Fetch and process multiple from ESXi
vms:
  - host: esxi1.example.com
    remote: /vmfs/volumes/ds1/vm1/vm1.vmdk
    fetch_all: true
  - host: esxi2.example.com
    remote: /vmfs/volumes/ds2/vm2/vm2.vmdk
    identity: ~/.ssh/key2
flatten: true           # Shared: Flatten all
out_format: qcow2       # Shared output format
parallel_processing: true  # Parallel fetch/fix
enable_recovery: true   # Recovery checkpoints
```

These examples can be customized further with options like `--use-v2v` for virt-v2v integration or `--post-v2v` for hybrid workflows. For full details, check the tool's help or epilog.


---

## 4. OVA Mode (Extract from OVA and Convert)

Use when you have an appliance OVA (tarball with OVF + VMDK(s)).

### Basic OVA extract + convert
```yaml
command: ova
ova: /path/to/appliance.ova
output_dir: ./out
flatten: true
to_output: appliance.qcow2
out_format: qcow2
compress: true
compress_level: 6
report: ova-report.md
```

### OVA + smoke test (UEFI, headless)
```yaml
command: ova
ova: /path/to/appliance.ova
output_dir: ./out
flatten: true
to_output: appliance-test.qcow2
out_format: qcow2
compress: true

libvirt_test: true
vm_name: ova-appliance-test
uefi: true
headless: true
timeout: 120
keep_domain: false
report: ova-smoke.md
```

## 5. OVF Mode (Parse OVF and Convert)

Use when you have an OVF descriptor file and disks in the same directory.

### Basic OVF parse + convert
```yaml
command: ovf
ovf: /path/to/vm.ovf
output_dir: ./out
flatten: true
to_output: ovf-vm.qcow2
out_format: qcow2
compress: true
```

### OVF + no GRUB mutation (when guest is fragile)
```yaml
command: ovf
ovf: /path/to/vm.ovf
output_dir: ./out
fstab_mode: stabilize-all
no_grub: true
regen_initramfs: false
flatten: true
to_output: ovf-safe.qcow2
out_format: qcow2
report: ovf-safe.md
```

## 6. Daemon Mode (Watch a Directory and Auto-Convert)

Use for “drop VMDKs here → get qcow2 there” pipelines (systemd service friendly).

### Basic daemon watch
```yaml
command: daemon
daemon: true
watch_dir: /srv/incoming-vmdk
output_dir: /srv/out
workdir: /srv/out/work
flatten: true
out_format: qcow2
compress: true
compress_level: 6
enable_recovery: true
log_file: /var/log/vmdk2kvm-daemon.log
report: daemon-report.md
```

### Daemon + strict safety (dry-run first)
```yaml
command: daemon
daemon: true
watch_dir: /srv/incoming-vmdk
output_dir: /srv/out
dry_run: true
print_fstab: true
fstab_mode: stabilize-all
flatten: false
log_file: /var/log/vmdk2kvm-daemon-preview.log
```

## 7. vSphere (pyvmomi) Mode (Discovery, Download, CBT)

Use when you want to **talk to vCenter/ESXi APIs** (pyvmomi): list VMs, inspect disks, download datastore files,
create snapshots, enable CBT, query changed areas, and do delta sync workflows.

### List all VM names
```yaml
command: vsphere
vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true
vs_action: list_vm_names
json: true
```

### Get VM details (by name)
```yaml
command: vsphere
vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true
vs_action: get_vm_by_name
name: myVM
json: true
```

### List disks for a VM
```yaml
command: vsphere
vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true
vs_action: vm_disks
vm_name: myVM
json: true
```

### Download a datastore file (descriptor, extent, vmx, nvram, logs)
```yaml
command: vsphere
vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true
dc_name: ha-datacenter

vs_action: download_datastore_file
datastore: datastore1
ds_path: "[datastore1] myVM/myVM.vmdk"
local_path: ./downloads/myVM.vmdk
chunk_size: 1048576
json: true
```

### Download a VM disk (select by index/label)
```yaml
command: vsphere
vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true

vs_action: download_vm_disk
vm_name: myVM
disk: 0
local_path: ./downloads/myVM-disk0.vmdk
chunk_size: 1048576
json: true
```

### Create a quiesced snapshot (safer reads)
```yaml
command: vsphere
vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true

vs_action: create_snapshot
vm_name: myVM
name: vmdk2kvm-pre-migration
quiesce: true
memory: false
description: "Created by vmdk2kvm"
json: true
```

### Enable CBT (Changed Block Tracking)
```yaml
command: vsphere
vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true

vs_action: enable_cbt
vm_name: myVM
json: true
```

### Query changed disk areas (CBT)
```yaml
command: vsphere
vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true

vs_action: query_changed_disk_areas
vm_name: myVM
snapshot_name: vmdk2kvm-cbt
disk: 0
start_offset: 0
change_id: "*"
json: true
```

### CBT delta sync scaffold (base download once, patch deltas)
```yaml
command: vsphere
vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true

vs_action: cbt_sync
vm_name: myVM
disk: 0
local_path: ./downloads/myVM-disk0.vmdk
enable_cbt: true
snapshot_name: vmdk2kvm-cbt
json: true
```

## Base + overrides pattern

This is the pattern that keeps you sane.

### base.yaml (team defaults)
```yaml
# base.yaml
output_dir: ./out
workdir: ./out/work
enable_recovery: true
fstab_mode: stabilize-all
print_fstab: true
regen_initramfs: true
flatten: true
out_format: qcow2
compress: true
compress_level: 6
checksum: true
report: report.md
verbose: 1
```

### overrides.yaml (per VM or per customer)
```yaml
# overrides.yaml
vmdk: /path/to/customer/vm.vmdk
to_output: customer-vm.qcow2
remove_vmware_tools: false     # keep tools (rare)
no_grub: true                  # skip grub touches (fragile guest)
dry_run: false
```

Run:
```bash
sudo ./vmdk2kvm.py --config base.yaml --config overrides.yaml local
```

### Batch overrides (multi-VM)
```yaml
# batch.yaml
output_dir: ./out
flatten: true
out_format: qcow2
compress: true
compress_level: 6
parallel_processing: true
enable_recovery: true

vms:
  - vmdk: /path/to/vm1.vmdk
    to_output: vm1.qcow2
  - vmdk: /path/to/vm2.vmdk
    to_output: vm2.qcow2
    compress: false      # override
  - vmdk: /path/to/win.vmdk
    to_output: win.qcow2
    virtio_drivers_dir: /path/to/virtio-win
```

## Troubleshooting patterns

### “Headless server” libvirt test fails with SDL / XDG errors
Use `headless: true` and remove any GUI video/graphics devices in your test XML generation.

```yaml
libvirt_test: true
headless: true
```

### Windows boots to recovery / INACCESSIBLE_BOOT_DEVICE after migration
Typical causes: storage driver isn’t boot-start or CDD missing. Make sure you inject virtio drivers **and** registry service + CriticalDeviceDatabase entries.

```yaml
virtio_drivers_dir: /path/to/virtio-win
# optional knobs if you wired them:
# enable_virtio_gpu: true
# enable_virtio_input: true
# enable_virtio_fs: true
```

Also: your tool’s “BCD backup scan” is safe to run offline. It won’t magically fix BCD, but it gives you backups and visibility.

### Snapshot chain conversions are slow / fail
Try:
- `flatten: true` first
- set `workdir` on fast local SSD
- enable `enable_recovery: true` so failures resume instead of restarting
- use `compress_level` carefully (high levels cost CPU)

### “Required arg missing” even though it’s in YAML
That usually means the YAML key doesn’t match the argparse destination name.
Prefer the exact CLI names: `to_output`, `out_format`, `virtio_drivers_dir`, `fetch_all`, `ssh_opt`, etc.
