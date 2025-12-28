# YAML Configuration Examples for vmdk2kvm.py

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
