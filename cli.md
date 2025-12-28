### Comprehensive List of All Features in the vmdk2kvm.py CLI

The tool is designed for seamless VMware VMDK to KVM/QEMU conversions, with emphasis on safety (e.g., dry-runs, backups), automation (e.g., daemon mode), and fixes (e.g., fstab stabilization, GRUB updates). Features are grouped logically, with descriptions, defaults, and examples where applicable. I've included all arguments from the code's `build_parser()` function, without adding, removing, or modifying any (i.e., "keeping the existing").

#### **Key Design Principles **
- **Config Support**: YAML/JSON configs can override CLI args (via `--config` repeatable). Keys match argparse dest names (dash/underscore interchangeable). Multi-file merging supported for overrides.
- **Safety Nets**: Dry-run mode, backups of critical files (e.g., fstab), error recovery with checkpointing, and Markdown reports.
- **Parallelism and Performance**: Parallel disk processing, progress bars for long ops, and customizable compression.
- **Inputs/Outputs**: Handles local VMDK, remote ESXi fetch, OVA/OVF extraction, live fixes via SSH, and vSphere/vCenter integration.
- **Fixes**: Filesystem/boot fixes (fstab, GRUB, btrfs, crypttab, mdadm), Windows BCD support, VMware tools removal, cloud-init injection.
- **Testing**: Libvirt and QEMU smoke tests.
- **Dependencies**: Requires Python 3, qemu-img, libguestfs; optional: PyYAML, termcolor, pyvmomi, requests, watchdog.
- **Version**: 3.1.0 (as per script).

#### **Global Options**
These apply across all subcommands.

- `--config` (repeatable, default: []): YAML/JSON config file(s). Later files override earlier ones. Example: `--config base.yaml --config overrides.yaml`.
- `--dump-config` (action: store_true): Print merged normalized config as JSON and exit.
- `--dump-args` (action: store_true): Print final parsed args as JSON and exit.
- `--version` (action: version): Show tool version (3.1.0).
- `-v, --verbose` (action: count, default: 0): Verbosity level (-v for INFO, -vv for DEBUG with traces).
- `--log-file` (default: None): Write logs to a file (timestamped, emoji-formatted).
- `--output-dir` (default: "./out"): Root directory for outputs (e.g., flattened images, qcow2 files).
- `--dry-run` (action: store_true): Preview changes without modifications (e.g., print fstab diffs, skip writes/conversions).
- `--no-backup` (action: store_true): Skip backups of critical guest files (e.g., fstab) – dangerous, use cautiously.
- `--print-fstab` (action: store_true): Print /etc/fstab before/after changes (even if already stable).
- `--workdir` (default: None): Directory for intermediate files (defaults to `<output-dir>/work`).
- `--flatten` (action: store_true): Flatten snapshot chains into a single image (qcow2/raw).
- `--flatten-format` (default: "qcow2", choices: ["qcow2", "raw"]): Format for flattened image.
- `--to-output` (default: None): Convert final image to this path (relative to output-dir or absolute).
- `--out-format` (default: "qcow2", choices: ["qcow2", "raw", "vdi"]): Output image format.
- `--compress` (action: store_true): Enable compression for qcow2 outputs.
- `--compress-level` (type: int, choices: 1-9, default: None): Compression level (defaults to 6 if --compress is used).
- `--checksum` (action: store_true): Compute SHA256 checksum of output image.
- `--fstab-mode` (default: "stabilize-all", choices: ["stabilize-all", "bypath-only", "noop"]): fstab rewrite strategy (stabilize to UUID/PARTUUID/LABEL, only fix by-path, or no changes).
- `--no-grub` (action: store_true): Skip GRUB root= updates and device.map cleanup.
- `--regen-initramfs` (action: store_true/false, default: True): Regenerate initramfs and GRUB config (distro-aware, safe fallbacks).
- `--remove-vmware-tools` (action: store_true): Remove VMware tools from Linux guests.
- `--cloud-init-config` (default: None): Path to YAML/JSON cloud-init config to inject.
- `--enable-recovery` (action: store_true): Enable checkpoint recovery for long operations (saves state in `<output-dir>/recovery`).
- `--parallel-processing` (action: store_true): Process multiple disks in parallel (up to 4 workers or CPU count).
- `--resize` (default: None): Resize root filesystem (enlarge only, e.g., "+10G" or "50G").
- `--report` (default: None): Write Markdown report with JSON data (relative to output-dir or absolute).
- `--virtio-drivers-dir` (default: None): Path to virtio-win drivers for Windows injection.
- `--post-v2v` (action: store_true): Run virt-v2v after internal fixes.
- `--libvirt-test` (action: store_true): Run libvirt smoke test on output image.
- `--qemu-test` (action: store_true): Run QEMU smoke test on output image.
- `--vm-name` (default: "converted-vm"): VM name for libvirt test.
- `--memory` (type: int, default: 2048): Memory in MiB for tests.
- `--vcpus` (type: int, default: 2): vCPUs for tests.
- `--uefi" (action: store_true): Use UEFI mode for tests (default: BIOS).
- `--timeout` (type: int, default: 60): Timeout (seconds) for libvirt state checks.
- `--keep-domain` (action: store_true): Keep libvirt domain after test.
- `--headless` (action: store_true): Run libvirt domain headless (no graphics).
- `--daemon` (action: store_true): Run in daemon mode (requires watchdog; watches for new VMDK files).
- `--watch-dir` (default: None): Directory to watch in daemon mode (required if --daemon).
- `--use-v2v` (action: store_true): Use virt-v2v for conversion if available (fallback to internal if not).

#### **Subcommands** (Required; via `cmd` dest)
These define the main mode of operation.

1. **local**: Offline fix for local VMDK.
   - `--vmdk` (required): Path to local VMDK descriptor (or monolithic VMDK).

2. **fetch-and-fix**: Fetch from remote ESXi via SSH/SCP and fix offline.
   - `--host` (required): ESXi host.
   - `--user` (default: "root"): SSH user.
   - `--port` (type: int, default: 22): SSH port.
   - `--identity` (default: None): SSH key file.
   - `--ssh-opt` (repeatable, default: None): Extra SSH/SCP options.
   - `--remote` (required): Remote path to VMDK descriptor (e.g., "/vmfs/volumes/datastore/VM/guest.vmdk").
   - `--fetch-dir` (default: None): Storage for fetched files (defaults to `<output-dir>/downloaded`).
   - `--fetch-all` (action: store_true): Recursively fetch full snapshot chain.

3. **ova**: Extract and fix from OVA package.
   - `--ova` (required): Path to OVA file.

4. **ovf**: Parse OVF and fix (disks in same dir).
   - `--ovf` (required): Path to OVF file.

5. **live-fix**: Live fix on running VM via SSH (no offline conversion).
   - `--host` (required): Host.
   - `--user` (default: "root"): SSH user.
   - `--port` (type: int, default: 22): SSH port.
   - `--identity` (default: None): SSH key file.
   - `--ssh-opt` (repeatable, default: None): Extra SSH options.
   - `--sudo` (action: store_true): Run remote commands via sudo -n.

6. **daemon**: Watch directory for new VMDKs (uses watchdog library).
   - (No additional args beyond global; requires --watch-dir).

7. **generate-systemd**: Generate systemd unit file for daemon mode.
   - `--output` (default: None): Write to file (else stdout).

8. **vsphere**: vSphere/vCenter actions (requires pyvmomi; optional requests for downloads).
   - `--vcenter` (required): vCenter/ESXi host/IP.
   - `--vc-user` (required): vCenter username.
   - `--vc-password` (default: None): vCenter password (or use --vc-password-env).
   - `--vc-password-env` (default: None): Env var for password.
   - `--vc-port` (type: int, default: 443): HTTPS port.
   - `--vc-insecure` (action: store_true): Disable TLS verification.
   - `--dc-name` (default: "ha-datacenter"): Datacenter name for URLs.
   - `--action` (default: "scan", choices: ["scan", "download", "cbt-sync"]): Action (scan VMs, download VMDK, CBT delta sync).
   - `--vm-name` (default: None): VM name (required for download/cbt-sync).
   - `--disk` (default: None): Disk index (0+) or label substring (default: first disk).
   - `--out` (default: None): Output path for download or local disk for cbt-sync.
   - `--chunk-size` (type: int, default: 1024*1024): Download chunk size (bytes).
   - `--enable-cbt` (action: store_true): Enable Changed Block Tracking (CBT) before sync.
   - `--snapshot-name` (default: "vmdk2kvm-cbt"): Snapshot name for CBT sync.

### Comprehensive CLI Example Use Cases for vmdk2kvm.py

Below is a complete set of example CLI use cases for the `vmdk2kvm.py` tool, derived from the script's features, parser arguments, subcommands, and documentation (e.g., from `cli.md` and the script header). These examples cover all major functionalities, including global options, subcommands, fixes, conversions, testing, daemon mode, and vSphere integration. I've grouped them logically for clarity.

Examples assume the tool is run with `sudo` where necessary (e.g., for libguestfs writes or root-required actions). Paths are placeholders—replace with actual values.

#### **1. Basic Local Conversion and Fix (Offline VMDK)**
   - **Description**: Flatten snapshot chain, fix fstab/GRUB, convert to qcow2 with compression, and compute checksum.
   - **Command**:
     ```
     sudo ./vmdk2kvm.py -v --output-dir ./out local \
       --vmdk /path/to/vm.vmdk \
       --flatten \
       --flatten-format qcow2 \
       --to-output vm-fixed.qcow2 \
       --out-format qcow2 \
       --compress \
       --compress-level 6 \
       --checksum \
       --fstab-mode stabilize-all \
       --regen-initramfs \
       --remove-vmware-tools
     ```
   - **Output**: Flattened image in `./out/work`, fixed qcow2 in `./out/vm-fixed.qcow2`, with SHA256 checksum logged.

#### **2. Dry-Run Inspection (No Writes)**
   - **Description**: Preview fstab changes and operations without modifying anything.
   - **Command**:
     ```
     sudo ./vmdk2kvm.py -vv --dry-run --print-fstab local \
       --vmdk /path/to/vm.vmdk \
       --flatten
     ```
   - **Use Case**: Debugging or verifying fixes before committing.

#### **3. Config-Driven Run (YAML/JSON Overrides)**
   - **Description**: Use config files for batch processing multiple VMs; dump config/args for verification.
   - **Command** (Single Config):
     ```
     sudo ./vmdk2kvm.py --config config.yaml local
     ```
   - **Command** (Multi-Config Merge):
     ```
     sudo ./vmdk2kvm.py --config base.yaml --config overrides.yaml local
     ```
   - **Command** (Dump Merged Config):
     ```
     ./vmdk2kvm.py --config config.yaml --dump-config
     ```
   - **Command** (Dump Parsed Args):
     ```
     ./vmdk2kvm.py --config config.yaml --dump-args
     ```
   - **Sample config.yaml** (from script's YAML_EXAMPLE):
     ```
     command: local
     vmdk: /path/to/vm.vmdk
     output_dir: ./out
     flatten: true
     to_output: vm-fixed.qcow2
     out_format: qcow2
     compress: true
     ```
   - **Use Case**: Automation for multiple VMs or repeatable migrations.

#### **4. Fetch from Remote ESXi and Fix**
   - **Description**: Fetch VMDK via SSH, flatten all snapshots, fix, and convert.
   - **Command**:
     ```
     sudo ./vmdk2kvm.py -v fetch-and-fix \
       --host esxi.host.com \
       --user root \
       --port 22 \
       --identity /path/to/key.pem \
       --ssh-opt StrictHostKeyChecking=no \
       --remote /vmfs/volumes/datastore/VM/vm.vmdk \
       --fetch-dir ./downloaded \
       --fetch-all \
       --flatten \
       --to-output vm-fixed.qcow2
     ```
   - **Use Case**: Migrating from remote VMware without manual download.

#### **5. OVA/OVF Extraction and Fix**
   - **Description**: Extract from OVA, fix disks, convert with parallel processing.
   - **Command** (OVA):
     ```
     sudo ./vmdk2kvm.py -v ova \
       --ova /path/to/vm.ova \
       --parallel-processing \
       --flatten \
       --to-output vm-fixed.qcow2
     ```
   - **Command** (OVF):
     ```
     sudo ./vmdk2kvm.py -v ovf \
       --ovf /path/to/vm.ovf \
       --flatten \
       --to-output vm-fixed.qcow2
     ```
   - **Use Case**: Handling exported VMware packages.

#### **6. Live Fix on Running VM (via SSH)**
   - **Description**: Fix fstab/GRUB on a live VMware VM without shutdown.
   - **Command**:
     ```
     sudo ./vmdk2kvm.py live-fix \
       --host vm.host.com \
       --user root \
       --port 22 \
       --identity /path/to/key.pem \
       --sudo \
       --regen-initramfs \
       --remove-vmware-tools
     ```
   - **Use Case**: In-place fixes before live migration.

#### **7. Advanced Fixes and Customizations**
   - **Description**: Force fstab rewrite, resize root, inject cloud-init, remove VMware tools, generate report.
   - **Command**:
     ```
     sudo ./vmdk2kvm.py -v local \
       --vmdk /path/to/vm.vmdk \
       --fstab-mode stabilize-all \
       --no-grub \
       --no-backup \
       --resize +10G \
       --cloud-init-config /path/to/cloud-init.yaml \
       --virtio-drivers-dir /path/to/virtio-win \
       --report report.md \
       --enable-recovery
     ```
   - **Use Case**: Customizing for specific guests (e.g., Windows with virtio, cloud-init for KVM).

#### **8. Testing Converted Images**
   - **Description**: Run libvirt or QEMU smoke tests post-conversion.
   - **Command** (Libvirt Test):
     ```
     sudo ./vmdk2kvm.py local \
       --vmdk /path/to/vm.vmdk \
       --to-output vm-fixed.qcow2 \
       --libvirt-test \
       --vm-name test-vm \
       --memory 4096 \
       --vcpus 4 \
       --uefi \
       --timeout 120 \
       --keep-domain \
       --headless
     ```
   - **Command** (QEMU Test):
     ```
     sudo ./vmdk2kvm.py local \
       --vmdk /path/to/vm.vmdk \
       --to-output vm-fixed.qcow2 \
       --qemu-test \
       --memory 4096 \
       --vcpus 4 \
       --uefi
     ```
   - **Use Case**: Verifying bootability in KVM.

#### **9. Virt-v2v Integration**
   - **Description**: Use virt-v2v for conversion, or post-internal fixes.
   - **Command**:
     ```
     sudo ./vmdk2kvm.py -v local \
       --vmdk /path/to/vm.vmdk \
       --use-v2v \
       --post-v2v
     ```
   - **Use Case**: Leveraging libvirt tools for complex conversions.

#### **10. Daemon Mode for Automation**
   - **Description**: Run as a service watching for new VMDKs.
   - **Command**:
     ```
     sudo ./vmdk2kvm.py --daemon --watch-dir /path/to/vmdk-watch --config config.yaml
     ```
   - **Command** (Generate Systemd Unit):
     ```
     ./vmdk2kvm.py generate-systemd --output /etc/systemd/system/vmdk2kvm.service
     ```
   - **Use Case**: Automated processing in a directory (e.g., for uploads).

#### **11. vSphere/vCenter Integration**
   - **Description**: Scan VMs, download VMDK, or sync changes via CBT.
   - **Command** (Scan VMs):
     ```
     ./vmdk2kvm.py vsphere \
       --vcenter vcenter.host.com \
       --vc-user admin \
       --vc-password password \
       --action scan
     ```
   - **Command** (Download VMDK):
     ```
     ./vmdk2kvm.py vsphere \
       --vcenter vcenter.host.com \
       --vc-user admin \
       --vc-password-env VC_PASS \
       --vc-insecure \
       --vm-name my-vm \
       --disk 0 \
       --out /path/to/vm.vmdk \
       --action download \
       --chunk-size 2097152
     ```
   - **Command** (CBT Sync):
     ```
     ./vmdk2kvm.py vsphere \
       --vcenter vcenter.host.com \
       --vc-user admin \
       --vc-password password \
       --vm-name my-vm \
       --out /path/to/local-vm.vmdk \
       --enable-cbt \
       --snapshot-name cbt-snap \
       --action cbt-sync
     ```
   - **Use Case**: Direct interaction with vSphere for migrations.

#### **12. Logging and Verbosity**
   - **Description**: Verbose logging to file.
   - **Command**:
     ```
     sudo ./vmdk2kvm.py -vv --log-file /var/log/vmdk2kvm.log local --vmdk /path/to/vm.vmdk
     ```
   - **Use Case**: Auditing or debugging long runs.

#### **Enhanced Examples**

Below are enhanced examples showcasing the usage of the `vmdk2kvm.py` CLI tool for various scenarios:

1. **Basic Conversion**:
   ```bash
   python vmdk2kvm.py local --vmdk /path/to/local.vmdk --output-dir ./converted --flatten --out-format qcow2
   ```
   - Converts a local VMDK file to a flattened QCOW2 image.

2. **Fetch and Fix from ESXi**:
   ```bash
   python vmdk2kvm.py fetch-and-fix --host esxi.example.com --user root --remote /vmfs/volumes/datastore/VM/guest.vmdk --output-dir ./converted --remove-vmware-tools
   ```
   - Fetches a VMDK file from an ESXi host and removes VMware tools during the conversion.

3. **OVA Extraction and Fix**:
   ```bash
   python vmdk2kvm.py ova --ova /path/to/guest.ova --output-dir ./converted --cloud-init-config ./cloud-init.yaml
   ```
   - Extracts and fixes a guest image from an OVA file, injecting a cloud-init configuration.

4. **OVF Parsing and Fix**:
   ```bash
   python vmdk2kvm.py ovf --ovf /path/to/guest.ovf --output-dir ./converted --resize +10G
   ```
   - Parses an OVF file, fixes the guest image, and resizes the root filesystem by 10GB.

5. **Daemon Mode**:
   ```bash
   python vmdk2kvm.py --daemon --watch-dir /path/to/vmdk-watch --output-dir ./converted
   ```
   - Watches a directory for new VMDK files and processes them automatically.

6. **Testing Converted Images**:
   ```bash
   python vmdk2kvm.py local --vmdk /path/to/local.vmdk --output-dir ./converted --libvirt-test --qemu-test
   ```
   - Runs Libvirt and QEMU smoke tests on the converted image.

7. **Advanced Compression**:
   ```bash
   python vmdk2kvm.py local --vmdk /path/to/local.vmdk --output-dir ./converted --compress --compress-level 9
   ```
   - Converts a VMDK file to a compressed QCOW2 image with maximum compression.
