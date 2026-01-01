 Offline VMware → KVM Conversion (vmdk2kvm)

This document describes an **offline, deterministic conversion** of a VMware
VMDK into a KVM-ready qcow2 image using **vmdk2kvm**.

The workflow is designed to be:
- reproducible
- boot-safe
- distro-agnostic
- hostile to `/dev/disk/by-path` (on purpose)

It is suitable for **RHEL / openSUSE / SLES / Fedora / Debian-family** guests
that must boot reliably after migration.

---

## How to run

```bash
sudo ./vmdk2kvm.py --config vmdk2kvm.yaml local
````

This runs **entirely offline** using libguestfs.
No VM is booted during conversion.

---

## What this configuration guarantees

* ✅ Snapshot chains are flattened first
* ✅ All filesystem references are rewritten to **stable identifiers**
* ✅ `root=` is fixed everywhere it matters
* ✅ Initramfs is regenerated safely (dracut-aware)
* ✅ Final image is compressed qcow2
* ✅ Before/after state is visible (fstab + report)
* ✅ No reliance on host-specific device paths

If this image boots in raw `qemu-system-x86_64`, it will boot under libvirt.

---

## Configuration: `vmdk2kvm.yaml`

```yaml
# Offline VMware → KVM conversion
#
# Run with:
#   sudo ./vmdk2kvm.py --config vmdk2kvm.yaml local
#
# Core guarantees:
# - No /dev/disk/by-path usage
# - Stable UUID/PARTUUID everywhere
# - Safe GRUB + initramfs regeneration
# - Deterministic output qcow2

command: local

# --------------------------------------------------------------------
# Input
# --------------------------------------------------------------------
# Source VMDK (can be a flat VMDK or part of a snapshot chain)
vmdk: /home/ssahani/tt/vmdk2kvm/downloads/esx8.0-rhel10.0beta-x86_64-efi/esx8.0-rhel10.0beta-x86_64-efi/esx8.0-rhel10.0beta-x86_64-efi_1-flat.vmdk

# --------------------------------------------------------------------
# Output layout
# --------------------------------------------------------------------
# Base output directory
output_dir: /home/ssahani/by-path/out

# Always flatten snapshot chains first (recommended)
flatten: true
flatten_format: qcow2

# Final image name (relative to output_dir)
to_output: rhel10-fixed.qcow2
out_format: qcow2

# Space + integrity
compress: true
checksum: true

# --------------------------------------------------------------------
# Filesystem + boot fixes
# --------------------------------------------------------------------

# Always show /etc/fstab before and after modification
print_fstab: true

# Rewrite *all* mount entries to stable identifiers.
# Priority order:
#   UUID= → PARTUUID= → LABEL=
fstab_mode: stabilize-all

# Fix GRUB kernel cmdline root=
no_grub: false

# Regenerate initramfs
# (openSUSE, RHEL, Fedora → dracut)
regen_initramfs: true

# Keep backups inside the guest filesystem
no_backup: false

# --------------------------------------------------------------------
# Safety / visibility
# --------------------------------------------------------------------

# Perform real writes (set true to preview only)
dry_run: false

# Verbosity:
#   0 = INFO
#   1 = verbose
#   2 = DEBUG (recommended while developing)
verbose: 2

# Optional log file
log_file: /home/ssahani/by-path/out/vmdk2kvm.log

# Optional Markdown report (recommended)
report: /home/ssahani/by-path/out/vmdk2kvm-report.md

# --------------------------------------------------------------------
# Optional smoke tests (disabled by default)
# --------------------------------------------------------------------
# libvirt_test: true
# vm_name: rhel10-fixed
# memory: 2048
# vcpus: 2
# uefi: true
# timeout: 30
# keep_domain: false
# headless: true
```

---

## What happens internally (high level)

1. **VMDK opened offline**

   * libguestfs only
   * no kernel boot, no udev races

2. **Snapshot flattening**

   * produces a single coherent disk image

3. **Filesystem inspection**

   * detects root, boot, EFI, LVM, Btrfs subvols

4. **fstab rewrite**

   * `/dev/disk/by-path/*` → `UUID=` / `PARTUUID=`
   * consistent across reboots and hypervisors

5. **GRUB fixup**

   * canonical sources first (BLS, `/etc/kernel/cmdline`)
   * generated configs treated as output only

6. **Initramfs regeneration**

   * dracut-aware
   * avoids host-only traps
   * virtio-safe

7. **Final qcow2 creation**

   * compressed
   * validated
   * checksum generated

---

## Expected output

After a successful run:

```text
/home/ssahani/by-path/out/
├── rhel10-fixed.qcow2
├── vmdk2kvm.log
└── vmdk2kvm-report.md
```

The image is ready for:

* raw `qemu-system-x86_64`
* libvirt (UEFI or BIOS)
* further automation (Packer, Ansible, Tinkerbell, etc.)

---

## Boot validation (recommended)

Always validate **outside libvirt first**:

```bash
qemu-system-x86_64 \
  -enable-kvm \
  -machine q35 \
  -cpu host \
  -m 4096 \
  -drive file=/var/tmp/rhel10-fixed.qcow2,if=virtio,format=qcow2 \
  -serial mon:stdio
```

If this boots, libvirt will not surprise you later.

---

## Philosophy

This config follows a simple rule:

> **If the guest cannot explain its own boot path in UUIDs, it does not deserve to boot.**

That harshness is what makes the result boring — and boring systems survive migrations.
