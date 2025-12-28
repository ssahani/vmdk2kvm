# fstab-bypath-to-uuid.py

Convert fragile `/dev/disk/by-path/*` entries in `/etc/fstab` into stable
identifiers (`UUID=`, `PARTUUID=`, or `LABEL=`).

This tool is designed for **VM migrations**, **disk reordering**, and
**hypervisor changes** (VMware → KVM, physical → virtual, etc.), where
`by-path` device names frequently break at boot.

The script is **Python 3.6 compatible** and safe for enterprise Linux
distributions (openSUSE 15, RHEL/CentOS 7/8, SLES).

---

## Why this exists

`/dev/disk/by-path/*` names depend on:
- PCI topology
- controller order
- firmware (BIOS vs UEFI)
- hypervisor quirks

After migration, those paths often change.

`UUID=` / `PARTUUID=` / `LABEL=` do **not**.

This script rewrites only what is unsafe, leaving everything else untouched.

---

## What it does

- Reads an fstab file (or stdin)
- Detects entries starting with `/dev/disk/by-path/`
- Resolves the symlink to the real block device
- Queries `blkid`
- Rewrites the entry using the first available identifier:
  1. `UUID=`
  2. `PARTUUID=`
  3. `LABEL=`
- Creates a timestamped backup when editing in place
- Preserves comments, whitespace, and non-device entries

---

## What it does *not* do

- Does **not** touch `UUID=`, `PARTUUID=`, or `LABEL=` entries
- Does **not** rewrite `/dev/mapper/*` or LVM paths
- Does **not** guess or invent identifiers
- Does **not** modify fstab unless explicitly run in-place

If a device cannot be safely resolved, the original entry is preserved.

---

## Requirements

- Python ≥ 3.6
- `blkid` (from util-linux)
- Read access to block devices
- Root privileges **only** when modifying `/etc/fstab`

---

## Usage

### Show help

```bash
./fstab-bypath-to-uuid.py --help
