## CLI reference (manpage-ish, but still human)

### Synopsis

```bash
vmdk2kvm.py [-v|-vv] [--log-file FILE] [--output-dir DIR]
            [--force] [--no-grub]
            [--flatten] [--workdir DIR]
            [--to-qcow2 PATH] [--compress]
            {local,fetch-and-fix} ...
```

### Global options

#### `-v`, `-vv`, `--verbose`

Increase logging verbosity.

- `-v`: INFO logs, slightly chattier.
- `-vv`: DEBUG logs, includes command execution traces and best-effort libguestfs trace output.

#### `--log-file LOG_FILE`

Write logs to a file (plain text timestamp format). Useful for bug reports and postmortems.

#### `--output-dir OUTPUT_DIR` (default: `./out`)

Root output directory for derived files.

- Flattened working images go under: `<output-dir>/work` (unless overridden).
- If `--to-qcow2` is a relative path, it is written under: `<output-dir>/<to-qcow2>`.

---

### Guest-fixing options (libguestfs)

#### `--force`

Rewrite even if entries already look stable (UUID/PARTUUID/LABEL/by-uuid paths, etc).

**Danger lever.** Default behavior is conservative. Use when:

- the guest has wrong/stale UUIDs,
- you explicitly want to normalize everything.

#### `--no-grub`

Skip GRUB `root=` updates entirely.

- The tool will still rewrite `/etc/fstab`.
- GRUB changes are best-effort edits to existing config files; disabling this avoids touching boot config.

---

### Flattening / working image options

#### `--flatten`

“Flatten” VMware snapshot chains by converting the VMDK chain into a single working qcow2:

- Input: VMDK descriptor (and its chain files present)
- Output: `flattened-<timestamp>.qcow2` under `--workdir`

This is the safest practical approach because it avoids mutating VMware metadata.

#### `--workdir WORKDIR`

Where to store flattened working images.

- Default: `<output-dir>/work`

---

### Output conversion options

#### `--to-qcow2 TO_QCOW2`

Convert the working image to qcow2 at the given path.

Rules:

- If this path is **relative**, it is treated as relative to `--output-dir`.
- If it is **absolute**, it is used as-is.

#### `--compress`

Apply qcow2 compression (`qemu-img convert -c`).

Tradeoffs:

- ✅ smaller qcow2
- ❌ slower conversion
- ❌ may increase CPU cost for some workloads when writing data later (depends on the access pattern)

---

## Subcommands

### `local`

Fix a **local** VMDK descriptor offline.

#### Required

- `--vmdk PATH`  
  Path to the local VMDK **descriptor** (the small text `.vmdk`).

#### Optional

- `--dry-run`  
  No writes. Prints proposed `/etc/fstab` changes (and skips output conversion).

Examples:

```bash
# Inspect only (no writes)
sudo ./vmdk2kvm.py -vv --output-dir ./out local \
  --vmdk ./openSUSE_Leap_15.4.vmdk \
  --flatten \
  --dry-run

# Full run (flatten + fix + qcow2)
sudo ./vmdk2kvm.py -vv --output-dir ./out local \
  --vmdk ./openSUSE_Leap_15.4.vmdk \
  --flatten \
  --to-qcow2 opensuse-15.4-fixed.qcow2 \
  --compress
```

---

### `fetch-and-fix`

Fetch a VMDK descriptor (+ referenced extent) from ESXi via scp, then fix offline.

#### Required

- `--host HOST`
- `--remote-vmdk REMOTE_DESCRIPTOR_PATH`  
  Example: `/vmfs/volumes/datastore/VM/guest.vmdk`

#### Optional

- `--user USER` (default: `root`)
- `--port PORT` (default: `22`)
- `--identity-file KEYFILE` (SSH key)
- `--ssh-opt OPT` (repeatable; extra args to ssh/scp)
- `--dry-run` (no writes)

Example:

```bash
sudo ./vmdk2kvm.py -v --output-dir ./out fetch-and-fix \
  --host esxi.example \
  --user root \
  --remote-vmdk /vmfs/volumes/datastore/VMs/openSUSE/openSUSE_Leap_15.4.vmdk \
  --flatten \
  --to-qcow2 opensuse-15.4-fixed.qcow2
```

---

## Why `/etc/fstab` breaks across hypervisors (the short horror story)

Linux mount configuration is often written in terms of *device nodes* such as:

- `/dev/sda2`
- `/dev/vda3`
- `/dev/nvme0n1p2`

That looks concrete, but it’s actually **a runtime nickname** assigned by the kernel
based on:

- disk controller type (SATA/SCSI/virtio/NVMe),
- PCI enumeration order,
- how the hypervisor presents devices,
- udev timing and rules,
- sometimes even firmware settings.

So the same disk that was `/dev/sda2` in VMware might become:

- `/dev/vda2` on virtio,
- `/dev/sdb2` if ordering flips,
- `/dev/nvme0n1p2` if presented as NVMe.

If `/etc/fstab` still says `/dev/sda2 / ext4 ...`, the guest boots into the void:
root mount fails, emergency shell happens, or it just sits there like a confused sea turtle.

### The fix: stable identifiers

Stable identifiers don’t depend on enumeration order:

- `UUID=`: filesystem UUID (portable and common)
- `PARTUUID=`: partition UUID (great for EFI too)
- `LABEL=`: human-friendly but can collide if reused
- `/dev/disk/by-*`: udev-generated symlinks (can be stable *within a topology*)

This tool rewrites `/etc/fstab` so that the guest says:

> “Mount the filesystem with this identity,”  
> not  
> “Mount whatever happens to be called /dev/sda2 today.”

### Why LVM needs special care

LVM uses `/dev/mapper/<vg>-<lv>` which is already fairly stable via device-mapper.
Over-eager rewriting can break initramfs expectations and boot ordering.

So the script defaults to:

- **do not rewrite LVM paths** unless `--force` is used.

### GRUB `root=...` is the sibling landmine

Even if fstab is fixed, GRUB might pass `root=/dev/sda2` to the kernel.
That can fail before systemd even gets a chance to read fstab.

This tool does a best-effort replace of `root=...` in:

- `/boot/grub/grub.cfg`
- `/boot/grub2/grub.cfg`
- `/etc/default/grub`

It’s intentionally simple (no chroot, no regeneration), but fixes a big chunk of
“it worked in VMware, why is KVM sad?” cases.

---

## Quick “what should I use?” cheatsheet

- Want best portability: **default** (UUID/PARTUUID)
- EFI guests: keep `/boot/efi` on `PARTUUID=` when possible (tool already prefers it)
- Don’t know if snapshots exist: use `--flatten`
- Want smaller qcow2: add `--compress`
- Something weird / stale in guest configs: consider `--force` (carefully)
