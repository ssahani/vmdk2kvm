# FAILURE_MODES.md — Real-world Migration Failures

This document lists failure classes observed in production and how vmdk2kvm addresses them.

---

## Failure: boots once, fails after reboot

**Cause**
- by-path fstab
- disk enumeration changed

**Fix**
- fstab rewrite to UUID/PARTUUID

---

## Failure: Windows blue-screen on first boot

**Cause**
- VirtIO driver not BOOT_START
- Missing CDD entries

**Fix**
- Offline registry injection

---

## Failure: qemu-img convert succeeds, data corrupted

**Cause**
- Snapshot chain not flattened
- Parent descriptor missing

**Fix**
- Recursive snapshot fetch + flatten

---

## Failure: libvirt test fails on headless server

**Cause**
- SDL / GTK graphics device
- Missing XDG_RUNTIME_DIR

**Fix**
- headless mode + no graphics devices

---

## Failure: virt-v2v succeeded but guest broken

**Cause**
- virt-v2v does not fix fstab by-path
- initramfs mismatch

**Fix**
- post_v2v pipeline with fixers
