# ARCHITECTURE.md — vmdk2kvm Internal Architecture

## Purpose
This document describes the **module-level architecture**, data flow, and invariants
inside `vmdk2kvm`. It is written for contributors and reviewers who need to understand
*where logic lives* and *why it is structured this way*.

---

## High-level flow

All execution modes map to a single conceptual pipeline:

FETCH → FLATTEN → INSPECT → FIX → CONVERT → VALIDATE

Each stage is optional depending on the command, but **ordering is never violated**.

---

## Top-level modules

```
vmdk2kvm/
├── cli/                 # argparse, config merge, entrypoints
├── config/              # YAML/JSON loading, merging, defaults
├── core/
│   ├── logger.py        # structured logging, verbosity, emoji-safe
│   ├── utils.py         # subprocess, hashing, filesystem helpers
│   └── recovery.py     # checkpoint + resume primitives
├── fetchers/
│   ├── local.py         # local VMDK handling
│   ├── esxi_ssh.py      # SSH/SCP fetcher
│   └── vsphere.py       # pyvmomi integration
├── flatten/
│   └── qemu_img.py      # snapshot chain flattening
├── inspectors/
│   └── guestfs.py       # libguestfs inspection wrapper
├── fixers/
│   ├── fstab_rewriter.py
│   ├── grub_fixer.py
│   ├── initramfs.py
│   ├── windows_fixer.py
│   └── vmware_cleanup.py
├── converters/
│   └── qemu_img.py
├── testers/
│   ├── libvirt.py
│   └── qemu.py
└── daemon/
    └── watcher.py
```

---

## Key architectural invariants

### 1. No fixer assumes runtime execution
All fixers operate **offline** unless explicitly in `live-fix` mode.

### 2. libguestfs is the only authority on guest state
We never guess filesystem layout or OS type.

### 3. by-path is never trusted
Any module touching `/etc/fstab`, GRUB, or crypttab must eliminate by-path usage.

### 4. Windows logic is isolated
Windows-specific behavior is *never* mixed into Linux fixers.

### 5. Idempotency where possible
Fixers are written to tolerate being run multiple times safely.

---

## Why this matters

This architecture allows:
- partial pipeline reuse
- safe retries
- deterministic behavior across hosts
- controlled failure points

Any new feature must fit **one pipeline stage** or justify a new one.
