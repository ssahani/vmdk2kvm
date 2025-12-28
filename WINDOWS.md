# WINDOWS.md — Windows Migration Deep Dive

This document explains how `vmdk2kvm` handles Windows guests and **why each step exists**.

---

## Problem statement

Windows does not boot based on:
- filesystem correctness
- disk visibility
- registry sanity alone

It boots based on **boot-critical drivers registered correctly**.

---

## Driver injection model

### Supported driver classes
- Storage: viostor / vioscsi
- Network: netkvm
- Balloon: balloon
- Optional: virtiofs, input, GPU

Drivers are copied to:
```
Windows/System32/drivers/
```

---

## Registry editing (offline)

### SYSTEM hive operations
- Opened via `hivex`
- Correct ControlSet detected
- Services created under:
  ```
  HKLM\SYSTEM\ControlSetXXX\Services
  ```

### Required service values
- `Type = 1` (kernel driver)
- `Start = BOOT or SYSTEM`
- `Group = Boot Bus Extender` (storage)

---

## CriticalDeviceDatabase (CDD)

Without CDD entries, Windows may:
- ignore a valid driver
- bind it too late
- blue-screen at boot

Entries are created under:
```
HKLM\SYSTEM\ControlSetXXX\Control\CriticalDeviceDatabase
```

Mapped by PCI vendor/device IDs.

---

## BCD handling

### What we do
- Detect BIOS and UEFI BCD stores
- Create timestamped backups
- Report presence and size

### What we do NOT do
- Binary patch BCD
- Run `bcdedit` offline

This avoids corrupting boot metadata.

---

## Common failure prevented

**INACCESSIBLE_BOOT_DEVICE**

Caused by:
- wrong StartType
- missing CDD
- wrong storage driver selected

`vmdk2kvm` fixes all three before first boot.
