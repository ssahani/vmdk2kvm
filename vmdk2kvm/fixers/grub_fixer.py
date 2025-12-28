# vmdk2kvm/fixers/grub_fixer.py
# ---------------------------------------------------------------------
# GRUB/root= stabilization + device.map cleanup + initramfs/grub regen
# Linux-only. Windows logic stays in windows_fixer.py.
#
# “All distros” in practice means:
#   1) Don’t assume one initramfs tool (support: update-initramfs, dracut, mkinitcpio, mkinitrd,
#      kernel-install, zypper-boot tools where present).
#   2) Don’t assume one bootloader layout (support: GRUB legacy, GRUB2, GRUB+BLS, systemd-boot,
#      extlinux/syslinux as best-effort).
#   3) Prefer editing canonical cmdline sources (BLS entries, /etc/kernel/cmdline, /etc/default/grub),
#      and treat generated grub.cfg as “regen output”, not the only source of truth.
#
# Strategy used here:
#   - Compute stable root= token (UUID/PARTUUID/LABEL) from the real root device.
#   - Update root= in:
#       * BLS entries (/boot/loader/entries/*.conf)  [covers systemd-boot + grub+bLS]
#       * /etc/kernel/cmdline (if exists)
#       * /etc/default/grub (if exists)
#       * grub.cfg files as fallback
#       * extlinux.conf/syslinux.cfg as fallback
#   - Regen initramfs using the best available tool *inside the guest*.
#   - Regen bootloader configs using best available tool *inside the guest*.
#
# NOTE: guestfs command execution requires the guest root to be mounted and PATH to include tools.
# ---------------------------------------------------------------------

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import re

import guestfs  # type: ignore
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from ..core.utils import U
from .fstab_rewriter import Ident, parse_btrfsvol_spec


# ---------------------------
# GRUB device.map cleanup
# ---------------------------

def remove_stale_device_map(self, g: guestfs.GuestFS) -> int:
    """
    Removes stale grub device.map files that often break after bus/controller changes
    (sda->vda, hd0 mappings, etc.).
    """
    removed = 0
    for p in ("/boot/grub2/device.map", "/boot/grub/device.map", "/etc/grub2-device.map"):
        try:
            if g.is_file(p):
                txt = U.to_text(g.read_file(p))
                # device.map often contains stale BIOS disk mappings after bus/controller change
                if "hd0" in txt or "sda" in txt or "vda" in txt:
                    self.logger.info(f"GRUB: removing stale device.map: {p}")
                    removed += 1
                    if not self.dry_run:
                        g.rm_f(p)
        except Exception:
            continue
    return removed


# ---------------------------
# Boot layout heuristics (offline)
# ---------------------------

def _guest_looks_uefi(g: guestfs.GuestFS) -> bool:
    """
    Offline heuristic for UEFI-installed guests.
    Look for:
      - ESP directory and EFI binaries
      - fstab entry mounting vfat to /boot/efi
    """
    try:
        if g.is_dir("/boot/efi") and g.is_dir("/boot/efi/EFI"):
            try:
                for x in g.find("/boot/efi/EFI"):
                    p = U.to_text(x)
                    if p.lower().endswith(".efi"):
                        return True
            except Exception:
                try:
                    if g.ls("/boot/efi/EFI"):
                        return True
                except Exception:
                    pass
    except Exception:
        pass

    try:
        if g.is_file("/etc/fstab"):
            fstab = U.to_text(g.read_file("/etc/fstab"))
            if re.search(r"^\S+\s+/boot/efi\s+vfat\b", fstab, flags=re.M):
                return True
    except Exception:
        pass

    return False


def _guest_has_bls(g: guestfs.GuestFS) -> bool:
    """
    Boot Loader Spec entries present?
      /boot/loader/entries/*.conf
    """
    try:
        return g.is_dir("/boot/loader/entries")
    except Exception:
        return False


# ---------------------------
# root= stabilization
# ---------------------------

def _stable_root_id(self, g: guestfs.GuestFS) -> Optional[str]:
    """
    Compute a stable root identifier usable as kernel cmdline root=...
    Returns something like UUID=..., PARTUUID=..., LABEL=..., etc.
    """
    root_dev = getattr(self, "root_dev", None)
    if not root_dev:
        return None

    if isinstance(root_dev, str) and root_dev.startswith("btrfsvol:"):
        dev, _sv = parse_btrfsvol_spec(root_dev)
        root_dev = dev.strip()

    if isinstance(root_dev, str) and root_dev.startswith("/dev/disk/by-"):
        try:
            rp = U.to_text(g.realpath(root_dev)).strip()
            if rp.startswith("/dev/"):
                root_dev = rp
        except Exception:
            pass

    if not isinstance(root_dev, str) or not root_dev.startswith("/dev/"):
        return None

    blk = Ident.g_blkid_map(g, root_dev)
    stable = Ident.choose_stable(blk)

    if not stable:
        try:
            parent = re.sub(r"p?\d+$", "", root_dev)
            if parent != root_dev and parent.startswith("/dev/"):
                blk2 = Ident.g_blkid_map(g, parent)
                stable2 = Ident.choose_stable(blk2)
                if stable2:
                    stable = stable2
        except Exception:
            pass

    return stable


def _replace_root_tokens(text: str, new_root: str) -> str:
    return re.sub(r"\broot=\S+", new_root, text)


def _update_file_replace_root(self, g: guestfs.GuestFS, path: str, new_root: str) -> bool:
    old = U.to_text(g.read_file(path))
    new = _replace_root_tokens(old, new_root)
    if new == old:
        return False

    self.logger.info(f"Updated root= in {path}" + (" (dry-run)" if self.dry_run else ""))
    if not self.dry_run:
        self.backup_file(g, path)
        g.write(path, new.encode("utf-8"))
    return True


def _update_bls_root(self, g: guestfs.GuestFS, new_root: str) -> int:
    changed = 0
    try:
        if not g.is_dir("/boot/loader/entries"):
            return 0
        for ent in g.ls("/boot/loader/entries"):
            ent_s = U.to_text(ent).strip()
            if not ent_s.endswith(".conf"):
                continue
            p = f"/boot/loader/entries/{ent_s}"
            if g.is_file(p) and _update_file_replace_root(self, g, p, new_root):
                changed += 1
    except Exception as e:
        self.logger.warning(f"BLS update failed: {e}")
    return changed


def _update_kernel_cmdline(self, g: guestfs.GuestFS, new_root: str) -> int:
    """
    Update canonical cmdline sources used by some distros:
      - /etc/kernel/cmdline   (systemd/kernel-install setups)
    """
    changed = 0
    for p in ("/etc/kernel/cmdline",):
        try:
            if g.is_file(p):
                if _update_file_replace_root(self, g, p, new_root):
                    changed += 1
        except Exception:
            continue
    return changed


def _update_default_grub(self, g: guestfs.GuestFS, new_root: str) -> int:
    """
    Update /etc/default/grub if present. This is canonical for many GRUB installs.
    """
    changed = 0
    p = "/etc/default/grub"
    try:
        if g.is_file(p):
            old = U.to_text(g.read_file(p))
            # Be slightly more conservative: prefer updating only GRUB_CMDLINE_LINUX* lines.
            def repl_line(m: re.Match[str]) -> str:
                line = m.group(0)
                return _replace_root_tokens(line, new_root)
            new = re.sub(r"^(GRUB_CMDLINE_LINUX(?:_DEFAULT)?=.*)$", repl_line, old, flags=re.M)
            if new != old:
                self.logger.info(f"Updated root= in {p}" + (" (dry-run)" if self.dry_run else ""))
                changed += 1
                if not self.dry_run:
                    self.backup_file(g, p)
                    g.write(p, new.encode("utf-8"))
    except Exception as e:
        self.logger.warning(f"Failed updating {p}: {e}")
    return changed


def _update_grub_cfg_fallback(self, g: guestfs.GuestFS, new_root: str) -> int:
    """
    Last-resort: directly edit generated grub.cfg.
    Some systems regenerate it on package update, so treat as fallback only.
    """
    changed = 0
    for p in ("/boot/grub2/grub.cfg", "/boot/grub/grub.cfg"):
        try:
            if g.is_file(p) and _update_file_replace_root(self, g, p, new_root):
                changed += 1
        except Exception as e:
            self.logger.warning(f"Failed updating {p}: {e}")
    return changed


def _update_extlinux_syslinux_fallback(self, g: guestfs.GuestFS, new_root: str) -> int:
    """
    Best-effort support for extlinux/syslinux-style configs (common in tiny distros/embedded).
    """
    changed = 0
    candidates = (
        "/boot/extlinux/extlinux.conf",
        "/extlinux/extlinux.conf",
        "/boot/syslinux/syslinux.cfg",
        "/syslinux/syslinux.cfg",
    )
    for p in candidates:
        try:
            if g.is_file(p) and _update_file_replace_root(self, g, p, new_root):
                changed += 1
        except Exception:
            continue
    return changed


def update_grub_root(self, g: guestfs.GuestFS) -> int:
    """
    Best-effort root= update across common Linux bootloader ecosystems.
    """
    if not getattr(self, "update_grub", False):
        return 0

    stable = _stable_root_id(self, g)
    if not stable:
        self.logger.warning("GRUB: could not find stable ID for root device; skipping root= update.")
        return 0

    new_root = f"root={stable}"
    looks_uefi = _guest_looks_uefi(g)
    has_bls = _guest_has_bls(g)
    self.logger.info(f"Boot heuristics: {'UEFI' if looks_uefi else 'BIOS'}; BLS={'yes' if has_bls else 'no'}")
    self.logger.info(f"Setting kernel cmdline {new_root}")

    changed = 0
    # Canonical sources first
    if has_bls:
        changed += _update_bls_root(self, g, new_root)
    changed += _update_kernel_cmdline(self, g, new_root)
    changed += _update_default_grub(self, g, new_root)

    # Fallbacks
    changed += _update_grub_cfg_fallback(self, g, new_root)
    changed += _update_extlinux_syslinux_fallback(self, g, new_root)

    return changed


# ---------------------------
# initramfs + bootloader regeneration (all distros)
# ---------------------------

def regen(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    """
    Linux-only initramfs + bootloader regen.
    Uses tool-detection inside the guest (works across distros if tools exist).
    """
    if not getattr(self, "regen_initramfs", False):
        return {"enabled": False}

    # If Windows, skip.
    try:
        if getattr(self, "inspect_root", None) and (U.to_text(g.inspect_get_type(self.inspect_root)).lower() == "windows"):
            self.logger.info("regen(): Windows guest detected; skipping Linux initramfs/bootloader regeneration.")
            return {"enabled": True, "skipped": "windows"}
    except Exception:
        pass

    looks_uefi = _guest_looks_uefi(g)
    has_bls = _guest_has_bls(g)

    distro = ""
    product = ""
    try:
        if getattr(self, "inspect_root", None):
            distro = (U.to_text(g.inspect_get_distro(self.inspect_root)) or "").lower()
            product = U.to_text(g.inspect_get_product_name(self.inspect_root)) or ""
    except Exception:
        pass

    info: Dict[str, Any] = {
        "enabled": True,
        "distro": distro,
        "product": product,
        "guest_boot": "uefi" if looks_uefi else "bios",
        "bls": has_bls,
    }

    if getattr(self, "dry_run", False):
        self.logger.info("DRY-RUN: skipping initramfs/bootloader regeneration.")
        info["dry_run"] = True
        return info

    self.logger.info(f"Regen plan: distro={distro or 'unknown'} boot={'UEFI' if looks_uefi else 'BIOS'} BLS={'yes' if has_bls else 'no'}")
    self.logger.info("🛠️ Regenerating initramfs and bootloader configs...")

    def run_guest(cmd: List[str]) -> Tuple[bool, str]:
        try:
            self.logger.info(f"Running (guestfs): {' '.join(cmd)}")
            out = g.command(cmd)
            return True, U.to_text(out)
        except Exception as e:
            return False, str(e)

    def guest_has_cmd(cmd: str) -> bool:
        # Best-effort: some appliances have `sh`, so use `command -v`.
        try:
            g.command(["sh", "-c", f"command -v {cmd} >/dev/null 2>&1"])
            return True
        except Exception:
            return False

    # Collect guest kernel versions (helps dracut/mkinitrd fallback)
    guest_kvers: List[str] = []
    try:
        if g.is_dir("/lib/modules"):
            guest_kvers = sorted([U.to_text(x) for x in g.ls("/lib/modules") if U.to_text(x).strip()])
    except Exception:
        guest_kvers = []
    info["guest_kernels"] = guest_kvers

    # -----------------
    # Initramfs regen: pick best available tool
    # -----------------
    initramfs_attempts: List[List[str]] = []
    if guest_has_cmd("update-initramfs"):
        initramfs_attempts += [["update-initramfs", "-u", "-k", "all"], ["update-initramfs", "-u"]]
    if guest_has_cmd("mkinitcpio"):
        initramfs_attempts += [["mkinitcpio", "-P"]]
    if guest_has_cmd("dracut"):
        initramfs_attempts += [["dracut", "-f"], ["dracut", "-f", "--regenerate-all"]]
        if guest_kvers:
            initramfs_attempts.insert(1, ["dracut", "-f", "--kver", guest_kvers[-1]])
    if guest_has_cmd("mkinitrd"):
        # Common on older SUSE
        initramfs_attempts += [["mkinitrd"]]

    # kernel-install style: regenerate using kernel-install if present (best-effort)
    # (Not always safe offline; still attempt if available.)
    if guest_has_cmd("kernel-install") and guest_kvers:
        # Reinstall the newest kernel entry; some distros require images present under /boot.
        initramfs_attempts += [["kernel-install", "add", guest_kvers[-1], f"/lib/modules/{guest_kvers[-1]}/vmlinuz"]]

    initramfs_ran: List[Dict[str, Any]] = []
    did_initramfs = False
    for cmd in initramfs_attempts:
        ok, out = run_guest(cmd)
        initramfs_ran.append({"cmd": cmd, "ok": ok, "out": out[-3000:]})
        if ok:
            did_initramfs = True
            break
    info["initramfs"] = {"attempts": initramfs_ran, "success": did_initramfs}

    # -----------------
    # Bootloader regen: pick best available tool(s)
    # -----------------
    boot_attempts: List[List[str]] = []

    # Debian/Ubuntu canonical
    if guest_has_cmd("update-grub"):
        boot_attempts.append(["update-grub"])

    # GRUB2 common across rpm distros
    if guest_has_cmd("grub2-mkconfig"):
        boot_attempts.append(["grub2-mkconfig", "-o", "/boot/grub2/grub.cfg"])
        # Some distros keep a second location
        if g.is_file("/boot/grub/grub.cfg") and guest_has_cmd("grub-mkconfig"):
            boot_attempts.append(["grub-mkconfig", "-o", "/boot/grub/grub.cfg"])

        # UEFI ESP vendor configs (only if file exists)
        if looks_uefi:
            for p in (
                "/boot/efi/EFI/redhat/grub.cfg",
                "/boot/efi/EFI/centos/grub.cfg",
                "/boot/efi/EFI/fedora/grub.cfg",
                "/boot/efi/EFI/opensuse/grub.cfg",
                "/boot/efi/EFI/ubuntu/grub.cfg",
                "/boot/efi/EFI/debian/grub.cfg",
            ):
                try:
                    if g.is_file(p):
                        boot_attempts.append(["grub2-mkconfig", "-o", p])
                except Exception:
                    pass

    # grubby (RHEL/Fedora) to push cmdline to all kernels (best-effort)
    # This only helps if root= is in args; our file edits already handle BLS/cmdline/default grub.
    if guest_has_cmd("grubby"):
        boot_attempts.append(["grubby", "--info=ALL"])  # non-destructive: at least validate tool presence

    # extlinux/syslinux: no universal regen tool; configs are edited directly earlier.
    # If extlinux is present, we can try reinstalling (best-effort).
    if guest_has_cmd("extlinux"):
        # Non-destructive: print version; avoids writing boot sectors offline.
        boot_attempts.append(["extlinux", "--version"])

    boot_ran: List[Dict[str, Any]] = []
    did_boot = False
    for cmd in boot_attempts:
        ok, out = run_guest(cmd)
        boot_ran.append({"cmd": cmd, "ok": ok, "out": out[-3000:]})
        if ok:
            did_boot = True
            # Keep going if multiple outputs are likely (e.g., grub2-mkconfig + esp grub.cfg)
            # but don't loop forever; we record attempts and stop on first success unless it's mkconfig.
            if cmd and cmd[0] not in ("grub2-mkconfig",):
                break
    info["bootloader"] = {"attempts": boot_ran, "success": did_boot}

    return info


# ---------------------------
# Optional: wire methods onto OfflineFSFix so existing self.* calls work.
# ---------------------------

def wire_into(cls: type) -> type:
    """
    Monkey-patch these helpers as instance methods:
      - remove_stale_device_map
      - update_grub_root
      - regen
    """
    setattr(cls, "remove_stale_device_map", remove_stale_device_map)
    setattr(cls, "update_grub_root", update_grub_root)
    setattr(cls, "regen", regen)
    return cls
