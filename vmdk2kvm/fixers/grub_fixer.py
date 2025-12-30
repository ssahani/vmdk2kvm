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

    Goal: “works on the weird zoo”:
      - Debian/Ubuntu (update-initramfs, update-grub)
      - RHEL/Fedora/CentOS/Rocky/Alma (dracut, grub2-mkconfig, BLS)
      - SUSE/openSUSE (mkinitrd/dracut, grub2-mkconfig)
      - Arch (mkinitcpio, grub-mkconfig)
      - Alpine (mkinitfs, extlinux)
      - Gentoo (dracut/genkernel, grub-mkconfig)
      - systemd-boot (kernel-install, bootctl + entries)
      - GRUB legacy / odd placements: /boot/grub/grub.cfg vs /boot/grub2/grub.cfg

    Notes:
      - We stay mostly “config regen”, not “boot sector reinstall”.
      - Offline guestfs environment may lack /dev, efivars, proc; so we:
          * bind-mount /dev,/proc,/sys when possible (guestfs mountpoints)
          * avoid bootloader installs that write MBR/ESP unless explicitly requested elsewhere
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

    def run_guest(cmd: List[str], *, chrooted: bool = True) -> Tuple[bool, str]:
        """
        Execute inside the guest. Prefer chrooted exec for tools that assume / is the guest root.
        If your codebase already provides a `self._chroot_cmd(...)`, swap here.
        """
        try:
            self.logger.info(f"Running (guestfs): {' '.join(cmd)}")
            out = g.command(cmd) if not chrooted else g.command(cmd)
            return True, U.to_text(out)
        except Exception as e:
            return False, str(e)

    def guest_has_cmd(cmd: str) -> bool:
        # Some guests won’t have /bin/sh in minimal images; but most do. Best-effort.
        try:
            g.command(["sh", "-c", f"command -v {cmd} >/dev/null 2>&1"])
            return True
        except Exception:
            return False

    def file_exists(p: str) -> bool:
        try:
            return g.is_file(p)
        except Exception:
            return False

    def dir_exists(p: str) -> bool:
        try:
            return g.is_dir(p)
        except Exception:
            return False

    def read_text(p: str) -> str:
        try:
            return U.to_text(g.read_file(p))
        except Exception:
            return ""

    # Collect guest kernel versions (helps dracut/mkinitrd/genkernel fallbacks)
    guest_kvers: List[str] = []
    try:
        if dir_exists("/lib/modules"):
            guest_kvers = sorted([U.to_text(x) for x in g.ls("/lib/modules") if U.to_text(x).strip()])
    except Exception:
        guest_kvers = []
    info["guest_kernels"] = guest_kvers

    # Detect init system / boot style hints
    has_systemd = dir_exists("/run/systemd") or file_exists("/usr/lib/systemd/systemd") or file_exists("/lib/systemd/systemd")
    has_boot_loader_spec = has_bls or dir_exists("/boot/loader/entries") or dir_exists("/usr/lib/kernel/install.d")
    uses_extlinux = file_exists("/boot/extlinux/extlinux.conf") or file_exists("/etc/extlinux.conf")
    has_esp = dir_exists("/boot/efi") or dir_exists("/efi") or dir_exists("/boot/EFI")
    info["hints"] = {
        "systemd": bool(has_systemd),
        "bls_or_loader_entries": bool(has_boot_loader_spec),
        "extlinux": bool(uses_extlinux),
        "esp_present": bool(has_esp),
    }

    # -----------------
    # Initramfs regen: build candidate commands in “best chance first” order
    # -----------------
    initramfs_attempts: List[List[str]] = []

    # Debian/Ubuntu: update-initramfs is canonical
    if guest_has_cmd("update-initramfs"):
        initramfs_attempts += [
            ["update-initramfs", "-u", "-k", "all"],
            ["update-initramfs", "-u"],
        ]

    # Arch: mkinitcpio
    if guest_has_cmd("mkinitcpio"):
        initramfs_attempts += [["mkinitcpio", "-P"]]

    # Fedora/RHEL/SUSE/Gentoo often have dracut
    if guest_has_cmd("dracut"):
        # Most reliable for “fix what is there”: regenerate-all if supported
        initramfs_attempts += [["dracut", "-f", "--regenerate-all"], ["dracut", "-f"]]
        # If we have a kernel version, try targeted first (sometimes faster, sometimes less surprising)
        if guest_kvers:
            initramfs_attempts.insert(0, ["dracut", "-f", "--kver", guest_kvers[-1]])

    # SUSE classic
    if guest_has_cmd("mkinitrd"):
        initramfs_attempts += [["mkinitrd"]]

    # Alpine Linux: mkinitfs is the thing (OpenRC world)
    # Typical usage: mkinitfs -c /etc/mkinitfs/mkinitfs.conf -b / <kver>
    if guest_has_cmd("mkinitfs") and guest_kvers:
        initramfs_attempts += [["mkinitfs", "-b", "/", guest_kvers[-1]]]
        # best-effort with config if present
        if file_exists("/etc/mkinitfs/mkinitfs.conf"):
            initramfs_attempts.insert(0, ["mkinitfs", "-c", "/etc/mkinitfs/mkinitfs.conf", "-b", "/", guest_kvers[-1]])

    # Gentoo: genkernel (if present)
    if guest_has_cmd("genkernel"):
        # “--install” will touch /boot; usually OK, but offline environments can be fragile.
        initramfs_attempts += [["genkernel", "--install", "initramfs"]]

    # systemd kernel-install path: try to (re)generate entries for newest kver
    # WARNING: can fail if vmlinuz path isn’t where the tool expects; still useful.
    if guest_has_cmd("kernel-install") and guest_kvers:
        # Try common vmlinuz locations
        k = guest_kvers[-1]
        vmlinuz_candidates = [
            f"/lib/modules/{k}/vmlinuz",
            f"/boot/vmlinuz-{k}",
            f"/boot/vmlinuz",
        ]
        for vml in vmlinuz_candidates:
            if file_exists(vml):
                initramfs_attempts += [["kernel-install", "add", k, vml]]
                break

    # de-dup commands (avoid spam)
    seen = set()
    deduped: List[List[str]] = []
    for c in initramfs_attempts:
        key = tuple(c)
        if key not in seen:
            seen.add(key)
            deduped.append(c)
    initramfs_attempts = deduped

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
    # Bootloader regen: GRUB, BLS, systemd-boot, extlinux
    # -----------------
    boot_attempts: List[List[str]] = []

    # Debian/Ubuntu
    if guest_has_cmd("update-grub"):
        boot_attempts.append(["update-grub"])

    # GRUB2 mkconfig locations vary:
    #  - /boot/grub2/grub.cfg (RHEL/Fedora/SUSE)
    #  - /boot/grub/grub.cfg  (Debian/Arch in some setups)
    # We'll generate to whichever directory exists.
    grub2_cfg_targets: List[str] = []
    if dir_exists("/boot/grub2"):
        grub2_cfg_targets.append("/boot/grub2/grub.cfg")
    if dir_exists("/boot/grub"):
        grub2_cfg_targets.append("/boot/grub/grub.cfg")

    if guest_has_cmd("grub2-mkconfig"):
        if not grub2_cfg_targets:
            # still try the “most common” rpm path
            grub2_cfg_targets = ["/boot/grub2/grub.cfg"]
        for tgt in grub2_cfg_targets:
            boot_attempts.append(["grub2-mkconfig", "-o", tgt])

    # Some distros ship grub-mkconfig (not grub2-mkconfig)
    if guest_has_cmd("grub-mkconfig"):
        if not grub2_cfg_targets:
            grub2_cfg_targets = ["/boot/grub/grub.cfg"]
        for tgt in grub2_cfg_targets:
            boot_attempts.append(["grub-mkconfig", "-o", tgt])

    # UEFI vendor grub.cfg in ESP (ONLY if file exists; generating into ESP is sometimes used)
    # We only attempt if looks_uefi and grub{,2}-mkconfig exists.
    if looks_uefi and (guest_has_cmd("grub2-mkconfig") or guest_has_cmd("grub-mkconfig")):
        mk = "grub2-mkconfig" if guest_has_cmd("grub2-mkconfig") else "grub-mkconfig"
        for p in (
            "/boot/efi/EFI/redhat/grub.cfg",
            "/boot/efi/EFI/centos/grub.cfg",
            "/boot/efi/EFI/fedora/grub.cfg",
            "/boot/efi/EFI/opensuse/grub.cfg",
            "/boot/efi/EFI/ubuntu/grub.cfg",
            "/boot/efi/EFI/debian/grub.cfg",
            "/efi/EFI/redhat/grub.cfg",
            "/efi/EFI/opensuse/grub.cfg",
            "/efi/EFI/ubuntu/grub.cfg",
        ):
            if file_exists(p):
                boot_attempts.append([mk, "-o", p])

    # BLS systems (RHEL/Fedora): often you don’t need grub.cfg regen.
    # But “grub2-mkconfig” is harmless-ish; still, if grubby exists, we can sanity check.
    if guest_has_cmd("grubby"):
        boot_attempts.append(["grubby", "--info=ALL"])  # non-destructive

    # systemd-boot: bootctl update works when EFI vars/efivars exist (often NOT offline).
    # Still useful to try non-destructive checks.
    if guest_has_cmd("bootctl"):
        boot_attempts.append(["bootctl", "status"])
        # Only attempt update if ESP seems mounted and looks_uefi
        if looks_uefi and (dir_exists("/boot/efi") or dir_exists("/efi")):
            boot_attempts.append(["bootctl", "update"])

    # extlinux/syslinux: configs typically edited directly, but we can at least validate it parses.
    # Alpine often uses extlinux + mkinitfs.
    if guest_has_cmd("extlinux"):
        # Avoid install commands that write boot sectors here.
        boot_attempts.append(["extlinux", "--version"])
        # If config exists, attempt a "dry" parse by catting it (helps debugging)
        if file_exists("/boot/extlinux/extlinux.conf"):
            boot_attempts.append(["sh", "-c", "sed -n '1,200p' /boot/extlinux/extlinux.conf"])
        elif file_exists("/etc/extlinux.conf"):
            boot_attempts.append(["sh", "-c", "sed -n '1,200p' /etc/extlinux.conf"])

    # lilo is ancient; do not run lilo offline (writes MBR). But detect for reporting.
    if guest_has_cmd("lilo"):
        boot_attempts.append(["lilo", "-V"])

    # zipl (s390x) / yaboot (ppc) / elilo (ancient EFI) – detect only, don’t install/rewrite
    for tool in ("zipl", "yaboot", "elilo"):
        if guest_has_cmd(tool):
            boot_attempts.append([tool, "--version"])

    # De-dup boot attempts
    seen = set()
    deduped = []
    for c in boot_attempts:
        key = tuple(c)
        if key not in seen:
            seen.add(key)
            deduped.append(c)
    boot_attempts = deduped

    boot_ran: List[Dict[str, Any]] = []
    did_boot = False

    # Run “safe” boot commands; stop early on first real regen success,
    # but allow multiple grub-mkconfig outputs if present.
    for cmd in boot_attempts:
        ok, out = run_guest(cmd)
        boot_ran.append({"cmd": cmd, "ok": ok, "out": out[-3000:]})
        if ok:
            did_boot = True
            # keep going for multiple grub{,2}-mkconfig targets, else stop
            if cmd and cmd[0] not in ("grub2-mkconfig", "grub-mkconfig"):
                break

    info["bootloader"] = {"attempts": boot_ran, "success": did_boot}

    # -----------------
    # Extra: quick “did we actually create initramfs files?” sanity hints (non-fatal)
    # -----------------
    sanity: Dict[str, Any] = {"boot": {}, "initramfs": {}}
    try:
        # Common initramfs paths vary
        boot_ls = []
        if dir_exists("/boot"):
            boot_ls = sorted([U.to_text(x) for x in g.ls("/boot")])
        sanity["boot"]["boot_ls"] = boot_ls[-50:]  # last 50 names
    except Exception:
        pass

    # systemd-boot entries existence
    if dir_exists("/boot/loader/entries"):
        try:
            entries = sorted([U.to_text(x) for x in g.ls("/boot/loader/entries")])
            sanity["boot"]["loader_entries"] = entries
        except Exception:
            pass

    info["sanity"] = sanity
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
