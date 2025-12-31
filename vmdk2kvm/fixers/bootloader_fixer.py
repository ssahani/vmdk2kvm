# vmdk2kvm/fixers/bootloader_fixer.py
# ---------------------------------------------------------------------
# Multi-bootloader detection + KVM-friendly “safe” fixes.
#
# Goals:
#   - Detect 7+ bootloader types (GRUB, GRUB2, systemd-boot, rEFInd, LILO, SYSLINUX, EXTLINUX)
#   - Determine "active" bootloader via heuristics (UEFI presence + config locations)
#   - Apply conservative fixes:
#       * add serial console (console=ttyS0,115200n8 console=tty0) when missing
#       * avoid destructive installs (no MBR/ESP reinstall here)
#       * preserve configs: backup before write (optional callback)
#
# Works with libguestfs GuestFS handle `g` (mounted guest).
# ---------------------------------------------------------------------

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from ..core.utils import U


class BootloaderType(Enum):
    GRUB = "grub"
    GRUB2 = "grub2"
    SYSTEMD_BOOT = "systemd-boot"
    REFIND = "refind"
    LILO = "lilo"
    SYSLINUX = "syslinux"
    EXTLINUX = "extlinux"
    UNKNOWN = "unknown"


@dataclass
class BootloaderInfo:
    type: BootloaderType
    version: Optional[str] = None
    config_files: List[str] = field(default_factory=list)
    install_paths: List[str] = field(default_factory=list)
    efi_path: Optional[str] = None
    boot_partition: Optional[str] = None
    detected: bool = False
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BootloaderFixResult:
    bootloaders: List[BootloaderInfo] = field(default_factory=list)
    active_bootloader: Optional[BootloaderType] = None
    fixes_applied: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class MultiBootloaderFixer:
    """
    Conservative multi-bootloader detection and KVM-friendly config edits.

    NOTE: This intentionally does NOT "install" bootloaders (grub-install/extlinux --install/etc).
          Offline conversion environments often lack efivars/proc/dev in the right shape, and
          reinstalling boot sectors is high blast-radius. We only tweak config text + add console.
    """

    # Bootloader detection patterns (paths only; commands are probed via shell in guest)
    BOOTLOADER_PATTERNS: Dict[BootloaderType, Dict[str, List[str]]] = {
        BootloaderType.GRUB: {
            "configs": ["/boot/grub/grub.conf", "/boot/grub/menu.lst"],
            "dirs": ["/boot/grub"],
            "binaries": ["grub"],
            "efi": ["/boot/efi/EFI/grub", "/efi/EFI/grub"],
        },
        BootloaderType.GRUB2: {
            "configs": ["/boot/grub2/grub.cfg", "/boot/grub/grub.cfg", "/etc/default/grub"],
            "dirs": ["/boot/grub2", "/etc/grub.d"],
            "binaries": ["grub2-install", "grub-install", "grub2-mkconfig", "grub-mkconfig"],
            "efi": ["/boot/efi/EFI/grub2", "/boot/efi/EFI/grub", "/efi/EFI/grub2", "/efi/EFI/grub"],
        },
        BootloaderType.SYSTEMD_BOOT: {
            "configs": ["/boot/loader/loader.conf", "/etc/kernel/cmdline"],
            "dirs": ["/boot/loader", "/usr/lib/systemd/boot/efi"],
            "binaries": ["bootctl", "kernel-install"],
            "efi": ["/boot/efi/EFI/systemd", "/boot/efi/EFI/BOOT", "/efi/EFI/systemd", "/efi/EFI/BOOT"],
        },
        BootloaderType.REFIND: {
            "configs": ["/boot/efi/EFI/refind/refind.conf", "/efi/EFI/refind/refind.conf"],
            "dirs": ["/boot/efi/EFI/refind", "/efi/EFI/refind"],
            "binaries": ["refind-install"],
            "efi": ["/boot/efi/EFI/refind", "/efi/EFI/refind"],
        },
        BootloaderType.LILO: {
            "configs": ["/etc/lilo.conf"],
            "dirs": ["/boot/lilo"],
            "binaries": ["lilo"],
            "efi": [],
        },
        BootloaderType.SYSLINUX: {
            "configs": ["/boot/syslinux/syslinux.cfg", "/boot/syslinux.cfg"],
            "dirs": ["/boot/syslinux"],
            "binaries": ["syslinux"],
            "efi": ["/boot/efi/EFI/syslinux", "/efi/EFI/syslinux"],
        },
        BootloaderType.EXTLINUX: {
            "configs": ["/boot/extlinux/extlinux.conf", "/extlinux/extlinux.conf"],
            "dirs": ["/boot/extlinux", "/extlinux"],
            "binaries": ["extlinux"],
            "efi": [],
        },
    }

    # A sane, broadly-compatible console set for VM debugging.
    SERIAL_CONSOLE_ARGS = "console=ttyS0,115200n8 console=tty0"
    # GRUB-ish defaults for serial terminal (opt-in only if missing).
    GRUB_SERIAL_TERMINAL = "GRUB_TERMINAL=\"console serial\""
    GRUB_SERIAL_COMMAND = (
        "GRUB_SERIAL_COMMAND=\"serial --speed=115200 --unit=0 --word=8 --parity=no --stop=1\""
    )

    def __init__(
        self,
        logger: logging.Logger,
        *,
        dry_run: bool = False,
        backup_cb: Optional[Callable[[str], None]] = None,
    ):
        self.logger = logger
        self.dry_run = dry_run
        self._backup_cb = backup_cb

    # ---------------------------
    # Small guest helpers
    # ---------------------------

    def _is_file(self, g, p: str) -> bool:
        try:
            return bool(g.is_file(p))
        except Exception:
            return False

    def _is_dir(self, g, p: str) -> bool:
        try:
            return bool(g.is_dir(p))
        except Exception:
            return False

    def _read_text(self, g, p: str) -> str:
        try:
            return U.to_text(g.read_file(p))
        except Exception:
            return ""

    def _write_text(self, g, p: str, text: str) -> None:
        if self._backup_cb:
            try:
                self._backup_cb(p)
            except Exception:
                # backup is best-effort; never fail the fixer because backups couldn’t be made
                pass
        g.write(p, text.encode("utf-8"))

    def _guest_has_cmd(self, g, cmd: str) -> bool:
        try:
            # Do NOT rely on which (often missing). `command -v` is POSIX-sh.
            g.command(["sh", "-c", f"command -v {cmd} >/dev/null 2>&1"])
            return True
        except Exception:
            return False

    def _guest_run(self, g, sh_cmd: str) -> str:
        try:
            out = g.command(["sh", "-c", sh_cmd])
            return U.to_text(out).strip()
        except Exception:
            return ""

    def _looks_uefi(self, g) -> bool:
        # Heuristic: ESP directories or EFI vendor paths
        for p in ("/boot/efi/EFI", "/efi/EFI", "/boot/EFI"):
            if self._is_dir(g, p):
                return True
        # fstab vfat on /boot/efi
        if self._is_file(g, "/etc/fstab"):
            txt = self._read_text(g, "/etc/fstab")
            if re.search(r"^\S+\s+/boot/efi\s+vfat\b", txt, flags=re.M):
                return True
        return False

    # ---------------------------
    # Detection
    # ---------------------------

    def detect_bootloaders(self, g) -> BootloaderFixResult:
        """Detect all bootloaders present in the guest."""
        result = BootloaderFixResult()

        for bl_type, patterns in self.BOOTLOADER_PATTERNS.items():
            info = BootloaderInfo(type=bl_type)

            for config in patterns.get("configs", []):
                if self._is_file(g, config):
                    info.config_files.append(config)
                    info.detected = True

            for directory in patterns.get("dirs", []):
                if self._is_dir(g, directory):
                    info.install_paths.append(directory)
                    info.detected = True

            for binary in patterns.get("binaries", []):
                if self._guest_has_cmd(g, binary):
                    info.details[f"binary_{binary}"] = True
                    info.detected = True

            for efi_path in patterns.get("efi", []):
                if self._is_dir(g, efi_path):
                    info.efi_path = efi_path
                    info.detected = True
                    break

            if info.detected:
                info.version = self._get_bootloader_version(g, bl_type)
                info.boot_partition = self._detect_boot_partition(g, info)
                result.bootloaders.append(info)

        result.active_bootloader = self._determine_active_bootloader(g, result.bootloaders)
        return result

    def _get_bootloader_version(self, g, bl_type: BootloaderType) -> Optional[str]:
        version_cmds = {
            BootloaderType.GRUB: "grub --version 2>/dev/null || true",
            BootloaderType.GRUB2: "grub2-install --version 2>/dev/null || grub-install --version 2>/dev/null || true",
            BootloaderType.SYSTEMD_BOOT: "bootctl --version 2>/dev/null || true",
            BootloaderType.REFIND: "refind-install --version 2>/dev/null || true",
            BootloaderType.LILO: "lilo -V 2>/dev/null || true",
            BootloaderType.SYSLINUX: "syslinux -v 2>/dev/null || true",
            BootloaderType.EXTLINUX: "extlinux -v 2>/dev/null || true",
        }
        cmd = version_cmds.get(bl_type)
        if not cmd:
            return None
        out = self._guest_run(g, cmd)
        if not out:
            return None
        # extract a plausible version token
        m = re.search(r"(\d+\.\d+(?:\.\d+)?)", out)
        return m.group(1) if m else None

    def _detect_boot_partition(self, g, info: BootloaderInfo) -> Optional[str]:
        # /etc/fstab: /boot is fairly canonical when separate
        if self._is_file(g, "/etc/fstab"):
            fstab = self._read_text(g, "/etc/fstab")
            for line in fstab.splitlines():
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                parts = s.split()
                if len(parts) >= 2 and parts[1] == "/boot":
                    return parts[0]

        # Otherwise, best-effort grep in configs for root=
        for cfg in info.config_files:
            txt = self._read_text(g, cfg)
            if not txt:
                continue
            for pat in (r"\broot=([^\s]+)", r"set\s+root=['\"]?([^\s'\"]+)"):
                m = re.search(pat, txt, flags=re.I)
                if m:
                    return m.group(1)

        return None

    def _determine_active_bootloader(self, g, bootloaders: List[BootloaderInfo]) -> Optional[BootloaderType]:
        if not bootloaders:
            return None

        uefi = self._looks_uefi(g)
        has = {b.type for b in bootloaders}

        # UEFI: prefer systemd-boot/refind/grub2 if present
        if uefi:
            for t in (BootloaderType.SYSTEMD_BOOT, BootloaderType.REFIND, BootloaderType.GRUB2):
                if t in has:
                    return t

        # BIOS / general: grub2 then grub then syslinux/extlinux then lilo
        for t in (BootloaderType.GRUB2, BootloaderType.GRUB, BootloaderType.SYSLINUX, BootloaderType.EXTLINUX, BootloaderType.LILO):
            if t in has:
                return t

        return bootloaders[0].type

    # ---------------------------
    # Fixes (conservative)
    # ---------------------------

    def apply_kvm_fixes(self, g, *, root_dev: Optional[str] = None) -> BootloaderFixResult:
        """
        Detect + apply safe KVM-friendly tweaks:
          - add serial console args where relevant
          - add GRUB serial terminal settings (only if missing)
          - set loader.conf timeout/editor for systemd-boot (only if missing)
        """
        result = self.detect_bootloaders(g)
        applied: Dict[str, Any] = {}

        for bl in result.bootloaders:
            try:
                if bl.type == BootloaderType.GRUB2:
                    applied[bl.type.value] = self._fix_grub2(g, bl)
                elif bl.type == BootloaderType.GRUB:
                    applied[bl.type.value] = self._fix_grub_legacy(g, bl)
                elif bl.type == BootloaderType.SYSTEMD_BOOT:
                    applied[bl.type.value] = self._fix_systemd_boot(g, bl)
                elif bl.type in (BootloaderType.SYSLINUX, BootloaderType.EXTLINUX):
                    applied[bl.type.value] = self._fix_syslinux_extlinux(g, bl)
                elif bl.type == BootloaderType.REFIND:
                    # rEFInd config varies wildly; we only *report* here.
                    applied[bl.type.value] = {"note": "rEFInd detected; no safe generic edits applied."}
                elif bl.type == BootloaderType.LILO:
                    applied[bl.type.value] = {"note": "LILO detected; avoiding edits (high risk, writes MBR on run)."}
                else:
                    applied[bl.type.value] = {"note": "Unknown bootloader; no edits."}
            except Exception as e:
                result.errors.append(f"{bl.type.value}: {e}")

        result.fixes_applied = applied
        return result

    def _ensure_line_kv(self, lines: List[str], key: str, value: str) -> Tuple[List[str], bool]:
        """
        Ensure a KEY=... line exists. If key exists, do nothing (preserve user config).
        """
        for ln in lines:
            if ln.strip().startswith(key + "="):
                return lines, False
        lines.append(f'{key}="{value}"')
        return lines, True

    def _ensure_grub_serial_blocks(self, content: str) -> Tuple[str, Dict[str, Any]]:
        """
        Add GRUB serial terminal directives if missing.
        Conservative: only add if key not present.
        """
        fixes: Dict[str, Any] = {}
        lines = content.splitlines()

        def has_key(k: str) -> bool:
            return any(l.strip().startswith(k + "=") for l in lines)

        changed = False
        if not has_key("GRUB_TERMINAL"):
            lines.append(self.GRUB_SERIAL_TERMINAL)
            fixes["GRUB_TERMINAL"] = "added"
            changed = True

        if not has_key("GRUB_SERIAL_COMMAND"):
            lines.append(self.GRUB_SERIAL_COMMAND)
            fixes["GRUB_SERIAL_COMMAND"] = "added"
            changed = True

        return ("\n".join(lines) + ("\n" if content.endswith("\n") else "")), fixes if changed else fixes

    def _append_console_args_to_grub_cmdline(self, content: str) -> Tuple[str, bool]:
        """
        Append console args to GRUB_CMDLINE_LINUX* lines if missing.
        Preserves existing args.
        """
        if "GRUB_CMDLINE_LINUX" not in content:
            return content, False

        def repl(m: re.Match[str]) -> str:
            line = m.group(0)
            # line looks like: GRUB_CMDLINE_LINUX="...."
            if "console=ttyS0" in line or "ttyS0," in line:
                return line
            # insert before ending quote (works for "...")
            return re.sub(r'"(\s*)$', f' {self.SERIAL_CONSOLE_ARGS}"\\1', line)

        new = re.sub(r"^(GRUB_CMDLINE_LINUX(?:_DEFAULT)?=.*)$", repl, content, flags=re.M)
        return new, (new != content)

    def _fix_grub2(self, g, bl: BootloaderInfo) -> Dict[str, Any]:
        fixes: Dict[str, Any] = {"changed": False, "files": {}}

        p = "/etc/default/grub"
        if not self._is_file(g, p):
            return {"changed": False, "note": "/etc/default/grub not found"}

        old = self._read_text(g, p)
        new = old

        # Add serial console args to kernel cmdline lines (only if missing)
        new, cmdline_changed = self._append_console_args_to_grub_cmdline(new)
        if cmdline_changed:
            fixes["files"][p] = fixes["files"].get(p, {})
            fixes["files"][p]["cmdline_console"] = "added"
            fixes["changed"] = True

        # Add GRUB_TERMINAL and GRUB_SERIAL_COMMAND if missing
        new2, added = self._ensure_grub_serial_blocks(new)
        if added:
            fixes["files"][p] = fixes["files"].get(p, {})
            fixes["files"][p].update(added)
            fixes["changed"] = True
            new = new2

        if fixes["changed"]:
            self.logger.info(f"bootloader_fixer: GRUB2 tweaks in {p}" + (" (dry-run)" if self.dry_run else ""))
            if not self.dry_run:
                self._write_text(g, p, new)

        return fixes

    def _fix_grub_legacy(self, g, bl: BootloaderInfo) -> Dict[str, Any]:
        fixes: Dict[str, Any] = {"changed": False, "files": {}}
        for p in ("/boot/grub/grub.conf", "/boot/grub/menu.lst"):
            if not self._is_file(g, p):
                continue
            old = self._read_text(g, p)
            if not old:
                continue

            # Append console args to "kernel ..." lines if missing
            changed = False
            out_lines: List[str] = []
            for ln in old.splitlines():
                s = ln.strip()
                if s.startswith("kernel") and "console=ttyS0" not in ln:
                    ln = ln + " " + self.SERIAL_CONSOLE_ARGS
                    changed = True
                out_lines.append(ln)

            if changed:
                fixes["changed"] = True
                fixes["files"][p] = {"kernel_console": "added"}
                self.logger.info(f"bootloader_fixer: GRUB legacy console tweak in {p}" + (" (dry-run)" if self.dry_run else ""))
                if not self.dry_run:
                    self._write_text(g, p, "\n".join(out_lines) + ("\n" if old.endswith("\n") else ""))

        if not fixes["changed"]:
            fixes["note"] = "No legacy GRUB configs updated (none found or already had console)"
        return fixes

    def _fix_systemd_boot(self, g, bl: BootloaderInfo) -> Dict[str, Any]:
        fixes: Dict[str, Any] = {"changed": False, "files": {}}

        # loader.conf: set small timeout + disable editor (only if missing)
        loader_conf = "/boot/loader/loader.conf"
        if self._is_file(g, loader_conf):
            old = self._read_text(g, loader_conf)
            lines = old.splitlines()
            changed = False

            if not any(l.strip().startswith("timeout") for l in lines):
                lines.append("timeout 3")
                changed = True
                fixes["files"][loader_conf] = fixes["files"].get(loader_conf, {})
                fixes["files"][loader_conf]["timeout"] = "added"

            if not any(l.strip().startswith("editor") for l in lines):
                lines.append("editor 0")
                changed = True
                fixes["files"][loader_conf] = fixes["files"].get(loader_conf, {})
                fixes["files"][loader_conf]["editor"] = "disabled"

            if changed:
                fixes["changed"] = True
                self.logger.info(f"bootloader_fixer: systemd-boot tweaks in {loader_conf}" + (" (dry-run)" if self.dry_run else ""))
                if not self.dry_run:
                    self._write_text(g, loader_conf, "\n".join(lines) + ("\n" if old.endswith("\n") else ""))

        # entries: add console args in "options ..." lines if missing
        entries_dir = "/boot/loader/entries"
        if self._is_dir(g, entries_dir):
            try:
                for ent in g.ls(entries_dir):
                    name = U.to_text(ent).strip()
                    if not name.endswith(".conf"):
                        continue
                    p = f"{entries_dir}/{name}"
                    if not self._is_file(g, p):
                        continue
                    old = self._read_text(g, p)
                    if "console=ttyS0" in old or "ttyS0," in old:
                        continue

                    out_lines: List[str] = []
                    changed = False
                    for ln in old.splitlines():
                        if ln.strip().startswith("options "):
                            ln = ln.rstrip() + " " + self.SERIAL_CONSOLE_ARGS
                            changed = True
                        out_lines.append(ln)

                    if changed:
                        fixes["changed"] = True
                        fixes["files"][p] = {"options_console": "added"}
                        self.logger.info(f"bootloader_fixer: systemd-boot entry console tweak in {p}" + (" (dry-run)" if self.dry_run else ""))
                        if not self.dry_run:
                            self._write_text(g, p, "\n".join(out_lines) + ("\n" if old.endswith("\n") else ""))

            except Exception as e:
                fixes["entries_error"] = str(e)

        if not fixes["changed"]:
            fixes.setdefault("note", "No systemd-boot changes needed/applied.")
        return fixes

    def _fix_syslinux_extlinux(self, g, bl: BootloaderInfo) -> Dict[str, Any]:
        fixes: Dict[str, Any] = {"changed": False, "files": {}}

        candidates = [
            "/boot/syslinux/syslinux.cfg",
            "/boot/syslinux.cfg",
            "/boot/extlinux/extlinux.conf",
            "/extlinux/extlinux.conf",
        ]

        for p in candidates:
            if not self._is_file(g, p):
                continue

            old = self._read_text(g, p)
            if not old or ("console=ttyS0" in old or "ttyS0," in old):
                continue

            out_lines: List[str] = []
            changed = False

            for ln in old.splitlines():
                up = ln.upper()
                if ("APPEND" in up or "KERNEL" in up) and "console=" not in ln:
                    # Conservative: append args at end of line
                    ln = ln.rstrip() + " " + self.SERIAL_CONSOLE_ARGS
                    changed = True
                out_lines.append(ln)

            if changed:
                fixes["changed"] = True
                fixes["files"][p] = {"console": "added"}
                self.logger.info(f"bootloader_fixer: syslinux/extlinux console tweak in {p}" + (" (dry-run)" if self.dry_run else ""))
                if not self.dry_run:
                    self._write_text(g, p, "\n".join(out_lines) + ("\n" if old.endswith("\n") else ""))

        if not fixes["changed"]:
            fixes.setdefault("note", "No syslinux/extlinux changes needed/applied.")
        return fixes
