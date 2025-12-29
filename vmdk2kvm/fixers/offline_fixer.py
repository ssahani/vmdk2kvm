# vmdk2kvm/fixers/offline_fixer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import guestfs  # type: ignore
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn, TimeRemainingColumn

from .. import __version__
from ..core.recovery_manager import RecoveryManager
from ..core.utils import U, blinking_progress, guest_has_cmd, guest_ls_glob
from ..core.validation_suite import ValidationSuite
from .fstab_rewriter import (
    IGNORE_MOUNTPOINTS,
    _BYPATH_PREFIX,
    Change,
    FstabMode,
    Ident,
    parse_btrfsvol_spec,
)
from .report_writer import write_report

# Delegated fixers (keep OfflineFSFix “thin”)
from . import network_fixer  # type: ignore
from . import grub_fixer  # type: ignore
from . import windows_fixer  # type: ignore
from .offline_vmware_tools_remover import OfflineVmwareToolsRemover


# ---------------------------------------------------------------------
# VMware removal result wrapper (report-friendly)
# ---------------------------------------------------------------------
@dataclass
class VmwareRemovalResult:
    enabled: bool = True
    removed_paths: List[str] = field(default_factory=list)
    removed_services: List[str] = field(default_factory=list)
    removed_symlinks: List[str] = field(default_factory=list)
    package_hints: List[str] = field(default_factory=list)
    touched_files: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "removed_paths": self.removed_paths,
            "removed_services": self.removed_services,
            "removed_symlinks": self.removed_symlinks,
            "package_hints": self.package_hints,
            "touched_files": self.touched_files,
            "warnings": self.warnings,
            "notes": self.notes,
            "errors": self.errors,
            "counts": {
                "removed_paths": len(self.removed_paths),
                "removed_services": len(self.removed_services),
                "removed_symlinks": len(self.removed_symlinks),
                "package_hints": len(self.package_hints),
                "touched_files": len(self.touched_files),
                "warnings": len(self.warnings),
                "notes": len(self.notes),
                "errors": len(self.errors),
            },
        }


# ---------------------------------------------------------------------
# OfflineFSFix (thin orchestrator)
# ---------------------------------------------------------------------
class OfflineFSFix:
    """
    Offline (libguestfs) fix engine (thin orchestrator):
      - robust root detection + safe mount
      - rewrite fstab/crypttab -> stable IDs
      - network config sanitization (delegated)
      - grub root/device.map + regen (delegated)
      - Windows hooks (delegated)
      - VMware tools removal (mounted-tree remover)
      - report + recovery checkpoints
      - FULL LUKS support: unlock + map + LVM activation + audit
    """

    _BTRFS_COMMON_SUBVOLS = ["@", "@/", "@root", "@rootfs", "@/.snapshots/1/snapshot"]
    _ROOT_HINT_FILES = ["/etc/fstab", "/etc/os-release", "/bin/sh", "/sbin/init"]
    _ROOT_STRONG_HINTS = ["/etc/passwd", "/usr/bin/env", "/var/lib", "/proc"]  # heuristic only

    def __init__(
        self,
        logger: logging.Logger,
        image: Path,
        *,
        dry_run: bool,
        no_backup: bool,
        print_fstab: bool,
        update_grub: bool,
        regen_initramfs: bool,
        fstab_mode: str,
        report_path: Optional[Path],
        remove_vmware_tools: bool = False,
        inject_cloud_init: Optional[Dict[str, Any]] = None,
        recovery_manager: Optional[RecoveryManager] = None,
        resize: Optional[str] = None,
        virtio_drivers_dir: Optional[str] = None,
        # ---- LUKS support (FULLY WIRED) ----
        luks_enable: bool = False,
        luks_passphrase: Optional[str] = None,
        luks_passphrase_env: Optional[str] = None,
        luks_keyfile: Optional[Path] = None,
        luks_mapper_prefix: str = "vmdk2kvm-crypt",
    ):
        self.logger = logger
        self.image = Path(image)
        self.dry_run = bool(dry_run)
        self.no_backup = bool(no_backup)
        self.print_fstab = bool(print_fstab)
        self.update_grub = bool(update_grub)
        self.regen_initramfs = bool(regen_initramfs)
        self.fstab_mode = FstabMode(fstab_mode)
        self.report_path = Path(report_path) if report_path else None
        self.remove_vmware_tools = bool(remove_vmware_tools)
        self.inject_cloud_init_data = inject_cloud_init or {}
        self.recovery_manager = recovery_manager
        self.resize = resize
        self.virtio_drivers_dir = virtio_drivers_dir

        # LUKS configuration
        self.luks_enable = bool(luks_enable)
        self.luks_passphrase = luks_passphrase
        self.luks_passphrase_env = luks_passphrase_env
        self.luks_keyfile = Path(luks_keyfile) if luks_keyfile else None
        self.luks_mapper_prefix = luks_mapper_prefix
        self._luks_opened: Dict[str, str] = {}  # luks_dev -> /dev/mapper/name

        self.inspect_root: Optional[str] = None
        self.root_dev: Optional[str] = None
        self.root_btrfs_subvol: Optional[str] = None

        self.report: Dict[str, Any] = {
            "tool": "vmdk2kvm",
            "version": __version__,
            "image": str(self.image),
            "dry_run": self.dry_run,
            "changes": {},
            "analysis": {},
            "timestamps": {"start": _dt.datetime.now().isoformat()},
        }

    # ---------------------------------------------------------------------
    # guestfs open/close helpers
    # ---------------------------------------------------------------------
    def open(self) -> guestfs.GuestFS:
        g = guestfs.GuestFS(python_return_dict=True)
        if self.logger.isEnabledFor(logging.DEBUG):
            try:
                g.set_trace(1)
            except Exception:
                pass
        # NOTE: read-only when dry_run (prevents accidental writes).
        g.add_drive_opts(str(self.image), readonly=self.dry_run)
        g.launch()
        return g

    @staticmethod
    def _safe_umount_all(g: guestfs.GuestFS) -> None:
        try:
            g.umount_all()
        except Exception:
            pass

    # -----------------------
    # LUKS / LVM
    # -----------------------
    def _read_luks_key_bytes(self) -> Optional[bytes]:
        # Keyfile wins
        try:
            if self.luks_keyfile and self.luks_keyfile.exists():
                return self.luks_keyfile.read_bytes()
        except Exception:
            pass
        pw = self.luks_passphrase
        if (not pw) and self.luks_passphrase_env:
            pw = os.environ.get(self.luks_passphrase_env)
        if pw:
            return pw.encode("utf-8")
        return None

    def _activate_lvm(self, g: guestfs.GuestFS) -> None:
        if not hasattr(g, "vgscan") or not hasattr(g, "vgchange_activate_all"):
            return
        try:
            g.vgscan()
        except Exception:
            return
        try:
            g.vgchange_activate_all(True)
        except Exception:
            try:
                g.vgchange_activate_all(1)
            except Exception:
                pass

    def _unlock_luks_devices(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        audit: Dict[str, Any] = {
            "attempted": False,
            "configured": False,
            "enabled": bool(self.luks_enable),
            "passphrase_env": self.luks_passphrase_env,
            "keyfile": str(self.luks_keyfile) if self.luks_keyfile else None,
            "luks_devices": [],
            "opened": [],
            "skipped": [],
            "errors": [],
        }
        if not self.luks_enable:
            audit["skipped"].append("luks_disabled")
            return audit

        key_bytes = self._read_luks_key_bytes()
        audit["configured"] = bool(key_bytes)
        if not key_bytes:
            audit["skipped"].append("no_key_material_configured")
            return audit
        if not hasattr(g, "cryptsetup_open"):
            audit["errors"].append("guestfs_missing:cryptsetup_open")
            return audit

        try:
            fsmap = g.list_filesystems() or {}
        except Exception as e:
            audit["errors"].append(f"list_filesystems_failed:{e}")
            return audit

        luks_devs = [U.to_text(dev) for dev, fstype in fsmap.items() if U.to_text(fstype) == "crypto_LUKS"]
        audit["luks_devices"] = luks_devs
        if not luks_devs:
            audit["skipped"].append("no_crypto_LUKS_devices_found")
            return audit

        audit["attempted"] = True
        for idx, dev in enumerate(luks_devs, 1):
            if dev in self._luks_opened:
                continue
            name = f"{self.luks_mapper_prefix}{idx}"
            try:
                g.cryptsetup_open(dev, name, key_bytes)
                mapped = f"/dev/mapper/{name}"
                self._luks_opened[dev] = mapped
                audit["opened"].append({"device": dev, "mapped": mapped})
                self.logger.info(f"LUKS: opened {dev} -> {mapped}")
            except Exception as e:
                audit["errors"].append({"device": dev, "error": str(e)})
                self.logger.warning(f"LUKS: failed to open {dev}: {e}")

        if audit["opened"]:
            self._activate_lvm(g)
        return audit

    # ---------------------------------------------------------------------
    # mount logic (safe + robust)
    # ---------------------------------------------------------------------
    def _mount_root_direct(self, g: guestfs.GuestFS, dev: str, subvol: Optional[str]) -> None:
        try:
            if subvol:
                self.root_btrfs_subvol = subvol
                opts = f"subvol={subvol}"
                if self.dry_run:
                    opts = f"ro,{opts}"
                g.mount_options(opts, dev, "/")
            else:
                if self.dry_run:
                    g.mount_ro(dev, "/")
                else:
                    g.mount(dev, "/")
            self.root_dev = dev
            self.logger.info(f"Mounted root at / using {dev}" + (f" (btrfs subvol={subvol})" if subvol else ""))
        except Exception as e:
            raise RuntimeError(f"Failed mounting root {dev} (subvol={subvol}): {e}")

    def _looks_like_root(self, g: guestfs.GuestFS) -> bool:
        # Cheap heuristics: a single hint is enough; more hints = stronger confidence.
        hits = 0
        for p in self._ROOT_HINT_FILES:
            try:
                if g.is_file(p):
                    hits += 1
            except Exception:
                continue
        for p in self._ROOT_STRONG_HINTS:
            try:
                if p.endswith("/"):
                    if g.is_dir(p[:-1]):
                        hits += 1
                else:
                    if g.is_file(p) or g.is_dir(p):
                        hits += 1
            except Exception:
                continue
        return hits >= 2  # avoid false positives on random partitions

    def detect_and_mount_root(self, g: guestfs.GuestFS) -> None:
        try:
            roots = g.inspect_os()
        except Exception:
            roots = []
        if not roots:
            self.logger.warning("inspect_os() found no roots; falling back to brute-force mount.")
            self.mount_root_bruteforce(g)
            return

        root = U.to_text(roots[0])
        self.inspect_root = root

        # Log identity (best-effort)
        product = "Unknown"
        distro = "unknown"
        major = 0
        minor = 0
        try:
            product_val = g.inspect_get_product_name(root)
            if product_val:
                product = U.to_text(product_val)
        except Exception:
            pass
        try:
            distro = U.to_text(g.inspect_get_distro(root))
        except Exception:
            pass
        try:
            major = g.inspect_get_major_version(root)
            minor = g.inspect_get_minor_version(root)
        except Exception:
            pass
        self.logger.info(f"Detected guest: {product} {major}.{minor} (distro={distro})")

        try:
            mp_map = g.inspect_get_mountpoints(root)
        except Exception:
            mp_map = {}

        root_spec = U.to_text(mp_map.get("/", "")).strip()
        if not root_spec:
            self.logger.warning("Inspection did not provide a root (/) devspec; brute-force mounting.")
            self.mount_root_bruteforce(g)
            return

        root_dev = root_spec
        subvol: Optional[str] = None
        if root_spec.startswith("btrfsvol:"):
            root_dev, subvol = parse_btrfsvol_spec(root_spec)
            root_dev = root_dev.strip()

        real: Optional[str] = None
        if root_dev.startswith("/dev/disk/by-"):
            try:
                rp = U.to_text(g.realpath(root_dev)).strip()
                if rp.startswith("/dev/"):
                    real = rp
            except Exception:
                real = None

        # by-path in guest inspection may be meaningless in a different VM topology
        if not real and root_dev.startswith("/dev/disk/by-path/"):
            self.logger.warning("Root spec is by-path and not resolvable; falling back to brute-force root detection.")
            self.mount_root_bruteforce(g)
            return

        if not real and root_dev.startswith("/dev/"):
            real = root_dev

        if not real:
            self.logger.warning("Could not determine root device from inspection; brute-force mounting.")
            self.mount_root_bruteforce(g)
            return

        try:
            self._mount_root_direct(g, real, subvol)
        except Exception as e:
            self.logger.warning(f"{e}; brute-force mounting.")
            self.mount_root_bruteforce(g)

    def _candidate_root_devices(self, g: guestfs.GuestFS) -> List[str]:
        """
        Build a *better-than-list_partitions()* candidate list:
          - after LUKS open + LVM activation, new mountables appear
          - list_filesystems() often includes LV paths
        """
        candidates: List[str] = []

        # 1) partitions (fast + common)
        try:
            candidates.extend([U.to_text(p) for p in (g.list_partitions() or [])])
        except Exception:
            pass

        # 2) mountable filesystems (skip swap + crypto_LUKS)
        try:
            fsmap = g.list_filesystems() or {}
            for dev, fstype in fsmap.items():
                d = U.to_text(dev)
                t = U.to_text(fstype)
                if t in ("swap", "crypto_LUKS"):
                    continue
                if d.startswith("/dev/"):
                    candidates.append(d)
        except Exception:
            pass

        # 3) LVs (if available)
        try:
            if hasattr(g, "lvs"):
                for lv in (g.lvs() or []):
                    d = U.to_text(lv)
                    if d.startswith("/dev/"):
                        candidates.append(d)
        except Exception:
            pass

        # Unique + stable-ish order (preserve first-seen)
        seen: set[str] = set()
        out: List[str] = []
        for d in candidates:
            if d and d not in seen:
                seen.add(d)
                out.append(d)
        return out

    def mount_root_bruteforce(self, g: guestfs.GuestFS) -> None:
        candidates = self._candidate_root_devices(g)
        if not candidates:
            U.die(self.logger, "Failed to list partitions/filesystems for brute-force mount.", 1)

        # Try normal mounts first
        for dev in candidates:
            self._safe_umount_all(g)
            try:
                if self.dry_run:
                    g.mount_ro(dev, "/")
                else:
                    g.mount(dev, "/")
                if self._looks_like_root(g):
                    self.root_dev = dev
                    self.logger.info(f"Fallback root detected at {dev}")
                    return
            except Exception:
                continue

        # Then attempt btrfs common subvols
        for dev in candidates:
            for sv in self._BTRFS_COMMON_SUBVOLS:
                self._safe_umount_all(g)
                try:
                    opts = f"subvol={sv}"
                    if self.dry_run:
                        opts = f"ro,{opts}"
                    g.mount_options(opts, dev, "/")
                    if self._looks_like_root(g):
                        self.root_dev = dev
                        self.root_btrfs_subvol = sv
                        self.logger.info(f"Fallback btrfs root detected at {dev} (subvol={sv})")
                        return
                except Exception:
                    continue

        U.die(self.logger, "Failed to mount root filesystem.", 1)

    # ---------------------------------------------------------------------
    # normalize validation results (bool/dict compatibility)
    # ---------------------------------------------------------------------
    @staticmethod
    def _normalize_validation_results(raw: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        norm: Dict[str, Dict[str, Any]] = {}
        for name, v in (raw or {}).items():
            if isinstance(v, dict):
                passed = bool(v.get("passed", v.get("ok", False)))
                critical = bool(v.get("critical", False))
                details = v.get("details")
                if details is None:
                    details = {k: v[k] for k in v.keys() if k not in ("passed", "ok", "critical")}
                norm[name] = {"passed": passed, "critical": critical, "details": details}
            elif isinstance(v, bool):
                norm[name] = {"passed": v, "critical": False, "details": {}}
            else:
                norm[name] = {"passed": False, "critical": False, "details": {"raw": repr(v)}}
        return norm

    @staticmethod
    def _summarize_validation(norm: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        total = len(norm)
        passed = sum(1 for r in norm.values() if r.get("passed"))
        failed = total - passed
        critical_failed = sum(1 for r in norm.values() if r.get("critical") and not r.get("passed"))
        return {"total": total, "passed": passed, "failed": failed, "critical_failed": critical_failed, "ok": failed == 0}

    # ---------------------------------------------------------------------
    # backup helper
    # ---------------------------------------------------------------------
    def backup_file(self, g: guestfs.GuestFS, path: str) -> None:
        if self.no_backup or self.dry_run:
            return
        try:
            if not g.is_file(path):
                return
        except Exception:
            return
        b = f"{path}.backup.vmdk2kvm.{U.now_ts()}"
        try:
            g.cp(path, b)
            self.logger.debug(f"Backup: {path} -> {b}")
        except Exception as e:
            self.logger.warning(f"Backup failed for {path}: {e}")

    # ---------------------------------------------------------------------
    # spec conversion logic (offline)
    # ---------------------------------------------------------------------
    def convert_spec(self, g: guestfs.GuestFS, spec: str) -> Tuple[str, str]:
        original = spec
        # btrfsvol:/dev/XXX//@/path -> treat stable mapping for underlying dev
        if spec.startswith("btrfsvol:"):
            dev, _sv = parse_btrfsvol_spec(spec)
            spec = dev.strip()
        if Ident.is_stable(spec):
            return original, "already-stable"
        # by-path -> real dev -> stable
        if spec.startswith(_BYPATH_PREFIX):
            mapped: Optional[str] = None
            try:
                rp = U.to_text(g.realpath(spec)).strip()
                if rp.startswith("/dev/"):
                    mapped = rp
            except Exception:
                mapped = None
            # If still not mapped, try your inference helper (root_dev optional)
            if not mapped:
                mapped = Ident.infer_partition_from_bypath(spec, self.root_dev) if self.root_dev else None
            if not mapped:
                return original, "by-path-unresolved"
            blk = Ident.g_blkid_map(g, mapped)
            stable = Ident.choose_stable(blk)
            if stable:
                return stable, f"mapped:{mapped}"
            return original, f"mapped:{mapped} no-id"
        # STABILIZE_ALL: rewrite any /dev/* to stable
        if self.fstab_mode == FstabMode.STABILIZE_ALL and spec.startswith("/dev/"):
            blk = Ident.g_blkid_map(g, spec)
            stable = Ident.choose_stable(blk)
            if stable:
                return stable, f"blkid:{spec}"
            return original, "dev-no-id"
        return original, "unchanged"

    # ---------------------------------------------------------------------
    # fstab rewrite + /tmp sanity
    # ---------------------------------------------------------------------
    def rewrite_fstab(self, g: guestfs.GuestFS) -> Tuple[int, List[Change], Dict[str, Any]]:
        fstab = "/etc/fstab"
        if self.fstab_mode == FstabMode.NOOP:
            self.logger.info("fstab: mode=noop (skipping)")
            return 0, [], {"reason": "noop"}
        try:
            if not g.is_file(fstab):
                self.logger.warning("fstab: /etc/fstab not found; skipping")
                return 0, [], {"reason": "missing"}
        except Exception:
            self.logger.warning("fstab: /etc/fstab check failed; skipping")
            return 0, [], {"reason": "missing"}

        before = U.to_text(g.read_file(fstab))
        if self.print_fstab:
            print("\n--- /etc/fstab (before) ---\n" + before)

        lines = before.splitlines()
        out_lines: List[str] = []
        changes: List[Change] = []
        total = 0
        entries = 0
        bypath = 0

        with Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task("Processing fstab lines", total=len(lines))
            for idx, line in enumerate(lines, 1):
                total += 1
                s = line.strip()
                if not s or s.startswith("#"):
                    out_lines.append(line)
                    progress.update(task, advance=1)
                    continue
                cols = s.split()
                if len(cols) < 4:
                    out_lines.append(line)
                    progress.update(task, advance=1)
                    continue
                spec, mp = cols[0], cols[1]
                if mp in IGNORE_MOUNTPOINTS:
                    out_lines.append(line)
                    progress.update(task, advance=1)
                    continue

                entries += 1
                if spec.startswith(_BYPATH_PREFIX):
                    bypath += 1

                if self.fstab_mode == FstabMode.BYPATH_ONLY and not (
                    spec.startswith(_BYPATH_PREFIX) or spec.startswith("btrfsvol:")
                ):
                    out_lines.append(line)
                    progress.update(task, advance=1)
                    continue

                new_spec, reason = self.convert_spec(g, spec)
                if new_spec != spec:
                    cols[0] = new_spec
                    out_lines.append("\t".join(cols))
                    changes.append(Change(idx, mp, spec, new_spec, reason))
                else:
                    out_lines.append(line)
                progress.update(task, advance=1)

        audit = {
            "total_lines": total,
            "entries": entries,
            "bypath_entries": bypath,
            "changed_entries": len(changes),
        }
        self.logger.info(
            f"fstab scan: total_lines={total} entries={entries} bypath_entries={bypath} changed_entries={len(changes)}"
        )

        # /tmp sanity (common for some minimal images)
        try:
            if not g.is_dir("/tmp"):
                self.logger.info("Fixing /tmp: creating directory inside guest")
                if not self.dry_run:
                    g.mkdir_p("/tmp")
                if not self.dry_run:
                    try:
                        g.chmod(0o1777, "/tmp")
                    except Exception:
                        pass
        except Exception as e:
            self.logger.warning(f"/tmp sanity fix failed: {e}")

        if not changes:
            if self.print_fstab:
                print("\n--- /etc/fstab (after - unchanged) ---\n" + before)
            return 0, [], audit

        for ch in changes:
            self.logger.info(f"fstab line {ch.line_no}: {ch.old} -> {ch.new} ({ch.mountpoint}) [{ch.reason}]")

        after = "\n".join(out_lines) + "\n"
        if self.print_fstab:
            print("\n--- /etc/fstab (after) ---\n" + after)

        if self.dry_run:
            self.logger.info(f"fstab: DRY-RUN: would apply {len(changes)} change(s).")
            return len(changes), changes, audit

        self.backup_file(g, fstab)
        g.write(fstab, after.encode("utf-8"))
        self.logger.info(f"/etc/fstab updated ({len(changes)} changes).")
        return len(changes), changes, audit

    # ---------------------------------------------------------------------
    # crypttab rewrite (LUKS)
    # ---------------------------------------------------------------------
    def rewrite_crypttab(self, g: guestfs.GuestFS) -> int:
        path = "/etc/crypttab"
        try:
            if not g.is_file(path):
                return 0
        except Exception:
            return 0

        before = U.to_text(g.read_file(path))
        out: List[str] = []
        changed = 0
        lines = before.splitlines()

        with Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task("Processing crypttab lines", total=len(lines))
            for line in lines:
                s = line.strip()
                if not s or s.startswith("#"):
                    out.append(line)
                    progress.update(task, advance=1)
                    continue
                cols = s.split()
                if len(cols) < 2:
                    out.append(line)
                    progress.update(task, advance=1)
                    continue
                name, spec = cols[0], cols[1]
                if Ident.is_stable(spec):
                    out.append(line)
                    progress.update(task, advance=1)
                    continue
                if self.fstab_mode == FstabMode.BYPATH_ONLY and not (
                    spec.startswith(_BYPATH_PREFIX) or spec.startswith("btrfsvol:")
                ):
                    out.append(line)
                    progress.update(task, advance=1)
                    continue
                new_spec, reason = self.convert_spec(g, spec)
                if new_spec != spec:
                    cols[1] = new_spec
                    out.append(" ".join(cols))
                    changed += 1
                    self.logger.info(f"crypttab: {name}: {spec} -> {new_spec} [{reason}]")
                else:
                    out.append(line)
                progress.update(task, advance=1)

        if changed == 0:
            return 0
        after = "\n".join(out) + "\n"
        if self.dry_run:
            self.logger.info(f"crypttab: DRY-RUN: would apply {changed} change(s).")
            return changed
        self.backup_file(g, path)
        g.write(path, after.encode("utf-8"))
        self.logger.info(f"/etc/crypttab updated ({changed} changes).")
        return changed

    # ---------------------------------------------------------------------
    # Delegated fixers (explicit wrappers; no monkey-patching)
    # ---------------------------------------------------------------------
    def fix_network_config(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        return network_fixer.fix_network_config(self, g)

    def remove_stale_device_map(self, g: guestfs.GuestFS) -> int:
        return grub_fixer.remove_stale_device_map(self, g)

    def update_grub_root(self, g: guestfs.GuestFS) -> int:
        return grub_fixer.update_grub_root(self, g)

    def regen(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        return grub_fixer.regen(self, g)

    # Windows delegation (fixes your earlier AttributeError: OfflineFSFix has no is_windows)
    def is_windows(self, g: guestfs.GuestFS) -> bool:
        return windows_fixer.is_windows(self, g)

    def windows_bcd_actual_fix(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        return windows_fixer.windows_bcd_actual_fix(self, g)

    def inject_virtio_drivers(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        return windows_fixer.inject_virtio_drivers(self, g)

    # ---------------------------------------------------------------------
    # VMware tools removal (mounted tree remover)
    # ---------------------------------------------------------------------
    def _mount_local_run_threaded(
        self,
        g: guestfs.GuestFS,
        mountpoint: Path,
        *,
        ready_timeout_s: float = 15.0,
    ) -> Tuple[bool, Optional[str], Optional[threading.Thread]]:
        """
        guestfs.mount_local_run() is a blocking FUSE loop.
        The correct pattern is:
          - mount_local(mountpoint)
          - start a background thread that calls mount_local_run()
          - do your host-side file operations against mountpoint
          - call umount_local() to stop the FUSE loop
        """
        err: List[str] = []
        started = False

        try:
            g.mount_local(str(mountpoint))
            started = True
        except Exception as e:
            return False, f"mount_local_failed:{e}", None

        def _runner() -> None:
            try:
                g.mount_local_run()
            except Exception as e:
                # If umount_local() interrupts, guestfs may throw; record but don't panic.
                err.append(str(e))

        t = threading.Thread(target=_runner, name="guestfs-mount-local-run", daemon=True)
        t.start()

        # "ready" heuristic: mountpoint becomes non-empty or at least root dir accessible.
        deadline = time.time() + ready_timeout_s
        while time.time() < deadline:
            try:
                # On success, we should see at least a few root entries soon-ish.
                if mountpoint.exists():
                    # If it's truly mounted, listing shouldn't throw and often isn't empty.
                    _ = list(mountpoint.iterdir())
                    return True, None, t
            except Exception:
                pass
            time.sleep(0.1)

        # Timed out: try to unmount and return error
        try:
            g.umount_local()
        except Exception:
            pass
        return False, "mount_local_ready_timeout", t

    def remove_vmware_tools_func(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        """
        Exposes the mounted guest filesystem via mount_local + a background mount_local_run(),
        then runs OfflineVmwareToolsRemover against that host-visible tree.

        IMPORTANT: mount_local_run() is blocking. This implementation avoids deadlock and
        always attempts umount_local() + cleanup.
        """
        if not self.remove_vmware_tools:
            return {"enabled": False}

        U.banner(self.logger, "VMware tools removal (OFFLINE)")
        res = VmwareRemovalResult(enabled=True)

        if self.dry_run:
            res.notes.append("dry_run: remover will only log; no changes written")
        if self.no_backup:
            res.notes.append("no_backup: remover will not create .bak copies")
        if not self.root_dev:
            res.errors.append("root_not_mounted")
            return res.as_dict()

        mnt = Path(tempfile.mkdtemp(prefix="vmdk2kvm.guestfs.mnt."))
        mounted_local = False
        t: Optional[threading.Thread] = None

        try:
            ok, why, t = self._mount_local_run_threaded(g, mnt)
            if not ok:
                res.errors.append(why or "mount_local_failed")
                return res.as_dict()
            mounted_local = True

            remover = OfflineVmwareToolsRemover(
                logger=self.logger,
                mount_point=mnt,
                dry_run=self.dry_run,
                no_backup=self.no_backup,
            )
            rr = remover.run()

            res.removed_paths = rr.removed_paths
            res.removed_services = rr.removed_services
            res.removed_symlinks = rr.removed_symlinks
            res.package_hints = rr.package_hints
            res.touched_files = rr.touched_files
            res.errors = rr.errors
            if getattr(rr, "warnings", None):
                res.warnings.extend(rr.warnings)

            return res.as_dict()

        finally:
            if mounted_local:
                try:
                    g.umount_local()
                except Exception:
                    pass
            if t:
                # Give the FUSE loop a moment to unwind cleanly
                t.join(timeout=3.0)
            try:
                shutil.rmtree(str(mnt), ignore_errors=True)
            except Exception:
                pass

    # ---------------------------------------------------------------------
    # disk usage analysis
    # ---------------------------------------------------------------------
    def analyze_disk_space(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        try:
            stats = g.statvfs("/")
            total = stats["bsize"] * stats["blocks"]
            free = stats["bsize"] * stats["bfree"]
            used = total - free
            used_pct = (used / total) * 100 if total > 0 else 0.0
            out = {
                "analysis": "success",
                "total_gb": total / (1024**3),
                "used_gb": used / (1024**3),
                "free_gb": free / (1024**3),
                "used_percent": round(used_pct, 1),
                "recommend_resize": used_pct > 80,
                "recommend_cleanup": used_pct > 90,
            }
            if out["recommend_resize"]:
                self.logger.warning(f"Disk usage {used_pct:.1f}% - consider resizing disk")
            if out["recommend_cleanup"]:
                self.logger.warning(f"Disk usage {used_pct:.1f}% - critical, cleanup recommended")
            return out
        except Exception as e:
            self.logger.debug(f"Disk analysis failed: {e}")
            return {"analysis": "failed", "error": str(e)}

    # ---------------------------------------------------------------------
    # validation suite
    # ---------------------------------------------------------------------
    def create_validation_suite(self, g: guestfs.GuestFS) -> ValidationSuite:
        suite = ValidationSuite(self.logger)

        def check_fstab_exists(_context):
            try:
                return g.is_file("/etc/fstab")
            except Exception:
                return False

        def check_boot_files(_context):
            for p in ("/boot", "/boot/grub", "/boot/grub2", "/boot/efi", "/efi"):
                try:
                    if g.is_dir(p):
                        return True
                except Exception:
                    continue
            return False

        def check_kernel_presence(_context):
            kernels = guest_ls_glob(g, "/boot/vmlinuz-*")
            return len(kernels) > 0

        def check_initramfs_tools(_context):
            tools = ["dracut", "update-initramfs", "mkinitcpio"]
            return any(guest_has_cmd(g, t) for t in tools)

        suite.add_check("fstab_exists", check_fstab_exists, critical=True)
        suite.add_check("boot_files_present", check_boot_files, critical=True)
        suite.add_check("kernel_present", check_kernel_presence, critical=True)
        suite.add_check("initramfs_tools", check_initramfs_tools, critical=False)
        return suite

    # ---------------------------------------------------------------------
    # resizing (image-level)
    # ---------------------------------------------------------------------
    def _resize_image_container(self) -> Optional[Dict[str, Any]]:
        if not self.resize:
            return None
        if self.dry_run:
            self.logger.info("DRY-RUN: skipping image resize")
            return {"image_resize": "skipped", "dry_run": True}
        try:
            cp = U.run_cmd(self.logger, ["qemu-img", "info", "--output=json", str(self.image)], capture=True)
            info = json.loads(cp.stdout or "{}")
            current_size = int(info.get("virtual-size", 0))
            if current_size <= 0:
                raise RuntimeError("qemu-img info did not return virtual-size")
            if str(self.resize).startswith("+"):
                add = U.human_to_bytes(str(self.resize)[1:])
                new_size = current_size + add
            else:
                new_size = U.human_to_bytes(str(self.resize))
            if new_size < current_size:
                self.logger.warning("Shrink not supported (requested size < current size)")
                return {"image_resize": "skipped", "reason": "shrink_not_supported"}
            cmd = ["qemu-img", "resize", str(self.image), str(new_size)]
            proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, universal_newlines=True)
            blinking_progress("Resizing image", proc)
            proc.wait()
            if proc.returncode != 0:
                raise subprocess.CalledProcessError(proc.returncode, cmd)
            self.logger.info(f"Resized image to {U.human_bytes(new_size)}")
            return {"image_resize": "ok", "new_size": new_size, "old_size": current_size}
        except Exception as e:
            self.logger.error(f"Image resize failed: {e}")
            return {"image_resize": "failed", "error": str(e)}

    # ---------------------------------------------------------------------
    # report writer
    # ---------------------------------------------------------------------
    def write_report(self) -> None:
        write_report(self)

    # ---------------------------------------------------------------------
    # main run
    # ---------------------------------------------------------------------
    def run(self) -> None:
        U.banner(self.logger, "Offline guest fix (libguestfs)")
        self.logger.info(f"Opening offline image: {self.image}")

        if self.recovery_manager:
            self.recovery_manager.save_checkpoint("start", {"image": str(self.image)})

        if self.resize:
            self.report["analysis"]["image_resize"] = self._resize_image_container()

        g = self.open()
        try:
            # 1) LUKS (optional but wired)
            luks_audit = self._unlock_luks_devices(g)
            self.report["analysis"]["luks"] = luks_audit
            self.logger.info(f"LUKS audit: {U.json_dump(luks_audit)}")

            # 2) LVM activation (safe even if no LVM)
            self._activate_lvm(g)

            # 3) Mount root
            self.detect_and_mount_root(g)

            # identity into report
            try:
                osr = U.to_text(g.read_file("/etc/os-release")) if g.is_file("/etc/os-release") else ""
            except Exception:
                osr = ""
            self.report["analysis"]["guest"] = {
                "inspect_root": self.inspect_root,
                "root_dev": self.root_dev,
                "root_btrfs_subvol": self.root_btrfs_subvol,
                "os_release": osr,
            }

            # validation (bool/dict compatible)
            suite = self.create_validation_suite(g)
            ctx = {"image": str(self.image), "root_dev": self.root_dev, "subvol": self.root_btrfs_subvol}
            raw = suite.run_all(ctx)
            norm = self._normalize_validation_results(raw)
            summary = self._summarize_validation(norm)
            self.report["validation"] = {"results": norm, "summary": summary}

            critical_failures = [name for name, r in norm.items() if r.get("critical") and not r.get("passed")]
            if critical_failures:
                self.logger.warning(f"Critical validation failures: {critical_failures}")

            if self.recovery_manager:
                self.recovery_manager.save_checkpoint(
                    "mounted",
                    {
                        "root_dev": self.root_dev,
                        "root_btrfs_subvol": self.root_btrfs_subvol,
                        "validation": self.report["validation"],
                    },
                )

            # fixes
            c_fstab, fstab_changes, fstab_audit = self.rewrite_fstab(g)
            c_crypt = self.rewrite_crypttab(g)
            network_audit = self.fix_network_config(g)
            c_devmap = self.remove_stale_device_map(g)
            c_grub = self.update_grub_root(g)

            # keep your existing mdraid_check()/inject_cloud_init()/resize_disk() if they exist
            mdraid = getattr(self, "mdraid_check")(g) if hasattr(self, "mdraid_check") else {"present": False}
            cloud_init = (
                getattr(self, "inject_cloud_init")(g) if hasattr(self, "inject_cloud_init") else {"enabled": False}
            )

            # Windows hooks (delegated)
            win = self.windows_bcd_actual_fix(g)
            virtio = self.inject_virtio_drivers(g)

            disk = self.analyze_disk_space(g)
            vmware_removal = self.remove_vmware_tools_func(g)
            regen_info = self.regen(g)

            if not self.dry_run:
                try:
                    g.sync()
                except Exception:
                    pass

            self._safe_umount_all(g)

            # report aggregation
            self.report["changes"] = {
                "fstab": c_fstab,
                "crypttab": c_crypt,
                "network": network_audit,
                "grub_root": c_grub,
                "grub_device_map_removed": c_devmap,
                "vmware_tools_removed": vmware_removal,
                "cloud_init_injected": cloud_init,
            }
            self.report["analysis"]["fstab_audit"] = fstab_audit
            self.report["analysis"]["fstab_changes"] = [vars(x) for x in fstab_changes]
            self.report["analysis"]["mdraid"] = mdraid
            self.report["analysis"]["windows"] = win
            self.report["analysis"]["virtio"] = virtio
            self.report["analysis"]["disk"] = disk
            self.report["analysis"]["regen"] = regen_info
            self.report["timestamps"]["end"] = _dt.datetime.now().isoformat()

        finally:
            try:
                # extra safety: if anything mounted, try to umount all
                self._safe_umount_all(g)
            except Exception:
                pass
            try:
                g.close()
            except Exception:
                pass

        self.write_report()
