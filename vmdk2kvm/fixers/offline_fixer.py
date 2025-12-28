from __future__ import annotations
import datetime as _dt
import json
import logging
import os
import re
import shlex
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from .. import __version__

from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn, TransferSpeedColumn, SpinnerColumn

import guestfs  # type: ignore

from ..core.utils import U, guest_has_cmd, guest_ls_glob
from ..core.recovery_manager import RecoveryManager
from ..core.validation_suite import ValidationSuite
from .fstab_rewriter import Ident, FstabMode, Change, parse_btrfsvol_spec, IGNORE_MOUNTPOINTS, _BYPATH_PREFIX
import tempfile
from ..config.config_loader import YAML_AVAILABLE, yaml
from ..core.utils import blinking_progress
class OfflineFSFix:
    """
    Offline (libguestfs) fix engine:
    - mount root safely (never by-path)
    - rewrite fstab (and crypttab) to stable identifiers
    - btrfs canonicalization: strip btrfsvol: wrappers, ensure /tmp exists, fix virt-v2v btrfsvol warnings
    - grub root= best effort + remove stale device.map
    - mdadm check hooks
    - initramfs/grub regen plan with dracut rescue
    - Windows BCD fixes
    - Network configuration updates
    - VMware Tools removal
    - Disk space analysis
    - Cloud-init integration
    """
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
    ):
        self.logger = logger
        self.image = image
        self.dry_run = dry_run
        self.no_backup = no_backup
        self.print_fstab = print_fstab
        self.update_grub = update_grub
        self.regen_initramfs = regen_initramfs
        self.fstab_mode = FstabMode(fstab_mode)
        self.report_path = report_path
        self.remove_vmware_tools = remove_vmware_tools
        self.inject_cloud_init_data = inject_cloud_init or {}
        self.recovery_manager = recovery_manager
        self.resize = resize
        self.virtio_drivers_dir = virtio_drivers_dir
        self.inspect_root: Optional[str] = None
        self.root_dev: Optional[str] = None
        self.root_btrfs_subvol: Optional[str] = None # NEW: subvol (if btrfs)
        self.report: Dict[str, Any] = {
            "image": str(image),
            "dry_run": dry_run,
            "changes": {},
            "analysis": {},
            "timestamps": {"start": _dt.datetime.now().isoformat()},
        }
    # ---------------------------
    # guestfs open + mount logic
    # ---------------------------
    def open(self) -> guestfs.GuestFS:
        g = guestfs.GuestFS(python_return_dict=True)
        if self.logger.isEnabledFor(logging.DEBUG):
            try:
                g.set_trace(1)
            except Exception:
                pass
        g.add_drive_opts(str(self.image), readonly=self.dry_run)
        g.launch()
        return g
    def _mount_root_direct(self, g: guestfs.GuestFS, dev: str, subvol: Optional[str]) -> None:
        """
        Mount root device directly at '/', with optional btrfs subvol= handling.
        """
        try:
            if subvol:
                self.root_btrfs_subvol = subvol
                opts = f"subvol={subvol}"
                if self.dry_run:
                    opts = f"ro,{opts}"
                # mount_options works for btrfs subvol and remains safe for other fs if subvol is None
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
    def detect_and_mount_root(self, g: guestfs.GuestFS) -> None:
        """
        Try inspect_os first; but DO NOT mount using inspect mountpoints if they contain by-path.
        Instead:
        - find root device from inspect
        - mount that root device directly on "/"
        - if inspection hints btrfsvol:..., use subvol= mount option
        """
        roots = g.inspect_os()
        if not roots:
            self.logger.warning("inspect_os() found no roots; falling back to brute-force mount.")
            self.mount_root_bruteforce(g)
            return
        root = U.to_text(roots[0])
        self.inspect_root = root
        product = U.to_text(g.inspect_get_product_name(root)) if g.inspect_get_product_name(root) else "Unknown"
        distro = U.to_text(g.inspect_get_distro(root))
        major = g.inspect_get_major_version(root)
        minor = g.inspect_get_minor_version(root)
        self.logger.info(f"Detected guest: {product} {major}.{minor} (distro={distro})")
        mp_map = g.inspect_get_mountpoints(root) # dict: mountpoint -> devspec
        root_spec = U.to_text(mp_map.get("/", "")).strip()
        # NEW: btrfs subvol mount option logic for root
        root_dev = root_spec
        subvol: Optional[str] = None
        if root_spec.startswith("btrfsvol:"):
            root_dev, subvol = parse_btrfsvol_spec(root_spec)
        # If by-path, try to resolve. If fails, brute-force.
        real = None
        if root_dev.startswith("/dev/disk/by-"):
            try:
                rp = U.to_text(g.realpath(root_dev)).strip()
                if rp.startswith("/dev/"):
                    real = rp
            except Exception:
                real = None
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
    def mount_root_bruteforce(self, g: guestfs.GuestFS) -> None:
        parts = [U.to_text(p) for p in g.list_partitions()]
        # Try plain mounts first, then btrfs common subvols if needed
        for dev in parts:
            try:
                if self.dry_run:
                    g.mount_ro(dev, "/")
                else:
                    g.mount(dev, "/")
                if g.is_file("/etc/fstab"):
                    self.root_dev = dev
                    self.logger.info(f"Fallback root detected at {dev}")
                    return
                g.umount("/")
            except Exception:
                try:
                    g.umount("/")
                except Exception:
                    pass
        # btrfs fallback subvol probe (only if we still didn't mount)
        common_subvols = ["@", "@/", "@root", "@rootfs", "@/.snapshots/1/snapshot"]
        for dev in parts:
            for sv in common_subvols:
                try:
                    opts = f"subvol={sv}"
                    if self.dry_run:
                        opts = f"ro,{opts}"
                    g.mount_options(opts, dev, "/")
                    if g.is_file("/etc/fstab"):
                        self.root_dev = dev
                        self.root_btrfs_subvol = sv
                        self.logger.info(f"Fallback btrfs root detected at {dev} (subvol={sv})")
                        return
                    g.umount("/")
                except Exception:
                    try:
                        g.umount("/")
                    except Exception:
                        pass
        U.die(self.logger, "Failed to mount root filesystem.", 1)
    # ---------------------------
    # backup helper
    # ---------------------------
    def backup_file(self, g: guestfs.GuestFS, path: str) -> None:
        if self.no_backup or self.dry_run:
            return
        if not g.is_file(path):
            return
        b = f"{path}.backup.vmdk2kvm.{U.now_ts()}"
        try:
            g.cp(path, b)
            self.logger.debug(f"Backup: {path} -> {b}")
        except Exception as e:
            self.logger.warning(f"Backup failed for {path}: {e}")
    # ---------------------------
    # spec conversion logic (offline)
    # ---------------------------
    def convert_spec(self, g: guestfs.GuestFS, spec: str) -> Tuple[str, str]:
        original = spec
        # 1) strip btrfsvol: wrapper
        if spec.startswith("btrfsvol:"):
            dev, _sv = parse_btrfsvol_spec(spec)
            spec = dev.strip()
        # 2) already stable?
        if Ident.is_stable(spec):
            return original, "already-stable"
        # 3) by-path mapping
        if spec.startswith(_BYPATH_PREFIX):
            mapped = None
            try:
                rp = U.to_text(g.realpath(spec)).strip()
                if rp.startswith("/dev/"):
                    mapped = rp
            except Exception:
                mapped = None
            if not mapped:
                mapped = Ident.infer_partition_from_bypath(spec, self.root_dev)
            if not mapped:
                return original, "by-path-unresolved"
            blk = Ident.g_blkid_map(g, mapped)
            stable = Ident.choose_stable(blk)
            if stable:
                return stable, f"mapped:{mapped}"
            return original, f"mapped:{mapped} no-id"
        # 4) optionally stabilize direct /dev nodes
        if self.fstab_mode == FstabMode.STABILIZE_ALL and spec.startswith("/dev/"):
            blk = Ident.g_blkid_map(g, spec)
            stable = Ident.choose_stable(blk)
            if stable:
                return stable, f"blkid:{spec}"
            return original, "dev-no-id"
        return original, "unchanged"
    # ---------------------------
    # fstab rewrite + canonicalize btrfsvol warnings + ensure /tmp
    # ---------------------------
    def rewrite_fstab(self, g: guestfs.GuestFS) -> Tuple[int, List[Change], Dict[str, Any]]:
        fstab = "/etc/fstab"
        if self.fstab_mode == FstabMode.NOOP:
            self.logger.info("fstab: mode=noop (skipping)")
            return 0, [], {"reason": "noop"}
        if not g.is_file(fstab):
            self.logger.warning("fstab: /etc/fstab not found; skipping")
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
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
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
                if self.fstab_mode == FstabMode.BYPATH_ONLY and not spec.startswith(_BYPATH_PREFIX) and not spec.startswith("btrfsvol:"):
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
        # Ensure /tmp exists; fixes random-seed stage and general sanity
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
        if len(changes) == 0:
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
    # ---------------------------
    # crypttab rewrite (LUKS)
    # ---------------------------
    def rewrite_crypttab(self, g: guestfs.GuestFS) -> int:
        path = "/etc/crypttab"
        if not g.is_file(path):
            return 0
        before = U.to_text(g.read_file(path))
        out: List[str] = []
        changed = 0
        lines = before.splitlines()
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
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
                if self.fstab_mode == FstabMode.BYPATH_ONLY and not spec.startswith(_BYPATH_PREFIX) and not spec.startswith("btrfsvol:"):
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
    # ---------------------------
    # mdraid support (mdadm check/update)
    # ---------------------------
    def mdraid_check(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        candidates = ["/etc/mdadm.conf", "/etc/mdadm/mdadm.conf"]
        found = None
        for p in candidates:
            if g.is_file(p):
                found = p
                break
        if not found:
            return {"present": False}
        txt = U.to_text(g.read_file(found))
        arrays = [ln for ln in txt.splitlines() if ln.strip().startswith("ARRAY")]
        info = {"present": True, "path": found, "array_lines": len(arrays)}
        if len(arrays) == 0:
            self.logger.warning(f"mdraid: {found} has no ARRAY lines; guest may rely on initramfs auto-assembly.")
        else:
            self.logger.info(f"mdraid: {found}: ARRAY lines={len(arrays)}")
        return info
    # ---------------------------
    # GRUB root= update + device.map cleanup
    # ---------------------------
    def remove_stale_device_map(self, g: guestfs.GuestFS) -> int:
        removed = 0
        for p in ["/boot/grub2/device.map", "/boot/grub/device.map", "/etc/grub2-device.map"]:
            try:
                if g.is_file(p):
                    txt = U.to_text(g.read_file(p))
                    if "sda" in txt:
                        self.logger.info(f"GRUB: removing stale device.map: {p}")
                        removed += 1
                        if not self.dry_run:
                            g.rm_f(p)
            except Exception:
                pass
        return removed
    def update_grub_root(self, g: guestfs.GuestFS) -> int:
        if not self.update_grub:
            return 0
        targets = ["/boot/grub2/grub.cfg", "/boot/grub/grub.cfg", "/etc/default/grub"]
        changed = 0
        if not self.root_dev:
            return 0
        blk = Ident.g_blkid_map(g, self.root_dev)
        stable = Ident.choose_stable(blk)
        if not stable:
            self.logger.warning("GRUB: could not find stable ID for root device; skipping root= update.")
            return 0
        new_root = f"root={stable}"
        self.logger.info(f"GRUB: setting {new_root}")
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
            task = progress.add_task("Updating GRUB files", total=len(targets))
            for p in targets:
                if not g.is_file(p):
                    progress.update(task, advance=1)
                    continue
                try:
                    old = U.to_text(g.read_file(p))
                    new = re.sub(r"\broot=\S+", new_root, old)
                    if new == old:
                        progress.update(task, advance=1)
                        continue
                    self.logger.info(f"Updated root= in {p}" + (" (dry-run)" if self.dry_run else ""))
                    changed += 1
                    if self.dry_run:
                        progress.update(task, advance=1)
                        continue
                    self.backup_file(g, p)
                    g.write(p, new.encode("utf-8"))
                except Exception as e:
                    self.logger.warning(f"Failed updating {p}: {e}")
                progress.update(task, advance=1)
        return changed
    # ---------------------------
    # Initramfs / grub regen (distro-aware + dracut fix)
    # ---------------------------
    def regen(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        if not self.regen_initramfs:
            return {"enabled": False}
        distro = ""
        version = ""
        if self.inspect_root:
            try:
                distro = (U.to_text(g.inspect_get_distro(self.inspect_root)) or "").lower()
                product = U.to_text(g.inspect_get_product_name(self.inspect_root)) or ""
                version = product.split()[-1] if product else ""
            except Exception:
                pass
        family = "unknown"
        if "opensuse" in distro or "sles" in distro or "suse" in distro:
            family = "suse/rhel"
        elif distro in ("fedora", "rhel", "centos", "rocky", "almalinux", "photon", "mariner"):
            family = "suse/rhel"
        elif distro in ("debian", "ubuntu"):
            family = "debian"
        elif distro == "arch":
            family = "arch"
        info: Dict[str, Any] = {"enabled": True, "distro": distro, "version": version, "family": family}
        if self.dry_run:
            self.logger.info("DRY-RUN: skipping initramfs/grub regeneration.")
            info["dry_run"] = True
            return info
        self.logger.info(f"Initramfs plan: distro={distro} version={version} family={family} supported=True")
        self.logger.info("🛠️ Regenerating initramfs and GRUB...")
        def run_guest(cmd: List[str]) -> Tuple[bool, str]:
            try:
                self.logger.info(f"Running (guestfs): {' '.join(cmd)}")
                out = g.command(cmd)
                return True, U.to_text(out)
            except Exception as e:
                return False, str(e)
        guest_kvers: List[str] = []
        try:
            if g.is_dir("/lib/modules"):
                guest_kvers = [U.to_text(x) for x in g.ls("/lib/modules") if U.to_text(x).strip()]
        except Exception:
            guest_kvers = []
        if family == "debian":
            ok, err = run_guest(["update-initramfs", "-u", "-k", "all"])
            if not ok:
                self.logger.warning(f"Initramfs cmd failed: update-initramfs -u -k all: {err}")
                run_guest(["update-initramfs", "-u"])
        elif family == "arch":
            ok, err = run_guest(["mkinitcpio", "-P"])
            if not ok:
                self.logger.warning(f"Initramfs cmd failed: mkinitcpio -P: {err}")
        else:
            ok, err = run_guest(["dracut", "-f"])
            if not ok and ("Cannot find module directory /lib/modules/" in err or "and --no-kernel" in err):
                self.logger.warning(f"Initramfs cmd failed: dracut -f: {err}")
                if guest_kvers:
                    kver = sorted(guest_kvers)[-1]
                    self.logger.info(f"dracut workaround: using guest kver={kver}")
                    ok2, err2 = run_guest(["dracut", "-f", "--kver", kver])
                    if not ok2:
                        self.logger.warning(f"Initramfs cmd failed: dracut -f --kver {kver}: {err2}")
                run_guest(["dracut", "-f", "--regenerate-all"])
        if family == "debian":
            run_guest(["update-grub"])
        else:
            run_guest(["grub2-mkconfig", "-o", "/boot/grub2/grub.cfg"])
            if g.is_file("/boot/grub/grub.cfg"):
                run_guest(["grub-mkconfig", "-o", "/boot/grub/grub.cfg"])
        info["guest_kernels"] = guest_kvers
        return info
    # ---------------------------
    # Windows BCD Actual Implementation
    # ---------------------------
    def is_windows(self, g: guestfs.GuestFS) -> bool:
        if not self.inspect_root:
            return False
        try:
            t = U.to_text(g.inspect_get_type(self.inspect_root))
            return t.lower() == "windows"
        except Exception:
            return False
    def windows_bcd_actual_fix(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        if not self.is_windows(g):
            return {"windows": False}
        self.logger.info("Windows detected - attempting BCD fixes")
        windows_paths = ["/Windows", "/winnt", "/WINDOWS"]
        windows_root = None
        for path in windows_paths:
            if g.is_dir(path):
                windows_root = path
                break
        if not windows_root:
            return {"windows": True, "bcd": "no_windows_directory"}
        bcd_path = f"{windows_root}/Boot/BCD"
        if not g.is_file(bcd_path):
            self.logger.warning(f"BCD store not found at {bcd_path}")
            return {"windows": True, "bcd": "no_bcd_store"}
        self.logger.info(f"Windows BCD store found: {bcd_path}")
        try:
            bcd_size = g.filesize(bcd_path)
            self.logger.info(f"BCD store size: {bcd_size} bytes")
            if not self.dry_run:
                backup_path = f"{bcd_path}.backup.vmdk2kvm.{U.now_ts()}"
                g.cp(bcd_path, backup_path)
                self.logger.info(f"BCD backup created: {backup_path}")
        except Exception as e:
            self.logger.warning(f"BCD inspection failed: {e}")
        return {"windows": True, "bcd": "basic_checks_completed", "path": bcd_path}
    # ---------------------------
    # Virtio Injection for Windows
    # ---------------------------
    def inject_virtio_drivers(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        if not self.virtio_drivers_dir or not Path(self.virtio_drivers_dir).exists():
            return {"injected": False, "reason": "no_dir"}
        if not self.is_windows(g):
            return {"injected": False, "reason": "not_windows"}
        if not self.inspect_root:
            return {"injected": False, "reason": "no_inspect"}
        major = g.inspect_get_major_version(self.inspect_root)
        arch = g.inspect_get_arch(self.inspect_root)
        if arch not in ('x86_64', 'i386'):
            arch_dir = 'amd64' if arch == 'x86_64' else 'x86'
        else:
            arch_dir = arch.lower()
        win_ver_map = {
            5: 'xp',
            6: 'win7',
            10: 'win10',
        }
        win_ver = win_ver_map.get(major, 'win10')
        driver_rel = f"viostor/{win_ver}/{arch_dir}/viostor.sys"
        driver_src = Path(self.virtio_drivers_dir) / driver_rel
        if not driver_src.exists():
            self.logger.warning(f"Virtio driver not found: {driver_src}")
            return {"injected": False, "reason": "no_driver"}
        driver_dst = "/Windows/System32/drivers/viostor.sys"
        if not self.dry_run:
            g.upload(str(driver_src), driver_dst)
        self.logger.info(f"Injected {driver_src} to {driver_dst}")
        hive_guest = "/Windows/System32/config/SYSTEM"
        if not g.is_file(hive_guest):
            self.logger.warning("SYSTEM hive not found")
            return {"injected": True, "registry": False}
        with tempfile.TemporaryDirectory() as td:
            local_hive = Path(td) / "SYSTEM"
            g.download(hive_guest, str(local_hive))
            write = 1 if not self.dry_run else 0
            try:
                h = g.hivex_open(str(local_hive), write=write)
                root = h.root()
                select = h.node_get_child(root, "Select")
                if select is None:
                    raise RuntimeError("No Select node")
                val = h.node_get_value(select, "Current")
                if val is None:
                    raise RuntimeError("No Current value")
                current = int.from_bytes(val["value"], "little")
                cs_name = f"ControlSet{current:03d}"
                cs = h.node_get_child(root, cs_name)
                if cs is None:
                    raise RuntimeError(f"No {cs_name}")
                services = h.node_get_child(cs, "Services") or h.node_add_child(cs, "Services")
                viostor_node = h.node_get_child(services, "viostor") or h.node_add_child(services, "viostor")
                h.node_set_value(viostor_node, dict(key="Type", t=4, value=(1).to_bytes(4, 'little')))
                h.node_set_value(viostor_node, dict(key="Start", t=4, value=(0).to_bytes(4, 'little')))
                h.node_set_value(viostor_node, dict(key="ErrorControl", t=4, value=(1).to_bytes(4, 'little')))
                image_path = "system32\\drivers\\viostor.sys"
                h.node_set_value(viostor_node, dict(key="ImagePath", t=1, value=image_path.encode('utf-8') + b'\0'))
                control = h.node_get_child(cs, "Control") or h.node_add_child(cs, "Control")
                cdd = h.node_get_child(control, "CriticalDeviceDatabase") or h.node_add_child(control, "CriticalDeviceDatabase")
                pci_id = "pci#ven_1af4&dev_1001&subsys_00081af4"
                pci_node = h.node_get_child(cdd, pci_id) or h.node_add_child(cdd, pci_id)
                h.node_set_value(pci_node, dict(key="Service", t=1, value=b"viostor\0"))
                class_guid = "{4D36E97B-E325-11CE-BFC1-08002BE10318}"
                h.node_set_value(pci_node, dict(key="ClassGUID", t=1, value=class_guid.encode('utf-8') + b'\0'))
                pci_id2 = "pci#ven_1af4&dev_1042&subsys_00081af4"
                pci_node2 = h.node_get_child(cdd, pci_id2) or h.node_add_child(cdd, pci_id2)
                h.node_set_value(pci_node2, dict(key="Service", t=1, value=b"viostor\0"))
                h.node_set_value(pci_node2, dict(key="ClassGUID", t=1, value=class_guid.encode('utf-8') + b'\0'))
                h.hivex_commit(None)
                h.hivex_close()
                if not self.dry_run:
                    g.upload(str(local_hive), hive_guest)
                self.logger.info("Virtio registry entries added")
                return {"injected": True, "registry": True}
            except Exception as e:
                self.logger.warning(f"Registry edit failed: {e}")
                return {"injected": True, "registry": False, "error": str(e)}
    # ---------------------------
    # Network Configuration Fixes
    # ---------------------------
    def fix_network_config(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        changes = []
        network_patterns = [
            "/etc/sysconfig/network-scripts/ifcfg-*",
            "/etc/netplan/*.yaml",
            "/etc/netplan/*.yml",
            "/etc/network/interfaces",
            "/etc/systemd/network/*.network",
            "/etc/systemd/network/*.netdev",
        ]
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
            task = progress.add_task("Fixing network configs", total=len(network_patterns))
            for pattern in network_patterns:
                try:
                    files = guest_ls_glob(g, pattern) # REPLACED g.glob()
                    for file_path in files:
                        if not (g.is_file(file_path)):
                            continue
                        content = U.to_text(g.read_file(file_path))
                        original_content = content
                        vmware_patterns = [
                            r"DEVICE=vmxnet3\b",
                            r"DEVICE=e1000\b",
                            r"TYPE=Ethernet.*?vmxnet3",
                            r"driver:\s*vmxnet3",
                            r"driver:\s*e1000",
                        ]
                        modified = False
                        for pat in vmware_patterns:
                            if re.search(pat, content, re.IGNORECASE):
                                content = re.sub(pat, "", content, flags=re.IGNORECASE)
                                modified = True
                        if "HWADDR" in content:
                            content = re.sub(
                                r'^(HWADDR=.*)$',
                                r'# \1 # Commented by vmdk2kvm',
                                content,
                                flags=re.MULTILINE
                            )
                            modified = True
                        if "NAME=" in content and "eth" not in content:
                            content = re.sub(
                                r'^(NAME=.*)$',
                                r'# \1 # Renamed for KVM\nNAME=eth0',
                                content,
                                flags=re.MULTILINE
                            )
                            modified = True
                        if 'netplan' in file_path and YAML_AVAILABLE:
                            try:
                                data = yaml.safe_load(content)
                                if 'network' in data and 'ethernets' in data['network']:
                                    for iface, cfg in data['network']['ethernets'].items():
                                        if 'match' in cfg:
                                            del cfg['match']
                                        cfg['set-name'] = 'eth0'
                                        cfg['dhcp4'] = True
                                    content = yaml.safe_dump(data)
                                    modified = True
                            except Exception as e:
                                self.logger.debug(f"Netplan YAML fix failed: {e}")
                        if modified and not self.dry_run:
                            self.backup_file(g, file_path)
                            g.write(file_path, content.encode("utf-8"))
                            changes.append(file_path)
                            self.logger.info(f"Updated network config: {file_path}")
                        elif modified:
                            self.logger.info(f"DRY-RUN: would update network config: {file_path}")
                except Exception as e:
                    self.logger.debug(f"Network config check for {pattern} failed: {e}")
                progress.update(task, advance=1)
        return {"updated_files": changes, "count": len(changes)}
    # ---------------------------
    # VMware Tools Removal
    # ---------------------------
    def remove_vmware_tools_func(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        if not self.remove_vmware_tools:
            return {"removed": False, "reason": "disabled"}
        removed_packages = []
        stopped_services = []
        vmware_packages = {
            "all": ["open-vm-tools", "vmware-tools"],
            "debian": ["open-vm-tools-desktop", "vmware-tools-desktop"],
            "redhat": ["open-vm-tools-desktop"],
            "suse": ["open-vm-tools-desktop"],
        }
        package_managers = {
            "dpkg": ["dpkg", "-l"],
            "rpm": ["rpm", "-qa"],
            "pacman": ["pacman", "-Q"],
        }
        installed_packages: List[str] = []
        for _pm, cmd in package_managers.items():
            try:
                if guest_has_cmd(g, cmd[0]): # REPLACED g.available()
                    output = U.to_text(g.command(cmd))
                    installed_packages = output.splitlines()
                    break
            except Exception:
                continue
        packages_to_remove = []
        for pkg_list in vmware_packages.values():
            for pkg in pkg_list:
                for installed in installed_packages:
                    if pkg.lower() in installed.lower():
                        packages_to_remove.append(pkg)
                        break
        if not packages_to_remove:
            return {"removed": False, "reason": "no_vmware_packages"}
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
            task = progress.add_task("Removing VMware packages", total=len(packages_to_remove))
            for pkg in packages_to_remove:
                if self.dry_run:
                    self.logger.info(f"DRY-RUN: would remove VMware package: {pkg}")
                    removed_packages.append(pkg)
                    progress.update(task, advance=1)
                    continue
                try:
                    for pm_cmd in (["apt-get", "remove", "-y"], ["yum", "remove", "-y"], ["zypper", "remove", "-y"]):
                        if guest_has_cmd(g, pm_cmd[0]): # REPLACED g.available()
                            g.command(pm_cmd + [pkg])
                            removed_packages.append(pkg)
                            self.logger.info(f"Removed VMware package: {pkg}")
                            break
                except Exception as e:
                    self.logger.warning(f"Failed to remove package {pkg}: {e}")
                progress.update(task, advance=1)
        service_files = ["/etc/init.d/vmware-tools", "/etc/systemd/system/vmware-tools.service"]
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
            task = progress.add_task("Removing VMware services", total=len(service_files))
            for service in service_files:
                if g.is_file(service):
                    if self.dry_run:
                        self.logger.info(f"DRY-RUN: would disable VMware service: {service}")
                        stopped_services.append(service)
                    else:
                        try:
                            if guest_has_cmd(g, "systemctl"):
                                g.command(["systemctl", "disable", os.path.basename(service)])
                                stopped_services.append(service)
                                self.logger.info(f"Disabled VMware service: {service}")
                            else:
                                raise RuntimeError("systemctl not present in guest appliance")
                        except Exception:
                            try:
                                g.rm_f(service)
                                stopped_services.append(service)
                                self.logger.info(f"Removed VMware service: {service}")
                            except Exception as e:
                                self.logger.warning(f"Failed to remove service {service}: {e}")
                progress.update(task, advance=1)
        return {
            "removed": len(removed_packages) > 0 or len(stopped_services) > 0,
            "packages": removed_packages,
            "services": stopped_services
        }
    # ---------------------------
    # Disk Space Analysis
    # ---------------------------
    def analyze_disk_space(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        try:
            stats = None
            try:
                stats = g.statvfs("/")
            except Exception:
                pass
            if not stats:
                return {"analysis": "failed", "error": "could_not_get_stats"}
            total = stats["bsize"] * stats["blocks"]
            free = stats["bsize"] * stats["bfree"]
            used = total - free
            used_pct = (used / total) * 100 if total > 0 else 0
            analysis = {
                "total_gb": total / (1024**3),
                "used_gb": used / (1024**3),
                "free_gb": free / (1024**3),
                "used_percent": round(used_pct, 1),
                "recommend_resize": used_pct > 80,
                "recommend_cleanup": used_pct > 90
            }
            if analysis["recommend_resize"]:
                self.logger.warning(f"Disk usage {used_pct:.1f}% - consider resizing disk")
                if analysis["recommend_cleanup"]:
                    self.logger.warning(f"Disk usage {used_pct:.1f}% - critical, cleanup recommended")
            return {"analysis": "success", **analysis}
        except Exception as e:
            self.logger.debug(f"Disk analysis failed: {e}")
            return {"analysis": "failed", "error": str(e)}
    # ---------------------------
    # Cloud-Init Integration
    # ---------------------------
    def inject_cloud_init(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        if not self.inject_cloud_init_data:
            return {"injected": False, "reason": "no_data"}
        cloud_dir = "/etc/cloud"
        cloud_installed = g.is_dir(cloud_dir)
        if not cloud_installed:
            self.logger.info("Cloud-init not detected in guest")
            if self.inject_cloud_init_data.get("install_if_missing", False) and not self.dry_run:
                try:
                    for pm_cmd in (["apt-get", "install", "-y"], ["yum", "install", "-y"], ["zypper", "install", "-y"]):
                        if guest_has_cmd(g, pm_cmd[0]): # REPLACED g.available()
                            g.command(pm_cmd + ["cloud-init"])
                            cloud_installed = True
                            self.logger.info("Installed cloud-init in guest")
                            break
                except Exception as e:
                    self.logger.warning(f"Failed to install cloud-init: {e}")
        if not cloud_installed:
            return {"injected": False, "reason": "cloud_init_not_available"}
        cloud_config = self.inject_cloud_init_data.get("config", {})
        if not cloud_config:
            return {"injected": False, "reason": "empty_config"}
        try:
            if not g.is_dir(cloud_dir):
                g.mkdir_p(cloud_dir)
            cloud_cfg_path = os.path.join(cloud_dir, "cloud.cfg")
            if YAML_AVAILABLE:
                yaml_data = yaml.dump(cloud_config, default_flow_style=False)
            else:
                yaml_data = json.dumps(cloud_config, indent=2)
            if not self.dry_run:
                self.backup_file(g, cloud_cfg_path)
                g.write(cloud_cfg_path, yaml_data.encode("utf-8"))
                self.logger.info(f"Injected cloud-init configuration: {cloud_cfg_path}")
                run_on_boot = "/var/lib/cloud/instance"
                if not g.is_dir(run_on_boot):
                    g.mkdir_p(run_on_boot)
                return {"injected": True, "path": cloud_cfg_path, "config": cloud_config}
            else:
                self.logger.info(f"DRY-RUN: would inject cloud-init configuration: {cloud_cfg_path}")
                return {"injected": True, "dry_run": True, "path": cloud_cfg_path, "config": cloud_config}
        except Exception as e:
            self.logger.warning(f"Failed to inject cloud-init: {e}")
            return {"injected": False, "reason": str(e)}
    # ---------------------------
    # report writing
    # ---------------------------
    def write_report(self) -> None:
        self.report["timestamps"]["end"] = _dt.datetime.now().isoformat()
        if not self.report_path:
            return
        p = self.report_path.expanduser().resolve()
        U.ensure_dir(p.parent)
        md: List[str] = []
        md.append("# vmdk2kvm Report")
        md.append("")
        md.append(f"- Date: `{_dt.datetime.now().isoformat()}`")
        md.append(f"- Version: `{__version__}`")
        md.append(f"- Image: `{self.image}`")
        md.append(f"- Dry-run: `{self.dry_run}`")
        md.append(f"- Root device: `{self.root_dev}`")
        if self.root_btrfs_subvol:
            md.append(f"- Root btrfs subvol: `{self.root_btrfs_subvol}`")
        md.append("")
        md.append("## Changes")
        md.append("```json")
        md.append(U.json_dump(self.report.get("changes", {})))
        md.append("```")
        md.append("")
        md.append("## Analysis")
        md.append("```json")
        md.append(U.json_dump(self.report.get("analysis", {})))
        md.append("```")
        if "validation" in self.report:
            md.append("")
            md.append("## Validation Results")
            md.append("```json")
            md.append(U.json_dump(self.report.get("validation", {})))
            md.append("```")
        if self.recovery_manager and self.recovery_manager.checkpoints:
            md.append("")
            md.append("## Recovery Checkpoints")
            checkpoints_summary = [
                {"stage": cp.stage, "timestamp": cp.timestamp, "completed": cp.completed}
                for cp in self.recovery_manager.checkpoints
            ]
            md.append("```json")
            md.append(U.json_dump(checkpoints_summary))
            md.append("```")
        if "virtio" in self.report["analysis"]:
            md.append("")
            md.append("## Virtio Injection")
            md.append("```json")
            md.append(U.json_dump(self.report["analysis"]["virtio"]))
            md.append("```")
        p.write_text("\n".join(md) + "\n", encoding="utf-8")
        self.logger.info(f"Report written: {p}")
    # ---------------------------
    # Validation checks
    # ---------------------------
    def create_validation_suite(self, g: guestfs.GuestFS) -> ValidationSuite:
        suite = ValidationSuite(self.logger)
        def check_fstab_exists(_context):
            return g.is_file("/etc/fstab")
        def check_boot_files(_context):
            boot_files = ["/boot", "/boot/grub", "/boot/grub2"]
            return any(g.is_dir(f) for f in boot_files)
        def check_kernel_presence(_context):
            # REPLACED g.glob() with guest_ls_glob()
            kernels = guest_ls_glob(g, "/boot/vmlinuz-*")
            return len(kernels) > 0
        def check_initramfs_tools(_context):
            tools = ["dracut", "update-initramfs", "mkinitcpio"]
            return any(guest_has_cmd(g, t) for t in tools) # REPLACED g.available()
        suite.add_check("fstab_exists", check_fstab_exists, critical=True)
        suite.add_check("boot_files_present", check_boot_files, critical=True)
        suite.add_check("kernel_present", check_kernel_presence, critical=True)
        suite.add_check("initramfs_tools", check_initramfs_tools, critical=False)
        return suite
    def resize_disk(self, g: guestfs.GuestFS) -> None:
        if not self.resize:
            return
        U.banner(self.logger, "Resize guest")
        if self.dry_run:
            self.logger.info("DRY-RUN: skipping resize")
            return
        try:
            parts = g.list_partitions()
            if len(parts) != 1:
                self.logger.warning("Resize: only single partition supported")
                return
            if not guest_has_cmd(g, "sgdisk"):
                self.logger.warning("Resize: sgdisk not available")
                return
            part = parts[0]
            dev = re.match(r"/dev/sd[a-z]", part).group(0)
            part_num = part[len(dev) + 1:]
            type_code = g.part_get_gpt_type(dev, int(part_num))
            g.command(["sgdisk", "-e", dev])
            g.command(["sgdisk", "-d", part_num, "-n", f"{part_num}:0:0", "-t", f"{part_num}:{type_code}", dev])
            g.blockdev_flushbufs(dev)
            self.logger.info("Resized partition")
            fs = g.vfs_type(part)
            g.umount_all()
            if fs.startswith('ext'):
                g.e2fsck_f(part)
                g.resize2fs(part)
            elif fs == 'ntfs':
                g.ntfsresize(part)
            elif fs == 'btrfs':
                g.mount(part, "/")
                g.btrfs_filesystem_resize("/", 0)
                g.umount_all()
            else:
                self.logger.warning(f"Resize fs not supported for {fs}")
                return
            self._mount_root_direct(g, self.root_dev, self.root_btrfs_subvol)
            self.logger.info("Resized filesystem")
        except Exception as e:
            self.logger.error(f"Resize failed: {e}")
    def run(self) -> None:
        U.banner(self.logger, "Offline guest fix (libguestfs)")
        self.logger.info(f"Opening offline image: {self.image}")
        if self.recovery_manager:
            self.recovery_manager.save_checkpoint("start", {"image": str(self.image)})
        if self.resize:
            try:
                U.banner(self.logger, "Resize disk image")
                cp = U.run_cmd(self.logger, ["qemu-img", "info", "--output=json", str(self.image)], capture=True)
                info = json.loads(cp.stdout)
                current_size = info['virtual-size']
                if self.resize.startswith('+'):
                    add = U.human_to_bytes(self.resize[1:])
                    new_size = current_size + add
                else:
                    new_size = U.human_to_bytes(self.resize)
                if new_size < current_size:
                    self.logger.warning("Shrink not supported")
                else:
                    cmd = ["qemu-img", "resize", str(self.image), str(new_size)]
                    process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, universal_newlines=True)
                    blinking_progress("Resizing image", process)
                    process.wait()
                    if process.returncode != 0:
                        raise subprocess.CalledProcessError(process.returncode, cmd)
                    self.logger.info(f"Resized image to {U.human_bytes(new_size)}")
            except Exception as e:
                self.logger.error(f"Image resize failed: {e}")
        g = self.open()
        try:
            self.detect_and_mount_root(g)
            if self.resize:
                self.resize_disk(g)
            validation_suite = self.create_validation_suite(g)
            validation_context = {"image": self.image, "root_dev": self.root_dev}
            validation_results = validation_suite.run_all(validation_context)
            self.report["validation"] = validation_results
            critical_failures = [
                name for name, result in validation_results.items()
                if result.get("critical") and not result.get("passed")
            ]
            if critical_failures:
                self.logger.warning(f"Critical validation failures: {critical_failures}")
            if self.recovery_manager:
                self.recovery_manager.save_checkpoint("mounted", {
                    "root_dev": self.root_dev,
                    "root_btrfs_subvol": self.root_btrfs_subvol,
                    "validation": validation_results
                })
            c_fstab, fstab_changes, fstab_audit = self.rewrite_fstab(g)
            c_crypt = self.rewrite_crypttab(g)
            network_changes = self.fix_network_config(g)
            c_devmap = self.remove_stale_device_map(g)
            c_grub = self.update_grub_root(g)
            mdraid = self.mdraid_check(g)
            win = self.windows_bcd_actual_fix(g)
            virtio = self.inject_virtio_drivers(g)
            disk_analysis = self.analyze_disk_space(g)
            vmware_removal = self.remove_vmware_tools_func(g)
            cloud_init = self.inject_cloud_init(g)
            regen_info = self.regen(g)
            if not self.dry_run:
                try:
                    g.sync()
                except Exception:
                    pass
            try:
                g.umount_all()
            except Exception:
                pass
            if self.recovery_manager:
                self.recovery_manager.mark_checkpoint_complete("mounted")
                self.recovery_manager.save_checkpoint("completed", {
                    "fstab_changes": c_fstab,
                    "crypttab_changes": c_crypt,
                    "network_changes": network_changes["count"]
                })
                self.recovery_manager.mark_checkpoint_complete("completed")
            self.report["changes"] = {
                "fstab": c_fstab,
                "crypttab": c_crypt,
                "network": network_changes,
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
            self.report["analysis"]["disk"] = disk_analysis
            self.report["analysis"]["regen"] = regen_info
        except Exception as e:
            if self.recovery_manager:
                recovery_data = self.recovery_manager.recover_from_checkpoint("error")
                if recovery_data:
                    self.logger.info(f"Recovery data available from checkpoint: {recovery_data}")
            raise
        finally:
            try:
                g.close()
            except Exception:
                pass
        self.write_report()
