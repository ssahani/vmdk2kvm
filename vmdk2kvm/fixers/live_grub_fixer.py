from __future__ import annotations

import logging
import re
import shlex
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from ..core.utils import U
from ..ssh.ssh_client import SSHClient


@dataclass
class LiveGrubFixReport:
    distro: str = ""
    root_source: str = ""
    stable_root: str = ""
    removed_device_maps: List[str] = None  # type: ignore
    updated_default_grub: bool = False
    updated_files: List[str] = None  # type: ignore
    commands_ran: List[Dict[str, str]] = None  # type: ignore
    warnings: List[str] = None  # type: ignore
    errors: List[str] = None  # type: ignore

    def __post_init__(self) -> None:
        if self.removed_device_maps is None:
            self.removed_device_maps = []
        if self.updated_files is None:
            self.updated_files = []
        if self.commands_ran is None:
            self.commands_ran = []
        if self.warnings is None:
            self.warnings = []
        if self.errors is None:
            self.errors = []


class LiveGrubFixer:
    """
    LIVE GRUB fix via SSH (enhanced, safer, more distro-aware):

      - remove stale device.map (only if it contains legacy disk names like sda/vda/hda)
      - update root= in /etc/default/grub (best effort; handles root=UUID=/PARTUUID=/LABEL= or /dev/*)
      - regen initramfs + grub config with distro detection + fallbacks
      - optional post-checks: verify grub.cfg contains root=stable_root (best effort)
    """

    def __init__(
        self,
        logger: logging.Logger,
        sshc: SSHClient,
        *,
        dry_run: bool,
        no_backup: bool,
        update_grub: bool,
        regen_initramfs: bool,
        prefer: Tuple[str, ...] = ("UUID", "PARTUUID", "LABEL", "PARTLABEL"),
    ):
        self.logger = logger
        self.sshc = sshc
        self.dry_run = dry_run
        self.no_backup = no_backup
        self.update_grub = update_grub
        self.regen_initramfs = regen_initramfs
        self.prefer = prefer
        self.report = LiveGrubFixReport()

    # ---------------------------
    # ssh helpers
    # ---------------------------
    def _ssh(self, cmd: str) -> str:
        self.logger.debug("SSH: %s", cmd)
        out = self.sshc.ssh(cmd) or ""
        return out

    def _sh(self, cmd: str, *, allow_fail: bool = True) -> Tuple[int, str]:
        """
        Run command via ssh with explicit rc.
        We avoid assuming SSHClient supports rc directly; instead we wrap shell.
        """
        wrapped = (
            "sh -lc "
            + shlex.quote(
                f"""
set -o pipefail
{cmd}
rc=$?
echo __VMDK2KVM_RC__=$rc
exit 0
""".strip()
            )
        )
        out = self._ssh(wrapped)
        rc = 0
        m = re.search(r"__VMDK2KVM_RC__=(\d+)", out)
        if m:
            rc = int(m.group(1))
            out = re.sub(r"\n?__VMDK2KVM_RC__=\d+\s*$", "", out, flags=re.M)
        else:
            # cannot determine; treat as success-ish
            rc = 0

        self.report.commands_ran.append({"cmd": cmd, "rc": str(rc)})
        if rc != 0 and not allow_fail:
            raise RuntimeError(f"Remote command failed rc={rc}: {cmd}")
        return rc, out

    def _remote_exists(self, path: str) -> bool:
        rc, out = self._sh(f"test -e {shlex.quote(path)} && echo OK || echo NO")
        return out.strip() == "OK"

    def _read_remote_file(self, path: str) -> str:
        _, out = self._sh(f"cat {shlex.quote(path)} 2>/dev/null || true")
        return out

    def _write_remote_file_atomic(self, path: str, content: str, mode: str = "0644") -> None:
        if self.dry_run:
            self.logger.info("DRY-RUN: would write %s (%d bytes)", path, len(content))
            return

        # mktemp may fail if /tmp is noexec? rare. Use /run if exists.
        _, tmp = self._sh("mktemp /tmp/vmdk2kvm.grubfix.XXXXXX 2>/dev/null || mktemp /run/vmdk2kvm.grubfix.XXXXXX", allow_fail=True)
        tmp = tmp.strip()
        if not tmp:
            raise RuntimeError("mktemp failed on remote host")

        # Write via heredoc safely
        self._sh(
            "sh -lc "
            + shlex.quote(
                f"cat > {shlex.quote(tmp)} <<'EOF'\n{content}\nEOF\nchmod {mode} {shlex.quote(tmp)} || true\n"
            ),
            allow_fail=False,
        )
        self._sh(f"mv -f {shlex.quote(tmp)} {shlex.quote(path)}", allow_fail=False)
        self._sh("sync || true", allow_fail=True)

    def _backup_remote_file(self, path: str) -> Optional[str]:
        if self.no_backup or self.dry_run:
            return None
        b = f"{path}.bak.vmdk2kvm.{U.now_ts()}"
        self._sh(f"cp -a {shlex.quote(path)} {shlex.quote(b)} 2>/dev/null || true")
        self.logger.info("Backup: %s -> %s", path, b)
        return b

    def _remove_remote_file(self, path: str) -> None:
        if self.dry_run:
            self.logger.info("DRY-RUN: would remove %s", path)
            return
        self._sh(f"rm -f {shlex.quote(path)} 2>/dev/null || true")
        self.logger.info("Removed %s (if existed)", path)

    # ---------------------------
    # detection helpers
    # ---------------------------
    def _detect_distro_id(self) -> str:
        _, out = self._sh(". /etc/os-release 2>/dev/null; echo ${ID:-} || true")
        return out.strip().lower()

    def _readlink_f(self, path: str) -> Optional[str]:
        _, out = self._sh(f"readlink -f -- {shlex.quote(path)} 2>/dev/null || true")
        s = out.strip()
        return s or None

    def _is_remote_blockdev(self, dev: str) -> bool:
        _, out = self._sh(f"test -b {shlex.quote(dev)} && echo OK || echo NO")
        return out.strip() == "OK"

    def _blkid(self, dev: str, key: str) -> Optional[str]:
        _, out = self._sh(f"blkid -s {shlex.quote(key)} -o value -- {shlex.quote(dev)} 2>/dev/null || true")
        v = out.strip()
        return v or None

    def _findmnt_root_source(self) -> str:
        # Try SOURCE first; if it's overlay or weird, try TARGET=/
        cmds = [
            "findmnt -n -o SOURCE / 2>/dev/null || true",
            "findmnt -n -o SOURCE -T / 2>/dev/null || true",
            "awk '$2==\"/\"{print $1; exit}' /proc/mounts 2>/dev/null || true",
        ]
        for c in cmds:
            _, out = self._sh(c)
            s = out.strip()
            if s:
                return s
        return ""

    def _convert_spec_to_stable(self, spec: str) -> str:
        """
        Convert a root source spec into a stable ID string if possible.

        Handles:
          - /dev/disk/by-path/* -> UUID=/PARTUUID=/LABEL=
          - /dev/sdXn, /dev/vdXn, /dev/nvme0n1pX -> UUID=/PARTUUID=/LABEL=
          - /dev/mapper/*: try to resolve to underlying block dev if possible; otherwise keep
          - already stable specs are returned unchanged
        """
        # Already stable?
        if re.match(r"^(UUID|PARTUUID|LABEL|PARTLABEL)=.+", spec):
            return spec

        resolved = spec

        # Follow symlinks if it's a by-* path
        if spec.startswith("/dev/disk/by-"):
            rp = self._readlink_f(spec)
            if rp:
                resolved = rp

        # LVM/crypt mapper is block dev but blkid may return nothing; still try.
        if resolved.startswith("/dev/mapper/"):
            # Sometimes there's a dm-* behind it
            rp = self._readlink_f(resolved)
            if rp and rp.startswith("/dev/"):
                resolved = rp

        if not resolved.startswith("/dev/"):
            return spec

        if not self._is_remote_blockdev(resolved):
            return spec

        for key in self.prefer:
            v = self._blkid(resolved, key)
            if v:
                return f"{key}={v}"

        # last-resort: keep as-is
        return spec

    # ---------------------------
    # operations
    # ---------------------------
    def remove_stale_device_map(self) -> int:
        removed = 0
        # include more candidates; distros differ
        paths = [
            "/boot/grub2/device.map",
            "/boot/grub/device.map",
            "/etc/grub2-device.map",
            "/etc/grub/device.map",
        ]
        # match common legacy disk names
        stale_re = re.compile(r"\b(sd[a-z]|vd[a-z]|hd[a-z])\b")

        for p in paths:
            if not self._remote_exists(p):
                continue
            txt = self._read_remote_file(p)
            if stale_re.search(txt):
                self.logger.info("GRUB: removing stale device.map: %s", p)
                if not self.dry_run:
                    self._backup_remote_file(p)
                self._remove_remote_file(p)
                self.report.removed_device_maps.append(p)
                removed += 1

        return removed

    def update_grub_root(self) -> bool:
        if not self.update_grub:
            return False

        root_src = self._findmnt_root_source()
        self.report.root_source = root_src

        if not root_src:
            msg = "GRUB root=: could not detect root source; skipping."
            self.logger.warning(msg)
            self.report.warnings.append(msg)
            return False

        stable = self._convert_spec_to_stable(root_src)
        self.report.stable_root = stable

        # If no improvement, keep quiet-ish
        if stable == root_src:
            self.logger.info("GRUB root=: already stable (or could not improve): %s", root_src)
            return False

        path = "/etc/default/grub"
        if not self._remote_exists(path):
            msg = f"GRUB root=: {path} not found; skipping."
            self.logger.warning(msg)
            self.report.warnings.append(msg)
            return False

        old = self._read_remote_file(path)
        if not old.strip():
            msg = f"GRUB root=: {path} unreadable/empty; skipping."
            self.logger.warning(msg)
            self.report.warnings.append(msg)
            return False

        # Patch both GRUB_CMDLINE_LINUX and GRUB_CMDLINE_LINUX_DEFAULT if present
        # - preserve quotes
        cmdline_re = re.compile(r'^(GRUB_CMDLINE_LINUX(?:_DEFAULT)?)=(["\'])(.*)\2\s*$')

        def patch_line(line: str) -> str:
            m = cmdline_re.match(line)
            if not m:
                return line
            key, quote, val = m.group(1), m.group(2), m.group(3)

            # replace any existing root=... token
            if re.search(r"\broot=", val):
                val2 = re.sub(r"\broot=[^\s\"']+", f"root={stable}", val)
            else:
                val2 = (val + f" root={stable}").strip()

            return f"{key}={quote}{val2}{quote}"

        new_lines = [patch_line(l) for l in old.splitlines()]
        new = "\n".join(new_lines) + "\n"

        if new == old:
            self.logger.info("GRUB root=: no change needed.")
            return False

        if self.dry_run:
            self.logger.info("DRY-RUN: would update %s (root=%s).", path, stable)
            self.report.updated_default_grub = True
            self.report.updated_files.append(path)
            return True

        self._backup_remote_file(path)
        self._write_remote_file_atomic(path, new, mode="0644")
        self.logger.info("GRUB root=: updated %s (root=%s).", path, stable)
        self.report.updated_default_grub = True
        self.report.updated_files.append(path)
        return True

    def regen_initramfs_and_grub(self) -> None:
        if not self.regen_initramfs:
            return

        distro = self._detect_distro_id()
        self.report.distro = distro

        if self.dry_run:
            self.logger.info("DRY-RUN: would regenerate initramfs + grub (distro=%s).", distro)
            return

        # initramfs
        if distro in ("debian", "ubuntu"):
            self._sh("update-initramfs -u -k all 2>/dev/null || update-initramfs -u 2>/dev/null || true")
            self._sh("update-grub 2>/dev/null || grub-mkconfig -o /boot/grub/grub.cfg 2>/dev/null || true")
        elif distro in ("arch", "manjaro"):
            self._sh("mkinitcpio -P 2>/dev/null || true")
            self._sh("grub-mkconfig -o /boot/grub/grub.cfg 2>/dev/null || true")
        else:
            # rhel/fedora/suse
            self._sh("dracut -f 2>/dev/null || dracut -f --regenerate-all 2>/dev/null || true")
            self._sh(
                "grub2-mkconfig -o /boot/grub2/grub.cfg 2>/dev/null || "
                "grub-mkconfig -o /boot/grub/grub.cfg 2>/dev/null || true"
            )

        self.logger.info("Live regen done.")

    def postcheck_grubcfg(self) -> None:
        """
        Best-effort sanity: does grub.cfg contain our stable root=?
        We don't fail the run on this; it's a warning signal.
        """
        stable = self.report.stable_root or ""
        if not stable:
            return
        candidates = ["/boot/grub2/grub.cfg", "/boot/grub/grub.cfg"]
        found_any = False
        for p in candidates:
            if not self._remote_exists(p):
                continue
            txt = self._read_remote_file(p)
            if stable in txt:
                found_any = True
                break
        if not found_any:
            msg = f"Postcheck: stable root '{stable}' not found in grub.cfg (may still be OK; grub may source /etc/default/grub later)."
            self.logger.warning(msg)
            self.report.warnings.append(msg)

    def run(self) -> Dict[str, Any]:
        U.banner(self.logger, "GRUB fix (SSH)")

        removed = self.remove_stale_device_map()
        updated = self.update_grub_root()

        # Regen if requested OR if we changed /etc/default/grub (common expectation)
        if self.regen_initramfs or updated:
            self.regen_initramfs_and_grub()

        # Best-effort postcheck
        try:
            self.postcheck_grubcfg()
        except Exception as e:
            self.logger.debug("Postcheck failed: %s", e)

        self.logger.info("GRUB fix: removed_device_maps=%d, updated_grub_root=%s", removed, updated)
        self.logger.info("GRUB fix completed.")

        # return a JSON-friendly dict
        return {
            "distro": self.report.distro,
            "root_source": self.report.root_source,
            "stable_root": self.report.stable_root,
            "removed_device_maps": self.report.removed_device_maps,
            "updated_default_grub": self.report.updated_default_grub,
            "updated_files": self.report.updated_files,
            "commands_ran": self.report.commands_ran,
            "warnings": self.report.warnings,
            "errors": self.report.errors,
            "dry_run": self.dry_run,
        }
