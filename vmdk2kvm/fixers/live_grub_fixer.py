# vmdk2kvm/fixers/live_fixer.py
from __future__ import annotations

import logging
import re
import shlex
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from ..core.utils import U
from ..ssh.ssh_client import SSHClient
from .live_grub_fixer import LiveGrubFixer


@dataclass(frozen=True)
class LiveFixerOptions:
    dry_run: bool
    no_backup: bool
    print_fstab: bool
    update_grub: bool
    regen_initramfs: bool
    remove_vmware_tools: bool = False


class LiveFixer:
    """
    Live fix via SSH:

      - Rewrite /etc/fstab: /dev/disk/by-path/* -> UUID=/PARTUUID=/LABEL=/PARTLABEL= (best-effort)
      - Optionally remove VMware tools (best-effort across distros)
      - Optionally run LiveGrubFixer (preferred) to stabilize root= + regen initramfs/bootloader

    Design goals:
      - Safe defaults + best-effort behavior
      - Minimal assumptions about distro/tooling
      - Deterministic edits: atomic writes + timestamped backups (unless disabled)
    """

    def __init__(
        self,
        logger: logging.Logger,
        sshc: SSHClient,
        *,
        dry_run: bool,
        no_backup: bool,
        print_fstab: bool,
        update_grub: bool,
        regen_initramfs: bool,
        remove_vmware_tools: bool = False,
    ):
        self.logger = logger
        self.sshc = sshc
        self.opts = LiveFixerOptions(
            dry_run=dry_run,
            no_backup=no_backup,
            print_fstab=print_fstab,
            update_grub=update_grub,
            regen_initramfs=regen_initramfs,
            remove_vmware_tools=remove_vmware_tools,
        )

    # ---------------------------------------------------------------------
    # SSH helpers
    # ---------------------------------------------------------------------

    def _ssh(self, cmd: str) -> str:
        self.logger.debug("SSH: %s", cmd)
        return self.sshc.ssh(cmd) or ""

    def _has(self, cmd: str) -> bool:
        return (
            self._ssh(f"command -v {shlex.quote(cmd)} >/dev/null 2>&1 && echo YES || echo NO").strip()
            == "YES"
        )

    def _remote_exists(self, path: str) -> bool:
        out = self._ssh(f"test -e {shlex.quote(path)} && echo OK || echo NO").strip()
        return out == "OK"

    def _read_remote_file(self, path: str) -> str:
        return self._ssh(f"cat {shlex.quote(path)} 2>/dev/null || true")

    def _write_remote_file_atomic(self, path: str, content: str, mode: str = "0644") -> None:
        """
        Atomic-ish update:
          - mktemp
          - write content
          - chmod
          - mv over target
        """
        if self.opts.dry_run:
            self.logger.info("DRY-RUN: would write %s (%d bytes)", path, len(content))
            return

        tmp = self._ssh("mktemp /tmp/vmdk2kvm.livefix.XXXXXX 2>/dev/null || mktemp /run/vmdk2kvm.livefix.XXXXXX").strip()
        if not tmp:
            raise RuntimeError("mktemp failed on remote host")

        payload = (
            f"cat > {shlex.quote(tmp)} <<'EOF'\n"
            f"{content}\n"
            "EOF\n"
            f"chmod {shlex.quote(mode)} {shlex.quote(tmp)} || true\n"
        )
        self._ssh("sh -lc " + shlex.quote(payload))
        self._ssh(f"mv -f {shlex.quote(tmp)} {shlex.quote(path)}")
        self._ssh("sync || true")

    def _readlink_f(self, path: str) -> Optional[str]:
        out = self._ssh(f"readlink -f -- {shlex.quote(path)} 2>/dev/null || true").strip()
        return out or None

    def _is_remote_blockdev(self, dev: str) -> bool:
        return self._ssh(f"test -b {shlex.quote(dev)} && echo OK || echo NO").strip() == "OK"

    def _blkid(self, dev: str, key: str) -> Optional[str]:
        out = self._ssh(
            f"blkid -s {shlex.quote(key)} -o value -- {shlex.quote(dev)} 2>/dev/null || true"
        ).strip()
        return out or None

    def _run_best_effort(self, cmds: List[str]) -> None:
        for c in cmds:
            if not c.strip():
                continue
            self._ssh(c)

    def _backup(self, path: str) -> Optional[str]:
        if self.opts.no_backup or self.opts.dry_run:
            return None
        b = f"{path}.bak.vmdk2kvm.{U.now_ts()}"
        self._ssh(f"cp -a {shlex.quote(path)} {shlex.quote(b)} 2>/dev/null || true")
        self.logger.info("Backup: %s -> %s", path, b)
        return b

    # ---------------------------------------------------------------------
    # fstab rewrite
    # ---------------------------------------------------------------------

    def _convert_spec_to_stable(self, spec: str) -> str:
        """
        Convert /dev/disk/by-path/* to a stable spec, preferring:
          UUID=, PARTUUID=, LABEL=, PARTLABEL=
        """
        resolved = self._readlink_f(spec)
        if not resolved:
            self.logger.debug("fstab: readlink -f failed for %s", spec)
            return spec

        if not self._is_remote_blockdev(resolved):
            self.logger.debug("fstab: resolved path is not a block dev: %s -> %s", spec, resolved)
            return spec

        for key, prefix in (
            ("UUID", "UUID="),
            ("PARTUUID", "PARTUUID="),
            ("LABEL", "LABEL="),
            ("PARTLABEL", "PARTLABEL="),
        ):
            v = self._blkid(resolved, key)
            if v:
                return prefix + v

        return spec

    @staticmethod
    def _split_comment(line: str) -> Tuple[str, str]:
        s = line.rstrip("\n")
        if not s.strip():
            return s, ""
        if s.lstrip().startswith("#"):
            return s, ""
        m = re.search(r"\s#", s)
        if not m:
            return s, ""
        i = m.start()
        return s[:i].rstrip(), s[i:].lstrip()

    def _rewrite_fstab(self, content: str) -> Tuple[str, int]:
        changed = 0
        out_lines: List[str] = []

        for line in content.splitlines(keepends=False):
            if not line.strip() or line.lstrip().startswith("#"):
                out_lines.append(line + "\n")
                continue

            data, comment = self._split_comment(line)
            parts = data.split()
            if len(parts) < 2:
                out_lines.append(line + "\n")
                continue

            spec = parts[0]
            if spec.startswith("/dev/disk/by-path/"):
                new_spec = self._convert_spec_to_stable(spec)
                if new_spec != spec:
                    parts[0] = new_spec
                    changed += 1

            rebuilt = "\t".join(parts)
            if comment:
                if not comment.startswith("#"):
                    comment = "# " + comment
                rebuilt = rebuilt + "\t" + comment

            out_lines.append(rebuilt.rstrip() + "\n")

        return "".join(out_lines), changed

    # ---------------------------------------------------------------------
    # VMware tools removal (multi-distro best-effort)
    # ---------------------------------------------------------------------

    def _remove_vmware_tools(self) -> None:
        self.logger.info("Removing VMware tools (live)...")

        pkgs = [
            "open-vm-tools",
            "open-vm-tools-desktop",
            "vmware-tools",
            "vmware-tools-desktop",
            "vmtoolsd",
        ]

        if self._has("apt-get"):
            self._run_best_effort(
                [
                    "DEBIAN_FRONTEND=noninteractive apt-get remove -y "
                    + " ".join(map(shlex.quote, pkgs))
                    + " 2>/dev/null || true",
                    "DEBIAN_FRONTEND=noninteractive apt-get autoremove -y 2>/dev/null || true",
                ]
            )
        elif self._has("dnf"):
            self._run_best_effort(
                ["dnf remove -y " + " ".join(map(shlex.quote, pkgs)) + " 2>/dev/null || true"]
            )
        elif self._has("yum"):
            self._run_best_effort(
                ["yum remove -y " + " ".join(map(shlex.quote, pkgs)) + " 2>/dev/null || true"]
            )
        elif self._has("zypper"):
            self._run_best_effort(
                ["zypper -n rm " + " ".join(map(shlex.quote, pkgs)) + " 2>/dev/null || true"]
            )
        elif self._has("pacman"):
            self._run_best_effort(
                ["pacman -Rns --noconfirm " + " ".join(map(shlex.quote, pkgs)) + " 2>/dev/null || true"]
            )
        elif self._has("apk"):
            self._run_best_effort(["apk del " + " ".join(map(shlex.quote, pkgs)) + " 2>/dev/null || true"])
        elif self._has("xbps-remove"):
            self._run_best_effort(
                ["xbps-remove -Ry " + " ".join(map(shlex.quote, pkgs)) + " 2>/dev/null || true"]
            )
        elif self._has("emerge"):
            self._run_best_effort(["emerge -C " + " ".join(map(shlex.quote, pkgs)) + " 2>/dev/null || true"])
        else:
            self.logger.warning("No known package manager found; skipping package removal.")

        # Service cleanup (systemd/OpenRC best-effort)
        self._run_best_effort(
            [
                "systemctl disable --now vmware-tools 2>/dev/null || true",
                "systemctl disable --now vmtoolsd 2>/dev/null || true",
                "rc-service vmware-tools stop 2>/dev/null || true",
                "rc-service vmtoolsd stop 2>/dev/null || true",
                "rc-update del vmware-tools default 2>/dev/null || true",
                "rc-update del vmtoolsd default 2>/dev/null || true",
                "rm -f /etc/init.d/vmware-tools /etc/init.d/vmtoolsd 2>/dev/null || true",
                "rm -f /etc/systemd/system/vmware-tools.service /etc/systemd/system/vmtoolsd.service 2>/dev/null || true",
            ]
        )

        uninstaller = "/usr/bin/vmware-uninstall-tools.pl"
        if self._remote_exists(uninstaller):
            self._run_best_effort([f"{shlex.quote(uninstaller)} 2>/dev/null || true"])

        self.logger.info("VMware tools removal attempted.")

    # ---------------------------------------------------------------------
    # Entrypoint
    # ---------------------------------------------------------------------

    def run(self) -> Dict[str, Any]:
        U.banner(self.logger, "Live fix (SSH)")
        self.sshc.check()

        # ---- fstab rewrite
        fstab = self._read_remote_file("/etc/fstab")
        if self.opts.print_fstab:
            print("\n--- /etc/fstab (live before) ---\n" + (fstab or ""))

        new_fstab, changed = self._rewrite_fstab(fstab or "")
        self.logger.info("fstab (live): changed_entries=%d", changed)

        if self.opts.print_fstab:
            print("\n--- /etc/fstab (live after) ---\n" + (new_fstab or ""))

        if changed > 0:
            if self.opts.dry_run:
                self.logger.info("DRY-RUN: would update /etc/fstab (live).")
            else:
                if not self.opts.no_backup:
                    self._backup("/etc/fstab")
                self._write_remote_file_atomic("/etc/fstab", new_fstab, mode="0644")
                self.logger.info("/etc/fstab updated (live).")

        # ---- optional VMware tools removal
        if self.opts.remove_vmware_tools:
            if self.opts.dry_run:
                self.logger.info("DRY-RUN: would remove VMware tools (live).")
            else:
                self._remove_vmware_tools()

        # ---- GRUB fixer (now owns distro detection + regen logic)
        grub_report: Optional[Dict[str, Any]] = None
        if self.opts.update_grub or self.opts.regen_initramfs:
            self.logger.info("Running LiveGrubFixer...")
            gf = LiveGrubFixer(
                logger=self.logger,
                sshc=self.sshc,
                dry_run=self.opts.dry_run,
                no_backup=self.opts.no_backup,
                update_grub=self.opts.update_grub,
                regen_initramfs=self.opts.regen_initramfs,
            )
            grub_report = gf.run()

        self.logger.info("Live fix completed.")
        return {
            "dry_run": self.opts.dry_run,
            "fstab_changed": changed,
            "update_grub": self.opts.update_grub,
            "regen_initramfs": self.opts.regen_initramfs,
            "remove_vmware_tools": self.opts.remove_vmware_tools,
            "grub_report": grub_report,
        }
