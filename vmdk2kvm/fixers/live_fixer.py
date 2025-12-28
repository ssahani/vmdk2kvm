from __future__ import annotations
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from ..core.utils import U
from ..ssh.ssh_client import SSHClient
from ..ssh.ssh_config import SSHConfig
import shlex
import re
class LiveFixer:
    """
    LIVE fix via SSH:
      - rewrite /etc/fstab by-path -> UUID/PARTUUID/LABEL/PARTLABEL (best-effort)
      - update grub root= best effort (optional)
      - regen initramfs/grub (optional)
      - remove VMware tools (optional)
    """
    def __init__(
        self,
        logger: logging.Logger,
        sshc: "SSHClient",
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
        self.dry_run = dry_run
        self.no_backup = no_backup
        self.print_fstab = print_fstab
        self.update_grub = update_grub
        self.regen_initramfs = regen_initramfs
        self.remove_vmware_tools = remove_vmware_tools
    def _ssh(self, cmd: str) -> str:
        self.logger.debug("SSH: %s", cmd)
        return self.sshc.ssh(cmd) or ""
    def _remote_exists(self, path: str) -> bool:
        out = self._ssh(f"test -e {shlex.quote(path)} && echo OK || echo NO").strip()
        return out == "OK"
    def _read_remote_file(self, path: str) -> str:
        return self._ssh(f"cat {shlex.quote(path)} 2>/dev/null || true")
    def _write_remote_file_atomic(self, path: str, content: str, mode: str = "0644") -> None:
        tmp = self._ssh("mktemp /tmp/vmdk2kvm.livefix.XXXXXX").strip()
        if not tmp:
            raise RuntimeError("mktemp failed on remote host")
        self._ssh(
            "sh -lc "
            + shlex.quote(
                f"cat > {shlex.quote(tmp)} <<'EOF'\n{content}\nEOF\nchmod {mode} {shlex.quote(tmp)} || true\n"
            )
        )
        self._ssh(f"mv -f {shlex.quote(tmp)} {shlex.quote(path)}")
    def _is_remote_blockdev(self, dev: str) -> bool:
        return self._ssh(f"test -b {shlex.quote(dev)} && echo OK || echo NO").strip() == "OK"
    def _readlink_f(self, path: str) -> Optional[str]:
        out = self._ssh(f"readlink -f -- {shlex.quote(path)} 2>/dev/null || true").strip()
        return out or None
    def _blkid(self, dev: str, key: str) -> Optional[str]:
        out = self._ssh(
            f"blkid -s {shlex.quote(key)} -o value -- {shlex.quote(dev)} 2>/dev/null || true"
        ).strip()
        return out or None
    def _convert_spec_to_stable(self, spec: str) -> str:
        resolved = self._readlink_f(spec)
        if not resolved:
            self.logger.debug("fstab: readlink -f failed for %s", spec)
            return spec
        if not self._is_remote_blockdev(resolved):
            self.logger.debug("fstab: resolved path is not block dev: %s -> %s", spec, resolved)
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
        lines = content.splitlines(keepends=False)
        for line in lines:
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
    def _remove_vmware_tools(self) -> None:
        self.logger.info("Removing VMware tools (live)...")
        has_apt = self._ssh("command -v apt-get >/dev/null 2>&1 && echo YES || echo NO").strip() == "YES"
        has_dnf = self._ssh("command -v dnf >/dev/null 2>&1 && echo YES || echo NO").strip() == "YES"
        has_yum = self._ssh("command -v yum >/dev/null 2>&1 && echo YES || echo NO").strip() == "YES"
        has_zypper = self._ssh("command -v zypper >/dev/null 2>&1 && echo YES || echo NO").strip() == "YES"
        pkgs = ["open-vm-tools", "vmware-tools", "open-vm-tools-desktop", "vmware-tools-desktop"]
        if has_apt:
            self._ssh("DEBIAN_FRONTEND=noninteractive apt-get remove -y " + " ".join(map(shlex.quote, pkgs)) + " 2>/dev/null || true")
            self._ssh("DEBIAN_FRONTEND=noninteractive apt-get autoremove -y 2>/dev/null || true")
        elif has_dnf:
            self._ssh("dnf remove -y " + " ".join(map(shlex.quote, pkgs)) + " 2>/dev/null || true")
        elif has_yum:
            self._ssh("yum remove -y " + " ".join(map(shlex.quote, pkgs)) + " 2>/dev/null || true")
        elif has_zypper:
            self._ssh("zypper -n rm " + " ".join(map(shlex.quote, pkgs)) + " 2>/dev/null || true")
        else:
            self.logger.warning("No known package manager found; skipping package removal.")
        self._ssh("systemctl disable --now vmware-tools 2>/dev/null || true")
        self._ssh("rm -f /etc/init.d/vmware-tools /etc/systemd/system/vmware-tools.service 2>/dev/null || true")
        self.logger.info("VMware tools removal attempted.")
    def _detect_distro_id(self) -> str:
        return (self._ssh(". /etc/os-release 2>/dev/null; echo ${ID:-} || true") or "").strip().lower()
    def _regen_initramfs_and_grub(self) -> None:
        distro = self._detect_distro_id()
        if distro in ("debian", "ubuntu"):
            self._ssh("update-initramfs -u -k all 2>/dev/null || update-initramfs -u 2>/dev/null || true")
            self._ssh("update-grub 2>/dev/null || true")
        else:
            self._ssh("dracut -f 2>/dev/null || dracut -f --regenerate-all 2>/dev/null || true")
            self._ssh("grub2-mkconfig -o /boot/grub2/grub.cfg 2>/dev/null || grub-mkconfig -o /boot/grub/grub.cfg 2>/dev/null || true")
        self.logger.info("Live regen done.")
    def _update_grub_root_best_effort(self) -> None:
        root_src = (self._ssh("findmnt -n -o SOURCE / 2>/dev/null || true") or "").strip()
        if not root_src:
            self.logger.warning("GRUB root=: could not detect root source; skipping.")
            return
        stable = root_src
        if root_src.startswith("/dev/disk/by-path/"):
            stable = self._convert_spec_to_stable(root_src)
        elif root_src.startswith("/dev/"):
            for key, prefix in (("UUID", "UUID="), ("PARTUUID", "PARTUUID="), ("LABEL", "LABEL="), ("PARTLABEL", "PARTLABEL=")):
                v = self._blkid(root_src, key)
                if v:
                    stable = prefix + v
                    break
        if stable == root_src:
            self.logger.info("GRUB root=: already stable (or could not improve): %s", root_src)
            return
        path = "/etc/default/grub"
        if not self._remote_exists(path):
            self.logger.warning("GRUB root=: %s not found; skipping.", path)
            return
        old = self._read_remote_file(path)
        if not old.strip():
            self.logger.warning("GRUB root=: {path} unreadable/empty; skipping.", path=path)
            return
        def patch_line(line: str) -> str:
            m = re.match(r'^(GRUB_CMDLINE_LINUX(?:_DEFAULT)?)=(["\'])(.*)\2\s*$', line)
            if not m:
                return line
            key, quote, val = m.group(1), m.group(2), m.group(3)
            if re.search(r"\broot=", val):
                val2 = re.sub(r"\broot=[^\s\"']+", f"root={stable}", val)
            else:
                val2 = (val + f" root={stable}").strip()
            return f"{key}={quote}{val2}{quote}"
        new_lines = [patch_line(l) for l in old.splitlines()]
        new = "\n".join(new_lines) + "\n"
        if new == old:
            self.logger.info("GRUB root=: no matching GRUB_CMDLINE_* lines; skipping.")
            return
        if self.dry_run:
            self.logger.info("DRY-RUN: would update %s (root={stable}).", path)
            return
        if not self.no_backup:
            b = f"{path}.bak.vmdk2kvm.{U.now_ts()}"
            self._ssh(f"cp -a {shlex.quote(path)} {shlex.quote(b)} 2>/dev/null || true")
            self.logger.info("Backup: %s -> {b}", path)
        self._write_remote_file_atomic(path, new, mode="0644")
        self.logger.info("GRUB root=: updated {path} (root={stable}).", path)
    # ---------------------------------------------------------
    # LiveFixer.run() (MANDATORY ENTRYPOINT) ✅
    # ---------------------------------------------------------
    def run(self) -> None:
        U.banner(self.logger, "Live fix (SSH)")
        self.sshc.check()
        fstab = self._read_remote_file("/etc/fstab")
        if self.print_fstab:
            print("\n--- /etc/fstab (live before) ---\n" + (fstab or ""))
        new_fstab, changed = self._rewrite_fstab(fstab or "")
        self.logger.info(f"fstab (live): changed_entries={changed}")
        if self.print_fstab:
            print("\n--- /etc/fstab (live after) ---\n" + (new_fstab or ""))
        if changed > 0 and not self.dry_run:
            if not self.no_backup:
                b = f"/etc/fstab.bak.vmdk2kvm.{U.now_ts()}"
                self._ssh(f"cp -a /etc/fstab {shlex.quote(b)} 2>/dev/null || true")
                self.logger.info(f"Backup: /etc/fstab -> {b}")
            self._write_remote_file_atomic("/etc/fstab", new_fstab, mode="0644")
            self.logger.info("/etc/fstab updated (live).")
        elif changed > 0 and self.dry_run:
            self.logger.info("DRY-RUN: would update /etc/fstab (live).")
        if self.remove_vmware_tools and not self.dry_run:
            self._remove_vmware_tools()
        if self.update_grub:
            self._update_grub_root_best_effort()
        if self.regen_initramfs and not self.dry_run:
            self._regen_initramfs_and_grub()
        self.logger.info("Live fix completed.")
