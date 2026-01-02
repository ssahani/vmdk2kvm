# vmdk2kvm/fixers/live_grub_fixer.py
from __future__ import annotations

import logging
import re
import shlex
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from ..core.utils import U
from ..ssh.ssh_client import SSHClient


# ---------------------------------------------------------------------
# Report model (JSON-friendly)
# ---------------------------------------------------------------------

@dataclass
class LiveGrubFixReport:
    distro_id: str = ""
    distro_like: List[str] = field(default_factory=list)
    family: str = ""

    root_source: str = ""
    root_resolved: str = ""
    stable_root: str = ""

    removed_device_maps: List[str] = field(default_factory=list)
    updated_default_grub: bool = False
    updated_files: List[str] = field(default_factory=list)

    commands_ran: List[Dict[str, str]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------
# Live fixer (SSH)
# ---------------------------------------------------------------------

class LiveGrubFixer:
    """
    LIVE boot fix via SSH (safer + more distro-aware):

      - Remove stale device.map (only if it looks auto-generated / references legacy disk names)
      - Stabilize root= in /etc/default/grub using UUID/PARTUUID/LABEL/PARTLABEL if possible
      - Best-effort initramfs + bootloader regeneration using capability detection (not distro-only)
      - Optional post-check: grep grub.cfg for stable root token (warning only)

    Knobs:
      - dry_run: do not mutate, but still probe/detect
      - no_backup: disable timestamped backups
      - update_grub: enable root= stabilization
      - regen_initramfs: enable initramfs + bootloader regeneration
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
        Run a command remotely and capture rc reliably.
        """
        wrapped = "sh -lc " + shlex.quote(
            f"""
set -o pipefail
{cmd}
rc=$?
echo __VMDK2KVM_RC__=$rc
exit 0
""".strip()
        )
        out = self._ssh(wrapped)

        rc = 0
        m = re.search(r"__VMDK2KVM_RC__=(\d+)", out)
        if m:
            rc = int(m.group(1))
            out = re.sub(r"\n?__VMDK2KVM_RC__=\d+\s*$", "", out, flags=re.M)
        else:
            rc = 0

        self.report.commands_ran.append({"cmd": cmd, "rc": str(rc)})
        if rc != 0 and not allow_fail:
            raise RuntimeError(f"Remote command failed rc={rc}: {cmd}")
        return rc, out

    def _has_cmd(self, name: str) -> bool:
        _, out = self._sh(f"command -v {shlex.quote(name)} >/dev/null 2>&1 && echo OK || echo NO")
        return out.strip() == "OK"

    def _remote_exists(self, path: str) -> bool:
        _, out = self._sh(f"test -e {shlex.quote(path)} && echo OK || echo NO")
        return out.strip() == "OK"

    def _read_remote_file(self, path: str) -> str:
        _, out = self._sh(f"cat {shlex.quote(path)} 2>/dev/null || true")
        return out

    def _write_remote_file_atomic(self, path: str, content: str, mode: str = "0644") -> None:
        if self.dry_run:
            self.logger.info("DRY-RUN: would write %s (%d bytes)", path, len(content))
            return

        _, tmp = self._sh(
            "mktemp /tmp/vmdk2kvm.grubfix.XXXXXX 2>/dev/null || mktemp /run/vmdk2kvm.grubfix.XXXXXX",
            allow_fail=True,
        )
        tmp = tmp.strip()
        if not tmp:
            raise RuntimeError("mktemp failed on remote host")

        self._sh(
            "sh -lc " + shlex.quote(
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

    def _read_os_release(self) -> Tuple[str, List[str]]:
        _, out = self._sh(
            r""". /etc/os-release 2>/dev/null || true
echo "ID=${ID:-}"
echo "ID_LIKE=${ID_LIKE:-}"
""",
            allow_fail=True,
        )
        did = ""
        like: List[str] = []
        for ln in out.splitlines():
            if ln.startswith("ID="):
                did = ln.split("=", 1)[1].strip().strip('"').lower()
            elif ln.startswith("ID_LIKE="):
                raw = ln.split("=", 1)[1].strip().strip('"')
                like = [x.strip().lower() for x in raw.split() if x.strip()]
        return did, like

    def _detect_family(self, did: str, like: List[str]) -> str:
        d = (did or "").lower()
        lk = {x.lower() for x in (like or [])}

        # Debian-ish
        if d in {"debian", "ubuntu", "linuxmint", "pop", "popos", "kali", "raspbian", "elementary", "zorin", "deepin"}:
            return "debian"
        if {"debian", "ubuntu"} & lk:
            return "debian"

        # RHEL-ish (+ common derivatives)
        if d in {"rhel", "centos", "fedora", "rocky", "almalinux", "oraclelinux", "ol", "redhat", "amzn", "amazonlinux", "mariner", "cbl-mariner", "photon"}:
            return "rhel"
        if {"rhel", "fedora", "centos", "redhat"} & lk:
            return "rhel"

        # SUSE-ish
        if d in {"sles", "sled", "opensuse", "opensuse-leap", "opensuse-tumbleweed", "suse"}:
            return "suse"
        if "suse" in lk:
            return "suse"

        # Arch-ish
        if d in {"arch", "manjaro", "endeavouros", "garuda"}:
            return "arch"
        if "arch" in lk:
            return "arch"

        # Alpine
        if d == "alpine" or "alpine" in lk:
            return "alpine"

        # Gentoo/Funtoo
        if d in {"gentoo", "funtoo"} or "gentoo" in lk:
            return "gentoo"

        # Void
        if d == "void" or "void" in lk:
            return "void"

        # NixOS
        if d == "nixos" or "nixos" in lk:
            return "nixos"

        return "other"

    def _detect_distro(self) -> None:
        did, like = self._read_os_release()
        fam = self._detect_family(did, like)
        self.report.distro_id = did
        self.report.distro_like = like
        self.report.family = fam

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
        cmds = [
            "findmnt -n -o SOURCE -T / 2>/dev/null || true",
            "findmnt -n -o SOURCE / 2>/dev/null || true",
            "awk '$2==\"/\"{print $1; exit}' /proc/mounts 2>/dev/null || true",
        ]
        for c in cmds:
            _, out = self._sh(c)
            s = out.strip()
            if not s:
                continue
            if s in {"overlay", "tmpfs"}:
                _, o2 = self._sh("findmnt -n -o SOURCE -T /sysroot 2>/dev/null || true")
                s2 = o2.strip()
                if s2 and s2 not in {"overlay", "tmpfs"}:
                    return s2
            return s
        return ""

    def _sanitize_root_spec(self, spec: str) -> str:
        s = (spec or "").strip()
        if not s:
            return s
        s = re.sub(r"\[.*\]$", "", s).strip()  # btrfs subvol suffix
        return s

    def _convert_spec_to_stable(self, spec: str) -> str:
        spec = self._sanitize_root_spec(spec)

        if re.match(r"^(UUID|PARTUUID|LABEL|PARTLABEL)=.+", spec):
            return spec

        resolved = spec

        if spec.startswith("/dev/disk/by-"):
            rp = self._readlink_f(spec)
            if rp:
                resolved = rp

        if resolved.startswith("/dev/mapper/"):
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

        return spec

    # ---------------------------
    # operations
    # ---------------------------

    def remove_stale_device_map(self) -> int:
        removed = 0
        paths = [
            "/boot/grub2/device.map",
            "/boot/grub/device.map",
            "/etc/grub2-device.map",
            "/etc/grub/device.map",
        ]
        stale_re = re.compile(r"\b(sd[a-z]|vd[a-z]|hd[a-z]|xvd[a-z]|nvme\d+n\d+)\b")

        for p in paths:
            if not self._remote_exists(p):
                continue
            txt = self._read_remote_file(p)

            # conservative: must mention disk tokens and hd-mapping style
            if stale_re.search(txt) and ("(hd" in txt or "hd0" in txt):
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

        root_src_s = self._sanitize_root_spec(root_src)
        root_resolved = root_src_s
        if root_src_s.startswith("/dev/disk/by-") or root_src_s.startswith("/dev/mapper/"):
            rp = self._readlink_f(root_src_s)
            if rp:
                root_resolved = rp
        self.report.root_resolved = root_resolved

        stable = self._convert_spec_to_stable(root_src_s)
        self.report.stable_root = stable

        if stable == root_src_s:
            self.logger.info("GRUB root=: already stable (or could not improve): %s", root_src_s)
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

        cmdline_re = re.compile(r'^(GRUB_CMDLINE_LINUX(?:_DEFAULT)?)=(["\'])(.*)\2\s*$')

        def patch_line(line: str) -> str:
            m = cmdline_re.match(line)
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

    # ---------------------------
    # regen logic (capability-first)
    # ---------------------------

    def _detect_grub_cfg_targets(self) -> List[str]:
        targets: List[str] = []
        if self._remote_exists("/boot/grub2"):
            targets.append("/boot/grub2/grub.cfg")
        if self._remote_exists("/boot/grub"):
            targets.append("/boot/grub/grub.cfg")
        if not targets:
            targets = ["/boot/grub2/grub.cfg", "/boot/grub/grub.cfg"]

        out: List[str] = []
        seen = set()
        for t in targets:
            if t not in seen:
                seen.add(t)
                out.append(t)
        return out

    def _run_best_effort_until_ok(self, label: str, cmds: List[str]) -> None:
        for c in cmds:
            rc, out = self._sh(c, allow_fail=True)
            if rc == 0:
                self.logger.info("%s: success: %s", label, c)
                return
            tail = (out or "")[-1200:].strip()
            self.logger.debug("%s: failed rc=%s cmd=%s out=%s", label, rc, c, tail)
        self.report.warnings.append(f"{label}: all attempts failed (non-fatal)")

    def regen_initramfs_and_grub(self) -> None:
        if not self.regen_initramfs:
            return

        # ensure we have distro info for logging/warnings
        if not self.report.distro_id:
            try:
                self._detect_distro()
            except Exception:
                pass

        did = self.report.distro_id
        fam = self.report.family

        if self.dry_run:
            self.logger.info("DRY-RUN: would regenerate initramfs + bootloader (id=%s family=%s).", did, fam)
            return

        # ---- initramfs ----
        initramfs_cmds: List[str] = []

        if self._has_cmd("update-initramfs"):
            initramfs_cmds += [
                "update-initramfs -u -k all 2>/dev/null",
                "update-initramfs -u 2>/dev/null",
            ]

        if self._has_cmd("mkinitcpio"):
            initramfs_cmds += ["mkinitcpio -P 2>/dev/null"]

        if self._has_cmd("dracut"):
            initramfs_cmds += [
                "dracut -f --regenerate-all 2>/dev/null",
                "dracut -f 2>/dev/null",
            ]

        if self._has_cmd("mkinitrd"):
            initramfs_cmds += ["mkinitrd 2>/dev/null"]

        if self._has_cmd("mkinitfs"):
            initramfs_cmds += [
                "k=$(uname -r 2>/dev/null || true); [ -n \"$k\" ] && mkinitfs -b / \"$k\" 2>/dev/null || true"
            ]

        if self._has_cmd("genkernel"):
            initramfs_cmds += ["genkernel --install initramfs 2>/dev/null"]

        if fam == "nixos":
            self.report.warnings.append("initramfs: nixos detected; skipping nixos-rebuild (manual step)")
        else:
            if initramfs_cmds:
                self._run_best_effort_until_ok("initramfs", initramfs_cmds)
            else:
                self.report.warnings.append("initramfs: no known initramfs tool detected; skipping")

        # ---- bootloader config ----
        grub_targets = self._detect_grub_cfg_targets()
        boot_cmds: List[str] = []

        if self._has_cmd("update-grub"):
            boot_cmds.append("update-grub 2>/dev/null")

        if self._has_cmd("grub2-mkconfig"):
            for tgt in grub_targets:
                boot_cmds.append(f"grub2-mkconfig -o {shlex.quote(tgt)} 2>/dev/null")

        if self._has_cmd("grub-mkconfig"):
            for tgt in grub_targets:
                boot_cmds.append(f"grub-mkconfig -o {shlex.quote(tgt)} 2>/dev/null")

        if self._has_cmd("bootctl"):
            boot_cmds.append("bootctl status 2>/dev/null || true")
            boot_cmds.append("bootctl update 2>/dev/null || true")

        if boot_cmds:
            for c in boot_cmds:
                self._sh(c, allow_fail=True)
        else:
            self.report.warnings.append("bootloader: no grub/bootctl tooling detected; skipping")

        self.logger.info("Live regen done (id=%s family=%s).", did, fam)

    def postcheck_grubcfg(self) -> None:
        stable = (self.report.stable_root or "").strip()
        if not stable:
            return

        token = f"root={stable}"
        candidates = self._detect_grub_cfg_targets()
        found_any = False

        for p in candidates:
            if not self._remote_exists(p):
                continue
            txt = self._read_remote_file(p)
            if token in txt or stable in txt:
                found_any = True
                break

        if not found_any:
            msg = (
                f"Postcheck: stable root '{stable}' not found in grub.cfg "
                f"(may still be OK; grub may source /etc/default/grub or BLS at boot)."
            )
            self.logger.warning(msg)
            self.report.warnings.append(msg)

    # ---------------------------
    # main entry
    # ---------------------------

    def run(self) -> Dict[str, Any]:
        U.banner(self.logger, "GRUB fix (SSH)")

        try:
            self._detect_distro()
        except Exception as e:
            self.report.warnings.append(f"distro_detect_failed:{e}")

        removed = 0
        updated = False

        try:
            removed = self.remove_stale_device_map()
        except Exception as e:
            msg = f"remove_device_map_failed:{e}"
            self.logger.warning(msg)
            self.report.warnings.append(msg)

        try:
            updated = self.update_grub_root()
        except Exception as e:
            msg = f"update_grub_root_failed:{e}"
            self.logger.warning(msg)
            self.report.warnings.append(msg)

        # Regen if requested OR if we changed /etc/default/grub (common expectation)
        if self.regen_initramfs or updated:
            try:
                self.regen_initramfs_and_grub()
            except Exception as e:
                msg = f"regen_failed:{e}"
                self.logger.warning(msg)
                self.report.warnings.append(msg)

        try:
            self.postcheck_grubcfg()
        except Exception as e:
            self.logger.debug("Postcheck failed: %s", e)

        self.logger.info("GRUB fix: removed_device_maps=%d, updated_grub_root=%s", removed, updated)
        self.logger.info("GRUB fix completed.")

        return {
            "distro_id": self.report.distro_id,
            "distro_like": self.report.distro_like,
            "family": self.report.family,
            "root_source": self.report.root_source,
            "root_resolved": self.report.root_resolved,
            "stable_root": self.report.stable_root,
            "removed_device_maps": self.report.removed_device_maps,
            "updated_default_grub": self.report.updated_default_grub,
            "updated_files": self.report.updated_files,
            "commands_ran": self.report.commands_ran,
            "warnings": self.report.warnings,
            "errors": self.report.errors,
            "dry_run": self.dry_run,
        }
