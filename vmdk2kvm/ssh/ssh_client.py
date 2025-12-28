from __future__ import annotations
import shlex
from pathlib import Path
from typing import List, Optional

from ..core.utils import U
from .ssh_config import SSHConfig
import logging
class SSHClient:
    def __init__(self, logger: logging.Logger, cfg: SSHConfig):
        self.logger = logger
        self.cfg = cfg
        self.use_rsync = U.which("rsync") is not None
    def _common(self) -> List[str]:
        opts: List[str] = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=accept-new"]
        if self.cfg.identity:
            opts += ["-i", self.cfg.identity]
        if self.cfg.ssh_opt:
            opts += self.cfg.ssh_opt
        return opts
    def _ssh_args(self) -> List[str]:
        return ["-p", str(self.cfg.port)] + self._common()
    def _scp_args(self) -> List[str]:
        return ["-P", str(self.cfg.port), "-p"] + self._common()
    def _rsync_args(self) -> List[str]:
        shell_parts = ["ssh"] + ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new"]
        if self.cfg.port != 22:
            shell_parts += ["-p", str(self.cfg.port)]
        if self.cfg.identity:
            shell_parts += ["-i", self.cfg.identity]
        if self.cfg.ssh_opt:
            shell_parts += self.cfg.ssh_opt
        return ["-avz", "--progress", "-e", " ".join(shlex.quote(x) for x in shell_parts)]
    def _maybe_sudo(self, cmd: str) -> str:
        if not self.cfg.sudo:
            return cmd
        return f"sudo -n sh -lc {shlex.quote(cmd)}"
    def ssh(self, cmd: str, *, capture: bool = True, timeout: Optional[int] = None) -> str:
        cmd = self._maybe_sudo(cmd)
        argv = ["ssh"] + self._ssh_args() + [f"{self.cfg.user}@{self.cfg.host}", cmd]
        cp = U.run_cmd(self.logger, argv, check=True, capture=capture, timeout=timeout)
        return (cp.stdout or "").strip()
    def check(self) -> None:
        out = self.ssh("echo OK", timeout=10).strip()
        if out != "OK":
            U.die(self.logger, f"SSH connectivity check failed: {out!r}", 1)
        self.logger.debug("SSH connectivity OK")
    def scp_from(self, remote: str, local: Path) -> None:
        U.ensure_dir(local.parent)
        if self.use_rsync:
            argv = ["rsync"] + self._rsync_args() + [f"{self.cfg.user}@{self.cfg.host}:{remote}", str(local)]
        else:
            argv = ["scp"] + self._scp_args() + [f"{self.cfg.user}@{self.cfg.host}:{remote}", str(local)]
        U.run_cmd(self.logger, argv, check=True, capture=False)
        self.logger.info(f"Copied {remote} -> {local}")
    def scp_to(self, local: Path, remote: str) -> None:
        if self.use_rsync:
            argv = ["rsync"] + self._rsync_args() + [str(local), f"{self.cfg.user}@{self.cfg.host}:{remote}"]
        else:
            argv = ["scp"] + self._scp_args() + [str(local), f"{self.cfg.user}@{self.cfg.host}:{remote}"]
        U.run_cmd(self.logger, argv, check=True, capture=False)
        self.logger.info(f"Copied {local} -> {remote}")
    def exists(self, remote: str) -> bool:
        out = self.ssh(f"test -e {shlex.quote(remote)} && echo OK || echo NO")
        return out.strip() == "OK"
