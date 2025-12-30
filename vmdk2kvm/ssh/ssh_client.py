from __future__ import annotations

import shlex
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Union

import logging

from ..core.utils import U
from .ssh_config import SSHConfig


@dataclass(frozen=True)
class SSHResult:
    rc: int
    stdout: str
    stderr: str
    argv: List[str]
    seconds: float


class SSHClient:
    """
    Minimal, production-safe SSH helper.

    Improvements vs original:
      - Proper quoting of remote command (avoids "double parsing" surprises)
      - Configurable connect timeout + strict host key policy
      - Better rsync/scp defaults (preserve times, partial transfers)
      - Support for retries (useful when ESXi/vCenter/host is slow to respond)
      - exists() uses POSIX-safe quoting and avoids accidental globbing
      - Optional non-throwing run() that returns rc/stdout/stderr
    """

    def __init__(self, logger: logging.Logger, cfg: SSHConfig):
        self.logger = logger
        self.cfg = cfg
        self.use_rsync = U.which("rsync") is not None

        # Defaults (can be extended via cfg.ssh_opt)
        self._connect_timeout = getattr(cfg, "connect_timeout", 10)
        self._server_alive_interval = getattr(cfg, "server_alive_interval", 10)
        self._server_alive_count = getattr(cfg, "server_alive_count", 3)
        self._strict_host_key = getattr(cfg, "strict_host_key_checking", "accept-new")  # accept-new | yes | no

        # Retry policy (optional)
        self._retries = int(getattr(cfg, "retries", 0) or 0)
        self._retry_sleep = float(getattr(cfg, "retry_sleep", 1.0) or 1.0)

    # ----------------------------
    # argv builders
    # ----------------------------

    def _common(self) -> List[str]:
        opts: List[str] = [
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={self._connect_timeout}",
            "-o",
            f"ServerAliveInterval={self._server_alive_interval}",
            "-o",
            f"ServerAliveCountMax={self._server_alive_count}",
            "-o",
            f"StrictHostKeyChecking={self._strict_host_key}",
        ]

        # Optional: keep known_hosts separate per tool-run if user config provides it
        known_hosts = getattr(self.cfg, "known_hosts_file", None)
        if known_hosts:
            opts += ["-o", f"UserKnownHostsFile={known_hosts}"]

        if self.cfg.identity:
            opts += ["-i", self.cfg.identity]

        if self.cfg.ssh_opt:
            opts += list(self.cfg.ssh_opt)

        return opts

    def _ssh_args(self) -> List[str]:
        return ["-p", str(self.cfg.port)] + self._common()

    def _scp_args(self) -> List[str]:
        # -p preserves times; add -q? (no, keep verbosity in logs)
        return ["-P", str(self.cfg.port), "-p"] + self._common()

    def _rsync_args(self) -> List[str]:
        # rsync over ssh; prefer partial + inplace for large images; keep progress
        # -a preserves perms/times/links, -H hardlinks, -x one filesystem
        shell_parts: List[str] = ["ssh"] + self._common()
        # NOTE: _common already includes ConnectTimeout, etc. but also includes -o StrictHostKeyChecking
        # We still must pass port via ssh -p (rsync doesn't use ssh's -p unless in -e string)
        if self.cfg.port != 22:
            shell_parts += ["-p", str(self.cfg.port)]

        # rsync flags:
        return [
            "-a",
            "-H",
            "--numeric-ids",
            "--info=progress2",
            "--partial",
            "--inplace",
            "-e",
            " ".join(shlex.quote(x) for x in shell_parts),
        ]

    # ----------------------------
    # command helpers
    # ----------------------------

    def _target(self) -> str:
        return f"{self.cfg.user}@{self.cfg.host}"

    def _maybe_sudo(self, cmd: str) -> str:
        if not getattr(self.cfg, "sudo", False):
            return cmd

        # Use -- to stop sudo from parsing args, then run a single sh -lc payload.
        # If sudo isn't allowed, sudo -n fails fast (good).
        return f"sudo -n -- sh -lc {shlex.quote(cmd)}"

    def _remote_sh(self, cmd: str) -> str:
        """
        Wrap remote command so it runs under POSIX sh -lc with proper quoting.
        This prevents issues where ssh remote gets a command with spaces/quotes,
        and different shells interpret it oddly.
        """
        # If the user already passed something that is shell-specific, we still run via sh -lc.
        return f"sh -lc {shlex.quote(cmd)}"

    def _run(
        self,
        argv: Sequence[str],
        *,
        check: bool,
        capture: bool,
        timeout: Optional[int],
    ) -> SSHResult:
        t0 = time.monotonic()
        cp = U.run_cmd(self.logger, list(argv), check=check, capture=capture, timeout=timeout)
        dt = time.monotonic() - t0
        return SSHResult(
            rc=int(getattr(cp, "returncode", 0) or 0),
            stdout=(cp.stdout or "") if capture else "",
            stderr=(cp.stderr or "") if capture else "",
            argv=list(argv),
            seconds=dt,
        )

    # ----------------------------
    # public API
    # ----------------------------

    def run(
        self,
        cmd: str,
        *,
        capture: bool = True,
        timeout: Optional[int] = None,
        check: bool = True,
    ) -> SSHResult:
        """
        Run a command on the remote host.

        - Uses sh -lc quoting to avoid remote shell gotchas
        - Optional retries (cfg.retries)
        - Returns SSHResult with rc/stdout/stderr/duration
        """
        raw = self._maybe_sudo(cmd)
        remote = self._remote_sh(raw)

        argv = ["ssh"] + self._ssh_args() + [self._target(), remote]

        last_err: Optional[Exception] = None
        attempts = 1 + self._retries
        for attempt in range(1, attempts + 1):
            try:
                res = self._run(argv, check=check, capture=capture, timeout=timeout)
                return res
            except Exception as e:
                last_err = e
                if attempt >= attempts:
                    raise
                self.logger.warning(f"SSH attempt {attempt}/{attempts} failed; retrying in {self._retry_sleep:.1f}s: {e}")
                time.sleep(self._retry_sleep)

        # unreachable, but keeps mypy happy
        raise last_err  # type: ignore[misc]

    def ssh(self, cmd: str, *, capture: bool = True, timeout: Optional[int] = None) -> str:
        """Backwards-compatible: returns stdout string, raises on failure."""
        res = self.run(cmd, capture=capture, timeout=timeout, check=True)
        return res.stdout.strip()

    def check(self) -> None:
        out = self.ssh("echo OK", timeout=10).strip()
        if out != "OK":
            U.die(self.logger, f"SSH connectivity check failed: {out!r}", 1)
        self.logger.debug("SSH connectivity OK")

    def scp_from(self, remote: str, local: Path) -> None:
        U.ensure_dir(local.parent)
        remote_spec = f"{self._target()}:{remote}"

        if self.use_rsync:
            argv = ["rsync"] + self._rsync_args() + [remote_spec, str(local)]
        else:
            argv = ["scp"] + self._scp_args() + [remote_spec, str(local)]

        self._run(argv, check=True, capture=False, timeout=None)
        self.logger.info(f"Copied {remote} -> {local}")

    def scp_to(self, local: Path, remote: str) -> None:
        remote_spec = f"{self._target()}:{remote}"

        if self.use_rsync:
            argv = ["rsync"] + self._rsync_args() + [str(local), remote_spec]
        else:
            argv = ["scp"] + self._scp_args() + [str(local), remote_spec]

        self._run(argv, check=True, capture=False, timeout=None)
        self.logger.info(f"Copied {local} -> {remote}")

    def exists(self, remote: str) -> bool:
        # Use POSIX test with proper quoting; avoid echo parsing surprises.
        # Print "1" or "0" reliably.
        q = shlex.quote(remote)
        res = self.run(f"test -e {q} && printf 1 || printf 0", capture=True, timeout=10, check=True)
        return res.stdout.strip() == "1"

    def is_file(self, remote: str) -> bool:
        q = shlex.quote(remote)
        res = self.run(f"test -f {q} && printf 1 || printf 0", capture=True, timeout=10, check=True)
        return res.stdout.strip() == "1"

    def is_dir(self, remote: str) -> bool:
        q = shlex.quote(remote)
        res = self.run(f"test -d {q} && printf 1 || printf 0", capture=True, timeout=10, check=True)
        return res.stdout.strip() == "1"

    def mkdir_p(self, remote_dir: str) -> None:
        q = shlex.quote(remote_dir)
        self.run(f"mkdir -p {q}", capture=False, timeout=30, check=True)

    def rm_rf(self, remote_path: str) -> None:
        q = shlex.quote(remote_path)
        self.run(f"rm -rf -- {q}", capture=False, timeout=60, check=True)

    def read_text(self, remote: str, *, max_bytes: int = 4 * 1024 * 1024) -> str:
        """
        Read remote file content safely (bounded).
        Uses head -c to avoid slurping multi-GB files.
        """
        q = shlex.quote(remote)
        # POSIX-ish: try head -c; fallback to dd if needed
        cmd = f"(head -c {int(max_bytes)} {q} 2>/dev/null || dd if={q} bs=1 count={int(max_bytes)} 2>/dev/null) || true"
        return self.ssh(cmd, timeout=30).rstrip("\n")
