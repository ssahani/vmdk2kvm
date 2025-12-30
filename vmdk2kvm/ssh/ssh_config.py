from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


def _is_probably_ipv6(host: str) -> bool:
    # cheap, good-enough heuristic (no ipaddress import needed)
    return ":" in (host or "")


def _scp_host(host: str) -> str:
    # scp/rsync need [v6] bracket form
    h = (host or "").strip()
    if _is_probably_ipv6(h) and not (h.startswith("[") and h.endswith("]")):
        return f"[{h}]"
    return h


@dataclass(frozen=True)
class SSHConfig:
    """
    Canonical SSH connection configuration.

    Designed for:
      - ESXi access
      - remote image fixes
      - smoke tests
      - automation / CI (non-interactive)
    """
    host: str
    user: str = "root"
    port: int = 22
    identity: Optional[Path] = None
    ssh_opts: List[str] = field(default_factory=list)

    # behavior
    sudo: bool = False
    connect_timeout: int = 10
    keepalive_interval: int = 30
    keepalive_count: int = 3

    # advanced
    jump_host: Optional[str] = None          # ProxyJump
    strict_host_key_checking: bool = False   # automation-safe default

    # --- additive: explicit non-interactive behavior (CI-safe) ---
    batch_mode: bool = True                  # never prompt for passwords/passphrases
    request_tty: bool = False                # True if you need remote TTY (rare)

    # --- additive: host key policy knobs ---
    known_hosts_file: Optional[Path] = None  # None => /dev/null when strict_host_key_checking=False
    accept_new_host_keys: bool = False       # OpenSSH 8.0+: StrictHostKeyChecking=accept-new

    # --- additive: performance / multiplexing knobs ---
    control_master: bool = False             # enable connection reuse
    control_path: Optional[Path] = None      # where to store control socket
    control_persist_s: int = 60              # keep master open for N seconds

    # -------------------------
    # Normalization & validation
    # -------------------------

    def __post_init__(self) -> None:
        host = (self.host or "").strip()
        if not host:
            raise ValueError("SSHConfig.host must not be empty")
        object.__setattr__(self, "host", host)

        user = (self.user or "").strip()
        if not user:
            raise ValueError("SSHConfig.user must not be empty")
        object.__setattr__(self, "user", user)

        if self.identity is not None:
            p = Path(self.identity).expanduser()
            object.__setattr__(self, "identity", p)

        if self.known_hosts_file is not None:
            kh = Path(self.known_hosts_file).expanduser()
            object.__setattr__(self, "known_hosts_file", kh)

        if self.jump_host is not None:
            j = (self.jump_host or "").strip()
            object.__setattr__(self, "jump_host", j or None)

        # normalize ssh_opts: strip empties, preserve order, de-dup
        if self.ssh_opts:
            cleaned: List[str] = []
            seen = set()
            for opt in self.ssh_opts:
                o = (opt or "").strip()
                if not o:
                    continue
                # keep stable order; de-dup exact strings
                if o not in seen:
                    cleaned.append(o)
                    seen.add(o)
            object.__setattr__(self, "ssh_opts", cleaned)

        if self.port <= 0 or self.port > 65535:
            raise ValueError(f"Invalid SSH port: {self.port}")

        for name, v in (
            ("connect_timeout", self.connect_timeout),
            ("keepalive_interval", self.keepalive_interval),
            ("keepalive_count", self.keepalive_count),
            ("control_persist_s", self.control_persist_s),
        ):
            if v < 0:
                raise ValueError(f"{name} must be >= 0 (got {v})")

        # accept-new only makes sense when strict_host_key_checking=True-ish
        # We won't error; we just avoid generating conflicting flags later.

    # -------------------------
    # Rendering helpers
    # -------------------------

    def target(self) -> str:
        """
        user@host form (ssh). (For scp/rsync use scp_target()).
        """
        return f"{self.user}@{self.host}"

    def scp_target(self) -> str:
        """
        user@[host] form suitable for scp/rsync when host might be IPv6.
        """
        return f"{self.user}@{_scp_host(self.host)}"

    def base_cmd(self) -> List[str]:
        """
        Base SSH command suitable for subprocess (no shell).
        """
        cmd: List[str] = [
            "ssh",
            "-p", str(self.port),
            "-o", f"ConnectTimeout={self.connect_timeout}",
            "-o", f"ServerAliveInterval={self.keepalive_interval}",
            "-o", f"ServerAliveCountMax={self.keepalive_count}",
        ]

        if self.batch_mode:
            cmd += ["-o", "BatchMode=yes"]

        if self.request_tty:
            cmd += ["-tt"]

        # Host key policy
        if self.strict_host_key_checking:
            if self.accept_new_host_keys:
                # OpenSSH supports this; if older ssh, it's ignored as unknown value? (actually it errors)
                # So we only emit it when explicitly asked.
                cmd += ["-o", "StrictHostKeyChecking=accept-new"]
            else:
                cmd += ["-o", "StrictHostKeyChecking=yes"]
        else:
            cmd += ["-o", "StrictHostKeyChecking=no"]

        if self.known_hosts_file is not None:
            cmd += ["-o", f"UserKnownHostsFile={self.known_hosts_file}"]
        else:
            # Keep your automation default: don't pollute the runner/user known_hosts
            if not self.strict_host_key_checking:
                cmd += ["-o", "UserKnownHostsFile=/dev/null"]

        if self.identity:
            cmd += ["-i", str(self.identity)]

        if self.jump_host:
            cmd += ["-J", self.jump_host]

        # Multiplexing (big win for repeated SSH ops)
        if self.control_master:
            cmd += ["-o", "ControlMaster=auto"]
            cmd += ["-o", f"ControlPersist={self.control_persist_s}s"]
            if self.control_path:
                cmd += ["-o", f"ControlPath={self.control_path}"]
            else:
                # Safe-ish default; users can override for very long paths
                cmd += ["-o", "ControlPath=~/.ssh/cm-%r@%h:%p"]

        for opt in self.ssh_opts:
            cmd += ["-o", opt]

        cmd.append(self.target())
        return cmd

    def remote_cmd(self, argv: List[str]) -> List[str]:
        """
        Build full SSH command executing a remote command.
        """
        if self.sudo:
            argv = ["sudo", "-n", "--"] + argv

        return self.base_cmd() + ["--"] + argv

    def scp_src(self, remote_path: str) -> str:
        """
        user@host:/path form for scp / rsync.
        """
        return f"{self.scp_target()}:{remote_path}"

    def scp_base_cmd(self) -> List[str]:
        """
        Base scp command (port + identity + options mirrored from SSH where relevant).
        Note: scp uses -P for port (not -p).
        """
        cmd: List[str] = ["scp", "-P", str(self.port)]
        if self.identity:
            cmd += ["-i", str(self.identity)]
        if self.batch_mode:
            cmd += ["-o", "BatchMode=yes"]

        # mirror host key policy
        if self.strict_host_key_checking:
            if self.accept_new_host_keys:
                cmd += ["-o", "StrictHostKeyChecking=accept-new"]
            else:
                cmd += ["-o", "StrictHostKeyChecking=yes"]
        else:
            cmd += ["-o", "StrictHostKeyChecking=no"]

        if self.known_hosts_file is not None:
            cmd += ["-o", f"UserKnownHostsFile={self.known_hosts_file}"]
        else:
            if not self.strict_host_key_checking:
                cmd += ["-o", "UserKnownHostsFile=/dev/null"]

        if self.jump_host:
            cmd += ["-o", f"ProxyJump={self.jump_host}"]

        for opt in self.ssh_opts:
            cmd += ["-o", opt]

        return cmd

    # -------------------------
    # Convenience helpers
    # -------------------------

    def describe(self) -> str:
        """
        Human-readable description (safe for logs).
        """
        parts = [f"{self.user}@{self.host}:{self.port}"]

        if self.identity:
            parts.append(f"key={self.identity}")

        if self.jump_host:
            parts.append(f"via={self.jump_host}")

        if self.sudo:
            parts.append("sudo")

        if self.batch_mode:
            parts.append("batch")

        if self.strict_host_key_checking:
            parts.append("hostkey=strict")
        else:
            parts.append("hostkey=off")

        if self.control_master:
            parts.append("mux")

        return " ".join(parts)
