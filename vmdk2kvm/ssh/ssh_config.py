from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


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

    # -------------------------
    # Normalization & validation
    # -------------------------

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("SSHConfig.host must not be empty")

        if self.identity is not None:
            object.__setattr__(self, "identity", Path(self.identity))

        if self.port <= 0 or self.port > 65535:
            raise ValueError(f"Invalid SSH port: {self.port}")

    # -------------------------
    # Rendering helpers
    # -------------------------

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

        if not self.strict_host_key_checking:
            cmd += [
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
            ]

        if self.identity:
            cmd += ["-i", str(self.identity)]

        if self.jump_host:
            cmd += ["-J", self.jump_host]

        for opt in self.ssh_opts:
            cmd += ["-o", opt]

        cmd.append(f"{self.user}@{self.host}")
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
        return f"{self.user}@{self.host}:{remote_path}"

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
        return " ".join(parts)
