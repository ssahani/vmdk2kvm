from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List

class SSHConfig:
    host: str
    user: str = "root"
    port: int = 22
    identity: Optional[str] = None
    ssh_opt: Optional[List[str]] = None
    sudo: bool = False
