from __future__ import annotations

from pathlib import Path
from typing import Any

# Keep this template in one place so both CLI help and the generator use the same text.
SYSTEMD_UNIT_TEMPLATE = """
[Unit]
Description=vmdk2kvm Daemon
After=network.target
[Service]
ExecStart=/usr/bin/python3 /path/to/vmdk2kvm.py --daemon --watch-dir=/path/to/watch --config=/path/to/config.yaml
Restart=always
User=root
Group=root
[Install]
WantedBy=multi-user.target
"""


def generate_systemd_unit(args: Any, logger) -> None:
    """Print or write a sample systemd unit file."""
    unit = SYSTEMD_UNIT_TEMPLATE
    out = getattr(args, "output", None)
    if out:
        Path(out).write_text(unit, encoding="utf-8")
        if logger:
            logger.info(f"Systemd unit written to {out}")
    else:
        print(unit)
