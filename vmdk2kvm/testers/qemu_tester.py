from __future__ import annotations
import logging
import os
import subprocess
from pathlib import Path
from typing import Optional

from ..core.utils import U
class QemuTest:
    @staticmethod
    def run(logger: logging.Logger, disk: Path, *, memory_mib: int, vcpus: int, uefi: bool) -> None:
        if U.which("qemu-system-x86_64") is None:
            U.die(logger, "qemu-system-x86_64 not found.", 1)
        cmd = [
            "qemu-system-x86_64",
            "-m", str(memory_mib),
            "-smp", str(vcpus),
            "-enable-kvm",
            "-cpu", "host",
            "-drive", f"file={disk},format=qcow2",
        ]
        if uefi:
            ovmf = None
            for p in ["/usr/share/OVMF/OVMF_CODE.fd", "/usr/share/edk2/ovmf/OVMF_CODE.fd", "/usr/share/qemu/OVMF_CODE.fd"]:
                if os.path.exists(p):
                    ovmf = p
                    break
            if ovmf:
                # ✅ FIXED: correct -bios arg (no accidental leading space)
                cmd += ["-bios", ovmf]
        U.banner(logger, "QEMU smoke test")
        logger.info("Launching QEMU (smoke test; exit code not enforced)…")
        U.run_cmd(logger, cmd, check=False, capture=False)
