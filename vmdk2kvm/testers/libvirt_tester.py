from __future__ import annotations
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

from ..core.utils import U
import time
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
class LibvirtTest:
    @staticmethod
    def run(logger: logging.Logger, disk: Path, *, name: str, memory_mib: int, vcpus: int, uefi: bool, timeout_s: int, keep: bool, headless: bool) -> None:
        if U.which("virsh") is None:
            U.die(logger, "virsh not found; cannot run libvirt test.", 1)
        ovmf_code = None
        ovmf_vars = None
        if uefi:
            for p in ["/usr/share/OVMF/OVMF_CODE.fd", "/usr/share/edk2/ovmf/OVMF_CODE.fd", "/usr/share/qemu/OVMF_CODE.fd"]:
                if os.path.exists(p):
                    ovmf_code = p
                    break
            for p in ["/usr/share/OVMF/OVMF_VARS.fd", "/usr/share/edk2/ovmf/OVMF_VARS.fd", "/usr/share/qemu/OVMF_VARS.fd"]:
                if os.path.exists(p):
                    ovmf_vars = p
                    break
            if not ovmf_code or not ovmf_vars:
                U.die(logger, "UEFI requested but OVMF not found.", 1)
        nvram = None
        if uefi:
            nvram = disk.parent / f"{name}.VARS.fd"
            if not nvram.exists():
                U.run_cmd(logger, ["cp", "-f", ovmf_vars, str(nvram)], check=True, capture=False)
        gfx = "" if not headless else "<graphics type='none'/>"
        os_xml = f"""
  <os>
    <type arch='x86_64' machine='q35'>hvm</type>
    {"<loader readonly='yes' type='pflash'>%s</loader>" % ovmf_code if uefi else ""}
    {"<nvram>%s</nvram>" % nvram if uefi else ""}
  </os>
""".strip()
        xml = f"""<domain type='kvm'>
  <name>{name}</name>
  <memory unit='MiB'>{memory_mib}</memory>
  <vcpu>{vcpus}</vcpu>
{os_xml}
  <features><acpi/><apic/></features>
  <cpu mode='host-passthrough'/>
  <devices>
    <disk type='file' device='disk'>
      <driver name='qemu' type='{disk.suffix.lstrip(".") or "qcow2"}'/>
      <source file='{disk}'/>
      <target dev='vda' bus='virtio'/>
    </disk>
    <interface type='network'><source network='default'/><model type='virtio'/></interface>
    <console type='pty'/>
    {gfx}
  </devices>
</domain>
"""
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".xml") as f:
            f.write(xml)
            xml_path = Path(f.name)
        U.banner(logger, "Libvirt smoke test")
        logger.info(f"Domain: {name}")
        try:
            U.run_cmd(logger, ["virsh", "destroy", name], check=False, capture=True)
            U.run_cmd(logger, ["virsh", "undefine", name, "--nvram"], check=False, capture=True)
            U.run_cmd(logger, ["virsh", "define", str(xml_path)], check=True, capture=True)
            U.run_cmd(logger, ["virsh", "start", name], check=True, capture=True)
            t0 = time.time()
            with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
                task = progress.add_task("Waiting for domain start", total=timeout_s)
                while True:
                    st = U.run_cmd(logger, ["virsh", "domstate", name], check=True, capture=True).stdout.strip().lower()
                    if "running" in st:
                        logger.info("Domain reached RUNNING state.")
                        break
                    if time.time() - t0 > timeout_s:
                        U.die(logger, f"Timeout waiting for domain to run (state={st})", 1)
                    time.sleep(1)
                    progress.update(task, advance=1)
        finally:
            if not keep:
                U.run_cmd(logger, ["virsh", "destroy", name], check=False, capture=True)
                U.run_cmd(logger, ["virsh", "undefine", name, "--nvram"], check=False, capture=True)
                logger.info("Cleaned up libvirt domain.")
            U.safe_unlink(xml_path)
