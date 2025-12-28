from __future__ import annotations

import logging
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional, Sequence

from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from ..core.utils import U

# ----------------------------
# Types / Config
# ----------------------------

GraphicsMode = Literal["none", "vnc", "spice"]
MachineType = Literal["pc", "q35"]


@dataclass(frozen=True)
class FirmwareConfig:
    """
    Firmware selection.
      - uefi=False => BIOS
      - uefi=True  => UEFI via OVMF (CODE+VARS)
    """
    uefi: bool = False


@dataclass(frozen=True)
class GraphicsConfig:
    """
    libvirt graphics:
      - none  => no display device
      - vnc   => VNC server (good for headless servers)
      - spice => SPICE server
    """
    mode: GraphicsMode = "none"
    listen: str = "127.0.0.1"
    autoport: bool = True
    port: Optional[int] = None
    passwd: Optional[str] = None
    keymap: Optional[str] = None


@dataclass(frozen=True)
class VideoConfig:
    """
    Video device config (only meaningful when graphics != none).
    Common models: qxl, virtio, vga, cirrus, bochs.
    """
    enabled: bool = False
    model: str = "qxl"
    vram: int = 65536


@dataclass(frozen=True)
class InputConfig:
    """
    Input devices. USB tablet is a big quality-of-life improvement for VNC/SPICE.
    """
    usb_tablet: bool = False


@dataclass(frozen=True)
class DomainConfig:
    """
    Domain “shape” and runtime knobs.
    """
    name: str
    memory_mib: int = 2048
    vcpus: int = 2
    machine: MachineType = "q35"
    network: str = "default"
    timeout_s: int = 60
    keep: bool = False


@dataclass(frozen=True)
class OVMFPaths:
    code: str
    vars: str


# ----------------------------
# Implementation
# ----------------------------

class LibvirtTest:
    """
    Libvirt “smoke test” runner:
      - define XML
      - start domain
      - wait for RUNNING
      - optional cleanup (destroy + undefine)
    """

    # Prefer these common distro paths, but be generous.
    _OVMF_CODE_CANDIDATES: Sequence[str] = (
        "/usr/share/OVMF/OVMF_CODE.fd",
        "/usr/share/edk2/ovmf/OVMF_CODE.fd",
        "/usr/share/qemu/OVMF_CODE.fd",
        "/usr/share/edk2/x64/OVMF_CODE.fd",
        "/usr/share/edk2/ovmf/OVMF_CODE.secboot.fd",
        "/usr/share/edk2/x64/OVMF_CODE.secboot.fd",
    )
    _OVMF_VARS_CANDIDATES: Sequence[str] = (
        "/usr/share/OVMF/OVMF_VARS.fd",
        "/usr/share/edk2/ovmf/OVMF_VARS.fd",
        "/usr/share/qemu/OVMF_VARS.fd",
        "/usr/share/edk2/x64/OVMF_VARS.fd",
        "/usr/share/edk2/ovmf/OVMF_VARS.secboot.fd",
        "/usr/share/edk2/x64/OVMF_VARS.secboot.fd",
    )

    @staticmethod
    def run(
        logger: logging.Logger,
        disk: Path,
        *,
        name: str,
        memory_mib: int,
        vcpus: int,
        uefi: bool,
        timeout_s: int,
        keep: bool,
        headless: bool,
        # Optional upgrades (safe defaults):
        machine: MachineType = "q35",
        network: str = "default",
        graphics_mode: Optional[GraphicsMode] = None,  # default derived from headless
        listen: str = "127.0.0.1",
        video_model: str = "qxl",
        video_vram: int = 65536,
        usb_tablet: bool = True,
        spice: bool = False,  # legacy-friendly switch: prefer SPICE over VNC if GUI
    ) -> None:
        """
        Backward-compatible signature with extra knobs.

        Typical GUI (like your working XML):
          uefi=False, machine="pc", headless=False (or graphics_mode="vnc"),
          video_model="qxl", usb_tablet=True.

        Typical headless:
          headless=True, graphics_mode="none".
        """
        if U.which("virsh") is None:
            U.die(logger, "virsh not found; cannot run libvirt test.", 1)

        disk = Path(disk)
        if not disk.exists():
            U.die(logger, f"Disk not found: {disk}", 1)

        fw = FirmwareConfig(uefi=uefi)
        dom = DomainConfig(
            name=name,
            memory_mib=memory_mib,
            vcpus=vcpus,
            machine=machine,
            network=network,
            timeout_s=timeout_s,
            keep=keep,
        )

        # Derive graphics defaults: your original code used headless => none.
        if graphics_mode is None:
            if headless:
                gm: GraphicsMode = "none"
            else:
                gm = "spice" if spice else "vnc"
        else:
            gm = graphics_mode

        gfx = GraphicsConfig(mode=gm, listen=listen, autoport=True)
        vid = VideoConfig(enabled=(gm != "none"), model=video_model, vram=video_vram)
        inp = InputConfig(usb_tablet=(usb_tablet and gm != "none"))

        ovmf = LibvirtTest._resolve_ovmf(logger, fw)
        nvram = LibvirtTest._prepare_nvram(logger, disk, dom.name, fw, ovmf)

        xml = LibvirtTest._build_domain_xml(
            logger=logger,
            disk=disk,
            dom=dom,
            fw=fw,
            ovmf=ovmf,
            nvram=nvram,
            gfx=gfx,
            vid=vid,
            inp=inp,
        )

        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".xml") as f:
            f.write(xml)
            xml_path = Path(f.name)

        U.banner(logger, "Libvirt smoke test")
        logger.info(f"Domain: {dom.name}")
        logger.info(f"Disk: {disk}")
        logger.info(f"Machine: {dom.machine} | Firmware: {'UEFI' if fw.uefi else 'BIOS'} | Graphics: {gfx.mode}")

        try:
            LibvirtTest._cleanup_domain(logger, dom.name)
            U.run_cmd(logger, ["virsh", "define", str(xml_path)], check=True, capture=True)
            U.run_cmd(logger, ["virsh", "start", dom.name], check=True, capture=True)
            LibvirtTest._wait_running(logger, dom.name, dom.timeout_s)
        finally:
            if not dom.keep:
                LibvirtTest._cleanup_domain(logger, dom.name)
                logger.info("Cleaned up libvirt domain.")
            U.safe_unlink(xml_path)

    # ----------------------------
    # Helpers
    # ----------------------------

    @staticmethod
    def _resolve_ovmf(logger: logging.Logger, fw: FirmwareConfig) -> Optional[OVMFPaths]:
        if not fw.uefi:
            return None

        code = next((p for p in LibvirtTest._OVMF_CODE_CANDIDATES if os.path.exists(p)), None)
        vars_ = next((p for p in LibvirtTest._OVMF_VARS_CANDIDATES if os.path.exists(p)), None)
        if not code or not vars_:
            U.die(logger, "UEFI requested but OVMF not found (CODE/VARS missing).", 1)
        return OVMFPaths(code=code, vars=vars_)

    @staticmethod
    def _prepare_nvram(
        logger: logging.Logger,
        disk: Path,
        name: str,
        fw: FirmwareConfig,
        ovmf: Optional[OVMFPaths],
    ) -> Optional[Path]:
        if not fw.uefi:
            return None
        assert ovmf is not None
        nvram = disk.parent / f"{name}.VARS.fd"
        if not nvram.exists():
            U.run_cmd(logger, ["cp", "-f", ovmf.vars, str(nvram)], check=True, capture=False)
        return nvram

    @staticmethod
    def _disk_format(logger: logging.Logger, disk: Path) -> str:
        # Prefer explicit suffix; if unknown, try qemu-img info; else assume qcow2.
        suf = disk.suffix.lower().lstrip(".")
        if suf in ("qcow2", "raw", "vmdk", "vdi"):
            return suf

        if U.which("qemu-img"):
            try:
                # keep dependencies light (no json parse). Extract "format" from output.
                out = U.run_cmd(logger, ["qemu-img", "info", "--output=json", str(disk)], check=True, capture=True).stdout
                key = '"format"'
                i = out.find(key)
                if i != -1:
                    seg = out[i : i + 120]
                    c = seg.find(":")
                    q1 = seg.find('"', c + 1)
                    q2 = seg.find('"', q1 + 1)
                    if c != -1 and q1 != -1 and q2 != -1:
                        fmt = seg[q1 + 1 : q2].strip().lower()
                        if fmt:
                            return fmt
            except Exception:
                pass

        return "qcow2"

    @staticmethod
    def _graphics_xml(gfx: GraphicsConfig) -> str:
        if gfx.mode == "none":
            return ""  # omit entirely

        attrs = [f"type='{gfx.mode}'", "autoport='yes'", f"listen='{gfx.listen}'"]
        if not gfx.autoport:
            attrs = [f"type='{gfx.mode}'", "autoport='no'"]
            if gfx.port is None:
                raise ValueError("graphics.autoport=False requires graphics.port")
            attrs.append(f"port='{int(gfx.port)}'")
            attrs.append(f"listen='{gfx.listen}'")

        if gfx.passwd:
            attrs.append(f"passwd='{gfx.passwd}'")
        if gfx.keymap:
            attrs.append(f"keymap='{gfx.keymap}'")
        return f"    <graphics {' '.join(attrs)}/>"

    @staticmethod
    def _video_xml(vid: VideoConfig, gfx: GraphicsConfig) -> str:
        if not vid.enabled or gfx.mode == "none":
            return ""
        return f"""    <video>
      <model type='{vid.model}' vram='{int(vid.vram)}'/>
    </video>"""

    @staticmethod
    def _input_xml(inp: InputConfig, gfx: GraphicsConfig) -> str:
        if gfx.mode == "none":
            return ""
        if inp.usb_tablet:
            return "    <input type='tablet' bus='usb'/>"
        return ""

    @staticmethod
    def _build_domain_xml(
        *,
        logger: logging.Logger,
        disk: Path,
        dom: DomainConfig,
        fw: FirmwareConfig,
        ovmf: Optional[OVMFPaths],
        nvram: Optional[Path],
        gfx: GraphicsConfig,
        vid: VideoConfig,
        inp: InputConfig,
    ) -> str:
        disk_fmt = LibvirtTest._disk_format(logger, disk)

        # BIOS domains benefit from explicit boot dev.
        # Your working XML uses machine='pc' + <boot dev='hd'/>.
        os_bits = [
            "  <os>",
            f"    <type arch='x86_64' machine='{dom.machine}'>hvm</type>",
        ]
        if fw.uefi:
            assert ovmf is not None and nvram is not None
            os_bits.append(f"    <loader readonly='yes' type='pflash'>{ovmf.code}</loader>")
            os_bits.append(f"    <nvram>{nvram}</nvram>")
        else:
            os_bits.append("    <boot dev='hd'/>")
        os_bits.append("  </os>")
        os_xml = "\n".join(os_bits)

        gfx_xml = LibvirtTest._graphics_xml(gfx)
        vid_xml = LibvirtTest._video_xml(vid, gfx)
        inp_xml = LibvirtTest._input_xml(inp, gfx)

        # Keep it simple and robust: virtio disk + virtio net + pty console always.
        # (You can add serial/channel later if you want cloud-init style access.)
        xml = f"""<domain type='kvm'>
  <name>{dom.name}</name>

  <memory unit='MiB'>{dom.memory_mib}</memory>
  <vcpu>{dom.vcpus}</vcpu>

{os_xml}

  <features>
    <acpi/>
    <apic/>
  </features>

  <cpu mode='host-passthrough'/>

  <devices>
    <disk type='file' device='disk'>
      <driver name='qemu' type='{disk_fmt}'/>
      <source file='{disk}'/>
      <target dev='vda' bus='virtio'/>
    </disk>

    <interface type='network'>
      <source network='{dom.network}'/>
      <model type='virtio'/>
    </interface>

    <console type='pty'/>
{gfx_xml}
{vid_xml}
{inp_xml}
  </devices>
</domain>
"""
        return xml

    @staticmethod
    def _cleanup_domain(logger: logging.Logger, name: str) -> None:
        # Be liberal: different libvirt versions differ on --nvram support.
        U.run_cmd(logger, ["virsh", "destroy", name], check=False, capture=True)
        U.run_cmd(logger, ["virsh", "undefine", name, "--nvram"], check=False, capture=True)
        U.run_cmd(logger, ["virsh", "undefine", name], check=False, capture=True)

    @staticmethod
    def _wait_running(logger: logging.Logger, name: str, timeout_s: int) -> None:
        t0 = time.time()
        with Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task("Waiting for domain start", total=timeout_s)
            while True:
                st = U.run_cmd(logger, ["virsh", "domstate", name], check=True, capture=True).stdout.strip().lower()
                if "running" in st:
                    logger.info("Domain reached RUNNING state.")
                    return
                if time.time() - t0 > timeout_s:
                    U.die(logger, f"Timeout waiting for domain to run (state={st})", 1)
                time.sleep(1)
                progress.update(task, advance=1)
