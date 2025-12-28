from __future__ import annotations

import logging
import os
import shlex
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional, Sequence

from ..core.utils import U


BootMode = Literal["bios", "uefi"]
DisplayMode = Literal["none", "gtk", "sdl", "vnc"]


@dataclass(frozen=True)
class QemuDisplay:
    """
    Display behavior:
      - none:   headless (-nographic + serial to stdio)
      - gtk/sdl: local GUI (requires a graphical session)
      - vnc:    remote display (safe on headless servers)
    """
    mode: DisplayMode = "none"
    vnc_listen: str = "127.0.0.1"
    vnc_display: int = 1  # :1 => TCP 5901


@dataclass(frozen=True)
class QemuNet:
    """
    User-mode networking (no root needed) by default.
    Optionally add SSH port-forward for easy reachability.
    """
    enabled: bool = True
    ssh_forward_host_port: Optional[int] = 2222  # host:2222 -> guest:22 (if guest runs sshd)


@dataclass(frozen=True)
class QemuMachine:
    """
    Machine & accel knobs.
    """
    machine: str = "q35"
    accel: str = "kvm"  # "kvm" or "tcg"
    cpu: str = "host"


class QemuTest:
    """
    QEMU smoke test runner.

    What “improve” means here:
      - robust qcow2/raw detection (don't hardcode format=qcow2)
      - UEFI done correctly (OVMF_CODE + OVMF_VARS via -drive if=pflash)
      - safe headless default (works over SSH with no XDG_RUNTIME_DIR drama)
      - adds virtio devices + user networking
      - prints a useful VNC hint if you enable VNC
      - optional timeout so it can “smoke” and then stop
    """

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
        memory_mib: int,
        vcpus: int,
        uefi: bool,
        # New knobs (safe defaults):
        display: Optional[QemuDisplay] = None,
        net: Optional[QemuNet] = None,
        machine: Optional[QemuMachine] = None,
        timeout_s: Optional[int] = 20,
        extra_args: Optional[Sequence[str]] = None,
    ) -> None:
        if U.which("qemu-system-x86_64") is None:
            U.die(logger, "qemu-system-x86_64 not found.", 1)

        disk = Path(disk)
        if not disk.exists():
            U.die(logger, f"Disk not found: {disk}", 1)

        display = display or QemuDisplay(mode="none")
        net = net or QemuNet(enabled=True, ssh_forward_host_port=2222)
        machine = machine or QemuMachine(machine="q35", accel="kvm", cpu="host")
        extra_args = list(extra_args or [])

        # accel fallback: if /dev/kvm missing, auto-switch to tcg (slow but works)
        if machine.accel == "kvm" and not os.path.exists("/dev/kvm"):
            logger.warning("/dev/kvm missing; falling back to TCG (no KVM acceleration).")
            machine = QemuMachine(machine=machine.machine, accel="tcg", cpu="max")

        img_fmt = QemuTest._detect_img_format(logger, disk)

        cmd = [
            "qemu-system-x86_64",
            "-machine", f"{machine.machine},accel={machine.accel}",
            "-m", str(int(memory_mib)),
            "-smp", str(int(vcpus)),
            "-cpu", machine.cpu,
            # Use virtio-blk for speed/compat; add discard + cache defaults
            "-drive", f"file={disk},if=virtio,format={img_fmt},cache=none,discard=unmap",
            # Nice-to-have: show early boot logs on serial (and don't hang on graphics)
            "-serial", "mon:stdio",
        ]

        # Display handling
        cmd += QemuTest._display_args(display)

        # Networking (user-mode by default)
        if net.enabled:
            cmd += QemuTest._net_args(net)

        # UEFI handling (correct way: pflash CODE + writable VARS)
        if uefi:
            ovmf_code, ovmf_vars = QemuTest._resolve_ovmf(logger)
            # writable vars copy per-run (keeps tests isolated)
            vars_copy = QemuTest._copy_ovmf_vars(logger, ovmf_vars)
            cmd += [
                "-drive", f"if=pflash,format=raw,readonly=on,file={ovmf_code}",
                "-drive", f"if=pflash,format=raw,file={vars_copy}",
            ]

        # Extra args last so caller can override
        cmd += list(extra_args)

        # “Smoke” semantics: run briefly then stop (so CI / automation doesn't hang forever)
        # Use timeout if provided; otherwise run interactively until user exits QEMU.
        U.banner(logger, "QEMU smoke test")
        logger.info(f"Disk: {disk} (format={img_fmt})")
        logger.info(f"Firmware: {'UEFI' if uefi else 'BIOS'} | Display: {display.mode} | Net: {'on' if net.enabled else 'off'}")

        if display.mode == "vnc":
            port = 5900 + int(display.vnc_display)
            logger.info(f"VNC: {display.vnc_listen}:{port} (display :{display.vnc_display})")

        logger.debug("QEMU command:\n  " + " ".join(shlex.quote(x) for x in cmd))

        if timeout_s is None:
            logger.info("Launching QEMU (no timeout; exit manually)…")
            U.run_cmd(logger, cmd, check=False, capture=False)
            return

        logger.info(f"Launching QEMU (smoke run for ~{timeout_s}s)…")
        p = subprocess.Popen(cmd)
        try:
            p.wait(timeout=float(timeout_s))
        except subprocess.TimeoutExpired:
            logger.info("Smoke timeout reached; terminating QEMU…")
            p.terminate()
            try:
                p.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                logger.warning("QEMU didn't exit on SIGTERM; killing…")
                p.kill()
                p.wait(timeout=5.0)

        # No enforced exit code for smoke test, but we log it.
        rc = p.returncode
        logger.info(f"QEMU exited with rc={rc} (not enforced).")

    # ----------------------------
    # Helpers
    # ----------------------------

    @staticmethod
    def _detect_img_format(logger: logging.Logger, disk: Path) -> str:
        suf = disk.suffix.lower().lstrip(".")
        if suf in ("qcow2", "raw", "vmdk", "vdi"):
            return suf

        if U.which("qemu-img"):
            try:
                out = U.run_cmd(logger, ["qemu-img", "info", "--output=json", str(disk)], check=True, capture=True).stdout
                key = '"format"'
                i = out.find(key)
                if i != -1:
                    seg = out[i : i + 140]
                    c = seg.find(":")
                    q1 = seg.find('"', c + 1)
                    q2 = seg.find('"', q1 + 1)
                    if c != -1 and q1 != -1 and q2 != -1:
                        fmt = seg[q1 + 1 : q2].strip().lower()
                        if fmt:
                            return fmt
            except Exception:
                pass

        logger.warning("Could not confidently detect image format; defaulting to qcow2.")
        return "qcow2"

    @staticmethod
    def _display_args(d: QemuDisplay) -> list[str]:
        if d.mode == "none":
            # headless, safe over SSH
            return ["-nographic"]
        if d.mode == "gtk":
            return ["-display", "gtk"]
        if d.mode == "sdl":
            return ["-display", "sdl"]
        if d.mode == "vnc":
            return ["-display", "none", "-vnc", f"{d.vnc_listen}:{int(d.vnc_display)}"]
        raise ValueError(f"Unknown display mode: {d.mode}")

    @staticmethod
    def _net_args(n: QemuNet) -> list[str]:
        # user-mode networking; virtio-net for speed/compat
        if n.ssh_forward_host_port is not None:
            hp = int(n.ssh_forward_host_port)
            return [
                "-netdev", f"user,id=n0,hostfwd=tcp::{hp}-:22",
                "-device", "virtio-net-pci,netdev=n0",
            ]
        return [
            "-netdev", "user,id=n0",
            "-device", "virtio-net-pci,netdev=n0",
        ]

    @staticmethod
    def _resolve_ovmf(logger: logging.Logger) -> tuple[str, str]:
        code = next((p for p in QemuTest._OVMF_CODE_CANDIDATES if os.path.exists(p)), None)
        vars_ = next((p for p in QemuTest._OVMF_VARS_CANDIDATES if os.path.exists(p)), None)
        if not code or not vars_:
            U.die(logger, "UEFI requested but OVMF not found (CODE/VARS missing).", 1)
        return code, vars_

    @staticmethod
    def _copy_ovmf_vars(logger: logging.Logger, ovmf_vars: str) -> str:
        # Put it under /tmp; caller doesn't need the path later.
        fd, dst = tempfile.mkstemp(prefix="vmdk2kvm-ovmf-vars-", suffix=".fd")
        os.close(fd)
        U.run_cmd(logger, ["cp", "-f", ovmf_vars, dst], check=True, capture=False)
        return dst
