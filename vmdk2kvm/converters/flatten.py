from __future__ import annotations
import logging
import re
import subprocess
from pathlib import Path

from ..core.utils import U
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
import os
from ..vmware.vmdk_parser import VMDK
class Flatten:
    @staticmethod
    def to_working(logger: logging.Logger, src: Path, outdir: Path, fmt: str) -> Path:
        if U.which("qemu-img") is None:
            U.die(logger, "qemu-img not found (install qemu-utils).", 1)
        U.ensure_dir(outdir)
        dst = outdir / f"working-flattened-{U.now_ts()}.{fmt}"
        U.banner(logger, "Flatten snapshot chain")
        logger.info("Flattening via qemu-img convert (single self-contained image)…")
        cmd = ["qemu-img", "convert", "-p", "-f", "vmdk", "-O", fmt, str(src), str(dst)]
        process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, universal_newlines=True)
        assert process.stderr is not None
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
            task = progress.add_task("Flattening", total=100)
            for line in process.stderr:
                m = re.search(r"\((\d+\.\d+)/100\%\)", line)
                if m:
                    progress_val = float(m.group(1))
                    progress.update(task, completed=progress_val)
        process.wait()
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, cmd)
        return dst

# --- Remote ESXi fetch helper (from original monolith) ---
class Fetch:
    @staticmethod
    def fetch_descriptor_and_extent(
        logger: logging.Logger,
        sshc: SSHClient,
        remote_desc: str,
        outdir: Path,
        fetch_all: bool,
    ) -> Path:
        U.banner(logger, "Fetch VMDK from remote")
        U.ensure_dir(outdir)
        sshc.check()
        if not sshc.exists(remote_desc):
            U.die(logger, f"Remote descriptor not found: {remote_desc}", 1)
        local_desc = outdir / os.path.basename(remote_desc)
        logger.info(f"Copying descriptor: {remote_desc}")
        sshc.scp_from(remote_desc, local_desc)
        extent_rel = VMDK.parse_extent(logger, local_desc)
        remote_dir = os.path.dirname(remote_desc)
        if extent_rel:
            remote_extent = os.path.join(remote_dir, extent_rel)
        else:
            stem = local_desc.stem
            remote_extent = os.path.join(remote_dir, f"{stem}-flat.vmdk")
        if sshc.exists(remote_extent):
            logger.info(f"Copying extent: {remote_extent}")
            sshc.scp_from(remote_extent, outdir / os.path.basename(remote_extent))
        else:
            logger.warning(f"Extent not found remotely: {remote_extent}")
        if fetch_all:
            cur = local_desc
            while True:
                parent = VMDK.parse_parent(logger, cur)
                if not parent:
                    break
                remote_parent = os.path.join(remote_dir, parent)
                local_parent = outdir / parent
                if sshc.exists(remote_parent):
                    logger.info(f"Copying parent descriptor: {remote_parent}")
                    sshc.scp_from(remote_parent, local_parent)
                    cur = local_parent
                else:
                    logger.warning(f"Parent descriptor missing: {remote_parent}")
                    break
        return local_desc
