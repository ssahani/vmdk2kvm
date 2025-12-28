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
        logger.debug(f"Entering to_working with src={src}, outdir={outdir}, fmt={fmt}")
        if U.which("qemu-img") is None:
            logger.error("qemu-img not found (install qemu-utils).")
            U.die(logger, "qemu-img not found (install qemu-utils).", 1)
        try:
            U.ensure_dir(outdir)
        except Exception as e:
            logger.error(f"Failed to ensure directory {outdir}: {e}")
            raise
        dst = outdir / f"working-flattened-{U.now_ts()}.{fmt}"
        logger.debug(f"Destination path set to: {dst}")
        U.banner(logger, "Flatten snapshot chain")
        logger.info("Flattening via qemu-img convert (single self-contained image)…")
        cmd = ["qemu-img", "convert", "-p", "-f", "vmdk", "-O", fmt, str(src), str(dst)]
        logger.debug(f"Executing command: {' '.join(cmd)}")
        try:
            process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, universal_newlines=True)
            assert process.stderr is not None
            with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
                task = progress.add_task("Flattening", total=100)
                for line in process.stderr:
                    logger.debug(f"stderr line: {line.strip()}")
                    m = re.search(r"\((\d+\.\d+)/100\%\)", line)
                    if m:
                        progress_val = float(m.group(1))
                        logger.debug(f"Progress value: {progress_val}")
                        progress.update(task, completed=progress_val)
            process.wait()
            logger.debug(f"Process return code: {process.returncode}")
            if process.returncode != 0:
                logger.error(f"Flattening process failed with return code: {process.returncode}")
                raise subprocess.CalledProcessError(process.returncode, cmd)
            logger.debug(f"Flattening completed successfully, returning {dst}")
            return dst
        except subprocess.CalledProcessError as e:
            logger.error(f"CalledProcessError during flattening: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during flattening: {e}")
            raise

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
        logger.debug(f"Entering fetch_descriptor_and_extent with remote_desc={remote_desc}, outdir={outdir}, fetch_all={fetch_all}")
        U.banner(logger, "Fetch VMDK from remote")
        try:
            U.ensure_dir(outdir)
        except Exception as e:
            logger.error(f"Failed to ensure directory {outdir}: {e}")
            raise
        try:
            sshc.check()
        except Exception as e:
            logger.error(f"SSH connection check failed: {e}")
            raise
        if not sshc.exists(remote_desc):
            logger.error(f"Remote descriptor not found: {remote_desc}")
            U.die(logger, f"Remote descriptor not found: {remote_desc}", 1)
        local_desc = outdir / os.path.basename(remote_desc)
        logger.debug(f"Local descriptor path: {local_desc}")
        logger.info(f"Copying descriptor: {remote_desc}")
        try:
            sshc.scp_from(remote_desc, local_desc)
        except Exception as e:
            logger.error(f"SCP failed for descriptor {remote_desc}: {e}")
            raise
        try:
            extent_rel = VMDK.parse_extent(logger, local_desc)
            logger.debug(f"Parsed extent_rel: {extent_rel}")
        except Exception as e:
            logger.error(f"Failed to parse extent from {local_desc}: {e}")
            raise
        remote_dir = os.path.dirname(remote_desc)
        logger.debug(f"Remote directory: {remote_dir}")
        if extent_rel:
            remote_extent = os.path.join(remote_dir, extent_rel)
        else:
            stem = local_desc.stem
            remote_extent = os.path.join(remote_dir, f"{stem}-flat.vmdk")
        logger.debug(f"Remote extent path: {remote_extent}")
        if sshc.exists(remote_extent):
            logger.info(f"Copying extent: {remote_extent}")
            try:
                sshc.scp_from(remote_extent, outdir / os.path.basename(remote_extent))
            except Exception as e:
                logger.error(f"SCP failed for extent {remote_extent}: {e}")
                raise
        else:
            logger.warning(f"Extent not found remotely: {remote_extent}")
        if fetch_all:
            cur = local_desc
            logger.debug(f"Starting parent fetch loop with cur={cur}")
            while True:
                try:
                    parent = VMDK.parse_parent(logger, cur)
                    logger.debug(f"Parsed parent: {parent}")
                except Exception as e:
                    logger.error(f"Failed to parse parent from {cur}: {e}")
                    raise
                if not parent:
                    break
                remote_parent = os.path.join(remote_dir, parent)
                local_parent = outdir / parent
                logger.debug(f"Remote parent: {remote_parent}, Local parent: {local_parent}")
                if sshc.exists(remote_parent):
                    logger.info(f"Copying parent descriptor: {remote_parent}")
                    try:
                        sshc.scp_from(remote_parent, local_parent)
                    except Exception as e:
                        logger.error(f"SCP failed for parent descriptor {remote_parent}: {e}")
                        raise
                    cur = local_parent
                else:
                    logger.warning(f"Parent descriptor missing: {remote_parent}")
                    break
        logger.debug(f"Fetch completed, returning {local_desc}")
        return local_desc