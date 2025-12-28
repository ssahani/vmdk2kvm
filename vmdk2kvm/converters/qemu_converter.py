from __future__ import annotations
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn

from ..core.utils import U
import re
import json
class Convert:
    @staticmethod
    def convert_image_with_progress(
        logger: logging.Logger,
        src: Path,
        dst: Path,
        *,
        out_format: str,
        compress: bool,
        compress_level: Optional[int] = None,
        progress_callback: Optional[Callable[[float], None]] = None,
    ) -> None:
        if U.which("qemu-img") is None:
            U.die(logger, "qemu-img not found.", 1)
        U.ensure_dir(dst.parent)
        total_size = 0
        try:
            info_cmd = ["qemu-img", "info", "--output=json", str(src)]
            info_result = subprocess.run(info_cmd, capture_output=True, text=True, check=True)
            info = json.loads(info_result.stdout)
            total_size = info.get("virtual-size", 0)
        except Exception as e:
            logger.debug(f"Could not get image info for progress: {e}")
        cmd = ["qemu-img", "convert", "-p", "-O", out_format]
        if compress and out_format == "qcow2":
            if compress_level is not None:
                cmd += ["-c", "-o", f"compression_type=zlib,compression_level={compress_level}"]
            else:
                cmd += ["-c"]
        cmd += [str(src), str(dst)]
        U.banner(logger, f"Convert to {out_format.upper()}")
        logger.info(f"Converting: {src} -> {dst} (compress={compress}, level={compress_level})")
        if progress_callback and total_size > 0:
            process = subprocess.Popen(
                cmd,
                stderr=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                universal_newlines=True
            )
            assert process.stderr is not None
            for line in process.stderr:
                if m := re.search(r"\((\d+\.\d+)/100\%\)", line):
                    progress = float(m.group(1)) / 100
                    progress_callback(progress)
                elif "(100.00/100%)" in line:
                    progress_callback(1.0)
            process.wait()
            if process.returncode != 0:
                U.die(logger, f"Conversion failed with exit code {process.returncode}", 1)
        else:
            process = subprocess.Popen(
                cmd,
                stderr=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                universal_newlines=True
            )
            assert process.stderr is not None
            with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
                task = progress.add_task("Converting", total=100)
                for line in process.stderr:
                    m = re.search(r"\((\d+\.\d+)/100\%\)", line)
                    if m:
                        progress_val = float(m.group(1))
                        progress.update(task, completed=progress_val)
            process.wait()
            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, cmd)
    @staticmethod
    def convert_image(
        logger: logging.Logger,
        src: Path,
        dst: Path,
        *,
        out_format: str,
        compress: bool,
        compress_level: Optional[int] = None,
    ) -> None:
        Convert.convert_image_with_progress(
            logger, src, dst,
            out_format=out_format,
            compress=compress,
            compress_level=compress_level,
            progress_callback=None
        )
    @staticmethod
    def validate(logger: logging.Logger, path: Path) -> None:
        if U.which("qemu-img") is None:
            return
        cp = U.run_cmd(logger, ["qemu-img", "check", str(path)], check=False, capture=True)
        if cp.returncode == 0:
            logger.info("Image validation: OK (qemu-img check)")
        else:
            logger.warning("Image validation: WARNING (qemu-img check reported issues)")
            logger.debug(cp.stdout or "")
            logger.debug(cp.stderr or "")
