from __future__ import annotations
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional, Callable
import time

from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn, DownloadColumn, TransferSpeedColumn

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
            logger.error("qemu-img not found.")
            U.die(logger, "qemu-img not found.", 1)
        try:
            U.ensure_dir(dst.parent)
        except Exception as e:
            logger.error(f"Failed to ensure directory {dst.parent}: {str(e)}")
            raise
        total_size = 0
        try:
            info_cmd = ["qemu-img", "info", "--output=json", str(src)]
            logger.debug(f"Executing info command: {' '.join(info_cmd)}")
            info_result = subprocess.run(info_cmd, capture_output=True, text=True, check=True)
            info = json.loads(info_result.stdout)
            total_size = info.get("virtual-size", 0)
            logger.debug(f"Retrieved virtual size: {total_size}")
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to get image info: return code {e.returncode}, stdout: {e.stdout}, stderr: {e.stderr}")
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse image info JSON: {str(e)}")
        except Exception as e:
            logger.warning(f"Unexpected error getting image info: {str(e)}")
        cmd = ["qemu-img", "convert", "-p", "-O", out_format]
        if compress and out_format == "qcow2":
            if compress_level is not None:
                cmd += ["-c", "-o", f"compression_type=zlib,compression_level={compress_level}"]
            else:
                cmd += ["-c"]
        cmd += [str(src), str(dst)]
        U.banner(logger, f"Convert to {out_format.upper()}")
        logger.info(f"Converting: {src} -> {dst} (compress={compress}, level={compress_level})")
        logger.debug(f"Executing conversion command: {' '.join(cmd)}")
        start_time = time.time()
        try:
            process = subprocess.Popen(
                cmd,
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
                universal_newlines=True
            )
        except Exception as e:
            logger.error(f"Failed to start conversion process: {str(e)}")
            raise
        assert process.stdout is not None
        assert process.stderr is not None
        last_completed = 0
        last_time = start_time
        last_log_time = start_time
        stderr_lines = []
        stdout_lines = []
        import threading
        def read_stdout():
            for line in process.stdout:
                stdout_lines.append(line.strip())
        stdout_thread = threading.Thread(target=read_stdout)
        stdout_thread.start()
        if progress_callback and total_size > 0:
            for line in process.stderr:
                stderr_lines.append(line.strip())
                if m := re.search(r"\((\d+\.\d+)/100\%\)", line):
                    progress = float(m.group(1)) / 100
                    current_time = time.time()
                    completed_bytes = progress * total_size
                    delta_bytes = completed_bytes - last_completed
                    delta_time = current_time - last_time
                    if delta_time > 0:
                        speed_mb_s = delta_bytes / delta_time / 1024 / 1024
                        if current_time - last_log_time >= 10:
                            logger.info(f"Current progress: {float(m.group(1)):.2f}%, speed: {speed_mb_s:.2f} MB/s")
                            last_log_time = current_time
                    last_completed = completed_bytes
                    last_time = current_time
                    progress_callback(progress)
                elif "(100.00/100%)" in line:
                    progress_callback(1.0)
            process.wait()
            stdout_thread.join()
            if process.returncode != 0:
                logger.error(f"Conversion failed with exit code {process.returncode}")
                logger.error("stdout output:\n" + "\n".join(stdout_lines))
                logger.error("stderr output:\n" + "\n".join(stderr_lines))
                U.die(logger, f"Conversion failed with exit code {process.returncode}", 1)
        else:
            use_bytes = total_size > 0
            progress = Progress(
                TextColumn("{task.description}"),
                BarColumn(),
                DownloadColumn() if use_bytes else TextColumn("{task.percentage:>3.0f}%"),
                TransferSpeedColumn() if use_bytes else None,
                TimeElapsedColumn(),
                TimeRemainingColumn()
            ) if use_bytes else Progress(
                TextColumn("{task.description}"),
                BarColumn(),
                TextColumn("{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                TimeRemainingColumn()
            )
            task_total = total_size if use_bytes else 100
            with progress:
                task = progress.add_task("Converting", total=task_total)
                for line in process.stderr:
                    stderr_lines.append(line.strip())
                    m = re.search(r"\((\d+\.\d+)/100\%\)", line)
                    if m:
                        progress_val = float(m.group(1))
                        completed = (progress_val / 100 * total_size) if use_bytes else progress_val
                        current_time = time.time()
                        delta_bytes = completed - last_completed if use_bytes else (progress_val - last_completed)
                        delta_time = current_time - last_time
                        if delta_time > 0:
                            speed = (delta_bytes / delta_time / 1024 / 1024) if use_bytes else (delta_bytes / delta_time)
                            if current_time - last_log_time >= 10:
                                unit = "MB/s" if use_bytes else "%/s"
                                logger.info(f"Current progress: {progress_val:.2f}%, speed: {speed:.2f} {unit}")
                                last_log_time = current_time
                        last_completed = completed if use_bytes else progress_val
                        last_time = current_time
                        progress.update(task, completed=completed)
                process.wait()
                stdout_thread.join()
                if process.returncode != 0:
                    logger.error(f"Conversion failed with exit code {process.returncode}")
                    logger.error("stdout output:\n" + "\n".join(stdout_lines))
                    logger.error("stderr output:\n" + "\n".join(stderr_lines))
                    raise subprocess.CalledProcessError(process.returncode, cmd)
        end_time = time.time()
        duration = end_time - start_time
        if stdout_lines:
            logger.debug("stdout output:\n" + "\n".join(stdout_lines))
        if stderr_lines:
            logger.debug("stderr output:\n" + "\n".join(stderr_lines))
        if total_size > 0 and duration > 0:
            speed_mb_s = total_size / duration / 1024 / 1024
            logger.info(f"Conversion completed in {duration:.2f} seconds at average speed of {speed_mb_s:.2f} MB/s")
        else:
            logger.info(f"Conversion completed in {duration:.2f} seconds")
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
            logger.warning("qemu-img not found, skipping validation.")
            return
        cmd = ["qemu-img", "check", str(path)]
        logger.debug(f"Executing validation command: {' '.join(cmd)}")
        cp = U.run_cmd(logger, cmd, check=False, capture=True)
        if cp.returncode == 0:
            logger.info("Image validation: OK (qemu-img check)")
        else:
            logger.warning("Image validation: WARNING (qemu-img check reported issues)")
            logger.debug(f"return code: {cp.returncode}")
            logger.debug("stdout:\n" + (cp.stdout or ""))
            logger.debug("stderr:\n" + (cp.stderr or ""))