# SPDX-License-Identifier: LGPL-3.0-or-later
from __future__ import annotations

import json
import logging
import re
import selectors
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional, Tuple

from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from ..core.utils import U


class Convert:
    """
    qemu-img convert wrapper with:
      ✅ "never-stall" progress: combines stderr % + output file-size polling (monotonic max)
      ✅ robust error handling: auto-fallback across incompatible flags (zstd/target-is-zero/-m/cache)
      ✅ atomic output (.part -> rename)
      ✅ flat VMDK descriptor preference

    The key reliability principle:
      - qemu-img option support varies wildly across distro/qemu versions
      - so we try a fast/best option set first, and on failure we downgrade options automatically
      - we also capture stderr tail and log it for the *real* reason
    """

    _RE_PAREN = re.compile(r"\((\d+(?:\.\d+)?)/100%\)")
    _RE_PROGRESS = re.compile(r"(?:progress|Progress)\s*[:=]\s*(\d+(?:\.\d+)?)")
    _RE_PERCENT = re.compile(r"(\d+(?:\.\d+)?)%")
    _RE_JSON = re.compile(r"^\s*\{.*\}\s*$")

    @dataclass(frozen=True)
    class ConvertOptions:
        cache_mode: str = "none"                 # none|writeback|unsafe|"" (disabled)
        threads: Optional[int] = None            # -m N
        target_is_zero: bool = True              # --target-is-zero
        compression_type: Optional[str] = "zstd" # zstd|zlib|None (omit)
        compression_level: Optional[int] = None  # compression_level=...
        preallocation: Optional[str] = None      # preallocation=metadata,...

        def short(self) -> str:
            return (
                f"cache={self.cache_mode or 'off'} "
                f"threads={self.threads or 'off'} "
                f"target_is_zero={'on' if self.target_is_zero else 'off'} "
                f"ctype={self.compression_type or 'omit'} "
                f"clevel={self.compression_level if self.compression_level is not None else 'omit'} "
                f"prealloc={self.preallocation or 'omit'}"
            )

    # Public API

    @staticmethod
    def convert_image_with_progress(
        logger: logging.Logger,
        src: Path,
        dst: Path,
        *,
        out_format: str,
        compress: bool,
        compress_level: Optional[int] = None,
        compression_type: Optional[str] = "zstd",
        progress_callback: Optional[Callable[[float], None]] = None,
        in_format: Optional[str] = None,
        preallocation: Optional[str] = None,
        atomic: bool = True,
        cache_mode: str = "none",
        threads: Optional[int] = None,
        target_is_zero: bool = True,
        ui_poll_s: float = 0.20,
        max_stderr_tail: int = 160,
    ) -> None:
        src = Path(src)
        dst = Path(dst)

        if U.which("qemu-img") is None:
            U.die(logger, "qemu-img not found.", 1)

        src = Convert._prefer_descriptor_for_flat(logger, src)
        if not src.is_file():
            raise FileNotFoundError(f"Source image file not found: {src}")

        U.ensure_dir(dst.parent)

        final_dst = dst
        tmp_dst = dst.with_suffix(dst.suffix + ".part") if atomic else dst

        virt_size, detected_fmt = Convert._qemu_img_info(logger, src)
        if in_format is None:
            in_format = detected_fmt

        base = Convert.ConvertOptions(
            cache_mode=cache_mode,
            threads=threads,
            target_is_zero=target_is_zero,
            compression_type=compression_type,
            compression_level=compress_level,
            preallocation=preallocation,
        )

        # Build a fallback plan (fastest first, most compatible last)
        plan = list(Convert._fallback_plan(base, out_format=out_format, compress=compress))

        U.banner(logger, f"Convert to {out_format.upper()}")
        logger.info(
            f"Converting: {src} -> {final_dst} "
            f"(in_format={in_format or 'auto'}, out_format={out_format}, compress={compress})"
        )

        last_error: Optional[subprocess.CalledProcessError] = None

        for attempt_no, opt in enumerate(plan, start=1):
            # fresh temp file each attempt
            if atomic and tmp_dst.exists():
                tmp_dst.unlink(missing_ok=True)

            cmd = Convert._build_convert_cmd(
                src=src,
                dst=tmp_dst,
                in_format=in_format,
                out_format=out_format,
                compress=compress,
                opt=opt,
            )

            logger.debug(f"[attempt {attempt_no}/{len(plan)}] opts: {opt.short()}")
            logger.debug(f"[attempt {attempt_no}/{len(plan)}] cmd:  {' '.join(cmd)}")

            rc, stderr_lines = Convert._run_convert_process(
                logger,
                cmd,
                tmp_dst=tmp_dst,
                virt_size=virt_size,
                ui_poll_s=ui_poll_s,
                progress_callback=progress_callback,
            )

            if rc == 0:
                if atomic:
                    tmp_dst.replace(final_dst)
                if progress_callback:
                    try:
                        progress_callback(1.0)
                    except Exception:
                        pass
                # optional debug tail
                if stderr_lines:
                    logger.debug("qemu-img stderr (tail):\n" + "\n".join(stderr_lines[-80:]))
                return

            # Non-zero: log REAL reason (tail)
            tail = "\n".join(stderr_lines[-max_stderr_tail:]) if stderr_lines else ""
            logger.error(f"Conversion attempt {attempt_no} failed (rc={rc}). opts: {opt.short()}")
            if tail:
                logger.error("qemu-img stderr (tail):\n" + tail)

            last_error = subprocess.CalledProcessError(rc, cmd)

            # If the error is clearly "unsupported option", we continue (that's the point).
            # If it's something else (IO error, permission), we also continue because some flags
            # (cache_mode/target-is-zero) can still tickle weird failures. Worst case: we exhaust the plan.
            continue

        # Exhausted fallback plan
        if atomic and tmp_dst.exists():
            try:
                tmp_dst.unlink()
            except Exception:
                pass
        assert last_error is not None
        raise last_error

    @staticmethod
    def convert_image(
        logger: logging.Logger,
        src: Path,
        dst: Path,
        *,
        out_format: str,
        compress: bool,
        compress_level: Optional[int] = None,
        in_format: Optional[str] = None,
    ) -> None:
        Convert.convert_image_with_progress(
            logger,
            src,
            dst,
            out_format=out_format,
            compress=compress,
            compress_level=compress_level,
            progress_callback=None,
            in_format=in_format,
        )

    @staticmethod
    def validate(logger: logging.Logger, path: Path) -> None:
        path = Convert._prefer_descriptor_for_flat(logger, Path(path))
        if not path.is_file():
            logger.warning(f"Image file not found for validation: {path}")
            return
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

    # Fallback Policy

    @staticmethod
    def _fallback_plan(
        base: ConvertOptions, *, out_format: str, compress: bool
    ) -> Iterable[ConvertOptions]:
        """
        Fast -> compatible ladder.
        We only downgrade options that are known to vary across qemu-img builds.
        """
        # Start exactly as requested
        yield base

        # Threading is commonly unsupported on older qemu-img (-m)
        if base.threads:
            yield Convert.ConvertOptions(
                cache_mode=base.cache_mode,
                threads=None,
                target_is_zero=base.target_is_zero,
                compression_type=base.compression_type,
                compression_level=base.compression_level,
                preallocation=base.preallocation,
            )

        # --target-is-zero isn't present in very old qemu
        if base.target_is_zero:
            yield Convert.ConvertOptions(
                cache_mode=base.cache_mode,
                threads=None,
                target_is_zero=False,
                compression_type=base.compression_type,
                compression_level=base.compression_level,
                preallocation=base.preallocation,
            )

        # qcow2 compression_type=zstd may be unsupported; downgrade to zlib
        if out_format == "qcow2" and compress:
            if base.compression_type == "zstd":
                yield Convert.ConvertOptions(
                    cache_mode=base.cache_mode,
                    threads=None,
                    target_is_zero=False,
                    compression_type="zlib",
                    compression_level=base.compression_level,
                    preallocation=base.preallocation,
                )
            # omit compression_type entirely (let qemu choose default)
            yield Convert.ConvertOptions(
                cache_mode=base.cache_mode,
                threads=None,
                target_is_zero=False,
                compression_type=None,
                compression_level=base.compression_level,
                preallocation=base.preallocation,
            )
            # omit compression_level too (older qemu might reject it)
            if base.compression_level is not None:
                yield Convert.ConvertOptions(
                    cache_mode=base.cache_mode,
                    threads=None,
                    target_is_zero=False,
                    compression_type=None,
                    compression_level=None,
                    preallocation=base.preallocation,
                )

        # Cache args sometimes trigger weirdness on strange storage; disable cache flags
        if base.cache_mode:
            yield Convert.ConvertOptions(
                cache_mode="",
                threads=None,
                target_is_zero=False,
                compression_type=None if (out_format == "qcow2" and compress) else base.compression_type,
                compression_level=None if (out_format == "qcow2" and compress) else base.compression_level,
                preallocation=base.preallocation,
            )

        # Final "bare minimum"
        yield Convert.ConvertOptions(
            cache_mode="",
            threads=None,
            target_is_zero=False,
            compression_type=None,
            compression_level=None,
            preallocation=None,
        )


    @staticmethod
    def _run_convert_process(
        logger: logging.Logger,
        cmd: list[str],
        *,
        tmp_dst: Path,
        virt_size: int,
        ui_poll_s: float,
        progress_callback: Optional[Callable[[float], None]],
    ) -> tuple[int, list[str]]:
        start = time.time()
        stderr_lines: list[str] = []

        proc = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
        assert proc.stderr is not None

        sel = selectors.DefaultSelector()
        sel.register(proc.stderr, selectors.EVENT_READ)

        use_bytes = virt_size > 0
        total = float(virt_size) if use_bytes else 100.0

        best_completed = 0.0
        last_seen_pct: Optional[float] = None
        last_io_tick = time.time()

        def update_best(v: float) -> None:
            nonlocal best_completed
            if v > best_completed:
                best_completed = v

        def parse_progress_pct(line: str) -> Optional[float]:
            s = (line or "").strip()
            if not s:
                return None

            if Convert._RE_JSON.match(s):
                try:
                    o = json.loads(s)
                    for k in ("progress", "percent", "pct"):
                        if k in o:
                            v = float(o[k])
                            return v if 0.0 <= v <= 100.0 else None
                except Exception:
                    return None

            m = Convert._RE_PAREN.search(s)
            if m:
                try:
                    return float(m.group(1))
                except Exception:
                    return None

            m = Convert._RE_PROGRESS.search(s)
            if m:
                try:
                    v = float(m.group(1))
                    return v if 0.0 <= v <= 100.0 else None
                except Exception:
                    return None

            m = Convert._RE_PERCENT.search(s)
            if m:
                try:
                    v = float(m.group(1))
                    return v if 0.0 <= v <= 100.0 else None
                except Exception:
                    return None

            return None

        def pct_to_completed(pct: float) -> Optional[float]:
            if virt_size <= 0:
                return None
            return (pct / 100.0) * float(virt_size)

        def size_done_bytes() -> Optional[float]:
            if virt_size <= 0:
                return None
            try:
                if not tmp_dst.exists():
                    return None
                out_sz = tmp_dst.stat().st_size
            except Exception:
                return None
            return float(out_sz if out_sz < virt_size else virt_size)

        def do_callback(completed: float) -> None:
            if not progress_callback:
                return
            frac = 1.0
            if virt_size > 0:
                frac = max(0.0, min(1.0, completed / float(virt_size)))
            try:
                progress_callback(frac)
            except Exception as e:
                logger.debug(f"progress_callback raised: {e}")

        # log throttles
        last_log_t = start
        last_log_completed = 0.0

        with Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            (DownloadColumn() if use_bytes else TaskProgressColumn()),
            (TransferSpeedColumn() if use_bytes else TextColumn("")),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task("Converting", total=total)

            while True:
                # read stderr when available (non-blocking)
                events = sel.select(timeout=ui_poll_s)
                for key, _ in events:
                    line = key.fileobj.readline()
                    if not line:
                        continue
                    last_io_tick = time.time()
                    stderr_lines.append(line.rstrip("\n"))

                    pct = parse_progress_pct(line)
                    if pct is None:
                        continue
                    last_seen_pct = pct

                    if use_bytes:
                        b = pct_to_completed(pct)
                        if b is not None:
                            update_best(b)
                    else:
                        update_best(float(pct))

                # always tick size-based progress (alive/progress signal)
                if use_bytes:
                    b2 = size_done_bytes()
                    if b2 is not None:
                        update_best(b2)

                # UI update (monotonic)
                progress.update(task, completed=best_completed)

                # descriptive label
                now = time.time()
                silent_for = now - last_io_tick
                if last_seen_pct is not None:
                    progress.update(task, description=f"Converting (qemu-img {last_seen_pct:.1f}% | quiet {silent_for:.1f}s)")
                else:
                    progress.update(task, description=f"Converting (stderr quiet {silent_for:.1f}s)")

                # throttled logs
                if now - last_log_t >= 10.0 and best_completed > last_log_completed:
                    if use_bytes and (now - start) > 0:
                        pct_now = 100.0 * best_completed / float(virt_size)
                        mb_s = (best_completed / (now - start)) / 1024 / 1024
                        logger.info(f"Conversion progress: {pct_now:.1f}% (~{mb_s:.1f} MB/s avg)")
                    else:
                        logger.info(f"Conversion progress: {best_completed:.1f}%")
                    last_log_t = now
                    last_log_completed = best_completed

                do_callback(best_completed)

                if proc.poll() is not None:
                    break

        rc = proc.wait()
        return rc, stderr_lines

    # -----------------------------
    # Cmd builder / helpers
    # -----------------------------

    @staticmethod
    def _build_convert_cmd(
        *,
        src: Path,
        dst: Path,
        in_format: Optional[str],
        out_format: str,
        compress: bool,
        opt: ConvertOptions,
    ) -> list[str]:
        cmd: list[str] = ["qemu-img", "convert", "-p"]

        if opt.cache_mode:
            cmd += ["-t", opt.cache_mode, "-T", opt.cache_mode]

        if opt.target_is_zero:
            cmd.append("--target-is-zero")

        if opt.threads and opt.threads > 0:
            cmd += ["-m", str(int(opt.threads))]

        if in_format:
            cmd += ["-f", in_format]

        cmd += ["-O", out_format]

        if out_format == "qcow2":
            opts: list[str] = []
            if opt.preallocation:
                opts.append(f"preallocation={opt.preallocation}")

            if compress:
                cmd.append("-c")
                if opt.compression_type:
                    opts.append(f"compression_type={opt.compression_type}")
                if opt.compression_level is not None:
                    opts.append(f"compression_level={int(opt.compression_level)}")

            if opts:
                cmd += ["-o", ",".join(opts)]

        cmd += [str(src), str(dst)]
        return cmd

    @staticmethod
    def _prefer_descriptor_for_flat(logger: logging.Logger, src: Path) -> Path:
        s = str(src)
        if s.endswith("-flat.vmdk"):
            descriptor = src.with_name(src.name.replace("-flat.vmdk", ".vmdk"))
            if descriptor.is_file():
                logger.info(f"Detected flat VMDK; using descriptor: {descriptor}")
                return descriptor
        return src

    @staticmethod
    def _qemu_img_info(logger: logging.Logger, src: Path) -> Tuple[int, Optional[str]]:
        info_cmd = ["qemu-img", "info", "--output=json", str(src)]
        logger.debug(f"Executing info command: {' '.join(info_cmd)}")
        info_result = subprocess.run(info_cmd, capture_output=True, text=True, check=True)
        info = json.loads(info_result.stdout or "{}")
        virt = int(info.get("virtual-size", 0) or 0)
        fmt = info.get("format")
        if fmt is not None and not isinstance(fmt, str):
            fmt = None
        return virt, fmt
