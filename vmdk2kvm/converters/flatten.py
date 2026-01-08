# SPDX-License-Identifier: LGPL-3.0-or-later
from __future__ import annotations

import json
import logging
import os
import re
import selectors
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Set, Tuple

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

from vmdk2kvm.ssh.ssh_client import SSHClient
from ..core.utils import U
from ..vmware.vmdk_parser import VMDK


# -----------------------------
# Helpers
# -----------------------------

@dataclass(frozen=True)
class _ProgressPolicy:
    ui_interval_s: float = 0.25
    ui_min_step_pct: float = 1.0
    log_interval_s: float = 10.0
    log_min_step_pct: float = 5.0
    io_poll_s: float = 0.20  # tick even when stderr is quiet


def _atomic_tmp(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".part")


def _unlink_quiet(p: Path) -> None:
    try:
        if p.exists():
            p.unlink()
    except Exception:
        pass


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


# -----------------------------
# Flatten
# -----------------------------

class Flatten:
    """
    Flatten snapshot chain into a single self-contained image.

    IMPORTANT CHANGE (fix for Photon Azure VHD / any non-VMDK):
      - We DO NOT hardcode '-f vmdk' anymore.
      - We detect the real input format via 'qemu-img info --output=json'
        and use '-f <format>' when known (vpc/vmdk/raw/qcow2/...).
      - If detection fails, we omit '-f' and let qemu-img autodetect.

    Everything else (fast FLAT path, retry ladder, progress, logging, atomic output) stays.
    """

    _RE_PAREN = re.compile(r"\((\d+(?:\.\d+)?)/100%\)")
    _RE_PERCENT = re.compile(r"(\d+(?:\.\d+)?)%")
    _RE_PROGRESS = re.compile(r"(?:progress|Progress)\s*[:=]\s*(\d+(?:\.\d+)?)")
    _RE_JSON = re.compile(r"^\s*\{.*\}\s*$")

    # VMDK descriptor FLAT line:
    # RW <sectors> FLAT "disk-flat.vmdk" 0
    _RE_VMDK_FLAT = re.compile(r'^\s*RW\s+\d+\s+FLAT\s+"([^"]+)"\s+\d+\s*$', re.MULTILINE)

    # -----------------------------
    # Public entry
    # -----------------------------

    @staticmethod
    def to_working(logger: logging.Logger, src: Path, outdir: Path, fmt: str) -> Path:
        if U.which("qemu-img") is None:
            U.die(logger, "qemu-img not found (install qemu-utils).", 1)

        src = Path(src)
        outdir = Path(outdir)

        if not src.exists():
            U.die(logger, f"Input image not found: {src}", 1)

        U.ensure_dir(outdir)

        # 0) VMDK-descriptor-only fast path (harmless for VHD; it will just return None)
        fast = Flatten._fast_path_flat(logger, src, outdir, fmt)
        if fast is not None:
            return fast

        final_dst = outdir / f"working-flattened-{U.now_ts()}.{fmt}"
        tmp_dst = _atomic_tmp(final_dst)
        _unlink_quiet(tmp_dst)

        U.banner(logger, "Flatten snapshot chain")
        logger.info("Flattening via qemu-img convert (single self-contained image)…")

        # 1) Detect input format + virtual size once (fixes VHD/VPC vs VMDK)
        info = Flatten._qemu_img_info(logger, src)
        in_fmt = (info.get("format") or "").strip() or None
        virt_size = int(info.get("virtual-size", 0) or 0)

        if in_fmt:
            logger.info(f"Detected input format: {in_fmt}")
        else:
            logger.warning("Could not detect input format; will rely on qemu-img autodetect (no -f).")

        policy = _ProgressPolicy()
        attempts = Flatten._flatten_cmd_attempts(src=src, tmp_dst=tmp_dst, fmt=fmt, in_fmt=in_fmt)

        last_err: Optional[subprocess.CalledProcessError] = None
        for i, cmd in enumerate(attempts, start=1):
            _unlink_quiet(tmp_dst)
            logger.debug(f"[flatten attempt {i}/{len(attempts)}] {' '.join(cmd)}")

            rc, stderr_lines = Flatten._run_qemu_img_with_live_progress(
                logger,
                cmd,
                tmp_dst=tmp_dst,
                virt_size=virt_size,
                policy=policy,
                task_label="Flattening",
            )

            if rc == 0:
                tmp_dst.replace(final_dst)
                logger.info(f"Flatten output: {final_dst}")
                return final_dst

            tail = "\n".join(stderr_lines[-160:]) if stderr_lines else ""
            logger.error(f"Flatten attempt {i} failed (rc={rc})")
            if tail:
                logger.error("qemu-img stderr (tail):\n" + tail)

            last_err = subprocess.CalledProcessError(rc, cmd)

        _unlink_quiet(tmp_dst)
        assert last_err is not None
        raise last_err

    # Attempts (NO --target-is-zero)

    @staticmethod
    def _flatten_cmd_attempts(*, src: Path, tmp_dst: Path, fmt: str, in_fmt: Optional[str]) -> list[list[str]]:
        """
        Build retry commands.
          - Prefer cache-bypass (-t/-T none) first.
          - Use -f <in_fmt> if known; otherwise omit -f (autodetect).
        """
        base_fast = ["qemu-img", "convert", "-p", "-t", "none", "-T", "none"]
        base_compat = ["qemu-img", "convert", "-p"]

        if in_fmt:
            base_fast += ["-f", in_fmt]
            base_compat += ["-f", in_fmt]

        return [
            base_fast + ["-O", fmt, str(src), str(tmp_dst)],
            base_compat + ["-O", fmt, str(src), str(tmp_dst)],
        ]

    @staticmethod
    def _raw_to_fmt_cmd_attempts(*, raw_src: Path, tmp_dst: Path, fmt: str) -> list[list[str]]:
        # Avoid --target-is-zero (requires -n and breaks across qemu versions)
        return [
            ["qemu-img", "convert", "-p", "-t", "none", "-T", "none", "-f", "raw", "-O", fmt, str(raw_src), str(tmp_dst)],
            ["qemu-img", "convert", "-p", "-f", "raw", "-O", fmt, str(raw_src), str(tmp_dst)],
        ]

    # Fast FLAT path (descriptor->extent byte copy)

    @staticmethod
    def _fast_path_flat(logger: logging.Logger, src: Path, outdir: Path, fmt: str) -> Optional[Path]:
        # Only makes sense for tiny VMDK descriptors
        try:
            if src.stat().st_size > 2 * 1024 * 1024:
                return None
            txt = src.read_text(errors="replace")
        except Exception:
            return None

        m = Flatten._RE_VMDK_FLAT.search(txt)
        if not m:
            return None

        href = m.group(1)
        href_norm = href.replace("\\", "/").lstrip("/")
        extent = (src.parent / href_norm).resolve()

        if not extent.exists():
            logger.warning(f"FLAT extent referenced but not found: {extent}")
            return None

        U.banner(logger, "Fast FLAT flatten")
        logger.info(f"Detected FLAT extent; using byte-copy fast path: {extent}")

        raw_dst = outdir / f"working-flat-{U.now_ts()}.raw"
        raw_tmp = _atomic_tmp(raw_dst)
        _unlink_quiet(raw_tmp)

        if U.which("cp") is not None:
            try:
                subprocess.run(
                    ["cp", "--reflink=auto", "--sparse=always", str(extent), str(raw_tmp)],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            except Exception:
                Flatten._copy_with_progress(logger, extent, raw_tmp)
        else:
            Flatten._copy_with_progress(logger, extent, raw_tmp)

        raw_tmp.replace(raw_dst)

        if fmt.lower() == "raw":
            logger.info(f"Fast FLAT output (raw): {raw_dst}")
            return raw_dst

        # raw -> fmt with progress + retries
        final_dst = outdir / f"working-flattened-{U.now_ts()}.{fmt}"
        tmp_dst = _atomic_tmp(final_dst)
        _unlink_quiet(tmp_dst)

        virt_size = raw_dst.stat().st_size
        policy = _ProgressPolicy()
        attempts = Flatten._raw_to_fmt_cmd_attempts(raw_src=raw_dst, tmp_dst=tmp_dst, fmt=fmt)

        last_err: Optional[subprocess.CalledProcessError] = None
        for i, cmd in enumerate(attempts, start=1):
            _unlink_quiet(tmp_dst)
            logger.debug(f"[raw->fmt attempt {i}/{len(attempts)}] {' '.join(cmd)}")

            rc, stderr_lines = Flatten._run_qemu_img_with_live_progress(
                logger,
                cmd,
                tmp_dst=tmp_dst,
                virt_size=virt_size,
                policy=policy,
                task_label=f"Converting raw -> {fmt}",
            )

            if rc == 0:
                tmp_dst.replace(final_dst)
                logger.info(f"Fast FLAT output: {final_dst}")
                return final_dst

            tail = "\n".join(stderr_lines[-160:]) if stderr_lines else ""
            logger.error(f"raw->fmt attempt {i} failed (rc={rc})")
            if tail:
                logger.error("qemu-img stderr (tail):\n" + tail)

            last_err = subprocess.CalledProcessError(rc, cmd)

        _unlink_quiet(tmp_dst)
        assert last_err is not None
        raise last_err


    @staticmethod
    def _run_qemu_img_with_live_progress(
        logger: logging.Logger,
        cmd: list[str],
        *,
        tmp_dst: Path,
        virt_size: int,
        policy: _ProgressPolicy,
        task_label: str,
    ) -> Tuple[int, list[str]]:
        stderr_lines: list[str] = []

        process = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
        assert process.stderr is not None

        sel = selectors.DefaultSelector()
        sel.register(process.stderr, selectors.EVENT_READ)

        use_bytes = virt_size > 0
        total = float(virt_size) if use_bytes else 100.0

        best_completed = 0.0
        last_seen_pct: Optional[float] = None
        last_io_tick = time.monotonic()

        last_ui_t = 0.0
        last_ui_pct = -1.0
        last_log_t = 0.0
        last_log_pct = -1.0

        def parse_progress_pct(line: str) -> Optional[float]:
            s = (line or "").strip()
            if not s:
                return None

            if Flatten._RE_JSON.match(s):
                try:
                    o = json.loads(s)
                    for k in ("progress", "percent", "pct"):
                        if k in o:
                            v = float(o[k])
                            return v if 0.0 <= v <= 100.0 else None
                except Exception:
                    return None

            m = Flatten._RE_PAREN.search(s)
            if m:
                try:
                    return float(m.group(1))
                except Exception:
                    return None

            m = Flatten._RE_PROGRESS.search(s)
            if m:
                try:
                    v = float(m.group(1))
                    return v if 0.0 <= v <= 100.0 else None
                except Exception:
                    return None

            m = Flatten._RE_PERCENT.search(s)
            if m:
                try:
                    v = float(m.group(1))
                    return v if 0.0 <= v <= 100.0 else None
                except Exception:
                    return None

            return None

        def pct_to_completed(pct: float) -> float:
            if use_bytes and virt_size > 0:
                return (pct / 100.0) * float(virt_size)
            return pct

        def size_based_completed() -> Optional[float]:
            if not use_bytes or virt_size <= 0:
                return None
            try:
                if not tmp_dst.exists():
                    return None
                out_sz = tmp_dst.stat().st_size
            except Exception:
                return None
            return float(out_sz if out_sz < virt_size else virt_size)

        def update_best(v: float) -> None:
            nonlocal best_completed
            if v > best_completed:
                best_completed = v

        with Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            (DownloadColumn() if use_bytes else TaskProgressColumn()),
            (TransferSpeedColumn() if use_bytes else TextColumn("")),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task(task_label, total=total)

            while True:
                events = sel.select(timeout=policy.io_poll_s)
                for key, _mask in events:
                    line = key.fileobj.readline()
                    if not line:
                        continue
                    last_io_tick = time.monotonic()
                    stderr_lines.append(line.rstrip("\n"))
                    logger.debug(f"qemu-img: {line.rstrip()}")

                    pct = parse_progress_pct(line)
                    if pct is None:
                        continue
                    last_seen_pct = pct
                    update_best(pct_to_completed(pct))

                b = size_based_completed()
                if b is not None:
                    update_best(b)

                now = time.monotonic()
                shown_pct = (best_completed / float(virt_size)) * 100.0 if (use_bytes and virt_size > 0) else best_completed
                shown_pct = _clamp(shown_pct)

                if (
                    (now - last_ui_t) >= policy.ui_interval_s
                    or (shown_pct - last_ui_pct) >= policy.ui_min_step_pct
                    or shown_pct >= 100.0
                ):
                    progress.update(task, completed=best_completed)
                    quiet_for = now - last_io_tick
                    if last_seen_pct is not None:
                        progress.update(task, description=f"{task_label} (qemu-img {last_seen_pct:.1f}% | quiet {quiet_for:.1f}s)")
                    else:
                        progress.update(task, description=f"{task_label} (stderr quiet {quiet_for:.1f}s)")
                    last_ui_t = now
                    last_ui_pct = shown_pct

                if (
                    (now - last_log_t) >= policy.log_interval_s
                    or (shown_pct - last_log_pct) >= policy.log_min_step_pct
                    or shown_pct >= 100.0
                ):
                    logger.info(f"{task_label} progress: {shown_pct:.1f}%")
                    last_log_t = now
                    last_log_pct = shown_pct

                # ✅ Only stop when the process actually exits
                if process.poll() is not None:
                    break

        rc = process.wait()
        return rc, stderr_lines

    # -----------------------------
    # Misc
    # -----------------------------

    @staticmethod
    def _qemu_img_info(logger: logging.Logger, src: Path) -> dict:
        """
        Detect input format + virtual size once.
        Returns {} on failure (caller can omit -f and keep virt_size=0).
        """
        try:
            cp = subprocess.run(
                ["qemu-img", "info", "--output=json", str(src)],
                check=True,
                capture_output=True,
                text=True,
            )
            info = json.loads(cp.stdout or "{}")
            if not isinstance(info, dict):
                return {}
            fmt = (info.get("format") or "").strip()
            vsz = info.get("virtual-size", 0)
            logger.debug(f"qemu-img info: format={fmt or 'unknown'} virtual-size={vsz}")
            return info
        except Exception as e:
            logger.debug(f"Could not determine qemu-img info via qemu-img info: {e}")
            return {}

    @staticmethod
    def _qemu_img_virtual_size(logger: logging.Logger, src: Path) -> int:
        # Kept for compatibility: other callers might use it.
        try:
            cp = subprocess.run(
                ["qemu-img", "info", "--output=json", str(src)],
                check=True,
                capture_output=True,
                text=True,
            )
            info = json.loads(cp.stdout or "{}")
            return int(info.get("virtual-size", 0) or 0)
        except Exception as e:
            logger.debug(f"Could not determine virtual size via qemu-img info: {e}")
            return 0

    @staticmethod
    def _copy_with_progress(logger: logging.Logger, src: Path, dst: Path, *, chunk_mb: int = 16) -> None:
        total = src.stat().st_size
        chunk = max(1, chunk_mb) * 1024 * 1024

        with Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task(f"Copying {src.name}", total=total)
            with open(src, "rb") as rf, open(dst, "wb") as wf:
                while True:
                    buf = rf.read(chunk)
                    if not buf:
                        break
                    wf.write(buf)
                    progress.update(task, advance=len(buf))


# Fetch (remote ESXi fetch helper)

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
        outdir = Path(outdir)
        U.ensure_dir(outdir)

        sshc.check()

        if not sshc.exists(remote_desc):
            U.die(logger, f"Remote descriptor not found: {remote_desc}", 1)

        remote_dir = os.path.dirname(remote_desc)

        local_desc = outdir / os.path.basename(remote_desc)
        logger.info(f"Copying descriptor: {remote_desc} -> {local_desc}")
        Fetch._scp_from_atomic(logger, sshc, remote_desc, local_desc)

        Fetch._fetch_extent_for_descriptor(logger, sshc, remote_dir, local_desc, outdir)

        if fetch_all:
            cur = local_desc
            seen: Set[str] = set()

            while True:
                parent = VMDK.parse_parent(logger, cur)
                if not parent:
                    break
                if parent in seen:
                    logger.warning(f"Parent loop detected at {parent}, stopping fetch")
                    break
                seen.add(parent)

                remote_parent_desc = os.path.join(remote_dir, parent)
                local_parent_desc = outdir / os.path.basename(parent)

                if not sshc.exists(remote_parent_desc):
                    logger.warning(f"Parent descriptor missing: {remote_parent_desc}")
                    break

                logger.info(f"Copying parent descriptor: {remote_parent_desc} -> {local_parent_desc}")
                Fetch._scp_from_atomic(logger, sshc, remote_parent_desc, local_parent_desc)

                Fetch._fetch_extent_for_descriptor(logger, sshc, remote_dir, local_parent_desc, outdir)
                cur = local_parent_desc

        return local_desc

    @staticmethod
    def _fetch_extent_for_descriptor(
        logger: logging.Logger,
        sshc: SSHClient,
        remote_dir: str,
        local_desc: Path,
        outdir: Path,
    ) -> Optional[Path]:
        extent_rel = VMDK.parse_extent(logger, local_desc)

        if extent_rel:
            extent_rel_norm = extent_rel.replace("\\", "/").lstrip("/")
            remote_extent = os.path.join(remote_dir, extent_rel_norm)
        else:
            stem = local_desc.stem
            remote_extent = os.path.join(remote_dir, f"{stem}-flat.vmdk")

        if not sshc.exists(remote_extent):
            logger.warning(f"Extent not found remotely: {remote_extent}")
            return None

        local_extent = outdir / os.path.basename(remote_extent)
        logger.info(f"Copying extent: {remote_extent} -> {local_extent}")
        Fetch._scp_from_atomic(logger, sshc, remote_extent, local_extent)
        return local_extent

    @staticmethod
    def _scp_from_atomic(logger: logging.Logger, sshc: SSHClient, remote: str, local: Path) -> None:
        local = Path(local)
        tmp = _atomic_tmp(local)
        _unlink_quiet(tmp)

        sshc.scp_from(remote, tmp)
        tmp.replace(local)
