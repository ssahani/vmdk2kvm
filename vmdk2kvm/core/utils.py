from __future__ import annotations
import datetime as _dt
import glob
import hashlib
import json
import logging
import os
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .exceptions import Fatal

if TYPE_CHECKING:  # pragma: no cover
    import guestfs  # type: ignore
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn, TransferSpeedColumn

class U:
    @staticmethod
    def die(logger: logging.Logger, msg: str, code: int = 1) -> None:
        logger.error(msg)
        raise Fatal(code, msg)
    @staticmethod
    def ensure_dir(p: Path) -> None:
        p.mkdir(parents=True, exist_ok=True)
    @staticmethod
    def which(prog: str) -> Optional[str]:
        from shutil import which as _which
        return _which(prog)
    @staticmethod
    def now_ts() -> str:
        return _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    @staticmethod
    def json_dump(obj: Any) -> str:
        try:
            return json.dumps(obj, indent=2, sort_keys=True, default=str)
        except Exception:
            return repr(obj)
    @staticmethod
    def human_bytes(n: Optional[int]) -> str:
        if n is None:
            return "unknown"
        x = float(n)
        for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
            if x < 1024 or unit == "TiB":
                return f"{x:.2f} {unit}"
            x /= 1024
        return f"{n} B"
    @staticmethod
    def banner(logger: logging.Logger, title: str) -> None:
        line = "─" * max(10, len(title) + 2)
        logger.info(line)
        logger.info(f" {title}")
        logger.info(line)
    @staticmethod
    def run_cmd(
        logger: logging.Logger,
        cmd: List[str],
        *,
        check: bool = True,
        capture: bool = False,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
    ) -> subprocess.CompletedProcess:
        pretty = " ".join(shlex.quote(x) for x in cmd)
        logger.debug(f"Running: {pretty}")
        try:
            return subprocess.run(
                cmd,
                check=check,
                capture_output=capture,
                text=True,
                env=env,
                timeout=timeout,
            )
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {pretty}\nstdout: {e.stdout}\nstderr: {e.stderr}")
            raise
        except Exception as e:
            logger.error(f"Command error: {pretty} {e}")
            raise
    @staticmethod
    def require_root_if_needed(logger: logging.Logger, write_actions: bool) -> None:
        if not write_actions:
            return
        if os.geteuid() != 0:
            U.die(logger, "This operation requires root. Re-run with sudo.", 1)
    @staticmethod
    def checksum(path: Path, algo: str = "sha256") -> str:
        h = hashlib.new(algo)
        total_size = path.stat().st_size
        with Progress(TextColumn("[progress.description]{task.description}"), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), TransferSpeedColumn(), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
            task = progress.add_task("Computing checksum", total=total_size)
            with open(path, "rb") as f:
                for blk in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(blk)
                    progress.update(task, advance=len(blk))
        return h.hexdigest()
    @staticmethod
    def safe_unlink(p: Path) -> None:
        try:
            p.unlink()
        except Exception:
            pass
    @staticmethod
    def to_text(x: Any) -> str:
        if x is None:
            return ""
        if isinstance(x, bytes):
            return x.decode("utf-8", "replace")
        return str(x)
    @staticmethod
    def human_to_bytes(s: str) -> int:
        s = s.upper().rstrip('B')
        suffixes = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4}
        for suffix, multiplier in suffixes.items():
            if s.endswith(suffix):
                return int(float(s[:-len(suffix)]) * multiplier)
        return int(float(s))

def guest_has_cmd(g: guestfs.GuestFS, cmd: str) -> bool:
    """
    Replacement for g.available() checks.
    Uses a shell inside the appliance so it works across guestfs builds.
    """
    try:
        out = g.command(["sh", "-lc", f"command -v {shlex.quote(cmd)} >/dev/null 2>&1 && echo YES || echo NO"])
        return U.to_text(out).strip() == "YES"
    except Exception:
        return False

def guest_ls_glob(g: guestfs.GuestFS, pattern: str) -> List[str]:
    """
    Replacement for g.glob().
    Uses shell expansion for globs and returns existing matches (one per line).
    """
    try:
        # -1 => one path per line, 2>/dev/null to avoid noise when no matches
        out = g.command(["sh", "-lc", f"ls -1 {pattern} 2>/dev/null || true"])
        lines = [ln.strip() for ln in U.to_text(out).splitlines() if ln.strip()]
        # ls may print literal when pattern doesn't match (depends on shell); guard by checking existence
        res: List[str] = []
        for p in lines:
            try:
                if g.is_file(p) or g.is_dir(p):
                    res.append(p)
            except Exception:
                # If is_file fails, keep conservative: include nothing
                pass
        return res
    except Exception:
        return []

def blinking_progress(logger, label: str, interval: float = 0.12):
    """Tiny spinner context manager for long-running external commands.

    Used so the CLI feels alive even when tools are quiet.
    If Rich is available, prefer Rich progress elsewhere; this is a lightweight fallback.
    """
    import contextlib
    import itertools
    import sys
    import threading
    import time

    @contextlib.contextmanager
    def _cm():
        stop = threading.Event()
        spinner = itertools.cycle(["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"])
        def run():
            while not stop.is_set():
                ch = next(spinner)
                sys.stderr.write(f"\r{ch} {label}")
                sys.stderr.flush()
                time.sleep(interval)
            sys.stderr.write(f"\r✅ {label}\n")
            sys.stderr.flush()
        t = threading.Thread(target=run, daemon=True)
        t.start()
        try:
            yield
        finally:
            stop.set()
            t.join(timeout=1.0)
    return _cm()
