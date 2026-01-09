# SPDX-License-Identifier: LGPL-3.0-or-later
# vmdk2kvm/vsphere/nfc_lease_client_govc.py
# -*- coding: utf-8 -*-
"""
NFC export/download via govc CLI (govmomi).

Why this exists:
- You already have a *custom* NFC data-plane downloader (requests + Range + retries).
- Sometimes you want the "just export it" path: let govc manage HttpNfcLease + keepalive
  + URL signing + downloads, and you simply orchestrate it reliably.

Important differences vs nfc_lease_client.py:
- govc is NOT a pure data-plane client. It performs control-plane + data-plane together
  for export.ovf / export.ova.
- There is no lease heartbeat callback here: govc keeps the lease alive internally.
- Resume semantics are best-effort: govc does not guarantee HTTP Range resume.
  We implement "idempotent skip" + retries around the govc command, and atomic publish.

Docs/refs:
- govc is shipped from vmware/govmomi and provides export.ovf / export.ova commands. :contentReference[oaicite:0]{index=0}
"""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
import tempfile
import time
import random
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Callable


# -----------------------------------------------------------------------------
# Errors
# -----------------------------------------------------------------------------

class NFCLeaseError(RuntimeError):
    """Generic NFC (govc) export/download error."""


class NFCLeaseCancelled(NFCLeaseError):
    """Raised when a caller cancels an in-progress export."""


# -----------------------------------------------------------------------------
# Types (kept compatible-ish with your existing signatures)
# -----------------------------------------------------------------------------

ProgressFn = Callable[[int, int, float], None]
CancelFn = Callable[[], bool]
LeaseHeartbeatFn = Callable[[int, int], None]  # accepted but not used (govc handles keepalive)


# -----------------------------------------------------------------------------
# Session / Config
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class GovcSessionSpec:
    """
    govc auth/session config.

    You can supply either explicit fields below, or rely on existing GOVC_* env
    already exported in the process environment.
    """
    url: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    # govc -k / GOVC_INSECURE
    insecure: Optional[bool] = None

    # Optional extras
    ca_certs: Optional[str] = None           # GOVC_TLS_CA_CERTS
    thumbprint: Optional[str] = None         # GOVC_THUMBPRINT
    token: Optional[str] = None              # GOVC_TOKEN (if you use it)
    debug: Optional[bool] = None             # GOVC_DEBUG (very noisy)
    persist_session: Optional[bool] = None   # GOVC_PERSIST_SESSION

    # Optional inventory context
    datacenter: Optional[str] = None         # GOVC_DATACENTER
    datastore: Optional[str] = None          # GOVC_DATASTORE
    folder: Optional[str] = None             # GOVC_FOLDER
    resource_pool: Optional[str] = None      # GOVC_RESOURCE_POOL
    host: Optional[str] = None               # GOVC_HOST
    cluster: Optional[str] = None            # GOVC_CLUSTER


@dataclass(frozen=True)
class GovcExportSpec:
    """
    What to export.

    vm: inventory path or name that govc can resolve (often "vm/MyVM" or "MyVM").
    out_dir: final output directory where exported files should land.
    """
    vm: str
    out_dir: Path

    # export options
    export_ova: bool = False  # if True, uses `govc export.ova`; else `govc export.ovf`
    name: Optional[str] = None  # optional target base directory name under out_dir

    # Pass-through flags (used only if set)
    dc: Optional[str] = None
    ds: Optional[str] = None
    folder: Optional[str] = None
    pool: Optional[str] = None
    host: Optional[str] = None
    cluster: Optional[str] = None

    # govc binary path (default: resolve from PATH)
    govc_bin: str = "govc"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _env_apply(session: GovcSessionSpec, base: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    env = dict(base or os.environ)

    def set_if(k: str, v: Optional[str]) -> None:
        if v is not None:
            env[k] = v

    def set_bool(k: str, v: Optional[bool]) -> None:
        if v is not None:
            env[k] = "1" if v else "0"

    set_if("GOVC_URL", session.url)
    set_if("GOVC_USERNAME", session.username)
    set_if("GOVC_PASSWORD", session.password)

    set_bool("GOVC_INSECURE", session.insecure)
    set_if("GOVC_TLS_CA_CERTS", session.ca_certs)
    set_if("GOVC_THUMBPRINT", session.thumbprint)
    set_if("GOVC_TOKEN", session.token)
    set_bool("GOVC_DEBUG", session.debug)
    set_bool("GOVC_PERSIST_SESSION", session.persist_session)

    set_if("GOVC_DATACENTER", session.datacenter)
    set_if("GOVC_DATASTORE", session.datastore)
    set_if("GOVC_FOLDER", session.folder)
    set_if("GOVC_RESOURCE_POOL", session.resource_pool)
    set_if("GOVC_HOST", session.host)
    set_if("GOVC_CLUSTER", session.cluster)

    return env


def _mkdirp(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _atomic_publish_dir(tmp_dir: Path, final_dir: Path) -> None:
    """
    Publish exported directory atomically (best-effort):
    - if final exists, we merge/overwrite files from tmp (safe for idempotent export)
    - then remove tmp
    """
    _mkdirp(final_dir)
    for root, _dirs, files in os.walk(tmp_dir):
        rel = Path(root).relative_to(tmp_dir)
        dst_root = final_dir / rel
        _mkdirp(dst_root)
        for fn in files:
            src = Path(root) / fn
            dst = dst_root / fn
            # overwrite is ok (export is deterministic per VM snapshot time)
            os.replace(str(src), str(dst))
    shutil.rmtree(tmp_dir, ignore_errors=True)


def _parse_govc_progress(line: str) -> Optional[Tuple[int, int, float]]:
    """
    Best-effort parsing of govc progress output.
    govc output formats vary by command/version; we keep it permissive.

    Recognizes:
      - "xx%" patterns (no bytes)
      - "<done> / <total>" bytes-ish if present
    """
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", line)
    if m:
        pct = float(m.group(1))
        return (0, 0, pct)
    return None


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------

class GovcNfcExporter:
    """
    govc-backed exporter.

    Guarantees we provide:
    - retries/backoff around the govc invocation
    - atomic publish of output into final out_dir subdir
    - optional "skip if already exported" heuristic
    """

    def __init__(self, logger: logging.Logger, session: GovcSessionSpec):
        self.logger = logger
        self.session = session

    def export(
        self,
        spec: GovcExportSpec,
        *,
        resume: bool = True,
        progress: Optional[ProgressFn] = None,
        progress_interval_s: float = 0.5,
        cancel: Optional[CancelFn] = None,
        heartbeat: Optional[LeaseHeartbeatFn] = None,  # accepted for signature compatibility; ignored
        max_retries: int = 5,
        base_backoff_s: float = 1.0,
        max_backoff_s: float = 20.0,
        jitter_s: float = 0.5,
        skip_if_present: bool = True,
    ) -> Path:
        """
        Export VM via govc into out_dir.

        Returns the directory containing OVF/OVA + disks:
          - export.ovf -> directory
          - export.ova -> file (we still stage into a temp dir, then publish file)

        Notes:
        - govc handles HttpNfcLease creation + keepalive + download.
        - `resume` is best-effort: we implement "skip if already present" + retries.
        """
        out_dir = Path(spec.out_dir).expanduser().resolve()
        _mkdirp(out_dir)

        target_name = spec.name or self._default_name_from_vm(spec.vm)
        final_path = out_dir / target_name

        # Fast path: already exported
        if skip_if_present and final_path.exists():
            if spec.export_ova:
                if final_path.is_file() and final_path.stat().st_size > 0:
                    self.logger.info("✅ govc: output already present, skipping: %s", final_path)
                    return final_path
            else:
                # OVF export usually yields a directory with .ovf + .mf + one or more disks.
                ovf_files = list(final_path.glob("*.ovf"))
                if final_path.is_dir() and ovf_files:
                    self.logger.info("✅ govc: output already present, skipping: %s", final_path)
                    return final_path

        # Stage into temp, then publish
        stage_parent = out_dir / f".{target_name}.govc.stage"
        _mkdirp(stage_parent)

        env = _env_apply(self.session)

        # Build command
        cmd: List[str] = [spec.govc_bin]
        if spec.export_ova:
            cmd += ["export.ova", "-vm", spec.vm]
        else:
            cmd += ["export.ovf", "-vm", spec.vm]

        # Optional flags (only if set)
        # These flags are common in govc; if your version differs, govc will error and we retry/fail loudly.
        # (We keep them optional for compatibility.)
        if spec.dc:
            cmd += ["-dc", spec.dc]
        if spec.ds:
            cmd += ["-ds", spec.ds]
        if spec.folder:
            cmd += ["-folder", spec.folder]
        if spec.pool:
            cmd += ["-pool", spec.pool]
        if spec.host:
            cmd += ["-host", spec.host]
        if spec.cluster:
            cmd += ["-cluster", spec.cluster]

        attempt = 0
        last_cb = 0.0

        while True:
            if cancel and cancel():
                raise NFCLeaseCancelled("Export cancelled")

            attempt += 1
            stage_dir = Path(
                tempfile.mkdtemp(prefix=f"{target_name}.", dir=str(stage_parent))
            )

            # Where govc should write:
            # - OVF export: target directory (govc creates files inside)
            # - OVA export: a file path
            if spec.export_ova:
                stage_out = stage_dir / f"{target_name}.ova"
                cmd_run = cmd + [str(stage_out)]
            else:
                stage_out = stage_dir / target_name
                cmd_run = cmd + [str(stage_out)]

            self.logger.info(
                "🚚 govc: export start (attempt %d/%d): %s",
                attempt,
                max_retries,
                " ".join(shlex.quote(x) for x in cmd_run),
            )

            try:
                self._run_govc(
                    cmd_run,
                    env=env,
                    cancel=cancel,
                    progress=progress,
                    progress_interval_s=progress_interval_s,
                    last_cb_holder=[last_cb],
                )
                last_cb = time.time()

                # Validate stage output exists
                if spec.export_ova:
                    if not stage_out.exists() or stage_out.stat().st_size <= 0:
                        raise NFCLeaseError(f"govc export produced empty OVA: {stage_out}")
                    # Publish file atomically
                    os.replace(str(stage_out), str(final_path))
                    shutil.rmtree(stage_dir, ignore_errors=True)
                else:
                    if not stage_out.exists() or not stage_out.is_dir():
                        raise NFCLeaseError(f"govc export did not create output dir: {stage_out}")
                    ovfs = list(stage_out.glob("*.ovf"))
                    if not ovfs:
                        raise NFCLeaseError(f"govc export output missing .ovf: {stage_out}")
                    # Publish directory (merge overwrite)
                    if final_path.exists() and final_path.is_file():
                        raise NFCLeaseError(f"Final path exists as file, expected dir: {final_path}")
                    _mkdirp(final_path)
                    _atomic_publish_dir(stage_out, final_path)
                    shutil.rmtree(stage_dir, ignore_errors=True)

                self.logger.info("✅ govc: export done: %s", final_path)
                return final_path

            except NFCLeaseCancelled:
                self.logger.warning("🛑 govc: export cancelled (kept stage dir): %s", stage_dir)
                raise
            except Exception as e:
                # Clean stage dir unless you want to keep for debugging
                shutil.rmtree(stage_dir, ignore_errors=True)

                if attempt >= int(max_retries):
                    raise NFCLeaseError(f"govc export failed after {attempt} attempts: {e}") from e

                backoff = min(float(max_backoff_s), float(base_backoff_s) * (2 ** (attempt - 1)))
                backoff += random.uniform(0.0, max(0.0, float(jitter_s)))

                # "resume" here means: we retry govc (which will re-export); we do not guarantee HTTP range resume.
                self.logger.warning(
                    "🔁 govc: transient export error: %s (retry %d/%d in %.2fs)",
                    e,
                    attempt,
                    int(max_retries),
                    backoff,
                )
                time.sleep(backoff)

    # -------------------------------------------------------------------------

    def _default_name_from_vm(self, vm: str) -> str:
        # sanitize like libvirt: replace slashes so path components are safe
        return vm.replace("/", "_").replace("\\", "_").strip() or "vm"

    def _run_govc(
        self,
        cmd: List[str],
        *,
        env: Dict[str, str],
        cancel: Optional[CancelFn],
        progress: Optional[ProgressFn],
        progress_interval_s: float,
        last_cb_holder: List[float],
    ) -> None:
        """
        Run govc and stream stdout/stderr to logger.

        We parse percentage-like lines and emit progress callbacks when possible.
        """
        # Use line-buffered text so we can parse progress.
        p = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )

        try:
            assert p.stdout is not None
            for line in p.stdout:
                if cancel and cancel():
                    try:
                        p.terminate()
                    except Exception:
                        pass
                    raise NFCLeaseCancelled("Export cancelled")

                line = line.rstrip("\n")
                if line:
                    # Log govc output (debug to avoid spam unless you want info)
                    self.logger.debug("govc: %s", line)

                # Best-effort progress parse
                if progress is not None:
                    parsed = _parse_govc_progress(line)
                    if parsed is not None:
                        done, total, pct = parsed
                        now = time.time()
                        if (now - last_cb_holder[0]) >= max(0.05, float(progress_interval_s)):
                            last_cb_holder[0] = now
                            progress(done, total, pct)

            rc = p.wait()
            if rc != 0:
                raise NFCLeaseError(f"govc exited with rc={rc}")
        finally:
            try:
                if p.stdout:
                    p.stdout.close()
            except Exception:
                pass


# -----------------------------------------------------------------------------
# Convenience wrapper (mirrors your download_many pattern)
# -----------------------------------------------------------------------------

def export_with_govc(
    logger: logging.Logger,
    session: GovcSessionSpec,
    vm: str,
    out_dir: Path,
    *,
    export_ova: bool = False,
    name: Optional[str] = None,
    # Compat knobs
    resume: bool = True,
    progress: Optional[ProgressFn] = None,
    progress_interval_s: float = 0.5,
    cancel: Optional[CancelFn] = None,
    heartbeat: Optional[LeaseHeartbeatFn] = None,  # ignored
    max_retries: int = 5,
) -> Path:
    spec = GovcExportSpec(
        vm=vm,
        out_dir=out_dir,
        export_ova=export_ova,
        name=name,
    )
    return GovcNfcExporter(logger, session).export(
        spec,
        resume=resume,
        progress=progress,
        progress_interval_s=progress_interval_s,
        cancel=cancel,
        heartbeat=heartbeat,
        max_retries=max_retries,
    )
