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

Enhancements in this version:
- Batch exporting multiple VMs to local output directory
- Optional inventory listing helpers (govc find) to discover VMs under folders/pools
- Result reporting for batch operations (success/skip/fail)
- Optional concurrency (ThreadPoolExecutor) with sane defaults
- Optional "keep_failed_stage" for post-mortem debugging
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
from typing import Dict, Iterable, List, Optional, Tuple, Callable, Sequence

from concurrent.futures import ThreadPoolExecutor, as_completed


class NFCLeaseError(RuntimeError):
    """Generic NFC (govc) export/download error."""


class NFCLeaseCancelled(NFCLeaseError):
    """Raised when a caller cancels an in-progress export."""


ProgressFn = Callable[[int, int, float], None]
CancelFn = Callable[[], bool]
LeaseHeartbeatFn = Callable[[int, int], None]  # accepted but not used (govc handles keepalive)


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


@dataclass(frozen=True)
class GovcBatchSpec:
    """
    Batch export spec: export many VMs to the same out_dir.

    vms: list of inventory paths or names.
    out_dir: output directory.
    export_ova: export as OVA instead of OVF directories.
    concurrency: number of parallel exports (default 1; safer for vCenter).
    stop_on_error: if True, abort after first failure.
    """
    vms: Sequence[str]
    out_dir: Path
    export_ova: bool = False
    concurrency: int = 1
    stop_on_error: bool = False
    skip_if_present: bool = True


@dataclass(frozen=True)
class GovcBatchResult:
    vm: str
    status: str  # "ok" | "skipped" | "failed" | "cancelled"
    path: Optional[Path] = None
    error: Optional[str] = None


# Helpers

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
            os.replace(str(src), str(dst))
    shutil.rmtree(tmp_dir, ignore_errors=True)


def _parse_govc_progress(line: str) -> Optional[Tuple[int, int, float]]:
    """
    Best-effort parsing of govc progress output.
    govc output formats vary by command/version; we keep it permissive.

    Recognizes:
      - "xx%" patterns (no bytes)
    """
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", line)
    if m:
        pct = float(m.group(1))
        return (0, 0, pct)
    return None


def _looks_like_complete_ovf_dir(p: Path) -> bool:
    """
    Heuristic "already exported" check for OVF directory:
    - has at least one .ovf
    - has at least one disk-ish file (.vmdk/.vhd/.vhdx/.img/.raw/.qcow2) OR a .mf
      (some exports include a manifest; disks might have different extensions depending on govc/export settings)
    """
    if not p.is_dir():
        return False
    if not list(p.glob("*.ovf")):
        return False
    disk_exts = (".vmdk", ".vhd", ".vhdx", ".img", ".raw", ".qcow2")
    has_disk = any(f.is_file() and f.suffix.lower() in disk_exts for f in p.iterdir())
    has_mf = bool(list(p.glob("*.mf")))
    return has_disk or has_mf


def _is_nonempty_file(p: Path) -> bool:
    try:
        return p.is_file() and p.stat().st_size > 0
    except Exception:
        return False


# Public API

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
        keep_failed_stage: bool = False,
    ) -> Path:
        """
        Export VM via govc into out_dir.

        Returns the path containing OVF/OVA + disks:
          - export.ovf -> directory
          - export.ova -> file (we stage in a temp dir, then publish file)

        Notes:
        - govc handles HttpNfcLease creation + keepalive + download.
        - `resume` is best-effort: we implement "skip if already present" + retries.
        """
        _ = resume  # best-effort; currently used via skip/retry semantics.

        out_dir = Path(spec.out_dir).expanduser().resolve()
        _mkdirp(out_dir)

        target_name = spec.name or self._default_name_from_vm(spec.vm)
        final_path = out_dir / target_name

        # Fast path: already exported
        if skip_if_present and final_path.exists():
            if spec.export_ova:
                if _is_nonempty_file(final_path):
                    self.logger.info("✅ govc: output already present, skipping: %s", final_path)
                    return final_path
            else:
                if _looks_like_complete_ovf_dir(final_path):
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
            stage_dir = Path(tempfile.mkdtemp(prefix=f"{target_name}.", dir=str(stage_parent)))

            # Where govc should write:
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

                # Validate stage output exists + publish
                if spec.export_ova:
                    if not _is_nonempty_file(stage_out):
                        raise NFCLeaseError(f"govc export produced empty OVA: {stage_out}")
                    # Publish file atomically
                    os.replace(str(stage_out), str(final_path))
                else:
                    if not stage_out.exists() or not stage_out.is_dir():
                        raise NFCLeaseError(f"govc export did not create output dir: {stage_out}")
                    if not list(stage_out.glob("*.ovf")):
                        raise NFCLeaseError(f"govc export output missing .ovf: {stage_out}")
                    if final_path.exists() and final_path.is_file():
                        raise NFCLeaseError(f"Final path exists as file, expected dir: {final_path}")
                    _mkdirp(final_path)
                    _atomic_publish_dir(stage_out, final_path)

                # Cleanup stage_dir on success
                shutil.rmtree(stage_dir, ignore_errors=True)

                self.logger.info("✅ govc: export done: %s", final_path)
                return final_path

            except NFCLeaseCancelled:
                self.logger.warning("🛑 govc: export cancelled (kept stage dir): %s", stage_dir)
                raise
            except Exception as e:
                # Clean stage dir unless debugging requested
                if keep_failed_stage:
                    self.logger.warning("🧪 govc: keeping failed stage dir for debugging: %s", stage_dir)
                else:
                    shutil.rmtree(stage_dir, ignore_errors=True)

                if attempt >= int(max_retries):
                    raise NFCLeaseError(f"govc export failed after {attempt} attempts: {e}") from e

                backoff = min(float(max_backoff_s), float(base_backoff_s) * (2 ** (attempt - 1)))
                backoff += random.uniform(0.0, max(0.0, float(jitter_s)))

                self.logger.warning(
                    "🔁 govc: transient export error: %s (retry %d/%d in %.2fs)",
                    e,
                    attempt,
                    int(max_retries),
                    backoff,
                )
                time.sleep(backoff)

    def export_many(
        self,
        spec: GovcBatchSpec,
        *,
        name_fn: Optional[Callable[[str], str]] = None,
        resume: bool = True,
        progress: Optional[Callable[[str, int, int, float], None]] = None,
        progress_interval_s: float = 0.5,
        cancel: Optional[CancelFn] = None,
        heartbeat: Optional[LeaseHeartbeatFn] = None,  # ignored
        max_retries: int = 5,
        base_backoff_s: float = 1.0,
        max_backoff_s: float = 20.0,
        jitter_s: float = 0.5,
        keep_failed_stage: bool = False,
    ) -> List[GovcBatchResult]:
        """
        Export multiple VMs to local out_dir.

        Returns a list of GovcBatchResult with per-VM status.
        - concurrency defaults to 1 (safe), but you can raise it.
        - progress callback (if provided) includes vm name as first arg.
        """
        _ = heartbeat  # signature compatibility; govc handles keepalive
        out_dir = Path(spec.out_dir).expanduser().resolve()
        _mkdirp(out_dir)

        vms = list(spec.vms)
        if not vms:
            return []

        conc = max(1, int(spec.concurrency))

        def _export_one(vm: str) -> GovcBatchResult:
            if cancel and cancel():
                return GovcBatchResult(vm=vm, status="cancelled", path=None, error="cancelled")

            name = (name_fn(vm) if name_fn else None)
            es = GovcExportSpec(
                vm=vm,
                out_dir=out_dir,
                export_ova=bool(spec.export_ova),
                name=name,
            )

            # Wrap per-VM progress
            vm_progress: Optional[ProgressFn]
            if progress is None:
                vm_progress = None
            else:
                def _p(done: int, total: int, pct: float) -> None:
                    progress(vm, done, total, pct)
                vm_progress = _p

            target_name = es.name or self._default_name_from_vm(es.vm)
            final_path = out_dir / target_name

            # Pre-skip classification
            if spec.skip_if_present and final_path.exists():
                if es.export_ova and _is_nonempty_file(final_path):
                    return GovcBatchResult(vm=vm, status="skipped", path=final_path)
                if (not es.export_ova) and _looks_like_complete_ovf_dir(final_path):
                    return GovcBatchResult(vm=vm, status="skipped", path=final_path)

            try:
                p = self.export(
                    es,
                    resume=resume,
                    progress=vm_progress,
                    progress_interval_s=progress_interval_s,
                    cancel=cancel,
                    heartbeat=None,
                    max_retries=max_retries,
                    base_backoff_s=base_backoff_s,
                    max_backoff_s=max_backoff_s,
                    jitter_s=jitter_s,
                    skip_if_present=spec.skip_if_present,
                    keep_failed_stage=keep_failed_stage,
                )
                return GovcBatchResult(vm=vm, status="ok", path=p)
            except NFCLeaseCancelled as e:
                return GovcBatchResult(vm=vm, status="cancelled", path=None, error=str(e))
            except Exception as e:
                return GovcBatchResult(vm=vm, status="failed", path=None, error=str(e))

        results: List[GovcBatchResult] = []

        if conc == 1:
            for vm in vms:
                r = _export_one(vm)
                results.append(r)
                if spec.stop_on_error and r.status == "failed":
                    break
                if cancel and cancel():
                    break
            return results

        # Parallel exports (be cautious with vCenter load)
        with ThreadPoolExecutor(max_workers=conc) as ex:
            futs = {ex.submit(_export_one, vm): vm for vm in vms}
            for fut in as_completed(futs):
                r = fut.result()
                results.append(r)
                if r.status == "failed" and spec.stop_on_error:
                    # Best-effort: can't cancel already running exports cleanly, but we stop collecting early.
                    break
                if cancel and cancel():
                    break

        # Keep original input order in results (nice UX)
        order = {vm: i for i, vm in enumerate(vms)}
        results.sort(key=lambda x: order.get(x.vm, 10**9))
        return results

    def list_vms(
        self,
        *,
        root: str = "/",
        kind: str = "v",
        govc_bin: str = "govc",
        extra_args: Optional[Sequence[str]] = None,
    ) -> List[str]:
        """
        Discover VM inventory paths using `govc find`.

        kind:
          - "v" => VMs (default)
          - other govc find kinds if you need them (host/datastore/etc.)

        Example:
          exporter.list_vms(root="/DC1/vm/Prod", kind="v")

        Returns inventory paths (strings), suitable to feed into export().
        """
        env = _env_apply(self.session)
        cmd = [govc_bin, "find", "-type", kind, root]
        if extra_args:
            cmd.extend(list(extra_args))

        self.logger.debug("govc: %s", " ".join(shlex.quote(x) for x in cmd))
        p = subprocess.run(cmd, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if p.returncode != 0:
            msg = (p.stderr or p.stdout or "").strip()
            raise NFCLeaseError(f"govc find failed rc={p.returncode}: {msg}")

        items = [ln.strip() for ln in (p.stdout or "").splitlines() if ln.strip()]
        # For VMs, govc returns paths like "/DC/vm/folder/MyVM"
        return items

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
                    self.logger.debug("govc: %s", line)

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


def export_many_with_govc(
    logger: logging.Logger,
    session: GovcSessionSpec,
    vms: Sequence[str],
    out_dir: Path,
    *,
    export_ova: bool = False,
    concurrency: int = 1,
    stop_on_error: bool = False,
    skip_if_present: bool = True,
    resume: bool = True,
    progress: Optional[Callable[[str, int, int, float], None]] = None,
    progress_interval_s: float = 0.5,
    cancel: Optional[CancelFn] = None,
    heartbeat: Optional[LeaseHeartbeatFn] = None,  # ignored
    max_retries: int = 5,
) -> List[GovcBatchResult]:
    """
    Convenience wrapper: export a list of VMs to local out_dir.
    """
    exporter = GovcNfcExporter(logger, session)
    bs = GovcBatchSpec(
        vms=vms,
        out_dir=out_dir,
        export_ova=export_ova,
        concurrency=concurrency,
        stop_on_error=stop_on_error,
        skip_if_present=skip_if_present,
    )
    return exporter.export_many(
        bs,
        resume=resume,
        progress=progress,
        progress_interval_s=progress_interval_s,
        cancel=cancel,
        heartbeat=heartbeat,
        max_retries=max_retries,
    )
