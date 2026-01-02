# -*- coding: utf-8 -*-
from __future__ import annotations

"""
govc / govmomi common helpers for vmdk2kvm.

This module centralizes:
  - GOVC_* environment seeding from vmdk2kvm args/config
  - govc command execution helpers (text + JSON)
  - datastore path normalization and resilient parsing of `govc datastore.ls -json`

Why:
  The repo previously had three slightly different GovmomiCLI helpers spread across:
    - vmware_client.py
    - vsphere_command.py
    - vsphere_mode.py
  That created drift (and bugs when govc JSON output shape differs by version).

Design goals:
  - Best-effort + additive: if govc isn't available, callers can fall back to pyvmomi
  - Be defensive about output shapes and path forms
  - Keep behavior stable: callers decide preference policy; this module provides primitives
  - Make debugging human-friendly: log what we normalize, what we run, and how we parse
"""

import json
import os
import re
import subprocess
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from ..core.exceptions import VMwareError

# Optional: use project JSON helpers if present (keeps formatting consistent)
try:  # pragma: no cover
    from ..core.utils import U  # type: ignore
except Exception:  # pragma: no cover
    U = None  # type: ignore


# --------------------------------------------------------------------------------------
# Datastore path normalization (accepts "[ds] path" or "path")
# --------------------------------------------------------------------------------------

_DS_BACKING_RE = re.compile(r"^\[(?P<ds>[^\]]+)\]\s*(?P<path>.+)$")


def normalize_ds_path(datastore: str, ds_path: str) -> Tuple[str, str]:
    """
    Normalize datastore paths for govc.

    Accepts:
      - "[datastore] folder/file"
      - "folder/file"
      - "/folder/file"

    Returns:
      (datastore, "folder/file")  # datastore-relative, no leading slash
    """
    s = (ds_path or "").strip()
    if not s:
        raise VMwareError("empty datastore path")

    m = _DS_BACKING_RE.match(s)
    if m:
        ds = (m.group("ds") or "").strip()
        path = (m.group("path") or "").strip()
        return (ds or datastore), path.lstrip("/")

    return datastore, s.lstrip("/")


# --------------------------------------------------------------------------------------
# Resilient parsing for `govc datastore.ls -json`
# --------------------------------------------------------------------------------------

def _flatten_any(obj: Any) -> List[Any]:
    """Flatten nested dict/list structures into a list of candidate file entries."""
    if obj is None:
        return []
    if isinstance(obj, list):
        out: List[Any] = []
        for v in obj:
            out.extend(_flatten_any(v))
        return out
    if isinstance(obj, dict):
        # Most common keys seen across govc versions
        for k in ("file", "File", "files", "Files", "items", "Items", "Elements", "elements"):
            v = obj.get(k)
            if isinstance(v, list):
                return v
        out: List[Any] = []
        for v in obj.values():
            if isinstance(v, (dict, list)):
                out.extend(_flatten_any(v))
        return out
    return []


def _extract_path(ent: Any) -> Optional[str]:
    if ent is None:
        return None
    if isinstance(ent, str):
        return ent
    if isinstance(ent, dict):
        # govc datastore.ls -json (newer) often uses: {"path": "..."}
        for k in ("path", "Path", "FilePath", "Name", "name"):
            v = ent.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def extract_paths_from_datastore_ls_json(data: Any) -> List[str]:
    """
    Extract file paths from govc datastore.ls -json output.

    Known shapes (examples):
      - [ { "folderPath": "[ds] folder/", "file": [ {"path": "a.vmdk"}, ... ] } ]
      - { "file": [ {"path": "a.vmdk"}, ... ] }
      - { "Files": [ {"Path": "a.vmdk"}, ... ] }
      - { "Elements": [ "a.vmdk", ... ] }

    Returns:
      List[str] of extracted paths (as provided by govc), with leading slashes stripped.
      Callers may further normalize relative-to-folder behavior.
    """
    raw = _flatten_any(data)
    out: List[str] = []
    for ent in raw:
        p = _extract_path(ent)
        if not p:
            continue
        out.append(str(p).lstrip("/"))
    # De-dup while preserving order
    seen = set()
    uniq: List[str] = []
    for p in out:
        if p in seen:
            continue
        seen.add(p)
        uniq.append(p)
    return uniq


# --------------------------------------------------------------------------------------
# govc runner (debuggable + defensive)
# --------------------------------------------------------------------------------------

@dataclass
class GovcConfig:
    govc_bin: str = "govc"
    disable: bool = False  # user requested no govmomi/govc

    vcenter: Optional[str] = None
    vc_user: Optional[str] = None
    vc_password: Optional[str] = None
    vc_password_env: Optional[str] = None
    vc_insecure: bool = False
    dc_name: Optional[str] = None


_GOVC_USAGE_MARKERS = (
    "Usage: govc <COMMAND>",
    "The available commands are listed below.",
    "govmomi is a Go library for interacting",
)


def _looks_like_govc_usage(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    return any(m in t for m in _GOVC_USAGE_MARKERS)


def _mask_secret(v: Optional[str]) -> str:
    if not v:
        return "<unset>"
    vv = str(v)
    if len(vv) <= 4:
        return "***"
    return f"{vv[:2]}***{vv[-2:]}"


def _log(logger: Any, level: str, msg: str, *args: Any) -> None:
    """Safe logger wrapper that never raises."""
    try:
        fn = getattr(logger, level, None)
        if callable(fn):
            fn(msg, *args)
            return
    except Exception:
        pass
    # last resort
    try:
        print((msg % args) if args else msg)
    except Exception:
        return


def _summarize_json_shape(data: Any) -> str:
    try:
        if data is None:
            return "None"
        if isinstance(data, list):
            if not data:
                return "list(len=0)"
            head = data[0]
            if isinstance(head, dict):
                keys = sorted(list(head.keys()))
                return f"list(len={len(data)}), head_keys={keys}"
            return f"list(len={len(data)}), head_type={type(head).__name__}"
        if isinstance(data, dict):
            keys = sorted(list(data.keys()))
            return f"dict(keys={keys})"
        return type(data).__name__
    except Exception:
        return "<shape-unavailable>"


def _json_loads_best_effort(text: str) -> Any:
    """
    Parse JSON, preferring project helper if it exists and is callable.
    Fixes the common failure mode: U is present but doesn't implement json_loads.
    """
    if U is not None:
        jl = getattr(U, "json_loads", None)
        if callable(jl):
            return jl(text)
    return json.loads(text)


class GovcRunner:
    """
    Minimal govc execution helper.

    - Seeds GOVC_* from args/config (additive: doesn't override user env unless missing).
    - Provides run_text/run_json + availability checks.
    - Detects the "usage blob" failure mode (arg parse failure) and throws clearly.
    - Emits debug logs describing normalization, env seeding, parsing, and results.
    """

    def __init__(self, *, logger: Any, args: Any):
        self.logger = logger
        self.args = args
        self.govc_bin = getattr(args, "govc_bin", None) or os.environ.get("GOVC_BIN", "govc")

    # -------- policy / availability

    def available(self) -> bool:
        try:
            p = subprocess.run(
                [self.govc_bin, "version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                text=True,
            )
            ok = (p.returncode == 0)
            _log(self.logger, "debug", "govc.available: bin=%s rc=%s", self.govc_bin, p.returncode)
            if not ok:
                _log(self.logger, "debug", "govc.available: stderr=%s", (p.stderr or "").strip()[:800])
            return ok
        except Exception as e:
            _log(self.logger, "debug", "govc.available: exception=%r", e)
            return False

    def enabled(self) -> bool:
        if bool(getattr(self.args, "no_govmomi", False)):
            _log(self.logger, "debug", "govc.enabled: disabled by --no-govmomi/args.no_govmomi")
            return False
        ok = self.available()
        _log(self.logger, "debug", "govc.enabled: %s", ok)
        return ok

    # -------- env + execution

    def env(self) -> Dict[str, str]:
        env = dict(os.environ)

        vc_host = getattr(self.args, "vcenter", None)
        vc_user = getattr(self.args, "vc_user", None)
        vc_pass = getattr(self.args, "vc_password", None)

        vc_pass_env = getattr(self.args, "vc_password_env", None)
        if not vc_pass and vc_pass_env:
            vc_pass = os.environ.get(str(vc_pass_env))

        if isinstance(vc_pass, str):
            vc_pass = vc_pass.strip() or None

        # Additive seeding (don't stomp user env)
        if vc_host and not env.get("GOVC_URL"):
            env["GOVC_URL"] = f"https://{vc_host}/sdk"
        if vc_user and not env.get("GOVC_USERNAME"):
            env["GOVC_USERNAME"] = str(vc_user)
        if vc_pass and not env.get("GOVC_PASSWORD"):
            env["GOVC_PASSWORD"] = str(vc_pass)

        insecure = bool(getattr(self.args, "vc_insecure", False))
        if insecure:
            env["GOVC_INSECURE"] = env.get("GOVC_INSECURE", "1")

        dc = getattr(self.args, "dc_name", None)
        if dc and not env.get("GOVC_DATACENTER"):
            env["GOVC_DATACENTER"] = str(dc)

        _log(
            self.logger,
            "debug",
            "govc.env: GOVC_URL=%s GOVC_USERNAME=%s GOVC_PASSWORD=%s GOVC_INSECURE=%s GOVC_DATACENTER=%s (vc_password_env=%s)",
            env.get("GOVC_URL", "<unset>"),
            env.get("GOVC_USERNAME", "<unset>"),
            _mask_secret(env.get("GOVC_PASSWORD")),
            env.get("GOVC_INSECURE", "<unset>"),
            env.get("GOVC_DATACENTER", "<unset>"),
            vc_pass_env or "<unset>",
        )
        return env

    def run_text(self, cmd: Sequence[str]) -> str:
        full = [self.govc_bin] + list(cmd)
        _log(self.logger, "debug", "govc.exec: %s", " ".join(full))

        p = subprocess.run(
            full,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=self.env(),
            text=True,
        )

        out = (p.stdout or "").strip()
        err = (p.stderr or "").strip()

        _log(self.logger, "debug", "govc.exec: rc=%s stdout_len=%d stderr_len=%d", p.returncode, len(out), len(err))
        if err:
            _log(self.logger, "debug", "govc.exec: stderr(head)=%s", err[:1200])
        if out:
            _log(self.logger, "debug", "govc.exec: stdout(head)=%s", out[:1200])

        if _looks_like_govc_usage(out) or _looks_like_govc_usage(err):
            raise VMwareError(
                "govc printed usage/help output instead of command output "
                "(likely argument parsing issue). "
                f"cmd={' '.join(full)}"
            )

        if p.returncode != 0:
            detail = err or out[:1200]
            raise VMwareError(f"govc failed ({p.returncode}): {detail}")

        return out

    def run_json(self, cmd: Sequence[str]) -> Any:
        out = self.run_text(cmd)
        if not out:
            _log(self.logger, "debug", "govc.json: empty output (cmd=%s)", " ".join(cmd))
            return None
        try:
            _log(self.logger, "debug", "govc.json: parsing len=%d head=%s", len(out), out[:200])
            data = _json_loads_best_effort(out)   # ✅ FIXED
            _log(self.logger, "debug", "govc.json: parsed shape=%s", _summarize_json_shape(data))
            return data
        except Exception as e:
            raise VMwareError(f"govc returned non-JSON output: {e}: {out[:2000]}")

    # -------- convenience helpers

    def datastore_ls_text(self, datastore: str, ds_dir: str) -> List[str]:
        ds, rel = normalize_ds_path(datastore, ds_dir)
        rel = rel.rstrip("/") + "/"
        _log(self.logger, "debug", "datastore_ls_text: ds=%s ds_dir=%s -> rel=%s", ds, ds_dir, rel)

        out = self.run_text(["datastore.ls", "-ds", str(ds), rel])
        lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
        _log(self.logger, "debug", "datastore_ls_text: lines=%d sample=%s", len(lines), lines[:8])
        return lines

    def datastore_ls_json(self, datastore: str, ds_dir: str) -> List[str]:
        """
        Returns paths from `govc datastore.ls -json` (relative to ds_dir when possible).
        """
        ds, rel = normalize_ds_path(datastore, ds_dir)
        rel = rel.rstrip("/") + "/"

        _log(self.logger, "debug", "datastore_ls_json: ds=%s ds_dir=%s -> rel=%s", ds, ds_dir, rel)

        # NOTE: govc arg order matters; path must be last.
        data = self.run_json(["datastore.ls", "-json", "-ds", str(ds), rel])

        paths = extract_paths_from_datastore_ls_json(data)
        _log(self.logger, "debug", "datastore_ls_json: extracted=%d sample=%s", len(paths), paths[:12])

        # Attempt to strip folder prefix when govc returns it (varies by version)
        base = rel.lstrip("/")
        prefix = base.rstrip("/") + "/"

        cleaned: List[str] = []
        stripped = 0
        for p in paths:
            pp = p.lstrip("/")
            if base and pp.startswith(prefix):
                pp = pp[len(prefix):]
                stripped += 1
            cleaned.append(pp)

        cleaned = [p for p in cleaned if p]
        _log(
            self.logger,
            "debug",
            "datastore_ls_json: cleaned=%d stripped_prefix=%d prefix=%s sample=%s",
            len(cleaned),
            stripped,
            prefix,
            cleaned[:12],
        )
        return cleaned
