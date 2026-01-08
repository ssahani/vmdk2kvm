# SPDX-License-Identifier: LGPL-3.0-or-later
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import glob
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn, TimeRemainingColumn

from ..core.utils import U

try:
    import yaml  # type: ignore

    YAML_AVAILABLE = True
except Exception:
    YAML_AVAILABLE = False


class Config:
    """
    Config loader/merger with:
      - JSON/YAML support
      - safe glob/dir expansion (correct suffix filtering)
      - deep merge with configurable list strategy
      - dash-key -> underscore normalization (deep)
      - optional HMAC signature verification
      - VM fan-out via 'vms' list, with deep-merge per VM override
      - argparse defaults application with type coercion
      - ✅ alias canonicalization (command<->cmd, vs_action<->action)
      - ✅ vSphere control-plane canonicalization (govc knobs, env aliases)
    """

    # -----------------------------
    # Public API
    # -----------------------------

    @staticmethod
    def load_one(logger: logging.Logger, path: str) -> Dict[str, Any]:
        p = Path(path).expanduser().resolve()
        if not p.exists():
            # Enhanced error message (no behavior change: still dies with code=1)
            Config._die_missing_config(logger, p, original_spec=path)

        # Verify signature if enabled
        Config.verify_signature(logger, p)

        try:
            raw = p.read_text(encoding="utf-8")
            if p.suffix.lower() == ".json":
                data = json.loads(raw)
            else:
                if not YAML_AVAILABLE:
                    U.die(logger, "PyYAML not installed. Install with: pip install PyYAML", 1)
                data = yaml.safe_load(raw) or {}
        except Exception as e:
            # Keep YAML-specific errors nice if available
            if YAML_AVAILABLE and isinstance(e, getattr(yaml, "YAMLError", Exception)):
                U.die(logger, f"Invalid YAML in config {p}: {e}", 1)
            U.die(logger, f"Failed to load config {p}: {e}", 1)

        if not isinstance(data, dict):
            U.die(logger, f"Config must be a mapping/dict: {p}", 1)

        # Normalize keys deeply: dash -> underscore
        out = Config._normalize_keys(logger, data, path=str(p))

        out = Config._canonicalize_aliases(out)

        logger.debug(f"Loaded config {p}:\n{U.json_dump(out)}")
        return out

    @staticmethod
    def verify_signature(logger: logging.Logger, config_path: Path) -> bool:
        """
        Verify config file signature for production use.

        Behavior:
          - If VM2KVM_CONFIG_SECRET is unset -> verification disabled (returns True).
          - If .sig file missing -> warn + allow (returns True). (You can tighten this if desired.)
          - If present and mismatched -> die.
        """
        secret = os.environ.get("VM2KVM_CONFIG_SECRET", "")
        if not secret:
            logger.debug("No config verification secret set (VM2KVM_CONFIG_SECRET)")
            return True

        sig_path = config_path.with_suffix(config_path.suffix + ".sig")
        if not sig_path.exists():
            logger.warning(f"No signature file found for config: {config_path}")
            return True

        try:
            config_content = config_path.read_bytes()
            expected_sig = hmac.new(secret.encode(), config_content, hashlib.sha256).hexdigest()
            actual_sig = sig_path.read_text(encoding="utf-8").strip()
            if not hmac.compare_digest(expected_sig, actual_sig):
                U.die(logger, f"Config signature verification failed for {config_path}", 1)
            logger.debug(f"Config signature verified: {config_path}")
            return True
        except Exception as e:
            logger.warning(f"Config signature verification error: {e}")
            return False

    @staticmethod
    def merge_dicts(
        base: Dict[str, Any],
        override: Dict[str, Any],
        *,
        list_mode: str = "replace",  # "replace" | "append" | "extend_unique"
    ) -> Dict[str, Any]:
        """
        Deep merge:
          - dict + dict => recurse
          - list => strategy by list_mode
          - scalar => override replaces

        list_mode:
          - replace: override list replaces base list
          - append:  base + override (concatenate)
          - extend_unique: concatenate but keep first occurrence (hashable only)
        """
        out: Dict[str, Any] = dict(base)

        for k, v in override.items():
            if k in out and isinstance(out[k], dict) and isinstance(v, dict):
                out[k] = Config.merge_dicts(out[k], v, list_mode=list_mode)
                continue

            if isinstance(out.get(k), list) and isinstance(v, list):
                if list_mode == "replace":
                    out[k] = v
                elif list_mode == "append":
                    out[k] = list(out[k]) + list(v)
                elif list_mode == "extend_unique":
                    merged: List[Any] = []
                    seen: set = set()
                    for item in list(out[k]) + list(v):
                        try:
                            if item in seen:
                                continue
                            seen.add(item)
                        except Exception:
                            # unhashable -> just append (best effort)
                            pass
                        merged.append(item)
                    out[k] = merged
                else:
                    out[k] = v
                continue

            out[k] = v

        return out

    @staticmethod
    def load_many(
        logger: logging.Logger,
        paths: List[str],
        *,
        list_mode: str = "replace",
    ) -> Dict[str, Any]:
        paths = Config.expand_configs(logger, paths)

        # Pre-check missing before the progress UI starts
        Config._precheck_missing_paths(logger, paths)

        conf: Dict[str, Any] = {}

        with Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task("Loading configs", total=max(1, len(paths)))
            if not paths:
                progress.update(task, completed=1)
                return conf

            for p in paths:
                conf = Config.merge_dicts(conf, Config.load_one(logger, p), list_mode=list_mode)
                progress.update(task, advance=1)

        conf = Config._canonicalize_aliases(conf)

        return conf

    @staticmethod
    def apply_as_defaults(
        logger: logging.Logger,
        parser: argparse.ArgumentParser,
        conf: Dict[str, Any],
        *,
        strict: bool = False,
    ) -> None:
        """
        Apply config values as argparse defaults.

        Enhancements:
          - type coercion using argparse action.type when present
          - turns off required if provided by config
          - optionally strict: die if config contains keys not present in argparse dests
        """
        if not conf:
            return

        valid_dests = Config._collect_argparse_dests(parser)

        if strict:
            unknown = sorted(set(conf.keys()) - valid_dests)
            if unknown:
                U.die(logger, f"Unknown config keys (no argparse dest): {unknown}", 1)

        def apply_actions(actions: List[argparse.Action], scope: str) -> None:
            for act in actions:
                dest = getattr(act, "dest", None)
                if not dest or dest not in conf:
                    continue

                raw_val = conf[dest]
                val = Config._coerce_argparse_value(logger, act, raw_val, scope=scope, dest=dest)

                logger.debug(f"[Config:{scope}] default {dest}: {act.default!r} -> {val!r}")
                act.default = val

                if getattr(act, "required", False) and val is not None:
                    act.required = False

        apply_actions(parser._actions, "global")

        sp_action = next((a for a in parser._actions if isinstance(a, argparse._SubParsersAction)), None)
        if sp_action:
            for name, sp in sp_action.choices.items():
                apply_actions(sp._actions, f"sub:{name}")

    @staticmethod
    def expand_configs(logger: logging.Logger, configs: List[str]) -> List[str]:
        """
        Expand list of config specs:
          - directories: include **/*.yml, **/*.yaml, **/*.json
          - glob patterns: expanded via glob.glob
          - files: passed through
        """
        expanded: List[str] = []

        for c in configs:
            p = Path(c).expanduser()

            if p.exists() and p.is_dir():
                # Correct suffix filtering (your original '*.[yaml|yml|json]' is a glob bug)
                for f in p.rglob("*"):
                    if f.is_file() and f.suffix.lower() in (".yaml", ".yml", ".json"):
                        expanded.append(str(f.resolve()))
                continue

            if "*" in c or "?" in c or ("[" in c and "]" in c):
                expanded.extend([str(Path(x).expanduser().resolve()) for x in glob.glob(c)])
                continue

            expanded.append(str(Path(c).expanduser().resolve()))

        # De-dup while preserving order
        seen = set()
        uniq: List[str] = []
        for x in expanded:
            if x not in seen:
                uniq.append(x)
                seen.add(x)

        logger.debug(f"Expanded configs: {uniq}")
        return uniq

    @staticmethod
    def load_vm_configs(
        logger: logging.Logger,
        paths: List[str],
        *,
        list_mode: str = "replace",
    ) -> List[Dict[str, Any]]:
        """
        Load configs; if a config has 'vms' list, fan-out into per-VM configs.
        Each VM entry deep-merges over the base config (minus 'vms').
        """
        vm_confs: List[Dict[str, Any]] = []
        paths = Config.expand_configs(logger, paths)

        # Pre-check missing before the progress UI starts
        Config._precheck_missing_paths(logger, paths)

        with Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task("Loading VM configs", total=max(1, len(paths)))
            if not paths:
                progress.update(task, completed=1)
                return vm_confs

            for path in paths:
                conf = Config.load_one(logger, path)

                vms = conf.get("vms")
                if isinstance(vms, list):
                    base = dict(conf)
                    base.pop("vms", None)

                    for idx, vm in enumerate(vms):
                        if not isinstance(vm, dict):
                            U.die(logger, f"'vms' entries must be mappings/dicts in {path} (index {idx})", 1)
                        vm_conf = Config.merge_dicts(base, vm, list_mode=list_mode)
                        vm_confs.append(vm_conf)
                else:
                    vm_confs.append(conf)

                progress.update(task, advance=1)

        vm_confs = [Config._canonicalize_aliases(c) for c in vm_confs]

        return vm_confs

    @staticmethod
    def _canonicalize_aliases(d: Dict[str, Any]) -> Dict[str, Any]:
        """
        Canonicalize common alias keys so YAML can stay stable while code evolves.

        Existing policy:
          - Keep 'command' canonical for new project readability,
            but populate 'cmd' for legacy/compat dispatchers.
          - Keep 'vs_action' canonical but populate 'action' (and vice-versa).

        Added policy (govc / control-plane):
          - Accept either 'vs_control_plane' or 'control_plane' and mirror to the other.
          - Accept either govc_* keys or GOVC_* style names (after normalization).
          - If govc_url not provided, allow vcenter to imply https://<vcenter>/sdk (used later).
          - If govc_user/password missing, allow falling back to vc_user/vc_password at runtime.
        """
        # command <-> cmd
        if "command" in d and "cmd" not in d:
            d["cmd"] = d["command"]
        elif "cmd" in d and "command" not in d:
            d["command"] = d["cmd"]

        # vs_action <-> action
        if "vs_action" in d and "action" not in d:
            d["action"] = d["vs_action"]
        elif "action" in d and "vs_action" not in d:
            d["vs_action"] = d["action"]

        # vs_control_plane <-> control_plane
        if "vs_control_plane" in d and "control_plane" not in d:
            d["control_plane"] = d["vs_control_plane"]
        elif "control_plane" in d and "vs_control_plane" not in d:
            d["vs_control_plane"] = d["control_plane"]

        # GOVC_* style aliases (after key normalization, these are likely "govc_url" etc,
        # but users may still write "GOVC_URL" in YAML; safe-load keeps case, normalize_keys
        # does not change case, so we map a few common ones explicitly.)
        # Prefer explicit govc_* if present.
        govc_map = {
            "GOVC_URL": "govc_url",
            "GOVC_USERNAME": "govc_user",
            "GOVC_USER": "govc_user",
            "GOVC_PASSWORD": "govc_password",
            "GOVC_PASSWORD_ENV": "govc_password_env",
            "GOVC_INSECURE": "govc_insecure",
            "GOVC_DATACENTER": "govc_datacenter",
            "GOVC_DATASTORE": "govc_ds",
            "GOVC_DS": "govc_ds",
            "GOVC_FOLDER": "govc_folder",
            "GOVC_CLUSTER": "govc_cluster",
            "GOVC_RESOURCE_POOL": "govc_resource_pool",
        }
        for src, dst in govc_map.items():
            if src in d and dst not in d:
                d[dst] = d[src]

        # Light normalization of vs_control_plane values if user provided one
        if "vs_control_plane" in d and isinstance(d["vs_control_plane"], str):
            d["vs_control_plane"] = d["vs_control_plane"].strip().lower()

        return d

    @staticmethod
    def _normalize_keys(logger: logging.Logger, obj: Any, *, path: str, _prefix: str = "") -> Any:
        """
        Recursively normalize dict keys:
          - replace '-' with '_' in keys
        """
        if isinstance(obj, dict):
            out: Dict[str, Any] = {}
            for k, v in obj.items():
                nk = str(k).replace("-", "_")
                if nk != k:
                    logger.debug(f"Normalized config key: {_prefix}{k} -> {_prefix}{nk} (file={path})")
                out[nk] = Config._normalize_keys(logger, v, path=path, _prefix=f"{_prefix}{nk}.")
            return out
        if isinstance(obj, list):
            return [Config._normalize_keys(logger, x, path=path, _prefix=_prefix) for x in obj]
        return obj

    @staticmethod
    def _collect_argparse_dests(parser: argparse.ArgumentParser) -> set:
        dests = set()
        for a in parser._actions:
            d = getattr(a, "dest", None)
            if d:
                dests.add(d)
        sp_action = next((a for a in parser._actions if isinstance(a, argparse._SubParsersAction)), None)
        if sp_action:
            for _, sp in sp_action.choices.items():
                for a in sp._actions:
                    d = getattr(a, "dest", None)
                    if d:
                        dests.add(d)
        return dests

    @staticmethod
    def _coerce_argparse_value(
        logger: logging.Logger,
        act: argparse.Action,
        raw_val: Any,
        *,
        scope: str,
        dest: str,
    ) -> Any:
        """
        Best-effort type coercion consistent with argparse.
        """
        nargs = getattr(act, "nargs", None)
        act_type = getattr(act, "type", None)

        if isinstance(act, (argparse._StoreTrueAction, argparse._StoreFalseAction)):
            if isinstance(raw_val, bool):
                return raw_val
            if isinstance(raw_val, str):
                s = raw_val.strip().lower()
                return s in ("1", "true", "yes", "y", "on")
            return bool(raw_val)

        if act_type is None:
            return raw_val

        def coerce_one(x: Any) -> Any:
            try:
                return act_type(x)
            except Exception:
                logger.debug(f"[Config:{scope}] could not coerce {dest} value {x!r} using {act_type}")
                return x

        if nargs in ("+", "*") or isinstance(raw_val, (list, tuple)):
            if isinstance(raw_val, (list, tuple)):
                return [coerce_one(x) for x in raw_val]
            return [coerce_one(raw_val)]

        return coerce_one(raw_val)


    @staticmethod
    def _precheck_missing_paths(logger: logging.Logger, paths: List[str]) -> None:
        """
        Fail early (before progress UI) if any expanded configs don't exist.
        This avoids partial progress bars and gives one clean actionable error.
        """
        if not paths:
            return
        missing = [p for p in paths if not Path(p).exists()]
        if not missing:
            return

        msg_lines: List[str] = ["Config file(s) not found:"]
        for m in missing[:20]:
            msg_lines.append(f"  - {m}")
        if len(missing) > 20:
            msg_lines.append(f"  ... and {len(missing) - 20} more")

        msg_lines.append("")
        msg_lines.append(Config._missing_config_help(missing[0], original_spec=None))

        U.die(logger, "\n".join(msg_lines), 1)

    @staticmethod
    def _die_missing_config(logger: logging.Logger, resolved: Path, *, original_spec: Optional[str] = None) -> None:
        msg = f"Config not found: {resolved}\n\n{Config._missing_config_help(str(resolved), original_spec=original_spec)}"
        U.die(logger, msg, 1)

    @staticmethod
    def _missing_config_help(resolved_path: str, *, original_spec: Optional[str]) -> str:
        """
        Build actionable hints for missing config paths.
        - Shows whether user passed a glob
        - Shows nearby configs in same directory
        - Suggests correct usage
        """
        rp = Path(resolved_path).expanduser()
        parent = rp.parent

        lines: List[str] = []

        if original_spec and (("*" in original_spec) or ("?" in original_spec) or ("[" in original_spec and "]" in original_spec)):
            lines.append(f"Note: the config argument looked like a glob pattern: {original_spec!r}")
            lines.append("      It expanded to zero matching files (or matched paths that don't exist).")

        if parent.exists() and parent.is_dir():
            candidates: List[Path] = []
            for ext in (".yaml", ".yml", ".json"):
                candidates.extend(sorted(parent.glob(f"*{ext}")))

            if candidates:
                lines.append("Configs found in that directory:")
                for c in candidates[:10]:
                    lines.append(f"  - {c.resolve()}")
                if len(candidates) > 10:
                    lines.append(f"  ... and {len(candidates) - 10} more")
            else:
                lines.append("That directory exists, but no *.yaml/*.yml/*.json configs were found there.")
        else:
            lines.append("Parent directory does not exist (or is not accessible).")

        lines.append("Tip: pass an absolute path, e.g. --config /full/path/config.yaml")
        return "\n".join(lines)
