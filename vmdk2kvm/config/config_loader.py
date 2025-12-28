from __future__ import annotations
import argparse
import glob
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from ..core.utils import U

try:
    import yaml  # type: ignore
    YAML_AVAILABLE = True
except Exception:
    YAML_AVAILABLE = False
class Config:
    @staticmethod
    def load_one(logger: logging.Logger, path: str) -> Dict[str, Any]:
        p = Path(path).expanduser().resolve()
        if not p.exists():
            U.die(logger, f"Config not found: {p}", 1)
        # Verify config signature if available
        Config.verify_signature(logger, p)
        try:
            if p.suffix.lower() == ".json":
                data = json.loads(p.read_text(encoding="utf-8"))
            else:
                if not YAML_AVAILABLE:
                    U.die(logger, "PyYAML not installed. Install with: pip install PyYAML", 1)
                data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as e:
            U.die(logger, f"Invalid YAML in config {p}: {e}", 1)
        except Exception as e:
            U.die(logger, f"Failed to load config {p}: {e}", 1)
        if not isinstance(data, dict):
            U.die(logger, f"Config must be a mapping/dict: {p}", 1)
        # normalize dash keys -> underscore keys
        out: Dict[str, Any] = {}
        for k, v in data.items():
            nk = str(k).replace("-", "_")
            out[nk] = v
            if nk != k:
                logger.debug(f"Normalized config key: {k} -> {nk}")
        logger.debug(f"Loaded config {p}:\n{U.json_dump(out)}")
        return out
    @staticmethod
    def verify_signature(logger: logging.Logger, config_path: Path) -> bool:
        """Verify config file signature for production use"""
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
            expected_sig = hmac.new(
                secret.encode(),
                config_content,
                hashlib.sha256
            ).hexdigest()
            actual_sig = sig_path.read_text().strip()
            if not hmac.compare_digest(expected_sig, actual_sig):
                U.die(logger, f"Config signature verification failed for {config_path}", 1)
            logger.debug(f"Config signature verified: {config_path}")
            return True
        except Exception as e:
            logger.warning(f"Config signature verification error: {e}")
            return False
    @staticmethod
    def merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep-ish merge:
        - dict + dict => recurse
        - list => override replaces (not concatenated)
        - scalar => override replaces
        """
        out = dict(base)
        for k, v in override.items():
            if k in out and isinstance(out[k], dict) and isinstance(v, dict):
                out[k] = Config.merge_dicts(out[k], v)
            else:
                out[k] = v
        return out
    @staticmethod
    def load_many(logger: logging.Logger, paths: List[str]) -> Dict[str, Any]:
        conf: Dict[str, Any] = {}
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
            task = progress.add_task("Loading configs", total=len(paths))
            for p in paths:
                conf = Config.merge_dicts(conf, Config.load_one(logger, p))
                progress.update(task, advance=1)
        return conf
    @staticmethod
    def apply_as_defaults(logger: logging.Logger, parser: argparse.ArgumentParser, conf: Dict[str, Any]) -> None:
        if not conf:
            return
        def apply_actions(actions: List[argparse.Action], scope: str) -> None:
            for act in actions:
                dest = getattr(act, "dest", None)
                if not dest or dest not in conf:
                    continue
                val = conf[dest]
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
        expanded = []
        for c in configs:
            p = Path(c).expanduser().resolve()
            if p.is_dir():
                for f in p.glob('**/*.[yaml|yml|json]'):
                    if f.is_file():
                        expanded.append(str(f))
            elif '*' in c or '?' in c:
                expanded.extend(glob.glob(c))
            else:
                expanded.append(c)
        logger.debug(f"Expanded configs: {expanded}")
        return expanded
    @staticmethod
    def load_vm_configs(logger: logging.Logger, paths: List[str]) -> List[Dict[str, Any]]:
        vm_confs = []
        paths = Config.expand_configs(logger, paths)
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
            task = progress.add_task("Loading VM configs", total=len(paths))
            for path in paths:
                conf = Config.load_one(logger, path)
                if 'vms' in conf:
                    for vm in conf['vms']:
                        vm_conf = conf.copy()
                        del vm_conf['vms']
                        vm_conf.update(vm)
                        vm_confs.append(vm_conf)
                else:
                    vm_confs.append(conf)
                progress.update(task, advance=1)
        return vm_confs
