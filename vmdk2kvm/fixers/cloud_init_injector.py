# SPDX-License-Identifier: LGPL-3.0-or-later
from __future__ import annotations

from typing import Dict, Any, Optional
import json
import os

import guestfs  # type: ignore

try:
    import yaml  # type: ignore
    YAML_AVAILABLE = True
except Exception:
    yaml = None
    YAML_AVAILABLE = False


def _render_yaml_or_json(obj: Any) -> str:
    # Cloud-init prefers YAML for most things, but JSON is acceptable for debug/inspection.
    if YAML_AVAILABLE:
        return yaml.safe_dump(obj, sort_keys=False)
    return json.dumps(obj, indent=2)


def _guest_has_any(g: guestfs.GuestFS, paths: list[str]) -> bool:
    for p in paths:
        try:
            if g.exists(p):
                return True
        except Exception:
            pass
    return False


def _guest_has_cmd(g: guestfs.GuestFS, cmd: str) -> bool:
    # Try common locations; don't assume PATH.
    candidates = [f"/usr/bin/{cmd}", f"/bin/{cmd}", f"/usr/sbin/{cmd}", f"/sbin/{cmd}"]
    return _guest_has_any(g, candidates)


def _is_cloud_init_present(g: guestfs.GuestFS) -> bool:
    # Prefer multiple weak signals instead of one brittle check.
    if _guest_has_any(g, ["/etc/cloud", "/usr/lib/cloud-init", "/var/lib/cloud"]):
        return True
    if _guest_has_cmd(g, "cloud-init"):
        return True
    # systemd unit names vary slightly across distros
    if _guest_has_any(g, [
        "/usr/lib/systemd/system/cloud-init.service",
        "/lib/systemd/system/cloud-init.service",
        "/etc/systemd/system/cloud-init.service",
    ]):
        return True
    return False


def _write_atomic_or_plain(
    self,
    g: guestfs.GuestFS,
    path: str,
    data: bytes,
) -> None:
    """
    Prefer atomic write: write temp + rename.
    Falls back to g.write if needed.
    """
    # If you already have a helper, use it.
    if hasattr(self, "write_file_atomic"):
        self.write_file_atomic(g, path, data)  # type: ignore[attr-defined]
        return

    # Minimal atomic fallback
    tmp = f"{path}.tmp.vmdk2kvm"
    g.write(tmp, data)
    g.rename(tmp, path)


def inject_cloud_init(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    """
    Offline, minimal-risk cloud-init injection.

    Supported payload (self.inject_cloud_init_data):
      {
        "install_if_missing": false,      # default: false (unsafe offline)
        "dropin_cfg": {...},              # optional dict -> /etc/cloud/cloud.cfg.d/99-vmdk2kvm.cfg
        "nocloud": {                      # optional NoCloud seed
          "user-data": {...} | "#cloud-config\n...",
          "meta-data": {...} | "...",
          "network-config": {...} | "..."
        },
        "seed_dir": "/var/lib/cloud/seed/nocloud-net"  # optional override
      }
    """
    # --- logging helpers (keep it local; do not change external behavior) ---
    logger = getattr(self, "logger", None)

    def _log(level: str, msg: str) -> None:
        if logger is None:
            return
        try:
            getattr(logger, level)(msg)
        except Exception:
            pass

    def _trace(msg: str) -> None:
        # Prefer Log.trace if your core logger has it; fall back to debug.
        try:
            from ..core.logger import Log  # type: ignore
            if logger is not None:
                Log.trace(logger, msg)
                return
        except Exception:
            pass
        _log("debug", msg)

    def _safe_repr(val: Any, max_len: int = 240) -> str:
        try:
            s = repr(val)
        except Exception:
            s = f"<unreprable {type(val).__name__}>"
        if len(s) > max_len:
            return s[:max_len] + "…"
        return s

    def _b(n: int) -> str:
        # human-ish bytes for logs; no external dependency
        try:
            for unit in ("B", "KiB", "MiB", "GiB"):
                if n < 1024 or unit == "GiB":
                    return f"{n:.0f}{unit}" if unit == "B" else f"{n/1024:.1f}{unit}"
                n = n // 1  # keep type stable
            return f"{n}B"
        except Exception:
            return str(n)

    _trace("☁️  cloud-init: inject_cloud_init(): enter")
    data = getattr(self, "inject_cloud_init_data", None)
    if not data:
        _trace("☁️  cloud-init: no inject_cloud_init_data set; skipping")
        return {"injected": False, "reason": "no_data"}

    if not isinstance(data, dict):
        # Keep behavior conservative: treat as no-data rather than crashing.
        _log("warning", f"☁️  cloud-init: inject_cloud_init_data is not a dict: {type(data).__name__}")
        return {"injected": False, "reason": "no_data"}

    dry = bool(getattr(self, "dry_run", False))
    _trace(f"☁️  cloud-init: dry_run={dry}")

    present = False
    try:
        present = _is_cloud_init_present(g)
        _trace(f"☁️  cloud-init: detected={present}")
    except Exception as e:
        _log("warning", f"☁️  cloud-init: detection failed (treating as missing): {e}")
        present = False

    if not present:
        _log("info", "Cloud-init not detected in guest.")
        # Installing packages offline via libguestfs command() is usually the wrong move.
        # Keep this behind an explicit opt-in.
        if data.get("install_if_missing", False):
            _log("warning", "☁️  cloud-init: install_if_missing requested, but offline install is not supported here")
            return {
                "injected": False,
                "reason": "cloud_init_missing_install_not_supported_offline",
                "hint": "Install cloud-init in the guest image (or enable install logic via a chroot-capable helper).",
            }
        return {"injected": False, "reason": "cloud_init_not_available"}

    results: Dict[str, Any] = {"injected": True, "dry_run": dry, "writes": []}

    # 1) Safe config drop-in (DO NOT overwrite /etc/cloud/cloud.cfg)
    dropin_cfg = data.get("dropin_cfg")
    if isinstance(dropin_cfg, dict) and dropin_cfg:
        cfgd = "/etc/cloud/cloud.cfg.d"
        dst = f"{cfgd}/99-vmdk2kvm.cfg"
        rendered = _render_yaml_or_json(dropin_cfg)

        _trace(f"☁️  cloud-init: dropin_cfg present -> {dst} bytes={len(rendered.encode('utf-8'))}")

        if dry:
            _log("info", f"DRY-RUN: would write cloud-init drop-in: {dst}")
            results["writes"].append({"path": dst, "bytes": len(rendered.encode("utf-8")), "kind": "dropin_cfg"})
        else:
            try:
                if not g.is_dir(cfgd):
                    _trace(f"📁 cloud-init: creating dir {cfgd}")
                    g.mkdir_p(cfgd)
            except Exception as e:
                # Let write fail naturally later if this is fatal; but log what happened.
                _log("warning", f"📁 cloud-init: could not ensure dir {cfgd}: {e}")

            # backup if exists
            try:
                if g.exists(dst):
                    _trace(f"🗄️  cloud-init: backing up existing {dst}")
                    self.backup_file(g, dst)
            except Exception as e:
                _log("warning", f"🗄️  cloud-init: backup failed for {dst}: {e}")

            try:
                _write_atomic_or_plain(self, g, dst, rendered.encode("utf-8"))
                _log("info", f"Wrote cloud-init drop-in: {dst}")
                results["writes"].append({"path": dst, "bytes": len(rendered.encode("utf-8")), "kind": "dropin_cfg"})
            except Exception as e:
                # preserve prior behavior? previously this would raise; keep raising, but log first.
                _log("error", f"💥 cloud-init: failed writing drop-in {dst}: {e}")
                raise
    else:
        _trace(f"☁️  cloud-init: no dropin_cfg (type={type(dropin_cfg).__name__})")

    # 2) NoCloud seed (portable way to inject user-data/meta-data/network-config)
    nocloud = data.get("nocloud")
    if isinstance(nocloud, dict) and nocloud:
        seed_dir = data.get("seed_dir") or "/var/lib/cloud/seed/nocloud-net"
        files = {
            "user-data": "user-data",
            "meta-data": "meta-data",
            "network-config": "network-config",
        }

        _trace(f"☁️  cloud-init: nocloud present -> seed_dir={seed_dir}")

        for k, fname in files.items():
            if k not in nocloud or nocloud[k] in (None, "", {}):
                _trace(f"☁️  cloud-init: nocloud {k} missing/empty; skipping")
                continue

            content = nocloud[k]
            _trace(f"🧩 cloud-init: rendering nocloud[{k}] type={type(content).__name__} value={_safe_repr(content)}")

            # allow dicts or raw strings
            if isinstance(content, dict):
                # user-data dict should be cloud-config YAML; meta-data/network-config also accept YAML.
                txt = _render_yaml_or_json(content)
                # For user-data, cloud-init likes the header when it’s YAML cloud-config.
                if k == "user-data" and not txt.lstrip().startswith("#cloud-config"):
                    txt = "#cloud-config\n" + txt
            else:
                txt = str(content)

            dst = os.path.join(seed_dir, fname)
            size = len(txt.encode("utf-8"))
            _trace(f"✍️  cloud-init: prepared {k} -> {dst} bytes={size}")

            if dry:
                _log("info", f"DRY-RUN: would write NoCloud seed: {dst}")
                results["writes"].append({"path": dst, "bytes": size, "kind": f"nocloud_{k}"})
                continue

            try:
                _trace(f"📁 cloud-init: ensuring seed dir {seed_dir}")
                g.mkdir_p(seed_dir)
            except Exception as e:
                _log("warning", f"📁 cloud-init: could not create seed dir {seed_dir}: {e}")

            try:
                if g.exists(dst):
                    _trace(f"🗄️  cloud-init: backing up existing {dst}")
                    self.backup_file(g, dst)
            except Exception as e:
                _log("warning", f"🗄️  cloud-init: backup failed for {dst}: {e}")

            try:
                _write_atomic_or_plain(self, g, dst, txt.encode("utf-8"))
                _log("info", f"Wrote NoCloud seed: {dst}")
                results["writes"].append({"path": dst, "bytes": size, "kind": f"nocloud_{k}"})
            except Exception as e:
                _log("error", f"💥 cloud-init: failed writing seed {dst}: {e}")
                raise

        # Optional: ensure cloud-init will look at NoCloud without needing kernel cmdline.
        # Typically it already does, but a tiny hint file is harmless.
        if not dry:
            try:
                ds_cfg = "/etc/cloud/cloud.cfg.d/99-vmdk2kvm-datasource.cfg"
                if not g.is_dir("/etc/cloud/cloud.cfg.d"):
                    _trace("📁 cloud-init: creating /etc/cloud/cloud.cfg.d")
                    g.mkdir_p("/etc/cloud/cloud.cfg.d")
                ds_payload = "datasource_list: [ NoCloud, None ]\n"
                if g.exists(ds_cfg):
                    _trace(f"🗄️  cloud-init: backing up existing {ds_cfg}")
                    self.backup_file(g, ds_cfg)
                _write_atomic_or_plain(self, g, ds_cfg, ds_payload.encode("utf-8"))
                results["writes"].append({"path": ds_cfg, "bytes": len(ds_payload), "kind": "datasource_hint"})
                _trace(f"✅ cloud-init: wrote datasource hint {ds_cfg} bytes={len(ds_payload)}")
            except Exception as e:
                # Not fatal
                _log("warning", f"Could not write datasource hint: {e}")
    else:
        _trace(f"☁️  cloud-init: no nocloud payload (type={type(nocloud).__name__})")

    if not results["writes"]:
        # Nothing to do isn’t a success; make it explicit.
        _trace("☁️  cloud-init: nothing written (no_payload)")
        return {"injected": False, "reason": "no_payload", "cloud_init_present": True}

    _trace(f"☁️  cloud-init: done; writes={len(results['writes'])}")
    return results
