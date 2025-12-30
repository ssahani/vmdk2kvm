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
    data = getattr(self, "inject_cloud_init_data", None)
    if not data:
        return {"injected": False, "reason": "no_data"}

    dry = bool(getattr(self, "dry_run", False))

    present = False
    try:
        present = _is_cloud_init_present(g)
    except Exception:
        present = False

    if not present:
        self.logger.info("Cloud-init not detected in guest.")
        # Installing packages offline via libguestfs command() is usually the wrong move.
        # Keep this behind an explicit opt-in.
        if data.get("install_if_missing", False):
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

        if dry:
            self.logger.info(f"DRY-RUN: would write cloud-init drop-in: {dst}")
            results["writes"].append({"path": dst, "bytes": len(rendered.encode("utf-8")), "kind": "dropin_cfg"})
        else:
            if not g.is_dir(cfgd):
                g.mkdir_p(cfgd)
            # backup if exists
            try:
                if g.exists(dst):
                    self.backup_file(g, dst)
            except Exception:
                pass
            _write_atomic_or_plain(self, g, dst, rendered.encode("utf-8"))
            self.logger.info(f"Wrote cloud-init drop-in: {dst}")
            results["writes"].append({"path": dst, "bytes": len(rendered.encode("utf-8")), "kind": "dropin_cfg"})

    # 2) NoCloud seed (portable way to inject user-data/meta-data/network-config)
    nocloud = data.get("nocloud")
    if isinstance(nocloud, dict) and nocloud:
        seed_dir = data.get("seed_dir") or "/var/lib/cloud/seed/nocloud-net"
        files = {
            "user-data": "user-data",
            "meta-data": "meta-data",
            "network-config": "network-config",
        }

        for k, fname in files.items():
            if k not in nocloud or nocloud[k] in (None, "", {}):
                continue

            content = nocloud[k]
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

            if dry:
                self.logger.info(f"DRY-RUN: would write NoCloud seed: {dst}")
                results["writes"].append({"path": dst, "bytes": len(txt.encode('utf-8')), "kind": f"nocloud_{k}"})
                continue

            g.mkdir_p(seed_dir)
            try:
                if g.exists(dst):
                    self.backup_file(g, dst)
            except Exception:
                pass
            _write_atomic_or_plain(self, g, dst, txt.encode("utf-8"))
            self.logger.info(f"Wrote NoCloud seed: {dst}")
            results["writes"].append({"path": dst, "bytes": len(txt.encode('utf-8')), "kind": f"nocloud_{k}"})

        # Optional: ensure cloud-init will look at NoCloud without needing kernel cmdline.
        # Typically it already does, but a tiny hint file is harmless.
        if not dry:
            try:
                ds_cfg = "/etc/cloud/cloud.cfg.d/99-vmdk2kvm-datasource.cfg"
                if not g.is_dir("/etc/cloud/cloud.cfg.d"):
                    g.mkdir_p("/etc/cloud/cloud.cfg.d")
                ds_payload = "datasource_list: [ NoCloud, None ]\n"
                if g.exists(ds_cfg):
                    self.backup_file(g, ds_cfg)
                _write_atomic_or_plain(self, g, ds_cfg, ds_payload.encode("utf-8"))
                results["writes"].append({"path": ds_cfg, "bytes": len(ds_payload), "kind": "datasource_hint"})
            except Exception as e:
                # Not fatal
                self.logger.warning(f"Could not write datasource hint: {e}")

    if not results["writes"]:
        # Nothing to do isn’t a success; make it explicit.
        return {"injected": False, "reason": "no_payload", "cloud_init_present": True}

    return results
