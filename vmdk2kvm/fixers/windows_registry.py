# vmdk2kvm/fixers/windows_registry.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import logging
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import guestfs  # type: ignore

from ..core.utils import U


# ---------------------------
# Logging helper
# ---------------------------

def _safe_logger(self) -> logging.Logger:
    lg = getattr(self, "logger", None)
    if isinstance(lg, logging.Logger):
        return lg
    return logging.getLogger("vmdk2kvm.windows_registry")


# ---------------------------
# Registry encoding helpers (CRITICAL)
# ---------------------------
# Windows registry strings in hives are typically UTF-16LE NUL-terminated.

def _reg_sz(s: str) -> bytes:
    return (s + "\0").encode("utf-16le", errors="ignore")


def _set_sz(h, node, key: str, s: str) -> None:
    # REG_SZ => t=1 in hivex
    h.node_set_value(node, {"key": key, "t": 1, "value": _reg_sz(s)})


def _set_expand_sz(h, node, key: str, s: str) -> None:
    # REG_EXPAND_SZ => t=2 in hivex
    h.node_set_value(node, {"key": key, "t": 2, "value": _reg_sz(s)})


def _set_dword(h, node, key: str, v: int) -> None:
    # REG_DWORD => t=4 in hivex
    h.node_set_value(node, {"key": key, "t": 4, "value": int(v).to_bytes(4, "little", signed=False)})


def _ensure_child(h, parent, name: str):
    ch = h.node_get_child(parent, name)
    if ch is None:
        ch = h.node_add_child(parent, name)
    return ch


def _delete_child_if_exists(h, parent, name: str) -> bool:
    """
    Remove a child key node if present (commonly StartOverride).
    """
    try:
        n = h.node_get_child(parent, name)
        if n is None:
            return False
        h.node_delete_child(n)
        return True
    except Exception:
        return False


def _hivex_read_sz(h, node, key: str) -> Optional[str]:
    """
    Read REG_SZ/REG_EXPAND_SZ-ish as text (best-effort).
    hivex returns bytes often UTF-16LE for string values.
    """
    try:
        v = h.node_get_value(node, key)
        if not v or "value" not in v:
            return None
        raw = v["value"]
        if isinstance(raw, (bytes, bytearray)):
            try:
                return raw.decode("utf-16le", errors="ignore").rstrip("\x00").strip() or None
            except Exception:
                try:
                    return raw.decode("utf-8", errors="ignore").rstrip("\x00").strip() or None
                except Exception:
                    return None
        return str(raw).strip() or None
    except Exception:
        return None


def _hivex_read_dword(h, node, key: str) -> Optional[int]:
    try:
        v = h.node_get_value(node, key)
        if not v or "value" not in v:
            return None
        raw = v["value"]
        if isinstance(raw, (bytes, bytearray)) and len(raw) >= 4:
            return int.from_bytes(raw[:4], "little", signed=False)
        if isinstance(raw, int):
            return raw
        return None
    except Exception:
        return None


def _detect_current_controlset(h, root) -> str:
    """
    Return active ControlSetXYZ name using SYSTEM\\Select\\Current.
    Falls back to ControlSet001 if anything looks off.
    """
    select = h.node_get_child(root, "Select")
    if select is None:
        return "ControlSet001"

    cur = h.node_get_value(select, "Current")
    if cur is None or "value" not in cur:
        return "ControlSet001"

    cur_raw = cur["value"]
    if isinstance(cur_raw, (bytes, bytearray)) and len(cur_raw) >= 4:
        current_set = int.from_bytes(cur_raw[:4], "little", signed=False)
    elif isinstance(cur_raw, int):
        current_set = int(cur_raw)
    else:
        current_set = 1

    return f"ControlSet{current_set:03d}"


# ---------------------------
# Public: SYSTEM hive edit (Services + CDD + StartOverride)
# ---------------------------

def edit_system_hive(
    self,
    g: guestfs.GuestFS,
    hive_path: str,
    drivers: List[Any],
    *,
    driver_type_storage_value: str,
    boot_start_value: int,
) -> Dict[str, Any]:
    """
    Edit SYSTEM hive offline to:
      - Create Services\\<driver> keys with correct Type/Start/ImagePath/Group
      - Add CriticalDeviceDatabase entries for STORAGE drivers
      - Remove StartOverride keys that frequently disable boot drivers

    NOTE: we keep this file independent from DriverType/Enum imports by passing:
      - driver_type_storage_value: usually "storage"
      - boot_start_value: usually 0 (BOOT)
    """
    logger = _safe_logger(self)
    dry_run = getattr(self, "dry_run", False)

    results: Dict[str, Any] = {
        "success": False,
        "dry_run": bool(dry_run),
        "registry_modified": False,
        "hive_path": hive_path,
        "errors": [],
        "services": [],
        "cdd": [],
        "startoverride_removed": [],
        "notes": [],
        "verified_services": [],
        "verification_errors": [],
    }

    try:
        if not g.is_file(hive_path):
            results["errors"].append(f"SYSTEM hive not found: {hive_path}")
            return results
    except Exception as e:
        results["errors"].append(f"Failed to stat hive {hive_path}: {e}")
        return results

    with tempfile.TemporaryDirectory() as tmpdir:
        local_hive = Path(tmpdir) / "SYSTEM"
        try:
            # Backup hive inside guest
            if not dry_run:
                ts = U.now_ts()
                backup_path = f"{hive_path}.vmdk2kvm.backup.{ts}"
                g.cp(hive_path, backup_path)
                logger.info(f"Hive backup created: {backup_path}")
                results["hive_backup"] = backup_path

            logger.info(f"Downloading SYSTEM hive: {hive_path} -> {local_hive}")
            g.download(hive_path, str(local_hive))

            orig_bytes = local_hive.read_bytes()
            original_hash = hashlib.sha256(orig_bytes).hexdigest()
            logger.debug(f"SYSTEM hive baseline sha256={original_hash}")

            write_mode = 0 if dry_run else 1
            h = g.hivex_open(str(local_hive), write=write_mode)
            root = h.root()

            cs_name = _detect_current_controlset(h, root)
            logger.info(f"Using control set: {cs_name}")

            control_set = h.node_get_child(root, cs_name)
            if control_set is None:
                logger.warning(f"{cs_name} missing; falling back to ControlSet001")
                cs_name = "ControlSet001"
                control_set = h.node_get_child(root, cs_name)
                if control_set is None:
                    raise RuntimeError("No usable ControlSet found (001/current)")

            services = _ensure_child(h, control_set, "Services")

            # Create / update Services\\<driver> entries
            for drv in drivers:
                try:
                    # Expected fields on drv:
                    #   drv.service_name, drv.dest_name, drv.start_type.value, drv.type.value (or drv.type str)
                    drv_type_value = getattr(getattr(drv, "type", None), "value", None) or str(getattr(drv, "type", ""))
                    svc_name = str(getattr(drv, "service_name"))
                    dest_name = str(getattr(drv, "dest_name"))
                    start_default = int(getattr(getattr(drv, "start_type", None), "value", getattr(drv, "start_type", 0)))

                    svc = h.node_get_child(services, svc_name)
                    action = "updated" if svc is not None else "created"
                    if svc is None:
                        svc = h.node_add_child(services, svc_name)

                    logger.info(f"Registry service {action}: Services\\{svc_name}")

                    _set_dword(h, svc, "Type", 1)           # SERVICE_KERNEL_DRIVER
                    _set_dword(h, svc, "ErrorControl", 1)   # normal error handling

                    start = start_default
                    if drv_type_value == driver_type_storage_value:
                        start = boot_start_value
                    _set_dword(h, svc, "Start", start)

                    if drv_type_value == driver_type_storage_value:
                        group = "SCSI miniport"
                    elif drv_type_value == "network":
                        group = "NDIS"
                    else:
                        group = "System Bus Extender"

                    _set_sz(h, svc, "Group", group)
                    _set_sz(h, svc, "ImagePath", fr"\SystemRoot\System32\drivers\{dest_name}")
                    _set_sz(h, svc, "DisplayName", svc_name)

                    removed = _delete_child_if_exists(h, svc, "StartOverride")
                    if removed:
                        logger.info(f"Removed StartOverride: Services\\{svc_name}\\StartOverride")
                        results["startoverride_removed"].append(svc_name)

                    results["services"].append(
                        {
                            "service": svc_name,
                            "type": drv_type_value,
                            "start": start,
                            "group": group,
                            "image": fr"\SystemRoot\System32\drivers\{dest_name}",
                            "action": action,
                        }
                    )
                except Exception as e:
                    msg = f"Failed to create/update service {getattr(drv, 'service_name', '?')}: {e}"
                    logger.error(msg)
                    results["errors"].append(msg)

            # CriticalDeviceDatabase for storage
            control = _ensure_child(h, control_set, "Control")
            cdd = _ensure_child(h, control, "CriticalDeviceDatabase")

            for drv in drivers:
                drv_type_value = getattr(getattr(drv, "type", None), "value", None) or str(getattr(drv, "type", ""))
                if drv_type_value != driver_type_storage_value:
                    continue

                svc_name = str(getattr(drv, "service_name"))
                class_guid = str(getattr(drv, "class_guid"))
                dev_name = str(getattr(drv, "name"))

                pci_ids = list(getattr(drv, "pci_ids", []) or [])
                for pci_id in pci_ids:
                    try:
                        node = h.node_get_child(cdd, pci_id)
                        action = "updated" if node is not None else "created"
                        if node is None:
                            node = h.node_add_child(cdd, pci_id)

                        _set_sz(h, node, "Service", svc_name)
                        _set_sz(h, node, "ClassGUID", class_guid)
                        _set_sz(h, node, "Class", "SCSIAdapter")
                        _set_sz(h, node, "DeviceDesc", dev_name)

                        logger.info(f"CDD {action}: {pci_id} -> {svc_name}")
                        results["cdd"].append({"pci_id": pci_id, "service": svc_name, "action": action})
                    except Exception as e:
                        msg = f"Failed CDD entry {pci_id} -> {svc_name}: {e}"
                        logger.error(msg)
                        results["errors"].append(msg)

            # Commit + upload if not dry_run
            if not dry_run:
                logger.info("Committing SYSTEM hive changes (hivex_commit)")
                h.hivex_commit(None)
                h.hivex_close()

                logger.info(f"Uploading modified SYSTEM hive back to guest: {hive_path}")
                g.upload(str(local_hive), hive_path)

                # Verify by hash
                with tempfile.TemporaryDirectory() as verify_tmp:
                    verify_path = Path(verify_tmp) / "SYSTEM_verify"
                    g.download(hive_path, str(verify_path))
                    new_hash = hashlib.sha256(verify_path.read_bytes()).hexdigest()

                if new_hash != original_hash:
                    results["registry_modified"] = True
                    logger.info(f"SYSTEM hive changed: sha256 {original_hash} -> {new_hash}")
                else:
                    logger.warning("SYSTEM hive appears unchanged after upload (unexpected)")

                # Post-edit verification
                with tempfile.TemporaryDirectory() as verify_dir:
                    verify_hive = Path(verify_dir) / "SYSTEM_verify"
                    g.download(hive_path, str(verify_hive))

                    vh = g.hivex_open(str(verify_hive), write=0)
                    vroot = vh.root()

                    vcs = vh.node_get_child(vroot, cs_name)
                    if vcs is None:
                        vcs = vh.node_get_child(vroot, "ControlSet001")

                    vservices = vh.node_get_child(vcs, "Services") if vcs else None
                    if vservices is None:
                        results["verification_errors"].append("Verification failed: Services node missing")
                    else:
                        for drv in drivers:
                            svc_name = str(getattr(drv, "service_name"))
                            drv_type_value = getattr(getattr(drv, "type", None), "value", None) or str(getattr(drv, "type", ""))
                            start_default = int(getattr(getattr(drv, "start_type", None), "value", getattr(drv, "start_type", 0)))

                            svc = vh.node_get_child(vservices, svc_name)
                            if svc is None:
                                results["verification_errors"].append(f"Missing service after edit: {svc_name}")
                                continue

                            got = _hivex_read_dword(vh, svc, "Start")
                            expected = start_default
                            if drv_type_value == driver_type_storage_value:
                                expected = boot_start_value

                            if got == expected:
                                results["verified_services"].append(svc_name)
                            else:
                                results["verification_errors"].append(
                                    f"{svc_name} Start mismatch: got={got} expected={expected}"
                                )

                    try:
                        vh.hivex_close()
                    except Exception:
                        pass

            else:
                try:
                    h.hivex_close()
                except Exception:
                    pass
                logger.info("Dry-run: registry edits computed but not committed/uploaded")

            results["success"] = len(results["errors"]) == 0
            results["notes"] += [
                "Storage services forced to BOOT start to prevent INACCESSIBLE_BOOT_DEVICE.",
                "StartOverride keys removed (if present) because they can silently disable drivers.",
                "Registry strings written as UTF-16LE REG_SZ (Windows-correct).",
                "CriticalDeviceDatabase populated for storage PCI IDs.",
            ]
            return results

        except Exception as e:
            msg = f"Registry editing failed: {e}"
            logger.error(msg)
            results["errors"].append(msg)
            return results


# ---------------------------
# Public: SYSTEM hive generic DWORD setter (for CrashControl etc.)
# ---------------------------

def set_system_dword(
    self,
    g: guestfs.GuestFS,
    hive_path: str,
    *,
    key_path: List[str],
    name: str,
    value: int,
) -> Dict[str, Any]:
    """
    Generic offline SYSTEM hive edit:
      HKLM\\SYSTEM\\<CurrentControlSet>\\<key_path...>\\<name> = REG_DWORD(value)

    Example:
      set_system_dword(..., key_path=["Control","CrashControl"], name="AutoReboot", value=0)
    """
    logger = _safe_logger(self)
    dry_run = getattr(self, "dry_run", False)

    out: Dict[str, Any] = {
        "success": False,
        "dry_run": bool(dry_run),
        "hive_path": hive_path,
        "key_path": list(key_path),
        "name": name,
        "value": int(value),
        "modified": False,
        "original": None,
        "new": None,
        "errors": [],
        "notes": [],
    }

    try:
        if not g.is_file(hive_path):
            out["errors"].append(f"SYSTEM hive not found: {hive_path}")
            return out
    except Exception as e:
        out["errors"].append(f"Failed to stat hive {hive_path}: {e}")
        return out

    with tempfile.TemporaryDirectory() as td:
        local = Path(td) / "SYSTEM"
        try:
            if not dry_run:
                ts = U.now_ts()
                backup_path = f"{hive_path}.vmdk2kvm.backup.{ts}"
                g.cp(hive_path, backup_path)
                out["hive_backup"] = backup_path

            g.download(hive_path, str(local))
            orig_hash = hashlib.sha256(local.read_bytes()).hexdigest()

            h = g.hivex_open(str(local), write=(0 if dry_run else 1))
            root = h.root()

            cs_name = _detect_current_controlset(h, root)
            cs = h.node_get_child(root, cs_name)
            if cs is None:
                cs_name = "ControlSet001"
                cs = h.node_get_child(root, cs_name)
            if cs is None:
                out["errors"].append("No usable ControlSet found (001/current)")
                try:
                    h.hivex_close()
                except Exception:
                    pass
                return out

            # Walk/ensure path under controlset
            node = cs
            for comp in key_path:
                node = _ensure_child(h, node, comp)

            old = _hivex_read_dword(h, node, name)
            out["original"] = old

            if old != int(value):
                _set_dword(h, node, name, int(value))
                out["modified"] = True
                out["new"] = int(value)
            else:
                out["new"] = old

            if not dry_run:
                h.hivex_commit(None)
                h.hivex_close()
                g.upload(str(local), hive_path)

                with tempfile.TemporaryDirectory() as vtd:
                    vlocal = Path(vtd) / "SYSTEM_verify"
                    g.download(hive_path, str(vlocal))
                    new_hash = hashlib.sha256(vlocal.read_bytes()).hexdigest()

                out["success"] = (new_hash != orig_hash) or (not out["modified"])
            else:
                try:
                    h.hivex_close()
                except Exception:
                    pass
                out["success"] = True

            out["notes"] += [
                f"ControlSet resolved and edited at: {cs_name}",
                "DWORD written as REG_DWORD (little-endian).",
            ]
            return out

        except Exception as e:
            out["errors"].append(f"SYSTEM dword set failed: {e}")
            return out


# ---------------------------
# Public: SOFTWARE hive DevicePath append
# ---------------------------

def append_devicepath_software_hive(
    self,
    g: guestfs.GuestFS,
    software_hive_path: str,
    append_path: str,
) -> Dict[str, Any]:
    """
    Add append_path to:
      HKLM\\SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\DevicePath
    """
    logger = _safe_logger(self)
    dry_run = getattr(self, "dry_run", False)

    out: Dict[str, Any] = {
        "success": False,
        "dry_run": bool(dry_run),
        "hive_path": software_hive_path,
        "modified": False,
        "original": None,
        "new": None,
        "errors": [],
        "notes": [],
    }

    try:
        if not g.is_file(software_hive_path):
            out["errors"].append(f"SOFTWARE hive not found: {software_hive_path}")
            return out
    except Exception as e:
        out["errors"].append(f"Failed to stat hive {software_hive_path}: {e}")
        return out

    with tempfile.TemporaryDirectory() as td:
        local = Path(td) / "SOFTWARE"
        try:
            g.download(software_hive_path, str(local))
            orig_hash = hashlib.sha256(local.read_bytes()).hexdigest()

            h = g.hivex_open(str(local), write=(0 if dry_run else 1))
            root = h.root()

            microsoft = h.node_get_child(root, "Microsoft")
            if microsoft is None:
                out["errors"].append("SOFTWARE hive missing Microsoft key")
                try:
                    h.hivex_close()
                except Exception:
                    pass
                return out

            windows = h.node_get_child(microsoft, "Windows")
            if windows is None:
                out["errors"].append("SOFTWARE hive missing Microsoft\\Windows key")
                try:
                    h.hivex_close()
                except Exception:
                    pass
                return out

            cv = h.node_get_child(windows, "CurrentVersion")
            if cv is None:
                out["errors"].append("SOFTWARE hive missing Microsoft\\Windows\\CurrentVersion key")
                try:
                    h.hivex_close()
                except Exception:
                    pass
                return out

            cur = _hivex_read_sz(h, cv, "DevicePath") or r"%SystemRoot%\inf"
            out["original"] = cur

            parts = [p.strip() for p in cur.split(";") if p.strip()]
            if append_path not in parts:
                parts.append(append_path)
            new = ";".join(parts)
            out["new"] = new

            if new != cur:
                logger.info(f"Updating DevicePath: +{append_path}")
                _set_expand_sz(h, cv, "DevicePath", new)
                out["modified"] = True
            else:
                logger.info("DevicePath already contains staging path; no change needed")

            if not dry_run:
                h.hivex_commit(None)
                h.hivex_close()
                g.upload(str(local), software_hive_path)

                with tempfile.TemporaryDirectory() as vtd:
                    vlocal = Path(vtd) / "SOFTWARE_verify"
                    g.download(software_hive_path, str(vlocal))
                    new_hash = hashlib.sha256(vlocal.read_bytes()).hexdigest()

                if new_hash != orig_hash:
                    out["success"] = True
                else:
                    out["success"] = not out["modified"]
            else:
                try:
                    h.hivex_close()
                except Exception:
                    pass
                out["success"] = True

            out["notes"] += [
                "DevicePath updated to help Windows PnP discover staged INF packages on first boot.",
                "Value written as REG_EXPAND_SZ (UTF-16LE).",
            ]
            return out

        except Exception as e:
            out["errors"].append(f"DevicePath update failed: {e}")
            return out


# ---------------------------
# Public: SOFTWARE hive RunOnce helper (for firstboot scripts)
# ---------------------------

def add_software_runonce(
    self,
    g: guestfs.GuestFS,
    software_hive_path: str,
    *,
    name: str,
    command: str,
) -> Dict[str, Any]:
    """
    Add/overwrite a RunOnce entry:

      HKLM\\SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\RunOnce\\<name> = "<command>"

    This is the simplest way to ensure a one-time firstboot action without
    relying on Task Scheduler or services.

    Note: Stored as REG_SZ (UTF-16LE).
    """
    logger = _safe_logger(self)
    dry_run = getattr(self, "dry_run", False)

    out: Dict[str, Any] = {
        "success": False,
        "dry_run": bool(dry_run),
        "hive_path": software_hive_path,
        "name": name,
        "command": command,
        "modified": False,
        "original": None,
        "new": None,
        "errors": [],
        "notes": [],
    }

    try:
        if not g.is_file(software_hive_path):
            out["errors"].append(f"SOFTWARE hive not found: {software_hive_path}")
            return out
    except Exception as e:
        out["errors"].append(f"Failed to stat hive {software_hive_path}: {e}")
        return out

    with tempfile.TemporaryDirectory() as td:
        local = Path(td) / "SOFTWARE"
        try:
            if not dry_run:
                ts = U.now_ts()
                backup_path = f"{software_hive_path}.vmdk2kvm.backup.{ts}"
                g.cp(software_hive_path, backup_path)
                out["hive_backup"] = backup_path

            g.download(software_hive_path, str(local))
            orig_hash = hashlib.sha256(local.read_bytes()).hexdigest()

            h = g.hivex_open(str(local), write=(0 if dry_run else 1))
            root = h.root()

            # Walk Microsoft\Windows\CurrentVersion\RunOnce
            microsoft = h.node_get_child(root, "Microsoft")
            if microsoft is None:
                microsoft = _ensure_child(h, root, "Microsoft")

            windows = h.node_get_child(microsoft, "Windows")
            if windows is None:
                windows = _ensure_child(h, microsoft, "Windows")

            cv = h.node_get_child(windows, "CurrentVersion")
            if cv is None:
                cv = _ensure_child(h, windows, "CurrentVersion")

            runonce = h.node_get_child(cv, "RunOnce")
            if runonce is None:
                runonce = _ensure_child(h, cv, "RunOnce")

            old = _hivex_read_sz(h, runonce, name)
            out["original"] = old

            if old != command:
                _set_sz(h, runonce, name, command)
                out["modified"] = True
                out["new"] = command
            else:
                out["new"] = old

            if not dry_run:
                h.hivex_commit(None)
                h.hivex_close()
                g.upload(str(local), software_hive_path)

                with tempfile.TemporaryDirectory() as vtd:
                    vlocal = Path(vtd) / "SOFTWARE_verify"
                    g.download(software_hive_path, str(vlocal))
                    new_hash = hashlib.sha256(vlocal.read_bytes()).hexdigest()

                out["success"] = (new_hash != orig_hash) or (not out["modified"])
            else:
                try:
                    h.hivex_close()
                except Exception:
                    pass
                out["success"] = True

            logger.info(f"RunOnce set: {name} -> {command}")
            out["notes"] += [
                r"RunOnce written at HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\RunOnce",
                "Value written as REG_SZ (UTF-16LE).",
            ]
            return out

        except Exception as e:
            out["errors"].append(f"RunOnce update failed: {e}")
            return out
