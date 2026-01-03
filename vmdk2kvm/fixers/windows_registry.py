# vmdk2kvm/fixers/windows_registry.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import logging
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import guestfs  # type: ignore
import hivex  # type: ignore

from ..core.utils import U

# ---------------------------------------------------------------------------
# Logging helper
# ---------------------------------------------------------------------------


def _safe_logger(self) -> logging.Logger:
    lg = getattr(self, "logger", None)
    if isinstance(lg, logging.Logger):
        return lg
    return logging.getLogger("vmdk2kvm.windows_registry")


# ---------------------------------------------------------------------------
# Robust hive download helpers
# ---------------------------------------------------------------------------


def _download_hive_local(logger: logging.Logger, g: guestfs.GuestFS, remote: str, local: Path) -> None:
    """
    Robustly download a hive from the guest to a local path.

    We've seen cases where g.download() does not materialize the local file
    (or produces an empty/truncated file) without raising. This helper:
      1) tries g.download()
      2) verifies local exists + size >= 4KiB
      3) falls back to g.read_file()/g.cat() and writes bytes locally
    """
    local.parent.mkdir(parents=True, exist_ok=True)

    # Attempt primary download path.
    try:
        logger.info("Downloading hive: %r -> %r", remote, str(local))
        g.download(remote, str(local))
    except Exception as e:
        logger.warning("g.download(%r, %r) failed: %s", remote, str(local), e)

    # Verify the file actually exists and is non-trivial.
    try:
        if local.exists() and local.stat().st_size >= 4096:
            return
    except Exception:
        pass

    # Fallback: read bytes from guestfs and write locally.
    logger.warning("Hive not materialized after download; falling back to guestfs read: %r", remote)
    data: Optional[bytes] = None

    for fn_name in ("read_file", "cat"):
        fn = getattr(g, fn_name, None)
        if not callable(fn):
            continue
        try:
            out = fn(remote)
            if isinstance(out, (bytes, bytearray)):
                data = bytes(out)
            else:
                # Some bindings return str; preserve raw-ish bytes best-effort.
                data = str(out).encode("latin-1", errors="ignore")
            break
        except Exception as e:
            logger.warning("%s(%r) failed: %s", fn_name, remote, e)

    if not data or len(data) < 4096:
        raise RuntimeError(
            f"Failed to download hive locally: remote={remote} local={local} (len={len(data) if data else 0})"
        )

    local.write_bytes(data)

    if not local.exists() or local.stat().st_size < 4096:
        raise RuntimeError(f"Local hive still missing after fallback: {local}")


def _log_mountpoints_best_effort(logger: logging.Logger, g: guestfs.GuestFS) -> None:
    """
    Helpful when hive paths are correct but the wrong partition is mounted as /.
    """
    try:
        mps = g.mountpoints()
        logger.debug("guestfs mountpoints=%r", mps)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Hivex node normalization (IMPORTANT)
# ---------------------------------------------------------------------------
# python-hivex bindings vary:
# - some return 0 for "not found"
# - some return None for "not found"
# We normalize to an int node id (0 means invalid).


NodeLike = Union[int, None]


def _node_id(n: NodeLike) -> int:
    """
    Normalize a node id to int. Invalid/missing => 0.
    """
    if n is None:
        return 0
    try:
        return int(n)
    except Exception:
        return 0


def _node_ok(n: NodeLike) -> bool:
    return _node_id(n) != 0


# ---------------------------------------------------------------------------
# Registry encoding helpers (CRITICAL)
# ---------------------------------------------------------------------------
# Windows registry strings in hives are typically UTF-16LE NUL-terminated.


def _reg_sz(s: str) -> bytes:
    # strings here are typically ASCII-safe; keep ignore for robustness
    return (s + "\0").encode("utf-16le", errors="ignore")


def _decode_reg_sz(raw: bytes) -> str:
    try:
        return raw.decode("utf-16le", errors="ignore").rstrip("\x00")
    except Exception:
        try:
            return raw.decode("utf-8", errors="ignore").rstrip("\x00")
        except Exception:
            return ""


def _mk_reg_value(name: str, t: int, value: bytes) -> Dict[str, Any]:
    """
    Create a value dict compatible with python-hivex.

    Many python-hivex versions accept dicts:
      {"key": <name>, "t": <type>, "value": <bytes>}
    """
    return {"key": name, "t": int(t), "value": value}


def _set_sz(h: hivex.Hivex, node: NodeLike, key: str, s: str) -> None:
    nid = _node_id(node)
    if nid == 0:
        raise RuntimeError(f"invalid registry node for setting {key}=REG_SZ")
    h.node_set_value(nid, _mk_reg_value(key, 1, _reg_sz(s)))


def _set_expand_sz(h: hivex.Hivex, node: NodeLike, key: str, s: str) -> None:
    nid = _node_id(node)
    if nid == 0:
        raise RuntimeError(f"invalid registry node for setting {key}=REG_EXPAND_SZ")
    h.node_set_value(nid, _mk_reg_value(key, 2, _reg_sz(s)))


def _set_dword(h: hivex.Hivex, node: NodeLike, key: str, v: int) -> None:
    nid = _node_id(node)
    if nid == 0:
        raise RuntimeError(f"invalid registry node for setting {key}=REG_DWORD")
    h.node_set_value(nid, _mk_reg_value(key, 4, int(v).to_bytes(4, "little", signed=False)))


def _ensure_child(h: hivex.Hivex, parent: NodeLike, name: str) -> int:
    pid = _node_id(parent)
    if pid == 0:
        raise RuntimeError(f"invalid parent node while ensuring child {name}")

    ch = _node_id(h.node_get_child(pid, name))
    if ch == 0:
        ch = _node_id(h.node_add_child(pid, name))
    if ch == 0:
        raise RuntimeError(f"failed to create child key {name}")
    return ch


def _delete_child_if_exists(h: hivex.Hivex, parent: NodeLike, name: str, *, logger: Optional[logging.Logger] = None) -> bool:
    """
    Remove a child key node if present (commonly StartOverride).

    python-hivex API signatures vary across versions/bindings. Common patterns:
      - node_delete_child(parent_node, child_node)
      - node_delete_child(parent_node, child_name)
      - (rare) node_delete_child(child_node)

    Important: do NOT bail on the first non-TypeError exception; some bindings
    raise other exception types for arg mismatch.
    """
    pid = _node_id(parent)
    if pid == 0:
        return False

    child = _node_id(h.node_get_child(pid, name))
    if child == 0:
        return False

    # Try plausible signatures (most correct first).
    tried: List[str] = []
    for args in ((pid, child), (pid, name), (child,)):
        tried.append(repr(args))
        try:
            h.node_delete_child(*args)  # type: ignore[misc]
            if logger:
                logger.debug("Deleted child key %r using node_delete_child%s", name, args)
            return True
        except Exception as e:
            # If this is an argument mismatch, many bindings throw TypeError,
            # but some throw generic Exception. Keep trying.
            if logger:
                logger.debug("node_delete_child%s failed for %r: %s", args, name, e)
            continue

    if logger:
        logger.warning("All node_delete_child signatures failed for %r (tried: %s)", name, ", ".join(tried))
    return False


def _hivex_read_value_dict(h: hivex.Hivex, node: NodeLike, key: str) -> Optional[Dict[str, Any]]:
    """
    Return a value dict {"key","t","value"} or None.
    """
    nid = _node_id(node)
    if nid == 0:
        return None
    try:
        v = h.node_get_value(nid, key)
        if not v or "value" not in v:
            return None
        return v
    except Exception:
        return None


def _hivex_read_sz(h: hivex.Hivex, node: NodeLike, key: str) -> Optional[str]:
    """
    Read REG_SZ/REG_EXPAND_SZ-ish as text (best-effort).
    """
    v = _hivex_read_value_dict(h, node, key)
    if not v:
        return None
    raw = v.get("value")
    if isinstance(raw, (bytes, bytearray)):
        s = _decode_reg_sz(bytes(raw)).strip()
        return s or None
    if raw is None:
        return None
    s2 = str(raw).strip()
    return s2 or None


def _hivex_read_dword(h: hivex.Hivex, node: NodeLike, key: str) -> Optional[int]:
    v = _hivex_read_value_dict(h, node, key)
    if not v:
        return None
    raw = v.get("value")
    if isinstance(raw, (bytes, bytearray)) and len(raw) >= 4:
        return int.from_bytes(bytes(raw)[:4], "little", signed=False)
    if isinstance(raw, int):
        return raw
    return None


def _detect_current_controlset(h: hivex.Hivex, root: NodeLike) -> str:
    """
    Return active ControlSetXYZ name using SYSTEM\\Select\\Current.
    Falls back to ControlSet001 if anything looks off.
    """
    r = _node_id(root)
    if r == 0:
        return "ControlSet001"

    select = _node_id(h.node_get_child(r, "Select"))
    if select == 0:
        return "ControlSet001"

    v = _hivex_read_value_dict(h, select, "Current")
    if not v:
        return "ControlSet001"

    cur_raw = v.get("value")
    if isinstance(cur_raw, (bytes, bytearray)) and len(cur_raw) >= 4:
        current_set = int.from_bytes(bytes(cur_raw)[:4], "little", signed=False)
    elif isinstance(cur_raw, int):
        current_set = int(cur_raw)
    else:
        current_set = 1

    return f"ControlSet{current_set:03d}"


# ---------------------------------------------------------------------------
# Hivex open helpers (LOCAL FILES ONLY)
# ---------------------------------------------------------------------------


def _open_hive_local(path: Path, *, write: bool) -> hivex.Hivex:
    """
    Open a *local* hive file using python-hivex.
    """
    if not path.exists():
        raise FileNotFoundError(f"hive local file missing: {path}")
    st = path.stat()
    if st.st_size < 4096:
        raise RuntimeError(f"hive local file too small ({st.st_size} bytes): {path}")
    # python-hivex expects write as int (0/1) in many versions
    return hivex.Hivex(str(path), write=(1 if write else 0))


def _close_best_effort(h: Optional[hivex.Hivex]) -> None:
    """
    Close hive handle across python-hivex versions.
    Some expose .close(); some expose .hivex_close().
    """
    if h is None:
        return
    try:
        if hasattr(h, "close") and callable(getattr(h, "close")):
            h.close()
            return
    except Exception:
        pass
    try:
        if hasattr(h, "hivex_close") and callable(getattr(h, "hivex_close")):
            h.hivex_close()
            return
    except Exception:
        pass


def _commit_best_effort(h: hivex.Hivex) -> None:
    """
    Commit changes across python-hivex versions.
    Some expose .commit(); some expose .hivex_commit().

    Signatures vary too:
      - commit(None) or commit()
      - hivex_commit(None) or hivex_commit()
    """
    if hasattr(h, "commit") and callable(getattr(h, "commit")):
        try:
            h.commit(None)  # type: ignore[arg-type]
            return
        except TypeError:
            h.commit()  # type: ignore[call-arg]
            return
        except Exception:
            pass

    if hasattr(h, "hivex_commit") and callable(getattr(h, "hivex_commit")):
        try:
            h.hivex_commit(None)  # type: ignore[arg-type]
            return
        except TypeError:
            h.hivex_commit()  # type: ignore[call-arg]
            return

    raise RuntimeError("python-hivex: no commit method found")


# ---------------------------------------------------------------------------
# Internal: normalize Start values (fixes NoneType -> int errors)
# ---------------------------------------------------------------------------


def _driver_start_default(drv: Any, *, fallback: int = 3) -> int:
    """
    Extract a sane default Start value from drv.start_type.

    We have seen driver descriptors where:
      - drv.start_type is an Enum with .value
      - drv.start_type is an int
      - drv.start_type.value is None  (=> your 'NoneType cannot be interpreted as int' error)

    We normalize to a valid int, using fallback (3 = DEMAND_START).
    """
    st = getattr(drv, "start_type", None)

    # Enum-like
    if st is not None and hasattr(st, "value"):
        v = getattr(st, "value", None)
        if v is None:
            return int(fallback)
        try:
            return int(v)
        except Exception:
            return int(fallback)

    if st is None:
        return int(fallback)

    try:
        return int(st)
    except Exception:
        return int(fallback)


def _driver_type_norm(drv: Any) -> str:
    """
    Normalize driver type for comparisons (string-safe).
    Handles Enum.value, ints, and arbitrary objects.
    """
    t = getattr(drv, "type", None)
    if t is None:
        return ""
    if hasattr(t, "value"):
        v = getattr(t, "value", None)
        if v is not None:
            return str(v)
    return str(t)


def _pci_id_normalize(pci_id: str) -> str:
    """
    Normalize CDD key names. Windows is case-insensitive but hive keys are stored as-is.
    Keep the original string, but strip whitespace.
    """
    return str(pci_id).strip()


# ---------------------------------------------------------------------------
# Public: SYSTEM hive edit (Services + CDD + StartOverride)
# ---------------------------------------------------------------------------


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
    """
    logger = _safe_logger(self)
    dry_run = bool(getattr(self, "dry_run", False))

    results: Dict[str, Any] = {
        "success": False,
        "dry_run": dry_run,
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

    # Fast preflight: does the hive exist inside the guest mount?
    try:
        if not g.is_file(hive_path):
            results["errors"].append(f"SYSTEM hive not found: {hive_path}")
            return results
    except Exception as e:
        results["errors"].append(f"Failed to stat hive {hive_path}: {e}")
        return results

    with tempfile.TemporaryDirectory() as tmpdir:
        local_hive = Path(tmpdir) / "SYSTEM"
        h: Optional[hivex.Hivex] = None

        try:
            _log_mountpoints_best_effort(logger, g)

            # Backup hive inside guest
            if not dry_run:
                ts = U.now_ts()
                backup_path = f"{hive_path}.vmdk2kvm.backup.{ts}"
                g.cp(hive_path, backup_path)
                logger.info("Hive backup created: %s", backup_path)
                results["hive_backup"] = backup_path

            # Download hive locally (robust)
            _download_hive_local(logger, g, hive_path, local_hive)

            original_hash = hashlib.sha256(local_hive.read_bytes()).hexdigest()
            logger.debug("SYSTEM hive baseline sha256=%s", original_hash)

            # Open local hive with python-hivex
            h = _open_hive_local(local_hive, write=(not dry_run))
            root = _node_id(h.root())
            if root == 0:
                raise RuntimeError("python-hivex root() returned invalid node")

            cs_name = _detect_current_controlset(h, root)
            logger.info("Using control set: %s", cs_name)

            control_set = _node_id(h.node_get_child(root, cs_name))
            if control_set == 0:
                logger.warning("%s missing; falling back to ControlSet001", cs_name)
                cs_name = "ControlSet001"
                control_set = _node_id(h.node_get_child(root, cs_name))
                if control_set == 0:
                    raise RuntimeError("No usable ControlSet found (001/current)")

            services = _ensure_child(h, control_set, "Services")

            storage_type_norm = str(driver_type_storage_value)

            # Services\<driver>
            for drv in drivers:
                try:
                    drv_type_value = _driver_type_norm(drv)
                    svc_name = str(getattr(drv, "service_name"))
                    dest_name = str(getattr(drv, "dest_name"))

                    start_default = _driver_start_default(drv, fallback=3)
                    svc = _node_id(h.node_get_child(services, svc_name))
                    action = "updated" if svc != 0 else "created"
                    if svc == 0:
                        svc = _node_id(h.node_add_child(services, svc_name))
                    if svc == 0:
                        raise RuntimeError(f"failed to open/create service key {svc_name}")

                    logger.info("Registry service %s: Services\\%s", action, svc_name)

                    # Always kernel driver (assumption for our staged virtio drivers)
                    _set_dword(h, svc, "Type", 1)  # SERVICE_KERNEL_DRIVER
                    _set_dword(h, svc, "ErrorControl", 1)

                    start = int(start_default)
                    if str(drv_type_value) == storage_type_norm:
                        start = int(boot_start_value)
                    _set_dword(h, svc, "Start", start)

                    if str(drv_type_value) == storage_type_norm:
                        group = "SCSI miniport"
                    elif str(drv_type_value) == "network":
                        group = "NDIS"
                    else:
                        group = "System Bus Extender"

                    _set_sz(h, svc, "Group", group)
                    _set_sz(h, svc, "ImagePath", fr"\SystemRoot\System32\drivers\{dest_name}")
                    _set_sz(h, svc, "DisplayName", svc_name)

                    removed = _delete_child_if_exists(h, svc, "StartOverride", logger=logger)
                    if removed:
                        logger.info("Removed StartOverride: Services\\%s\\StartOverride", svc_name)
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
                drv_type_value = _driver_type_norm(drv)
                if str(drv_type_value) != storage_type_norm:
                    continue

                svc_name = str(getattr(drv, "service_name"))
                class_guid = str(getattr(drv, "class_guid"))
                dev_name = str(getattr(drv, "name"))

                pci_ids = list(getattr(drv, "pci_ids", []) or [])
                for pci_id in pci_ids:
                    pci_id = _pci_id_normalize(pci_id)
                    if not pci_id:
                        continue
                    try:
                        node = _node_id(h.node_get_child(cdd, pci_id))
                        action = "updated" if node != 0 else "created"
                        if node == 0:
                            node = _node_id(h.node_add_child(cdd, pci_id))
                        if node == 0:
                            raise RuntimeError(f"failed to open/create CDD node {pci_id}")

                        _set_sz(h, node, "Service", svc_name)
                        _set_sz(h, node, "ClassGUID", class_guid)
                        _set_sz(h, node, "Class", "SCSIAdapter")
                        _set_sz(h, node, "DeviceDesc", dev_name)

                        logger.info("CDD %s: %s -> %s", action, pci_id, svc_name)
                        results["cdd"].append({"pci_id": pci_id, "service": svc_name, "action": action})
                    except Exception as e:
                        msg = f"Failed CDD entry {pci_id} -> {svc_name}: {e}"
                        logger.error(msg)
                        results["errors"].append(msg)

            if not dry_run:
                try:
                    logger.info("Committing SYSTEM hive changes (python-hivex commit)")
                    _commit_best_effort(h)
                finally:
                    _close_best_effort(h)
                    h = None

                logger.info("Uploading modified SYSTEM hive back to guest: %s", hive_path)
                g.upload(str(local_hive), hive_path)

                with tempfile.TemporaryDirectory() as verify_tmp:
                    verify_path = Path(verify_tmp) / "SYSTEM_verify"
                    _download_hive_local(logger, g, hive_path, verify_path)
                    new_hash = hashlib.sha256(verify_path.read_bytes()).hexdigest()

                if new_hash != original_hash:
                    results["registry_modified"] = True
                    logger.info("SYSTEM hive changed: sha256 %s -> %s", original_hash, new_hash)
                else:
                    logger.warning("SYSTEM hive appears unchanged after upload (unexpected)")

                # Optional verification readback (best-effort)
                with tempfile.TemporaryDirectory() as verify_dir:
                    verify_hive = Path(verify_dir) / "SYSTEM_verify"
                    _download_hive_local(logger, g, hive_path, verify_hive)
                    vh: Optional[hivex.Hivex] = None
                    try:
                        vh = _open_hive_local(verify_hive, write=False)
                        vroot = _node_id(vh.root())
                        vcs = _node_id(vh.node_get_child(vroot, cs_name))
                        if vcs == 0:
                            vcs = _node_id(vh.node_get_child(vroot, "ControlSet001"))
                        vservices = _node_id(vh.node_get_child(vcs, "Services")) if vcs != 0 else 0

                        if vservices == 0:
                            results["verification_errors"].append("Verification failed: Services node missing")
                        else:
                            for drv in drivers:
                                svc_name = str(getattr(drv, "service_name"))
                                drv_type_value = _driver_type_norm(drv)
                                start_default = _driver_start_default(drv, fallback=3)

                                svc = _node_id(vh.node_get_child(vservices, svc_name))
                                if svc == 0:
                                    results["verification_errors"].append(f"Missing service after edit: {svc_name}")
                                    continue

                                got = _hivex_read_dword(vh, svc, "Start")
                                expected = int(start_default)
                                if str(drv_type_value) == storage_type_norm:
                                    expected = int(boot_start_value)

                                if got == expected:
                                    results["verified_services"].append(svc_name)
                                else:
                                    results["verification_errors"].append(
                                        f"{svc_name} Start mismatch: got={got} expected={expected}"
                                    )
                    finally:
                        _close_best_effort(vh)
            else:
                logger.info("Dry-run: registry edits computed but not committed/uploaded")

            results["success"] = len(results["errors"]) == 0
            results["notes"] += [
                "Storage services forced to BOOT start to prevent INACCESSIBLE_BOOT_DEVICE.",
                "StartOverride keys removed (if present) because they can silently disable drivers.",
                "Registry strings written as UTF-16LE REG_SZ (Windows-correct).",
                "CriticalDeviceDatabase populated for storage PCI IDs.",
                "Node ids normalized across python-hivex versions (0 vs None).",
                "Driver start_type None handled with fallback Start=3 (demand).",
                "Driver type comparisons normalized via _driver_type_norm().",
            ]
            return results

        except Exception as e:
            msg = f"Registry editing failed: {e}"
            logger.error(msg)
            results["errors"].append(msg)
            return results
        finally:
            _close_best_effort(h)


# ---------------------------------------------------------------------------
# Public: SYSTEM hive generic DWORD setter (for CrashControl etc.)
# ---------------------------------------------------------------------------


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
    """
    logger = _safe_logger(self)
    dry_run = bool(getattr(self, "dry_run", False))

    out: Dict[str, Any] = {
        "success": False,
        "dry_run": dry_run,
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
        h: Optional[hivex.Hivex] = None
        try:
            _log_mountpoints_best_effort(logger, g)

            if not dry_run:
                ts = U.now_ts()
                backup_path = f"{hive_path}.vmdk2kvm.backup.{ts}"
                g.cp(hive_path, backup_path)
                out["hive_backup"] = backup_path

            _download_hive_local(logger, g, hive_path, local)
            orig_hash = hashlib.sha256(local.read_bytes()).hexdigest()

            h = _open_hive_local(local, write=(not dry_run))
            root = _node_id(h.root())
            if root == 0:
                out["errors"].append("Invalid hivex root()")
                return out

            cs_name = _detect_current_controlset(h, root)
            cs = _node_id(h.node_get_child(root, cs_name))
            if cs == 0:
                cs_name = "ControlSet001"
                cs = _node_id(h.node_get_child(root, cs_name))
            if cs == 0:
                out["errors"].append("No usable ControlSet found (001/current)")
                return out

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
                try:
                    _commit_best_effort(h)
                finally:
                    _close_best_effort(h)
                    h = None

                g.upload(str(local), hive_path)

                with tempfile.TemporaryDirectory() as vtd:
                    vlocal = Path(vtd) / "SYSTEM_verify"
                    _download_hive_local(logger, g, hive_path, vlocal)
                    new_hash = hashlib.sha256(vlocal.read_bytes()).hexdigest()

                out["success"] = (new_hash != orig_hash) or (not out["modified"])
            else:
                out["success"] = True

            out["notes"] += [
                f"ControlSet resolved and edited at: {cs_name}",
                "DWORD written as REG_DWORD (little-endian).",
                "Node ids normalized across python-hivex versions (0 vs None).",
            ]
            return out

        except Exception as e:
            out["errors"].append(f"SYSTEM dword set failed: {e}")
            return out
        finally:
            _close_best_effort(h)


# ---------------------------------------------------------------------------
# Public: SOFTWARE hive DevicePath append
# ---------------------------------------------------------------------------


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
    dry_run = bool(getattr(self, "dry_run", False))

    out: Dict[str, Any] = {
        "success": False,
        "dry_run": dry_run,
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
        h: Optional[hivex.Hivex] = None
        try:
            _log_mountpoints_best_effort(logger, g)

            # Backup (match other SOFTWARE edits)
            if not dry_run:
                ts = U.now_ts()
                backup_path = f"{software_hive_path}.vmdk2kvm.backup.{ts}"
                g.cp(software_hive_path, backup_path)
                out["hive_backup"] = backup_path

            _download_hive_local(logger, g, software_hive_path, local)
            orig_hash = hashlib.sha256(local.read_bytes()).hexdigest()

            h = _open_hive_local(local, write=(not dry_run))
            root = _node_id(h.root())
            if root == 0:
                out["errors"].append("Invalid hivex root()")
                return out

            microsoft = _node_id(h.node_get_child(root, "Microsoft"))
            if microsoft == 0:
                out["errors"].append("SOFTWARE hive missing Microsoft key")
                return out

            windows = _node_id(h.node_get_child(microsoft, "Windows"))
            if windows == 0:
                out["errors"].append("SOFTWARE hive missing Microsoft\\Windows key")
                return out

            cv = _node_id(h.node_get_child(windows, "CurrentVersion"))
            if cv == 0:
                out["errors"].append("SOFTWARE hive missing Microsoft\\Windows\\CurrentVersion key")
                return out

            cur = _hivex_read_sz(h, cv, "DevicePath") or r"%SystemRoot%\inf"
            out["original"] = cur

            parts = [p.strip() for p in cur.split(";") if p.strip()]
            if append_path not in parts:
                parts.append(append_path)
            new = ";".join(parts)
            out["new"] = new

            if new != cur:
                logger.info("Updating DevicePath: +%s", append_path)
                _set_expand_sz(h, cv, "DevicePath", new)
                out["modified"] = True
            else:
                logger.info("DevicePath already contains staging path; no change needed")

            if not dry_run:
                try:
                    _commit_best_effort(h)
                finally:
                    _close_best_effort(h)
                    h = None

                g.upload(str(local), software_hive_path)

                with tempfile.TemporaryDirectory() as vtd:
                    vlocal = Path(vtd) / "SOFTWARE_verify"
                    _download_hive_local(logger, g, software_hive_path, vlocal)
                    new_hash = hashlib.sha256(vlocal.read_bytes()).hexdigest()

                out["success"] = (new_hash != orig_hash) or (not out["modified"])
            else:
                out["success"] = True

            out["notes"] += [
                "DevicePath updated to help Windows PnP discover staged INF packages on first boot.",
                "Value written as REG_EXPAND_SZ (UTF-16LE).",
                "Node ids normalized across python-hivex versions (0 vs None).",
                "Backup created alongside other SOFTWARE edits.",
            ]
            return out

        except Exception as e:
            out["errors"].append(f"DevicePath update failed: {e}")
            return out
        finally:
            _close_best_effort(h)


# ---------------------------------------------------------------------------
# Public: SOFTWARE hive RunOnce helper (for firstboot scripts)
# ---------------------------------------------------------------------------


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
    """
    logger = _safe_logger(self)
    dry_run = bool(getattr(self, "dry_run", False))

    out: Dict[str, Any] = {
        "success": False,
        "dry_run": dry_run,
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
        h: Optional[hivex.Hivex] = None
        try:
            _log_mountpoints_best_effort(logger, g)

            if not dry_run:
                ts = U.now_ts()
                backup_path = f"{software_hive_path}.vmdk2kvm.backup.{ts}"
                g.cp(software_hive_path, backup_path)
                out["hive_backup"] = backup_path

            _download_hive_local(logger, g, software_hive_path, local)
            orig_hash = hashlib.sha256(local.read_bytes()).hexdigest()

            h = _open_hive_local(local, write=(not dry_run))
            root = _node_id(h.root())
            if root == 0:
                out["errors"].append("Invalid hivex root()")
                return out

            microsoft = _node_id(h.node_get_child(root, "Microsoft"))
            if microsoft == 0:
                microsoft = _ensure_child(h, root, "Microsoft")

            windows = _node_id(h.node_get_child(microsoft, "Windows"))
            if windows == 0:
                windows = _ensure_child(h, microsoft, "Windows")

            cv = _node_id(h.node_get_child(windows, "CurrentVersion"))
            if cv == 0:
                cv = _ensure_child(h, windows, "CurrentVersion")

            runonce = _node_id(h.node_get_child(cv, "RunOnce"))
            if runonce == 0:
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
                try:
                    _commit_best_effort(h)
                finally:
                    _close_best_effort(h)
                    h = None

                g.upload(str(local), software_hive_path)

                with tempfile.TemporaryDirectory() as vtd:
                    vlocal = Path(vtd) / "SOFTWARE_verify"
                    _download_hive_local(logger, g, software_hive_path, vlocal)
                    new_hash = hashlib.sha256(vlocal.read_bytes()).hexdigest()

                out["success"] = (new_hash != orig_hash) or (not out["modified"])
            else:
                out["success"] = True

            logger.info("RunOnce set: %s -> %s", name, command)
            out["notes"] += [
                r"RunOnce written at HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\RunOnce",
                "Value written as REG_SZ (UTF-16LE).",
                "Node ids normalized across python-hivex versions (0 vs None).",
            ]
            return out

        except Exception as e:
            out["errors"].append(f"RunOnce update failed: {e}")
            return out
        finally:
            _close_best_effort(h)
