# vmdk2kvm/vmware/vsphere_command.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from ..core.exceptions import VMwareError
from ..core.utils import U
from .vmware_client import VMwareClient, V2VExportOptions


def _p(s: Optional[str]) -> Optional[Path]:
    if not s:
        return None
    return Path(s).expanduser()


def _merged_cfg(args: Any, conf: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge CLI + YAML config into a single dict for VMwareClient.from_config().
    CLI overrides config. We also populate vs_* aliases for compat.
    """
    cfg: Dict[str, Any] = dict(conf or {})

    vcenter = getattr(args, "vcenter", None)
    vc_user = getattr(args, "vc_user", None)
    vc_password = getattr(args, "vc_password", None)
    vc_password_env = getattr(args, "vc_password_env", None)
    vc_port = getattr(args, "vc_port", None)
    vc_insecure = getattr(args, "vc_insecure", None)
    dc_name = getattr(args, "dc_name", None)

    cfg.update(
        {
            # canonical
            "vcenter": vcenter,
            "vc_user": vc_user,
            "vc_password": vc_password,
            "vc_password_env": vc_password_env,
            "vc_port": vc_port,
            "vc_insecure": vc_insecure,
            "dc_name": dc_name,
            # aliases (some parts of your repo used vs_* historically)
            "vs_host": vcenter,
            "vs_user": vc_user,
            "vs_password": vc_password,
            "vs_password_env": vc_password_env,
            "vs_port": vc_port,
            "vs_insecure": vc_insecure,
        }
    )

    # Drop None so config can still supply defaults
    return {k: v for k, v in cfg.items() if v is not None}


def _json_enabled(args: Any) -> bool:
    return bool(getattr(args, "json", False))


def _emit(args: Any, logger: Any, payload: Any, human: Optional[str] = None) -> None:
    if _json_enabled(args):
        print(U.json_dump(payload))
    else:
        if human:
            logger.info("%s", human)
        else:
            logger.info("%s", U.json_dump(payload))


def _require(args: Any, name: str) -> Any:
    if not hasattr(args, name):
        raise VMwareError(f"Missing required arg: {name}")
    v = getattr(args, name)
    if v is None:
        raise VMwareError(f"Missing required arg: {name}")
    return v


# --------------------------------------------------------------------------------------
# Actions
# --------------------------------------------------------------------------------------

def _list_vm_names(client: VMwareClient, args: Any) -> Any:
    names = client.list_vm_names()
    _emit(args, client.logger, {"vms": names})
    if not _json_enabled(args):
        for n in names:
            client.logger.info("%s", n)
    return names


def _get_vm_by_name(client: VMwareClient, args: Any) -> Any:
    name = _require(args, "name")
    vm = client.get_vm_by_name(name)
    if vm is None:
        raise VMwareError(f"VM not found: {name!r}")

    s = getattr(vm, "summary", None)
    cfg = getattr(s, "config", None) if s else None
    runtime = getattr(s, "runtime", None) if s else None
    guest = getattr(s, "guest", None) if s else None

    out = {
        "name": getattr(vm, "name", None),
        "moId": getattr(vm, "_moId", None),
        "uuid": getattr(cfg, "uuid", None),
        "instanceUuid": getattr(cfg, "instanceUuid", None),
        "powerState": str(getattr(runtime, "powerState", None)),
        "guestFullName": getattr(guest, "guestFullName", None),
        "vmPathName": getattr(cfg, "vmPathName", None),
        "datacenter": client.vm_datacenter_name(vm),
        "esx_host": getattr(getattr(getattr(vm, "runtime", None), "host", None), "name", None),
    }
    _emit(args, client.logger, out)
    return out


def _vm_disks(client: VMwareClient, args: Any) -> Any:
    vm_name = _require(args, "vm_name")
    vm = client.get_vm_by_name(vm_name)
    if vm is None:
        raise VMwareError(f"VM not found: {vm_name!r}")

    disks = client.vm_disks(vm)
    out = []
    for i, d in enumerate(disks):
        label = getattr(getattr(d, "deviceInfo", None), "label", None)
        key = getattr(d, "key", None)
        cap = getattr(d, "capacityInKB", None)
        backing = getattr(d, "backing", None)
        fname = getattr(backing, "fileName", None) if backing else None
        out.append(
            {
                "index": i,
                "label": str(label) if label else None,
                "device_key": int(key) if key is not None else None,
                "capacity_kb": int(cap) if cap is not None else None,
                "backing_file": str(fname) if fname else None,
            }
        )

    _emit(args, client.logger, {"vm": vm_name, "disks": out})
    return out


def _select_disk(client: VMwareClient, args: Any) -> Any:
    vm_name = _require(args, "vm_name")
    label_or_index = getattr(args, "label_or_index", None)
    vm = client.get_vm_by_name(vm_name)
    if vm is None:
        raise VMwareError(f"VM not found: {vm_name!r}")

    d = client.select_disk(vm, label_or_index)
    label = getattr(getattr(d, "deviceInfo", None), "label", None)
    key = getattr(d, "key", None)
    backing = getattr(d, "backing", None)
    fname = getattr(backing, "fileName", None) if backing else None

    out = {
        "vm": vm_name,
        "selector": label_or_index,
        "label": str(label) if label else None,
        "device_key": int(key) if key is not None else None,
        "backing_file": str(fname) if fname else None,
    }
    _emit(args, client.logger, out)
    return out


def _download_datastore_file(client: VMwareClient, args: Any) -> Any:
    datastore = _require(args, "datastore")
    ds_path = _require(args, "ds_path")
    local_path = Path(_require(args, "local_path")).expanduser()
    chunk_size = int(getattr(args, "chunk_size", 1024 * 1024) or 1024 * 1024)
    dc_name = getattr(args, "dc_name", None)

    client.download_datastore_file(
        datastore=datastore,
        ds_path=ds_path,
        local_path=local_path,
        dc_name=dc_name,
        chunk_size=chunk_size,
    )
    out = {"ok": True, "local_path": str(local_path)}
    _emit(args, client.logger, out)
    return out


def _create_snapshot(client: VMwareClient, args: Any) -> Any:
    vm_name = _require(args, "vm_name")
    snap_name = _require(args, "name")
    quiesce = bool(getattr(args, "quiesce", True))
    memory = bool(getattr(args, "memory", False))
    description = getattr(args, "description", "Created by vmdk2kvm") or "Created by vmdk2kvm"

    vm = client.get_vm_by_name(vm_name)
    if vm is None:
        raise VMwareError(f"VM not found: {vm_name!r}")

    snap = client.create_snapshot(vm, snap_name, quiesce=quiesce, memory=memory, description=description)
    out = {
        "ok": True,
        "vm": vm_name,
        "snapshot_name": snap_name,
        "snapshot_moref": client.snapshot_moref(snap),
    }
    _emit(args, client.logger, out)
    return out


def _enable_cbt(client: VMwareClient, args: Any) -> Any:
    vm_name = _require(args, "vm_name")
    vm = client.get_vm_by_name(vm_name)
    if vm is None:
        raise VMwareError(f"VM not found: {vm_name!r}")
    client.enable_cbt(vm)
    out = {"ok": True, "vm": vm_name, "cbt_enabled": True}
    _emit(args, client.logger, out)
    return out


def _query_changed_disk_areas(client: VMwareClient, args: Any) -> Any:
    vm_name = _require(args, "vm_name")
    snapshot_name = _require(args, "snapshot_name")
    start_offset = int(getattr(args, "start_offset", 0) or 0)
    change_id = str(getattr(args, "change_id", "*") or "*")

    device_key = getattr(args, "device_key", None)
    disk_sel = getattr(args, "disk", None)

    vm = client.get_vm_by_name(vm_name)
    if vm is None:
        raise VMwareError(f"VM not found: {vm_name!r}")

    snap_tree = _find_snapshot_by_name(vm, snapshot_name)
    if snap_tree is None:
        raise VMwareError(f"Snapshot not found by name: {snapshot_name!r}")

    if device_key is None:
        d = client.select_disk(vm, disk_sel)
        device_key = int(getattr(d, "key", 0) or 0)
        if not device_key:
            raise VMwareError("Could not resolve device_key from selected disk")

    r = client.query_changed_disk_areas(
        vm,
        snapshot=snap_tree.snapshot,
        device_key=int(device_key),
        start_offset=start_offset,
        change_id=change_id,
    )

    out = {
        "vm": vm_name,
        "snapshot": snapshot_name,
        "device_key": int(device_key),
        "start_offset": start_offset,
        "change_id": change_id,
        "changedArea_count": len(getattr(r, "changedArea", []) or []),
        "length": int(getattr(r, "length", 0) or 0),
    }
    _emit(args, client.logger, out)
    return out


def _download_vm_disk(client: VMwareClient, args: Any) -> Any:
    vm_name = _require(args, "vm_name")
    disk_sel = getattr(args, "disk", None)
    local_path = Path(_require(args, "local_path")).expanduser()
    chunk_size = int(getattr(args, "chunk_size", 1024 * 1024) or 1024 * 1024)

    vm = client.get_vm_by_name(vm_name)
    if vm is None:
        raise VMwareError(f"VM not found: {vm_name!r}")

    d = client.select_disk(vm, disk_sel)
    backing = client._vm_disk_backing_filename(d)  # usually "[datastore] folder/file.vmdk"
    ds_name, rel_path = client.parse_backing_filename(backing)

    dc = client.resolve_datacenter_for_vm(vm_name, getattr(args, "dc_name", None))

    client.download_datastore_file(
        datastore=ds_name,
        ds_path=rel_path,
        local_path=local_path,
        dc_name=dc,
        chunk_size=chunk_size,
    )

    out = {"ok": True, "vm": vm_name, "disk": disk_sel, "remote": backing, "local_path": str(local_path)}
    _emit(args, client.logger, out)
    return out


def _cbt_sync(client: VMwareClient, args: Any) -> Any:
    """
    Scaffold: enable CBT + snapshot + one-shot QueryChangedDiskAreas summary.
    (Real delta patching requires VDDK/NBD reads + applying extents into the base image.)
    """
    vm_name = _require(args, "vm_name")
    disk_sel = getattr(args, "disk", None)
    local_path = Path(_require(args, "local_path")).expanduser()
    enable = bool(getattr(args, "enable_cbt", False))
    snapshot_name = getattr(args, "snapshot_name", "vmdk2kvm-cbt") or "vmdk2kvm-cbt"
    change_id = str(getattr(args, "change_id", "*") or "*")

    vm = client.get_vm_by_name(vm_name)
    if vm is None:
        raise VMwareError(f"VM not found: {vm_name!r}")

    if enable:
        client.enable_cbt(vm)

    snap = client.create_snapshot(vm, snapshot_name, quiesce=True, memory=False)
    d = client.select_disk(vm, disk_sel)
    device_key = int(getattr(d, "key", 0) or 0)
    if not device_key:
        raise VMwareError("Could not resolve device_key for selected disk")

    # base pull (non-CBT-aware) so you at least have a consistent local artifact
    _download_vm_disk(
        client,
        _ArgsShim(
            vm_name=vm_name,
            disk=disk_sel,
            local_path=str(local_path),
            chunk_size=1024 * 1024,
            dc_name=getattr(args, "dc_name", None),
            json=getattr(args, "json", False),
        ),
    )

    r = client.query_changed_disk_areas(
        vm,
        snapshot=snap,  # your vmware_client may accept VirtualMachineSnapshot or tree; adjust there if needed
        device_key=device_key,
        start_offset=0,
        change_id=change_id,
    )

    out = {
        "ok": True,
        "vm": vm_name,
        "disk": disk_sel,
        "snapshot_moref": client.snapshot_moref(snap),
        "device_key": device_key,
        "change_id": change_id,
        "changedArea_count": len(getattr(r, "changedArea", []) or []),
    }
    _emit(args, client.logger, out)
    return out


def _download_only_vm(client: VMwareClient, args: Any) -> Any:
    """
    VM folder pull via /folder (no virt-v2v, no inspection).
    """
    vm_name = _require(args, "vm_name")
    out_dir = getattr(args, "output_dir", None) or getattr(args, "output_dir", None) or getattr(args, "output_dir", "./out")
    out_dir_path = Path(out_dir).expanduser()

    include_globs = tuple(getattr(args, "vs_include_glob", None) or []) or ("*",)
    exclude_globs = tuple(getattr(args, "vs_exclude_glob", None) or []) or ()

    opt = V2VExportOptions(
        vm_name=vm_name,
        export_mode="download_only",
        output_dir=out_dir_path,
        datacenter=getattr(args, "dc_name", "auto") or "auto",
        download_only_include_globs=include_globs,
        download_only_exclude_globs=exclude_globs,
        download_only_concurrency=int(getattr(args, "vs_concurrency", 4) or 4),
        download_only_max_files=int(getattr(args, "vs_max_files", 5000) or 5000),
        download_only_use_async_http=bool(getattr(args, "vs_use_async_http", True)),
        download_only_fail_on_missing=bool(getattr(args, "vs_fail_on_missing", False)),
    )

    res = client.export_vm(opt)
    out = {"ok": True, "vm": vm_name, "output_dir": str(res)}
    _emit(args, client.logger, out)
    return out


def _vddk_download_disk(client: VMwareClient, args: Any) -> Any:
    """
    ✅ Fixes: 'unknown action: vddk_download_disk'
    Routes to VMwareClient.export_vm(export_mode="vddk_download") which should call your vddk_client.
    """
    vm_name = _require(args, "vm_name")
    disk_sel = getattr(args, "disk", None)
    local_path = Path(_require(args, "local_path")).expanduser()

    opt = V2VExportOptions(
        vm_name=vm_name,
        export_mode="vddk_download",
        output_dir=local_path.parent,
        vddk_download_disk=disk_sel,
        vddk_download_output=local_path,
        vddk_libdir=_p(getattr(args, "vs_vddk_libdir2", None)),
        vddk_thumbprint=getattr(args, "vs_vddk_thumbprint2", None),
        vddk_transports=getattr(args, "vs_vddk_transports2", None),
        no_verify=bool(getattr(args, "vs_no_verify2", False)),
    )

    res = client.export_vm(opt)
    out = {"ok": True, "vm": vm_name, "disk": disk_sel, "local_path": str(res)}
    _emit(args, client.logger, out)
    return out


# --------------------------------------------------------------------------------------
# Snapshot lookup (name -> SnapshotTree node)
# --------------------------------------------------------------------------------------

def _find_snapshot_by_name(vm_obj: Any, name: str) -> Optional[Any]:
    target = (name or "").strip()
    if not target:
        return None

    snap = getattr(vm_obj, "snapshot", None)
    roots = getattr(snap, "rootSnapshotList", None) if snap else None
    if not roots:
        return None

    stack = list(roots)
    while stack:
        node = stack.pop()
        if str(getattr(node, "name", "") or "") == target:
            return node
        kids = getattr(node, "childSnapshotList", None) or []
        stack.extend(list(kids))
    return None


class _ArgsShim:
    """Tiny shim so we can reuse action funcs without argparse objects."""
    def __init__(self, **kw: Any):
        for k, v in kw.items():
            setattr(self, k, v)


# --------------------------------------------------------------------------------------
# Router
# --------------------------------------------------------------------------------------

_ACTIONS: Dict[str, Callable[[VMwareClient, Any], Any]] = {
    "list_vm_names": _list_vm_names,
    "get_vm_by_name": _get_vm_by_name,
    "vm_disks": _vm_disks,
    "select_disk": _select_disk,
    "download_datastore_file": _download_datastore_file,
    "create_snapshot": _create_snapshot,
    "enable_cbt": _enable_cbt,
    "query_changed_disk_areas": _query_changed_disk_areas,
    "download_vm_disk": _download_vm_disk,
    "cbt_sync": _cbt_sync,
    "download_only_vm": _download_only_vm,
    # ✅ the one you needed
    "vddk_download_disk": _vddk_download_disk,
}


def run_vsphere_command(args: Any, conf: Optional[Dict[str, Any]], logger: Any) -> int:
    """
    Entry point for:  vmdk2kvm.py vsphere <action> ...
    Your orchestrator/vsphere_mode should call this.

    Expects:
      - args.vs_action is set (argparse nested subparser)
      - args has vcenter/vc_user/creds, etc.
      - conf is merged config dict (may be empty)
      - logger is your rich logger
    """
    action = getattr(args, "vs_action", None)
    if not action:
        raise VMwareError("Missing vs_action (argparse should have required=True)")

    if action not in _ACTIONS:
        raise VMwareError(f"vsphere: unknown action: {action}")

    cfg = _merged_cfg(args, conf)

    client = VMwareClient.from_config(
        logger=logger,
        cfg=cfg,
        port=getattr(args, "vc_port", None),
        insecure=getattr(args, "vc_insecure", None),
        timeout=None,
    )

    with client:
        _ACTIONS[action](client, args)

    return 0
