# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import fnmatch
import json
import logging
import os
import sys
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from pyVmomi import vim, vmodl

# Optional: Rich progress UI (TTY friendly). Falls back to plain logs if Rich not available.
try:  # pragma: no cover
    from rich.progress import (
        Progress,
        SpinnerColumn,
        BarColumn,
        TextColumn,
        TimeElapsedColumn,
        TransferSpeedColumn,
    )
except Exception:  # pragma: no cover
    Progress = None  # type: ignore
    SpinnerColumn = BarColumn = TextColumn = TimeElapsedColumn = TransferSpeedColumn = None  # type: ignore

# Optional: silence urllib3 TLS warnings when verify=False
try:  # pragma: no cover
    import urllib3  # type: ignore
except Exception:  # pragma: no cover
    urllib3 = None  # type: ignore


from ..core.exceptions import Fatal, VMwareError
from .vmware_client import REQUESTS_AVAILABLE, VMwareClient
from .govc_common import GovcRunner, extract_paths_from_datastore_ls_json, normalize_ds_path


class GovmomiCLI(GovcRunner):
    """
    Thin wrapper around govmomi tooling via `govc` (recommended).

    This is intentionally best-effort + additive: if govc isn't present or not configured,
    callers should fall back to pyvmomi.

    Compared to the older in-file implementation, this version:
      - Centralizes GOVC_* env seeding + JSON parsing in govc_common.py
      - Supports newer govc JSON output shapes for datastore.ls (e.g. the `file:[{path:...}]` form)
    """

    def __init__(self, logger: logging.Logger, args: argparse.Namespace):
        super().__init__(logger=logger, args=args)

    # -------------------------
    # govc helpers we use
    # -------------------------

    def list_vm_names(self) -> List[Dict[str, Any]]:
        """
        Returns list of VM dicts.

        Uses:
          - govc find -type m -json .
          - govc vm.info -json per vm (bounded by govc_max_detail)

        If inventory is too large, returns only names + inventory paths.
        """
        found = self.run_json(["find", "-type", "m", "-json", "."]) or {}
        vms = (found.get("Elements") or [])
        if not isinstance(vms, list):
            vms = []

        max_detail = int(getattr(self.args, "govc_max_detail", 500) or 500)
        if len(vms) > max_detail:
            try:
                self.logger.info(
                    f"govc: inventory has {len(vms)} VMs; returning names only (govc_max_detail={max_detail})"
                )
            except Exception:
                pass
            out = [{"name": str(p).split("/")[-1], "path": p} for p in vms]
            return sorted(out, key=lambda x: x.get("name", ""))

        detailed: List[Dict[str, Any]] = []
        for pth in vms:
            try:
                info = self.run_json(["vm.info", "-json", str(pth)]) or {}
                arr = info.get("VirtualMachines") or []
                if not arr:
                    continue
                vm = arr[0]
                cfg = (vm.get("Config") or {})
                runtime = (vm.get("Runtime") or {})
                guest = (vm.get("Guest") or {})
                summary = (vm.get("Summary") or {})
                detailed.append(
                    {
                        "name": cfg.get("Name") or str(pth).split("/")[-1],
                        "runtime.powerState": runtime.get("PowerState"),
                        "summary.overallStatus": (summary.get("OverallStatus") or ""),
                        "summary.guest.guestFullName": (cfg.get("GuestFullName") or ""),
                        "summary.config.memorySizeMB": cfg.get("MemoryMB"),
                        "summary.config.numCpu": cfg.get("NumCPU"),
                        "summary.config.vmPathName": (cfg.get("VmPathName") or ""),
                        "summary.config.instanceUuid": cfg.get("InstanceUuid"),
                        "summary.config.uuid": cfg.get("Uuid"),
                        "guest.guestState": guest.get("GuestState"),
                        "path": pth,
                    }
                )
            except Exception as e:
                try:
                    self.logger.debug(f"govc: vm.info failed for {pth}: {e}")
                except Exception:
                    pass
                detailed.append({"name": str(pth).split("/")[-1], "path": pth, "error": str(e)})

        return sorted(detailed, key=lambda x: x.get("name", ""))

    def datastore_ls(self, datastore: str, folder: str) -> List[str]:
        """
        List files under a datastore folder via govc.

        Returns:
          Filenames/relative paths under `folder` (no leading slash).

        Notes:
          - We call `govc datastore.ls -json -ds <ds> <folder/>` and then parse defensively.
          - govc output shapes vary by version (some return `file:[{path:...}]`).
        """
        ds, rel = normalize_ds_path(datastore, folder or "")
        rel = rel.strip().lstrip("/")  # govc wants no leading slash
        rel = rel.rstrip("/")  # we'll add slash for directory
        rel_dir = (rel + "/") if rel else ""

        candidates: List[str]
        if rel_dir:
            candidates = [rel_dir, "/" + rel_dir]
        else:
            candidates = ["", "/"]

        base = rel_dir.lstrip("/")  # used to strip prefixes when govc returns full paths
        prefix = base.rstrip("/") + "/" if base else ""

        for cand in candidates:
            try:
                data = self.run_json(["datastore.ls", "-json", "-ds", ds, cand]) or {}
                paths = extract_paths_from_datastore_ls_json(data)

                out: List[str] = []
                for p in paths:
                    relp = str(p).lstrip("/")
                    if prefix and relp.startswith(prefix):
                        relp = relp[len(prefix):]
                    if relp:
                        out.append(relp)
                return out
            except Exception as e:
                try:
                    self.logger.debug(f"govc datastore.ls failed for candidate '{cand}': {e}")
                except Exception:
                    pass
                continue

        return []
class VsphereMode:
    """CLI entry for vSphere actions: scan / download / cbt-sync."""

    def __init__(self, logger: logging.Logger, args: argparse.Namespace):
        self.logger = logger
        self.args = args
        self.govc = GovmomiCLI(logger, args)

    # ✅ ADDITIVE: centralized dc_name resolution (no behavior change)
    def _dc_name(self) -> str:
        """
        Resolve datacenter name safely.

        Behavior (unchanged):
          - If user supplied --dc-name, use it
          - Else default to 'ha-datacenter'
        """
        v = getattr(self.args, "dc_name", None)
        return v if v else "ha-datacenter"

    def _prefer_govmomi(self) -> bool:
        """
        If govc/govmomi is available and user didn't disable it, prefer it for:
          - list_vm_names (inventory traversal is often better)
          - datastore listing (for download-only flows)
        """
        if bool(getattr(self.args, "no_govmomi", False)):
            return False
        return self.govc.available()

    # ----------------------------------------------------------------------------------
    # Download-only VM folder helpers
    # ----------------------------------------------------------------------------------

    def _parse_vm_datastore_dir(self, vmx_path: str) -> Tuple[str, str]:
        """
        vm.summary.config.vmPathName looks like:
          "[datastore1] folder/vm.vmx"
        Return: (datastore_name, folder_path)
        """
        s = (vmx_path or "").strip()
        if not s.startswith("[") or "]" not in s:
            raise VMwareError(f"Unexpected vmPathName format: {vmx_path}")
        ds = s[1 : s.index("]")]
        rest = s[s.index("]") + 1 :].strip()  # "folder/vm.vmx"
        if "/" not in rest:
            folder = ""
        else:
            folder = rest.rsplit("/", 1)[0].lstrip("/")
        return ds, folder

    def _parse_datastore_dir_override(self, s: str, *, default_ds: Optional[str] = None) -> Tuple[str, str]:
        """
        Override parser for download_only_vm folder listing.

        Accepts:
          - "folder/subfolder/"                (uses default_ds)
          - "[ds] folder/subfolder/"           (explicit ds)
          - "[ds] folder/subfolder/vm.vmx"     (explicit ds + file; dirname used)

        Returns: (ds_name, folder)
        """
        t = (s or "").strip()
        if not t:
            raise VMwareError("Empty vs_datastore_dir override")

        if t.startswith("[") and "]" in t:
            ds = t[1 : t.index("]")]
            rest = t[t.index("]") + 1 :].strip()
            rest = rest.lstrip("/")
            if "/" in rest:
                folder = rest.rsplit("/", 1)[0]
            else:
                folder = ""
            return ds, folder.strip("/")

        if not default_ds:
            raise VMwareError("vs_datastore_dir provided without datastore and default datastore is unknown")

        folder = t.strip().lstrip("/").rstrip("/")
        # If user passed a file-like tail, take dirname
        if "/" in folder and "." in folder.split("/")[-1]:
            folder = folder.rsplit("/", 1)[0]
        return str(default_ds), folder.strip("/")

    def _find_datastore_obj(self, client: VMwareClient, datastore_name: str) -> vim.Datastore:
        """
        Find a vim.Datastore object by name using inventory.
        Best-effort across folders/datacenters.
        """
        content = client._content()

        def iter_children(obj):
            try:
                return list(getattr(obj, "childEntity", []) or [])
            except Exception:
                return []

        for top in iter_children(content.rootFolder):
            try:
                if isinstance(top, vim.Datacenter):
                    for ds in (top.datastore or []):
                        if ds.name == datastore_name:
                            return ds
                elif isinstance(top, vim.Folder):
                    for child in iter_children(top):
                        if isinstance(child, vim.Datacenter):
                            for ds in (child.datastore or []):
                                if ds.name == datastore_name:
                                    return ds
            except Exception:
                continue

        raise VMwareError(f"Datastore not found in inventory: {datastore_name}")

    def _list_vm_folder_files_pyvmomi(
        self,
        client: VMwareClient,
        datastore_obj: vim.Datastore,
        ds_name: str,
        folder: str,
        include_glob: List[str],
        exclude_glob: List[str],
        max_files: int,
    ) -> List[str]:
        """
        Use HostDatastoreBrowser to list files in the VM folder.
        Returns list of datastore-relative paths like: "folder/file.vmdk"
        """
        browser = datastore_obj.browser  # vim.HostDatastoreBrowser
        ds_folder_path = f"[{ds_name}] {folder}" if folder else f"[{ds_name}]"

        spec = vim.HostDatastoreBrowserSearchSpec()
        spec.details = vim.FileQueryFlags(fileOwner=True, fileSize=True, fileType=True, modification=True)
        spec.sortFoldersFirst = True

        task = browser.SearchDatastore_Task(datastorePath=ds_folder_path, searchSpec=spec)
        client.wait_for_task(task)

        result = getattr(task.info, "result", None)
        if not result:
            return []

        files: List[str] = []
        base = folder.rstrip("/")

        for f in getattr(result, "file", []) or []:
            name = getattr(f, "path", None)
            if not name:
                continue
            rel = f"{base}/{name}" if base else name

            if include_glob and not any(fnmatch.fnmatch(rel, pat) or fnmatch.fnmatch(name, pat) for pat in include_glob):
                continue
            if exclude_glob and any(fnmatch.fnmatch(rel, pat) or fnmatch.fnmatch(name, pat) for pat in exclude_glob):
                continue

            files.append(rel)

            if max_files and len(files) > max_files:
                raise VMwareError(f"Refusing to download > max_files={max_files} (found so far: {len(files)})")

        return files

    def _list_vm_folder_files(
        self,
        client: VMwareClient,
        datastore_obj: vim.Datastore,
        ds_name: str,
        folder: str,
        include_glob: List[str],
        exclude_glob: List[str],
        max_files: int,
    ) -> List[str]:
        """
        Prefer govmomi/govc for datastore listing when available, else fall back to pyvmomi.
        """
        if self._prefer_govmomi():
            try:
                rels = self.govc.datastore_ls(ds_name, folder)
                files: List[str] = []
                base = folder.rstrip("/")
                for name in rels:
                    rel = f"{base}/{name}" if base and name else (base or name)
                    if not rel:
                        continue
                    bn = rel.split("/")[-1]
                    if include_glob and not any(fnmatch.fnmatch(rel, pat) or fnmatch.fnmatch(bn, pat) for pat in include_glob):
                        continue
                    if exclude_glob and any(fnmatch.fnmatch(rel, pat) or fnmatch.fnmatch(bn, pat) for pat in exclude_glob):
                        continue
                    files.append(rel)
                    if max_files and len(files) > max_files:
                        raise VMwareError(f"Refusing to download > max_files={max_files} (found so far: {len(files)})")
                return files
            except Exception as e:
                self.logger.debug(f"govc datastore listing failed; falling back to pyvmomi: {e}")

        return self._list_vm_folder_files_pyvmomi(
            client=client,
            datastore_obj=datastore_obj,
            ds_name=ds_name,
            folder=folder,
            include_glob=include_glob,
            exclude_glob=exclude_glob,
            max_files=max_files,
        )

    def _download_one_folder_file(
        self,
        client: VMwareClient,
        vc_host: str,
        dc_name: str,
        ds_name: str,
        ds_path: str,
        local_path: Path,
        verify_tls: bool,
        *,
        on_bytes: Optional[Any] = None,
        chunk_size: int = 1024 * 1024,
    ) -> None:
        """
        Download a single datastore file via /folder endpoint using the session cookie from VMwareClient.
        """
        if not REQUESTS_AVAILABLE:
            raise VMwareError("requests not installed. Install: pip install requests")
        quoted_path = quote(ds_path, safe="/")
        url = f"https://{vc_host}/folder/{quoted_path}?dcPath={quote(dc_name)}&dsName={quote(ds_name)}"
        headers = {"Cookie": client._session_cookie()}

        # Silence urllib3 warnings when verify is disabled (common for lab vCenters)
        if not verify_tls and urllib3 is not None:  # pragma: no cover
            try:
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # type: ignore[attr-defined]
            except Exception:
                pass

        local_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = local_path.with_suffix(local_path.suffix + ".part")

        with requests.get(url, headers=headers, verify=verify_tls, stream=True) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", "0") or "0")
            got = 0

            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    f.write(chunk)
                    got += len(chunk)
                    if on_bytes is not None:
                        try:
                            on_bytes(len(chunk), total)
                        except Exception:
                            # progress must never break downloads
                            pass

        os.replace(tmp, local_path)

    # ----------------------------------------------------------------------------------

    def run(self) -> int:
        vc_host = self.args.vcenter
        vc_user = self.args.vc_user
        vc_pass = self.args.vc_password

        if not vc_pass and getattr(self.args, "vc_password_env", None):
            vc_pass = os.environ.get(self.args.vc_password_env)

        if isinstance(vc_pass, str):
            vc_pass = vc_pass.strip()
        if not vc_pass:
            vc_pass = None

        if not vc_host or not vc_user or not vc_pass:
            raise Fatal(2, "vsphere: --vcenter, --vc-user, and --vc-password (or --vc-password-env) are required")

        client = VMwareClient(
            self.logger,
            vc_host,
            vc_user,
            vc_pass,
            port=self.args.vc_port,
            insecure=self.args.vc_insecure,
        )
        try:
            client.connect()
        except VMwareError as e:
            raise Fatal(2, f"vsphere: Connection failed: {e}")

        try:
            action = self.args.vs_action

            # ------------------------------------------------------------------
            # list_vm_names: prefer govmomi/govc when present (more robust inventory)
            # ------------------------------------------------------------------
            if action == "list_vm_names":
                if self._prefer_govmomi():
                    try:
                        vms = self.govc.list_vm_names()
                        self.logger.info(f"VMs found (govc): {len(vms)}")
                        if self.args.json:
                            print(json.dumps(vms, indent=2, default=str))
                        else:
                            for vm in vms:
                                print(vm.get("name", "Unnamed VM"))
                        return 0
                    except Exception as e:
                        self.logger.warning(f"govc list_vm_names failed; falling back to pyvmomi: {e}")

                # ---- pyvmomi fallback (your original behavior)
                try:
                    content = client._content()
                    container = content.rootFolder
                    viewType = [vim.VirtualMachine]
                    recursive = True
                    containerView = content.viewManager.CreateContainerView(container, viewType, recursive)
                    try:
                        traversal = vmodl.query.PropertyCollector.TraversalSpec(
                            name="traverseEntities",
                            type=vim.view.ContainerView,
                            path="view",
                            skip=False,
                        )
                        obj_spec = vmodl.query.PropertyCollector.ObjectSpec(
                            obj=containerView,
                            skip=True,
                            selectSet=[traversal],
                        )
                        property_spec = vmodl.query.PropertyCollector.PropertySpec(
                            type=vim.VirtualMachine,
                            all=False,
                            pathSet=[
                                "name",
                                "runtime.powerState",
                                "summary.overallStatus",
                                "summary.guest.guestFullName",
                                "summary.config.memorySizeMB",
                                "summary.config.numCpu",
                                "summary.config.vmPathName",
                                "summary.config.instanceUuid",
                                "summary.config.uuid",
                                "guest.guestState",
                            ],
                        )
                        filter_spec = vmodl.query.PropertyCollector.FilterSpec(
                            propSet=[property_spec],
                            objectSet=[obj_spec],
                        )
                        props = content.propertyCollector.RetrieveContents([filter_spec])
                        vms = []
                        for obj in props:
                            properties = {}
                            for prop in obj.propSet:
                                properties[prop.name] = prop.val
                            properties["moId"] = obj.obj._moId
                            vms.append(properties)
                        vms = sorted(vms, key=lambda x: x.get("name", ""))
                        self.logger.info(f"VMs found: {len(vms)}")
                        if self.args.json:
                            print(json.dumps(vms, indent=2, default=str))
                        else:
                            for vm in vms:
                                print(vm.get("name", "Unnamed VM"))
                    finally:
                        containerView.Destroy()
                except Exception as e:
                    raise Fatal(2, f"vsphere list_vm_names: Failed to retrieve VM list: {e}")
                return 0

            if action == "get_vm_by_name":
                if not self.args.name:
                    raise Fatal(2, "vsphere get_vm_by_name: --name is required")
                vm = client.get_vm_by_name(self.args.name)
                if not vm:
                    raise Fatal(2, f"vsphere: VM not found: {self.args.name}")
                output = {
                    "name": vm.name,
                    "moId": vm._moId,
                    "powerState": vm.runtime.powerState,
                    "overallStatus": str(vm.summary.overallStatus),
                    "guestOS": vm.summary.config.guestFullName,
                    "memoryMB": vm.summary.config.memorySizeMB,
                    "numCpu": vm.summary.config.numCpu,
                    "path": vm.summary.config.vmPathName,
                    "instance_uuid": vm.summary.config.instanceUuid,
                    "bios_uuid": vm.summary.config.uuid,
                    "guestState": vm.guest.guestState,
                    "summary": str(vm.summary),
                    "hardwareVersion": vm.config.version,
                    "numDisks": len(client.vm_disks(vm)),
                }
                if self.args.json:
                    print(json.dumps(output, indent=2, default=str))
                else:
                    print(f"VM: {vm.name}")
                    print(f"Summary: {vm.summary}")
                return 0

            if action == "vm_disks":
                if not self.args.vm_name:
                    raise Fatal(2, "vsphere vm_disks: --vm-name is required")
                vm = client.get_vm_by_name(self.args.vm_name)
                if not vm:
                    raise Fatal(2, f"vsphere: VM not found: {self.args.vm_name}")
                try:
                    disks = client.vm_disks(vm)
                except Exception as e:
                    raise Fatal(2, f"vsphere vm_disks: Failed to retrieve disks: {e}")
                disk_list = []
                for idx, disk in enumerate(disks):
                    backing = disk.backing
                    disk_info = {
                        "index": idx,
                        "label": disk.deviceInfo.label if hasattr(disk, "deviceInfo") else "disk",
                        "key": disk.key,
                        "capacity_gb": (
                            disk.capacityInBytes / (1024**3)
                            if hasattr(disk, "capacityInBytes")
                            else disk.capacityInKB / 1024 / 1024
                        ),
                        "backing_file": backing.fileName if hasattr(backing, "fileName") else None,
                        "mode": backing.mode if hasattr(backing, "mode") else None,
                        "thinProvisioned": backing.thinProvisioned if hasattr(backing, "thinProvisioned") else None,
                        "diskType": type(backing).__name__,
                        "controllerKey": disk.controllerKey,
                        "unitNumber": disk.unitNumber,
                    }
                    disk_list.append(disk_info)
                if self.args.json:
                    print(json.dumps(disk_list, indent=2, default=str))
                else:
                    for disk_info in disk_list:
                        print(f"Disk {disk_info['index']}: {disk_info['label']}")
                        print(f"  Key: {disk_info['key']}")
                        print(f"  Capacity: {disk_info['capacity_gb']:.2f} GB")
                        print(f"  Backing: {disk_info['backing_file']}")
                return 0

            if action == "select_disk":
                if not self.args.vm_name:
                    raise Fatal(2, "vsphere select_disk: --vm-name is required")
                vm = client.get_vm_by_name(self.args.vm_name)
                if not vm:
                    raise Fatal(2, f"vsphere: VM not found: {self.args.vm_name}")
                try:
                    disk = client.select_disk(vm, self.args.label_or_index)
                except VMwareError as e:
                    raise Fatal(2, f"vsphere select_disk: {e}")
                backing = disk.backing
                output = {
                    "label": disk.deviceInfo.label if hasattr(disk, "deviceInfo") else "disk",
                    "key": disk.key,
                    "capacity_gb": (
                        disk.capacityInBytes / (1024**3)
                        if hasattr(disk, "capacityInBytes")
                        else disk.capacityInKB / 1024 / 1024
                    ),
                    "backing_file": backing.fileName if hasattr(backing, "fileName") else None,
                    "mode": backing.mode if hasattr(backing, "mode") else None,
                    "thinProvisioned": backing.thinProvisioned if hasattr(backing, "thinProvisioned") else None,
                    "diskType": type(backing).__name__,
                    "controllerKey": disk.controllerKey,
                    "unitNumber": disk.unitNumber,
                }
                if self.args.json:
                    print(json.dumps(output, indent=2, default=str))
                else:
                    print(f"Selected Disk: {output['label']}")
                    print(f"  Key: {output['key']}")
                    print(f"  Capacity: {output['capacity_gb']:.2f} GB")
                    print(f"  Backing: {output['backing_file']}")
                return 0

            if action == "download_datastore_file":
                if not all([self.args.datastore, self.args.ds_path, self.args.local_path]):
                    raise Fatal(2, "vsphere download_datastore_file: --datastore, --ds-path, --local-path are required")
                local_path = Path(self.args.local_path).resolve()
                dc_name = self._dc_name()
                chunk_size = int(getattr(self.args, "chunk_size", 1024 * 1024))
                try:
                    client.download_datastore_file(
                        datastore=self.args.datastore,
                        ds_path=self.args.ds_path,
                        local_path=local_path,
                        dc_name=dc_name,
                        chunk_size=chunk_size,
                    )
                except VMwareError as e:
                    raise Fatal(2, f"vsphere download_datastore_file: {e}")
                output = {
                    "status": "success",
                    "local_path": str(local_path),
                    "datastore": self.args.datastore,
                    "ds_path": self.args.ds_path,
                    "dc_name": dc_name,
                }
                if self.args.json:
                    print(json.dumps(output, indent=2))
                else:
                    print(f"Downloaded [{self.args.datastore}] {self.args.ds_path} to {local_path}")
                return 0

            if action == "create_snapshot":
                if not all([self.args.vm_name, self.args.name]):
                    raise Fatal(2, "vsphere create_snapshot: --vm-name, --name are required")
                vm = client.get_vm_by_name(self.args.vm_name)
                if not vm:
                    raise Fatal(2, f"vsphere: VM not found: {self.args.vm_name}")
                quiesce = self.args.quiesce
                memory = self.args.memory
                description = self.args.description
                try:
                    snap = client.create_snapshot(vm, self.args.name, quiesce=quiesce, memory=memory, description=description)
                except VMwareError as e:
                    raise Fatal(2, f"vsphere create_snapshot: {e}")
                output = {
                    "name": snap.name,
                    "description": snap.description,
                    "createTime": str(snap.createTime),
                    "id": snap.id,
                    "state": snap.state,
                    "quiesced": snap.quiesced,
                    "vm_name": self.args.vm_name,
                }
                if self.args.json:
                    print(json.dumps(output, indent=2, default=str))
                else:
                    print(f"Snapshot created: {snap.name}")
                return 0

            if action == "enable_cbt":
                if not self.args.vm_name:
                    raise Fatal(2, "vsphere enable_cbt: --vm-name is required")
                vm = client.get_vm_by_name(self.args.vm_name)
                if not vm:
                    raise Fatal(2, f"vsphere: VM not found: {self.args.vm_name}")
                was_enabled = vm.config.changeTrackingEnabled if vm.config else False
                try:
                    client.enable_cbt(vm)
                except VMwareError as e:
                    raise Fatal(2, f"vsphere enable_cbt: {e}")
                now_enabled = vm.config.changeTrackingEnabled if vm.config else False
                output = {
                    "vm_name": self.args.vm_name,
                    "was_enabled": was_enabled,
                    "now_enabled": now_enabled,
                }
                if self.args.json:
                    print(json.dumps(output, indent=2, default=str))
                else:
                    if now_enabled:
                        print("CBT enabled on VM" if not was_enabled else "CBT was already enabled on VM")
                return 0

            if action == "query_changed_disk_areas":
                if not all([self.args.vm_name, self.args.snapshot_name]):
                    raise Fatal(2, "vsphere query_changed_disk_areas: --vm-name, --snapshot-name are required")
                vm = client.get_vm_by_name(self.args.vm_name)
                if not vm:
                    raise Fatal(2, f"vsphere: VM not found: {self.args.vm_name}")

                snapshots = []
                if vm.snapshot:

                    def traverse(snap_list):
                        for snap in snap_list:
                            snapshots.append(snap)
                            traverse(snap.childSnapshotList)

                    traverse(vm.snapshot.rootSnapshotList)

                snap_info = next((s for s in snapshots if s.name == self.args.snapshot_name), None)
                if not snap_info:
                    raise Fatal(2, f"Snapshot not found: {self.args.snapshot_name}")
                snapshot = snap_info.snapshot

                if self.args.disk:
                    try:
                        disk = client.select_disk(vm, self.args.disk)
                    except VMwareError as e:
                        raise Fatal(2, f"vsphere query_changed_disk_areas: Failed to select disk: {e}")
                    device_key = disk.key
                elif self.args.device_key:
                    device_key = self.args.device_key
                else:
                    raise Fatal(2, "vsphere query_changed_disk_areas: --device-key or --disk is required")

                start_offset = self.args.start_offset
                change_id = getattr(self.args, "change_id", None)
                try:
                    changed = client.query_changed_disk_areas(
                        vm, snapshot=snapshot, device_key=device_key, start_offset=start_offset, change_id=change_id
                    )
                except Exception as e:
                    raise Fatal(2, f"vsphere query_changed_disk_areas: Failed to query changed areas: {e}")

                changed_areas = [{"start": a.start, "length": a.length} for a in changed.changedDiskAreas]
                output = {
                    "startOffset": changed.startOffset,
                    "length": changed.length,
                    "changed_areas": changed_areas,
                    "vm_name": self.args.vm_name,
                    "snapshot_name": self.args.snapshot_name,
                    "device_key": device_key,
                }
                if self.args.json:
                    print(json.dumps(output, indent=2, default=str))
                else:
                    print(json.dumps(changed_areas, indent=2))
                return 0

            if action == "download_vm_disk":
                if not all([self.args.vm_name, self.args.local_path]):
                    raise Fatal(2, "vsphere download_vm_disk: --vm-name, --local-path are required")
                vm = client.get_vm_by_name(self.args.vm_name)
                if not vm:
                    raise Fatal(2, f"vsphere: VM not found: {self.args.vm_name}")

                try:
                    disk = client.select_disk(vm, self.args.disk)
                except VMwareError as e:
                    raise Fatal(2, f"vsphere download_vm_disk: Failed to select disk: {e}")

                backing = getattr(disk, "backing", None)
                file_name = getattr(backing, "fileName", None)
                if not file_name:
                    raise Fatal(2, "vsphere: could not read disk backing filename")

                try:
                    datastore, ds_path = client.parse_backing_filename(file_name)
                except VMwareError as e:
                    raise Fatal(2, f"vsphere download_vm_disk: Failed to parse backing filename: {e}")

                local_path = Path(self.args.local_path).resolve()
                dc_name = self._dc_name()
                chunk_size = int(getattr(self.args, "chunk_size", 1024 * 1024))

                try:
                    client.download_datastore_file(
                        datastore=datastore,
                        ds_path=ds_path,
                        local_path=local_path,
                        dc_name=dc_name,
                        chunk_size=chunk_size,
                    )
                except VMwareError as e:
                    raise Fatal(2, f"vsphere download_vm_disk: {e}")

                output = {
                    "status": "success",
                    "local_path": str(local_path),
                    "vm_name": self.args.vm_name,
                    "disk_key": disk.key,
                    "datastore": datastore,
                    "ds_path": ds_path,
                    "dc_name": dc_name,
                }
                if self.args.json:
                    print(json.dumps(output, indent=2))
                else:
                    print(f"Downloaded disk from VM {self.args.vm_name} to {local_path}")
                return 0

            # ✅ download-only VM folder pull (now with govmomi listing when available)
            if action == "download_only_vm":
                if not getattr(self.args, "vm_name", None):
                    raise Fatal(2, "vsphere download_only_vm: --vm_name is required")

                vm = client.get_vm_by_name(self.args.vm_name)
                if not vm:
                    raise Fatal(2, f"vsphere: VM not found: {self.args.vm_name}")

                out_dir = Path(self.args.output_dir).expanduser().resolve()
                out_dir.mkdir(parents=True, exist_ok=True)

                include_glob = list(getattr(self.args, "vs_include_glob", None) or ["*"])
                exclude_glob = list(getattr(self.args, "vs_exclude_glob", None) or [])
                concurrency = int(getattr(self.args, "vs_concurrency", 4) or 4)
                max_files = int(getattr(self.args, "vs_max_files", 5000) or 5000)
                fail_on_missing = bool(getattr(self.args, "vs_fail_on_missing", False))

                vmx_path = None
                try:
                    vmx_path = vm.summary.config.vmPathName if vm.summary and vm.summary.config else None
                except Exception:
                    vmx_path = None

                if not vmx_path:
                    raise Fatal(2, "vsphere download_only_vm: cannot determine VM folder (vm.summary.config.vmPathName missing)")

                ds_name, folder = self._parse_vm_datastore_dir(str(vmx_path))

                # ✅ YAML/CLI override: force datastore folder even if summary lies
                override = getattr(self.args, "vs_datastore_dir", None)
                if override:
                    try:
                        ds_name, folder = self._parse_datastore_dir_override(str(override), default_ds=ds_name)
                        self.logger.info(f"download_only_vm: using vs_datastore_dir override: [{ds_name}] {folder or '.'}")
                    except Exception as e:
                        raise Fatal(2, f"vsphere download_only_vm: invalid vs_datastore_dir={override!r}: {e}")

                ds_obj = self._find_datastore_obj(client, ds_name)

                files = self._list_vm_folder_files(
                    client=client,
                    datastore_obj=ds_obj,
                    ds_name=ds_name,
                    folder=folder,
                    include_glob=include_glob,
                    exclude_glob=exclude_glob,
                    max_files=max_files,
                )

                if not files:
                    output = {
                        "status": "success",
                        "vm_name": self.args.vm_name,
                        "datastore": ds_name,
                        "folder": folder,
                        "matched": 0,
                        "downloaded": 0,
                        "output_dir": str(out_dir),
                        "include_glob": include_glob,
                        "exclude_glob": exclude_glob,
                        "used_govmomi": self._prefer_govmomi(),
                    }
                    if self.args.json:
                        print(json.dumps(output, indent=2, default=str))
                    else:
                        print("No files matched; nothing downloaded.")
                    return 0

                self.logger.info(
                    f"download_only_vm: matched {len(files)} files in [{ds_name}] {folder or '.'} "
                    f"(listing={'govc' if self._prefer_govmomi() else 'pyvmomi'})"
                )

                verify_tls = not client.insecure
                dc_name = self._dc_name()

                
                downloaded: List[str] = []
                errors: List[str] = []

                # Progress UI (TTY-only, and suppressed in --json mode to avoid corrupting JSON output)
                progress = None
                files_task = None
                bytes_task = None
                progress_lock = threading.Lock()

                if (Progress is not None) and (not getattr(self.args, "json", False)):
                    try:
                        progress = Progress(
                            SpinnerColumn(),
                            TextColumn("[bold]{task.description}[/bold]"),
                            BarColumn(),
                            TextColumn("{task.completed}/{task.total}" if "{task.total}" else ""),
                            TransferSpeedColumn(),
                            TimeElapsedColumn(),
                            transient=False,
                        )
                        files_task = progress.add_task("files", total=len(files))
                        # bytes task is indeterminate (total unknown); we still advance it for live throughput.
                        bytes_task = progress.add_task("bytes", total=None)
                    except Exception:
                        progress = None
                        files_task = None
                        bytes_task = None

                def _job(ds_path: str) -> None:
                    local_path = out_dir / ds_path

                    def _on_bytes(n: int, total: int) -> None:
                        if progress is None:
                            return
                        with progress_lock:
                            if bytes_task is not None:
                                progress.advance(bytes_task, n)
                            if files_task is not None:
                                # keep the currently active filename visible
                                progress.update(files_task, description=f"downloading: {ds_path}")

                    try:
                        self._download_one_folder_file(
                            client=client,
                            vc_host=vc_host,
                            dc_name=dc_name,
                            ds_name=ds_name,
                            ds_path=ds_path,
                            local_path=local_path,
                            verify_tls=verify_tls,
                            on_bytes=_on_bytes,
                        )
                        downloaded.append(ds_path)
                        if progress is not None:
                            with progress_lock:
                                if files_task is not None:
                                    progress.advance(files_task, 1)
                    except Exception as e:
                        msg = f"{ds_path}: {e}"
                        errors.append(msg)
                        if progress is not None:
                            with progress_lock:
                                if files_task is not None:
                                    progress.update(files_task, description=f"error: {ds_path}")
                        if fail_on_missing:
                            raise


                
                # Run downloads (optionally under Rich progress context)
                def _run_all() -> None:
                    if concurrency <= 1:
                        for p in files:
                            _job(p)
                    else:
                        with ThreadPoolExecutor(max_workers=concurrency) as ex:
                            futs = {ex.submit(_job, p): p for p in files}
                            for fut in as_completed(futs):
                                try:
                                    fut.result()
                                except Exception:
                                    if fail_on_missing:
                                        raise Fatal(2, f"download_only_vm: failed: {errors[-1] if errors else 'unknown'}")

                if progress is not None:
                    with progress:
                        _run_all()
                else:
                    _run_all()


                output = {
                    "status": "success" if not errors else "partial",
                    "vm_name": self.args.vm_name,
                    "datastore": ds_name,
                    "folder": folder,
                    "output_dir": str(out_dir),
                    "matched": len(files),
                    "downloaded": len(downloaded),
                    "errors": errors,
                    "include_glob": include_glob,
                    "exclude_glob": exclude_glob,
                    "concurrency": concurrency,
                    "dc_name": dc_name,
                    "verify_tls": verify_tls,
                    "used_govmomi": self._prefer_govmomi(),
                    "govc_bin": self.govc.govc_bin if self._prefer_govmomi() else None,
                    "vs_datastore_dir": str(override) if override else None,
                }
                if self.args.json:
                    print(json.dumps(output, indent=2, default=str))
                else:
                    print(f"Downloaded {len(downloaded)}/{len(files)} files into {out_dir}")
                    if errors:
                        print("Some downloads failed:")
                        for e in errors[:20]:
                            print(f"  - {e}")
                        if len(errors) > 20:
                            print(f"  ... and {len(errors)-20} more")
                return 0

            if action == "cbt_sync":
                if not all([self.args.vm_name, self.args.local_path]):
                    raise Fatal(2, "vsphere cbt_sync: --vm-name, --local-path are required")

                vm = client.get_vm_by_name(self.args.vm_name)
                if not vm:
                    raise Fatal(2, f"vsphere: VM not found: {self.args.vm_name}")

                try:
                    disk = client.select_disk(vm, self.args.disk)
                except VMwareError as e:
                    raise Fatal(2, f"vsphere cbt_sync: Failed to select disk: {e}")

                backing = getattr(disk, "backing", None)
                file_name = getattr(backing, "fileName", None)
                if not file_name:
                    raise Fatal(2, "vsphere: could not read disk backing filename")

                try:
                    datastore, ds_path = client.parse_backing_filename(file_name)
                except VMwareError as e:
                    raise Fatal(2, f"vsphere cbt_sync: Failed to parse backing filename: {e}")

                local_disk = Path(self.args.local_path).resolve()
                if not local_disk.exists():
                    raise Fatal(2, f"vsphere: local disk file does not exist for cbt-sync: {local_disk}")

                was_enabled = vm.config.changeTrackingEnabled if vm.config else False
                if self.args.enable_cbt:
                    try:
                        client.enable_cbt(vm)
                    except VMwareError as e:
                        raise Fatal(2, f"vsphere cbt_sync: Failed to enable CBT: {e}")

                snap_name = self.args.snapshot_name or "vmdk2kvm-cbt"
                try:
                    snap = client.create_snapshot(vm, snap_name, quiesce=True, memory=False)
                except VMwareError as e:
                    raise Fatal(2, f"vsphere cbt_sync: Failed to create snapshot: {e}")

                done = 0
                num_ranges = 0
                try:
                    device_key = disk.key

                    change_id = getattr(self.args, "change_id", None)

                    changed = client.query_changed_disk_areas(
                        vm,
                        snapshot=snap,
                        device_key=device_key,
                        start_offset=0,
                        change_id=change_id,
                    )

                    if not getattr(changed, "changedDiskAreas", None):
                        self.logger.info("No changed blocks reported by CBT")
                        num_ranges = 0
                        done = 0
                    else:
                        num_ranges = len(changed.changedDiskAreas)

                        if not REQUESTS_AVAILABLE:
                            raise Fatal(2, "requests not installed. Install: pip install requests")

                        dc_name = self._dc_name()
                        quoted = quote(ds_path, safe="/")
                        url = f"https://{vc_host}/folder/{quoted}?dcPath={quote(dc_name)}&dsName={quote(datastore)}"
                        headers = {"Cookie": client._session_cookie()}
                        verify = not client.insecure

                        total = sum(int(a.length) for a in changed.changedDiskAreas)
                        done = 0
                        self.logger.info(f"Syncing {num_ranges} ranges ({total/(1024**2):.1f} MiB)")

                        with open(local_disk, "rb+") as f:
                            for a in changed.changedDiskAreas:
                                start = int(a.start)
                                length = int(a.length)
                                end = start + length - 1

                                h = dict(headers)
                                h["Range"] = f"bytes={start}-{end}"

                                try:
                                    r = requests.get(url, headers=h, verify=verify)
                                    r.raise_for_status()
                                except requests.RequestException as e:
                                    raise Fatal(2, f"vsphere cbt_sync: HTTP request failed: {e}")

                                data = r.content
                                f.seek(start)
                                f.write(data)

                                done += length
                                if total:
                                    self.logger.debug(
                                        f"CBT sync: {done/(1024**2):.1f} MiB / {total/(1024**2):.1f} MiB ({(done/total)*100:.1f}%)"
                                    )

                    self.logger.info("CBT sync completed")

                except Exception as e:
                    raise Fatal(2, f"vsphere cbt_sync: Failed during sync: {e}")

                finally:
                    try:
                        task = snap.RemoveSnapshot_Task(removeChildren=False)
                        client.wait_for_task(task)
                    except Exception as e:
                        self.logger.warning(f"Failed to remove snapshot: {e}")

                output = {
                    "status": "success",
                    "vm_name": self.args.vm_name,
                    "local_path": str(local_disk),
                    "synced_bytes": done,
                    "num_ranges": num_ranges,
                    "cbt_was_enabled": was_enabled,
                    "cbt_now_enabled": vm.config.changeTrackingEnabled if vm.config else False,
                    "snapshot_name": snap_name,
                    "dc_name": self._dc_name(),
                }
                if self.args.json:
                    print(json.dumps(output, indent=2, default=str))
                else:
                    print(f"CBT sync completed: synced {done} bytes in {num_ranges} ranges")
                return 0

            raise Fatal(2, f"vsphere: unknown action: {action}")

        finally:
            try:
                client.disconnect()
            except Exception as e:
                self.logger.warning(f"Failed to disconnect: {e}")
