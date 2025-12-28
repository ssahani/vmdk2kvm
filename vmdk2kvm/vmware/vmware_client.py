from __future__ import annotations
import logging
import os
import re
import ssl
import time
from pathlib import Path
from typing import Any, List, Optional, Tuple

from ..core.exceptions import VMwareError

# Optional: vSphere / vCenter integration (pyvmomi + requests)
try:
    from pyVim.connect import SmartConnect, Disconnect  # type: ignore
    from pyVmomi import vim, vmodl  # type: ignore
    PYVMOMI_AVAILABLE = True
except Exception:  # pragma: no cover
    SmartConnect = None  # type: ignore
    Disconnect = None  # type: ignore
    vim = None  # type: ignore
    vmodl = None  # type: ignore
    PYVMOMI_AVAILABLE = False

try:
    import requests  # type: ignore
    REQUESTS_AVAILABLE = True
except Exception:  # pragma: no cover
    requests = None  # type: ignore
    REQUESTS_AVAILABLE = False
class VMwareClient:
    """Minimal vSphere/vCenter client (pyvmomi + HTTP download via session cookie)."""

    def __init__(self, logger: logging.Logger, host: str, user: str, password: str, *, port: int = 443, insecure: bool = False):
        self.logger = logger
        self.host = host
        self.user = user
        self.password = password
        self.port = int(port)
        self.insecure = bool(insecure)
        self.si = None

    def connect(self) -> None:
        if not PYVMOMI_AVAILABLE:
            raise VMwareError("pyvmomi not installed. Install: pip install pyvmomi")
        try:
            if self.insecure:
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            else:
                ctx = ssl.create_default_context()
            self.si = SmartConnect(host=self.host, user=self.user, pwd=self.password, port=self.port, sslContext=ctx)
            self.logger.info(f"Connected to vSphere: {self.host}:{self.port}")
        except Exception as e:
            raise VMwareError(f"Failed to connect to vSphere: {e}")

    def disconnect(self) -> None:
        try:
            if self.si:
                Disconnect(self.si)
        finally:
            self.si = None

    def _content(self):
        if not self.si:
            raise VMwareError("Not connected")
        return self.si.RetrieveContent()

    def get_vm_by_name(self, name: str):
        content = self._content()
        view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
        try:
            for vm in view.view:
                if vm.name == name:
                    return vm
            return None
        finally:
            view.Destroy()

    def list_vm_names(self) -> List[str]:
        content = self._content()
        view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
        try:
            return sorted([vm.name for vm in view.view])
        finally:
            view.Destroy()

    def wait_for_task(self, task) -> None:
        while task.info.state not in (vim.TaskInfo.State.success, vim.TaskInfo.State.error):
            time.sleep(1)
        if task.info.state == vim.TaskInfo.State.error:
            raise VMwareError(str(task.info.error))

    def vm_disks(self, vm) -> List[Any]:
        disks = []
        for dev in getattr(vm.config.hardware, "device", []):
            if isinstance(dev, vim.vm.device.VirtualDisk):
                disks.append(dev)
        return disks

    def select_disk(self, vm, label_or_index: Optional[str]) -> Any:
        disks = self.vm_disks(vm)
        if not disks:
            raise VMwareError("No virtual disks found on VM")

        if label_or_index is None:
            return disks[0]

        s = str(label_or_index).strip()
        if s.isdigit():
            idx = int(s)
            if idx < 0 or idx >= len(disks):
                raise VMwareError(f"Disk index out of range: {idx} (found {len(disks)})")
            return disks[idx]

        # label match (case-insensitive)
        sl = s.lower()
        for d in disks:
            label = getattr(getattr(d, "deviceInfo", None), "label", "") or ""
            if sl in label.lower():
                return d
        raise VMwareError(f"No disk matching label: {s}")

    @staticmethod
    def parse_backing_filename(file_name: str) -> Tuple[str, str]:
        # VMware backing filename like: "[datastore1] vm/vm.vmdk"
        m = re.match(r"\[(.+?)\]\s+(.*)", file_name)
        if not m:
            raise VMwareError(f"Could not parse backing filename: {file_name}")
        return m.group(1), m.group(2)

    def _session_cookie(self) -> str:
        if not self.si:
            raise VMwareError("Not connected")
        # pyvmomi stores session cookie on stub
        cookie = getattr(self.si, "_stub", None)
        cookie = getattr(cookie, "cookie", None)
        if not cookie:
            raise VMwareError("Could not obtain session cookie")
        return cookie

    def download_datastore_file(self, *, datastore: str, ds_path: str, local_path: Path, dc_name: str = "ha-datacenter", chunk_size: int = 1024 * 1024) -> None:
        if not REQUESTS_AVAILABLE:
            raise VMwareError("requests not installed. Install: pip install requests")

        url = f"https://{self.host}/folder/{ds_path}?dcPath={dc_name}&dsName={datastore}"
        headers = {"Cookie": self._session_cookie()}

        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Downloading datastore file: [{datastore}] {ds_path} -> {local_path}")

        verify = not self.insecure
        try:
            with requests.get(url, headers=headers, stream=True, verify=verify) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-length", "0") or "0")
                got = 0
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        f.write(chunk)
                        got += len(chunk)
                        if total and got and got % (128 * 1024 * 1024) < chunk_size:
                            self.logger.info(f"Download progress: {got/(1024**2):.1f} MiB / {total/(1024**2):.1f} MiB ({(got/total)*100:.1f}%)")
        except Exception as e:
            if local_path.exists():
                try:
                    local_path.unlink()
                except Exception:
                    pass
            raise VMwareError(f"Download failed: {e}")

    def create_snapshot(self, vm, name: str, *, quiesce: bool = True, memory: bool = False, description: str = "Created by vmdk2kvm") -> Any:
        task = vm.CreateSnapshot_Task(name=name, description=description, memory=memory, quiesce=quiesce)
        self.wait_for_task(task)
        return task.info.result

    def enable_cbt(self, vm) -> None:
        if not getattr(vm.capability, "changeTrackingSupported", False):
            raise VMwareError("CBT not supported on this VM")
        if getattr(vm.config, "changeTrackingEnabled", False):
            return
        spec = vim.vm.ConfigSpec()
        spec.changeTrackingEnabled = True
        task = vm.ReconfigVM_Task(spec)
        self.wait_for_task(task)

    def query_changed_disk_areas(self, vm, *, snapshot, device_key: int, start_offset: int = 0, change_id: str = "*"):
        return vm.QueryChangedDiskAreas(snapshot=snapshot, deviceKey=device_key, startOffset=start_offset, changeId=change_id)
