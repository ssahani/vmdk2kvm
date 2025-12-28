from __future__ import annotations
import logging
import os
import re
import ssl
import time
from pathlib import Path
from typing import Any, List, Optional, Tuple
import asyncio

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

try:
    import aiohttp  # type: ignore
    AIOHTTP_AVAILABLE = True
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore
    AIOHTTP_AVAILABLE = False

try:
    import aiofiles  # type: ignore
    AIOFILES_AVAILABLE = True
except Exception:  # pragma: no cover
    aiofiles = None  # type: ignore
    AIOFILES_AVAILABLE = False

class VMwareClient:
    """Minimal vSphere/vCenter client (pyvmomi + HTTP download via session cookie)."""

    def __init__(self, logger: logging.Logger, host: str, user: str, password: str, *, port: int = 443, insecure: bool = False, timeout: Optional[float] = None):
        self.logger = logger
        self.host = host
        self.user = user
        self.password = password
        self.port = int(port)
        self.insecure = bool(insecure)
        self.timeout = timeout
        self.si = None
        self.logger.debug(f"Initialized VMwareClient for host: {self.host}:{self.port}, user: {self.user}, insecure: {self.insecure}, timeout: {self.timeout}")

    def __enter__(self):
        self.logger.debug("Entering context manager")
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.debug("Exiting context manager")
        self.disconnect()
        if exc_type is not None:
            self.logger.error(f"Exception in context: {exc_type.__name__}: {exc_val}")
        return False  # Do not suppress exceptions

    async def __aenter__(self):
        self.logger.debug("Entering async context manager")
        await self.async_connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.logger.debug("Exiting async context manager")
        await self.async_disconnect()
        if exc_type is not None:
            self.logger.error(f"Exception in async context: {exc_type.__name__}: {exc_val}")
        return False  # Do not suppress exceptions

    def connect(self) -> None:
        self.logger.debug(f"Attempting to connect to {self.host}:{self.port} with user {self.user}")
        if not PYVMOMI_AVAILABLE:
            self.logger.error("pyvmomi not installed")
            raise VMwareError("pyvmomi not installed. Install: pip install pyvmomi")
        try:
            if self.insecure:
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                self.logger.debug("Using insecure SSL context")
            else:
                ctx = ssl.create_default_context()
                self.logger.debug("Using default SSL context")
            self.si = SmartConnect(host=self.host, user=self.user, pwd=self.password, port=self.port, sslContext=ctx)
            self.logger.info(f"Connected to vSphere: {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Failed to connect to vSphere: {e}")
            raise VMwareError(f"Failed to connect to vSphere: {e}")

    async def async_connect(self) -> None:
        self.logger.debug(f"Attempting to connect to {self.host}:{self.port} with user {self.user}")
        if not PYVMOMI_AVAILABLE:
            self.logger.error("pyvmomi not installed")
            raise VMwareError("pyvmomi not installed. Install: pip install pyvmomi")
        try:
            if self.insecure:
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                self.logger.debug("Using insecure SSL context")
            else:
                ctx = ssl.create_default_context()
                self.logger.debug("Using default SSL context")
            self.si = await asyncio.to_thread(SmartConnect, host=self.host, user=self.user, pwd=self.password, port=self.port, sslContext=ctx)
            self.logger.info(f"Connected to vSphere: {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Failed to connect to vSphere: {e}")
            raise VMwareError(f"Failed to connect to vSphere: {e}")

    def disconnect(self) -> None:
        self.logger.debug("Attempting to disconnect from vSphere")
        try:
            if self.si:
                Disconnect(self.si)
                self.logger.debug("Disconnected successfully")
        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")
        finally:
            self.si = None

    async def async_disconnect(self) -> None:
        self.logger.debug("Attempting to disconnect from vSphere")
        try:
            if self.si:
                await asyncio.to_thread(Disconnect, self.si)
                self.logger.debug("Disconnected successfully")
        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")
        finally:
            self.si = None

    def _content(self):
        self.logger.debug("Retrieving content")
        if not self.si:
            self.logger.error("Not connected to vSphere")
            raise VMwareError("Not connected")
        try:
            return self.si.RetrieveContent()
        except Exception as e:
            self.logger.error(f"Failed to retrieve content: {e}")
            raise VMwareError(f"Failed to retrieve content: {e}")

    def get_vm_by_name(self, name: str):
        self.logger.debug(f"Searching for VM by name: {name}")
        content = self._content()
        view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
        try:
            for vm in view.view:
                if vm.name == name:
                    self.logger.debug(f"Found VM: {name}")
                    return vm
            self.logger.debug(f"VM not found: {name}")
            return None
        except Exception as e:
            self.logger.error(f"Error while searching for VM {name}: {e}")
            raise VMwareError(f"Error while searching for VM {name}: {e}")
        finally:
            try:
                view.Destroy()
            except Exception as e:
                self.logger.warning(f"Failed to destroy view: {e}")

    def list_vm_names(self) -> List[str]:
        self.logger.debug("Listing all VM names")
        content = self._content()
        view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
        try:
            vm_names = sorted([vm.name for vm in view.view])
            self.logger.debug(f"Found {len(vm_names)} VMs")
            return vm_names
        except Exception as e:
            self.logger.error(f"Error while listing VM names: {e}")
            raise VMwareError(f"Error while listing VM names: {e}")
        finally:
            try:
                view.Destroy()
            except Exception as e:
                self.logger.warning(f"Failed to destroy view: {e}")

    def wait_for_task(self, task) -> None:
        self.logger.debug(f"Waiting for task: {task}")
        try:
            while task.info.state not in (vim.TaskInfo.State.success, vim.TaskInfo.State.error):
                time.sleep(1)
            if task.info.state == vim.TaskInfo.State.error:
                error_msg = str(task.info.error)
                self.logger.error(f"Task failed: {error_msg}")
                raise VMwareError(error_msg)
            self.logger.debug("Task completed successfully")
        except Exception as e:
            self.logger.error(f"Error while waiting for task: {e}")
            raise VMwareError(f"Error while waiting for task: {e}")

    def vm_disks(self, vm) -> List[Any]:
        self.logger.debug(f"Getting disks for VM: {vm.name}")
        try:
            disks = []
            for dev in getattr(vm.config.hardware, "device", []):
                if isinstance(dev, vim.vm.device.VirtualDisk):
                    disks.append(dev)
            self.logger.debug(f"Found {len(disks)} disks")
            return disks
        except Exception as e:
            self.logger.error(f"Error getting disks for VM {vm.name}: {e}")
            raise VMwareError(f"Error getting disks for VM {vm.name}: {e}")

    def select_disk(self, vm, label_or_index: Optional[str]) -> Any:
        self.logger.debug(f"Selecting disk for VM {vm.name} with selector: {label_or_index}")
        try:
            disks = self.vm_disks(vm)
            if not disks:
                self.logger.error("No virtual disks found on VM")
                raise VMwareError("No virtual disks found on VM")

            if label_or_index is None:
                self.logger.debug("Selecting first disk by default")
                return disks[0]

            s = str(label_or_index).strip()
            if s.isdigit():
                idx = int(s)
                if idx < 0 or idx >= len(disks):
                    self.logger.error(f"Disk index out of range: {idx} (found {len(disks)})")
                    raise VMwareError(f"Disk index out of range: {idx} (found {len(disks)})")
                self.logger.debug(f"Selected disk at index {idx}")
                return disks[idx]

            # label match (case-insensitive)
            sl = s.lower()
            for d in disks:
                label = getattr(getattr(d, "deviceInfo", None), "label", "") or ""
                if sl in label.lower():
                    self.logger.debug(f"Selected disk with label matching: {s}")
                    return d
            self.logger.error(f"No disk matching label: {s}")
            raise VMwareError(f"No disk matching label: {s}")
        except Exception as e:
            self.logger.error(f"Error selecting disk: {e}")
            raise VMwareError(f"Error selecting disk: {e}")

    @staticmethod
    def parse_backing_filename(file_name: str) -> Tuple[str, str]:
        # VMware backing filename like: "[datastore1] vm/vm.vmdk"
        m = re.match(r"\[(.+?)\]\s+(.*)", file_name)
        if not m:
            raise VMwareError(f"Could not parse backing filename: {file_name}")
        return m.group(1), m.group(2)

    def _session_cookie(self) -> str:
        self.logger.debug("Retrieving session cookie")
        if not self.si:
            self.logger.error("Not connected to vSphere")
            raise VMwareError("Not connected")
        try:
            # pyvmomi stores session cookie on stub
            cookie = getattr(self.si, "_stub", None)
            cookie = getattr(cookie, "cookie", None)
            if not cookie:
                self.logger.error("Could not obtain session cookie")
                raise VMwareError("Could not obtain session cookie")
            self.logger.debug("Session cookie retrieved successfully")
            return cookie
        except Exception as e:
            self.logger.error(f"Error retrieving session cookie: {e}")
            raise VMwareError(f"Error retrieving session cookie: {e}")

    def download_datastore_file(self, *, datastore: str, ds_path: str, local_path: Path, dc_name: str = "ha-datacenter", chunk_size: int = 1024 * 1024) -> None:
        self.logger.debug(f"Downloading datastore file: [{datastore}] {ds_path} -> {local_path}")
        if not REQUESTS_AVAILABLE:
            self.logger.error("requests not installed")
            raise VMwareError("requests not installed. Install: pip install requests")

        url = f"https://{self.host}/folder/{ds_path}?dcPath={dc_name}&dsName={datastore}"
        headers = {"Cookie": self._session_cookie()}
        self.logger.debug(f"Download URL: {url}")

        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Downloading datastore file: [{datastore}] {ds_path} -> {local_path}")

        verify = not self.insecure
        try:
            with requests.get(url, headers=headers, stream=True, verify=verify, timeout=self.timeout) as r:
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
            self.logger.debug("Download completed successfully")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP request failed: {e}")
            if local_path.exists():
                try:
                    local_path.unlink()
                    self.logger.debug("Cleaned up partial download file")
                except Exception as cleanup_e:
                    self.logger.warning(f"Failed to clean up partial download: {cleanup_e}")
            raise VMwareError(f"Download failed: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during download: {e}")
            if local_path.exists():
                try:
                    local_path.unlink()
                    self.logger.debug("Cleaned up partial download file")
                except Exception as cleanup_e:
                    self.logger.warning(f"Failed to clean up partial download: {cleanup_e}")
            raise VMwareError(f"Download failed: {e}")

    async def async_download_datastore_file(self, *, datastore: str, ds_path: str, local_path: Path, dc_name: str = "ha-datacenter", chunk_size: int = 1024 * 1024) -> None:
        self.logger.debug(f"Async downloading datastore file: [{datastore}] {ds_path} -> {local_path}")
        if not AIOHTTP_AVAILABLE or not AIOFILES_AVAILABLE:
            self.logger.error("aiohttp or aiofiles not installed")
            raise VMwareError("aiohttp and aiofiles not installed. Install: pip install aiohttp aiofiles")

        url = f"https://{self.host}/folder/{ds_path}?dcPath={dc_name}&dsName={datastore}"
        headers = {"Cookie": self._session_cookie()}
        self.logger.debug(f"Async download URL: {url}")

        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Async downloading datastore file: [{datastore}] {ds_path} -> {local_path}")

        ssl_param = True
        if self.insecure:
            ssl_param = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_param.check_hostname = False
            ssl_param.verify_mode = ssl.CERT_NONE
            self.logger.debug("Using insecure SSL context for async download")

        timeout_param = aiohttp.ClientTimeout(total=self.timeout) if self.timeout is not None else None

        try:
            async with aiohttp.ClientSession(timeout=timeout_param) as session:
                async with session.get(url, headers=headers, ssl=ssl_param) as response:
                    response.raise_for_status()
                    total = int(response.headers.get("content-length", "0") or "0")
                    got = 0
                    async with aiofiles.open(local_path, "wb") as f:
                        async for chunk in response.content.iter_chunked(chunk_size):
                            if not chunk:
                                continue
                            await f.write(chunk)
                            got += len(chunk)
                            if total and got and got % (128 * 1024 * 1024) < chunk_size:
                                self.logger.info(f"Download progress: {got/(1024**2):.1f} MiB / {total/(1024**2):.1f} MiB ({(got/total)*100:.1f}%)")
            self.logger.debug("Async download completed successfully")
        except aiohttp.ClientError as e:
            self.logger.error(f"HTTP request failed: {e}")
            if local_path.exists():
                try:
                    await asyncio.to_thread(local_path.unlink)
                    self.logger.debug("Cleaned up partial download file")
                except Exception as cleanup_e:
                    self.logger.warning(f"Failed to clean up partial download: {cleanup_e}")
            raise VMwareError(f"Download failed: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during async download: {e}")
            if local_path.exists():
                try:
                    await asyncio.to_thread(local_path.unlink)
                    self.logger.debug("Cleaned up partial download file")
                except Exception as cleanup_e:
                    self.logger.warning(f"Failed to clean up partial download: {cleanup_e}")
            raise VMwareError(f"Download failed: {e}")

    def create_snapshot(self, vm, name: str, *, quiesce: bool = True, memory: bool = False, description: str = "Created by vmdk2kvm") -> Any:
        self.logger.debug(f"Creating snapshot for VM {vm.name}: name={name}, quiesce={quiesce}, memory={memory}, description={description}")
        try:
            task = vm.CreateSnapshot_Task(name=name, description=description, memory=memory, quiesce=quiesce)
            self.wait_for_task(task)
            result = task.info.result
            self.logger.debug(f"Snapshot created successfully: {result}")
            return result
        except Exception as e:
            self.logger.error(f"Error creating snapshot: {e}")
            raise VMwareError(f"Error creating snapshot: {e}")

    def enable_cbt(self, vm) -> None:
        self.logger.debug(f"Enabling CBT for VM: {vm.name}")
        try:
            if not getattr(vm.capability, "changeTrackingSupported", False):
                self.logger.error("CBT not supported on this VM")
                raise VMwareError("CBT not supported on this VM")
            if getattr(vm.config, "changeTrackingEnabled", False):
                self.logger.debug("CBT already enabled")
                return
            spec = vim.vm.ConfigSpec()
            spec.changeTrackingEnabled = True
            task = vm.ReconfigVM_Task(spec)
            self.wait_for_task(task)
            self.logger.debug("CBT enabled successfully")
        except Exception as e:
            self.logger.error(f"Error enabling CBT: {e}")
            raise VMwareError(f"Error enabling CBT: {e}")

    def query_changed_disk_areas(self, vm, *, snapshot, device_key: int, start_offset: int = 0, change_id: str = "*"):
        self.logger.debug(f"Querying changed disk areas for VM {vm.name}: snapshot={snapshot}, device_key={device_key}, start_offset={start_offset}, change_id={change_id}")
        try:
            result = vm.QueryChangedDiskAreas(snapshot=snapshot, deviceKey=device_key, startOffset=start_offset, changeId=change_id)
            self.logger.debug("Query completed successfully")
            return result
        except Exception as e:
            self.logger.error(f"Error querying changed disk areas: {e}")
            raise VMwareError(f"Error querying changed disk areas: {e}")