from __future__ import annotations
import logging
import os
import re
import ssl
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, Generator

from ..core.exceptions import VMwareError

# Optional: vSphere / vCenter integration (pyvmomi + requests)
try:
    from pyVim.connect import SmartConnect, Disconnect  # type: ignore
    from pyVmomi import vim, vmodl  # type: ignore
    from pyVim.task import WaitForTask  # type: ignore
    PYVMOMI_AVAILABLE = True
except Exception:  # pragma: no cover
    SmartConnect = None  # type: ignore
    Disconnect = None  # type: ignore
    vim = None  # type: ignore
    vmodl = None  # type: ignore
    WaitForTask = None  # type: ignore
    PYVMOMI_AVAILABLE = False

try:
    import requests  # type: ignore
    from requests.adapters import HTTPAdapter  # type: ignore
    REQUESTS_AVAILABLE = True
except Exception:  # pragma: no cover
    requests = None  # type: ignore
    REQUESTS_AVAILABLE = False

try:
    # Optional, only used to silence warnings when --vc-insecure is set.
    import urllib3  # type: ignore
    URLLIB3_AVAILABLE = True
except Exception:  # pragma: no cover
    urllib3 = None  # type: ignore
    URLLIB3_AVAILABLE = False

try:
    from .vmdk_parser import VMDK  # type: ignore
    VMDK_PARSER_AVAILABLE = True
except Exception:  # pragma: no cover
    VMDK = None  # type: ignore
    VMDK_PARSER_AVAILABLE = False

class VMwareClient:
    """Enhanced vSphere/vCenter client with comprehensive pyvmomi features."""
    
    def __init__(self, logger: logging.Logger, host: str, user: str, password: str, *, port: int = 443, insecure: bool = False, timeout: int = 900, pool_size: int = 10):
        self.logger = logger
        self.host = host
        self.user = user
        self.password = password
        self.port = int(port)
        self.insecure = bool(insecure)
        self.timeout = timeout
        self.pool_size = pool_size
        self.si = None
        self.content = None
        
    def connect(self) -> None:
        """Connect to vSphere with enhanced error handling, timeout, connection pooling, and retry mechanism."""
        if not PYVMOMI_AVAILABLE:
            raise VMwareError("pyvmomi not installed. Install: pip install pyvmomi")
        
        if self.insecure:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        else:
            ctx = ssl.create_default_context()
        
        max_retries = 3
        retry_delay = 5
        retries = 0
        
        while retries < max_retries:
            try:
                # Connection with timeout handling and pool size for performance
                self.si = SmartConnect(
                    host=self.host,
                    user=self.user,
                    pwd=self.password,
                    port=self.port,
                    sslContext=ctx,
                    connectionPoolTimeout=self.timeout
                )
                
                self.content = self.si.RetrieveContent()
                
                self.logger.info(f"Connected to vSphere: {self.host}:{self.port}")
                self.logger.info(f"API Version: {self.si.content.about.apiVersion}")
                self.logger.info(f"vCenter: {self.si.content.about.fullName}")
                
                return
            
            except vmodl.fault.InvalidLogin as e:
                raise VMwareError(f"Invalid login credentials: {e}")
            
            except Exception as e:
                retries += 1
                self.logger.warning(f"Connection attempt {retries}/{max_retries} failed: {e}. Retrying in {retry_delay} seconds...")
                if retries == max_retries:
                    raise VMwareError(f"Failed to connect to vSphere after {max_retries} retries: {e}")
                time.sleep(retry_delay)
    
    def disconnect(self) -> None:
        """Disconnect from vSphere."""
        try:
            if self.si:
                Disconnect(self.si)
                self.logger.info(f"Disconnected from {self.host}")
        except Exception as e:
            self.logger.warning(f"Error during disconnect: {e}")
        finally:
            self.si = None
            self.content = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
    
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

    def get_vm_by_uuid(self, uuid: str):
        """Get VM by BIOS UUID."""
        if not self.content:
            raise VMwareError("Not connected")
        
        search_index = self.content.searchIndex
        vm = search_index.FindByUuid(None, uuid, True, True)
        return vm if isinstance(vm, vim.VirtualMachine) else None
    
    def get_vm_by_dns_name(self, dns_name: str):
        """Get VM by DNS name."""
        if not self.content:
            raise VMwareError("Not connected")
        
        search_index = self.content.searchIndex
        vm = search_index.FindByDnsName(None, dns_name, True)
        return vm if isinstance(vm, vim.VirtualMachine) else None
    
    def get_vm_by_ip(self, ip_address: str):
        """Get VM by IP address."""
        if not self.content:
            raise VMwareError("Not connected")
        
        search_index = self.content.searchIndex
        vm = search_index.FindByIp(None, ip_address, True)
        return vm if isinstance(vm, vim.VirtualMachine) else None
    
    def list_vm_names(self) -> List[str]:
        content = self._content()
        view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
        try:
            return sorted([vm.name for vm in view.view])
        finally:
            view.Destroy()

    def get_all_vms(self, recursive: bool = True) -> List[vim.VirtualMachine]:
        """Get all VMs in the inventory."""
        if not self.content:
            raise VMwareError("Not connected")
        
        container = self.content.rootFolder
        view_type = [vim.VirtualMachine]
        container_view = self.content.viewManager.CreateContainerView(
            container, view_type, recursive
        )
        
        try:
            return list(container_view.view)
        finally:
            container_view.Destroy()
    
    def get_vm_details(self, vm) -> Dict[str, Any]:
        """Get comprehensive VM details."""
        details = {
            'name': vm.name,
            'uuid': vm.config.uuid if vm.config else None,
            'guest_id': vm.config.guestId if vm.config else None,
            'memory_mb': vm.config.hardware.memoryMB if vm.config and vm.config.hardware else None,
            'num_cpu': vm.config.hardware.numCPU if vm.config and vm.config.hardware else None,
            'power_state': vm.runtime.powerState if vm.runtime else None,
            'connection_state': vm.runtime.connectionState if vm.runtime else None,
            'tools_status': vm.guest.toolsStatus if vm.guest else None,
            'ip_address': vm.guest.ipAddress if vm.guest and vm.guest.ipAddress else None,
            'host_name': vm.guest.hostName if vm.guest else None,
            'annotation': vm.config.annotation if vm.config else None,
            'folder': vm.parent.name if vm.parent else None,
            'resource_pool': vm.resourcePool.name if vm.resourcePool else None,
            'datastore': [ds.name for ds in vm.datastore] if vm.datastore else [],
            'network': [net.name for net in vm.network] if vm.network else [],
        }
        
        # Add disk information
        disks = []
        for dev in getattr(vm.config.hardware, "device", []):
            if isinstance(dev, vim.vm.device.VirtualDisk):
                disk_info = {
                    'label': getattr(getattr(dev, "deviceInfo", None), "label", ""),
                    'key': dev.key,
                    'capacity_mb': dev.capacityInKB / 1024 if dev.capacityInKB else 0,
                    'backing_type': type(dev.backing).__name__ if dev.backing else None,
                    'file_name': dev.backing.fileName if dev.backing else None,
                    'thin_provisioned': getattr(dev.backing, 'thinProvisioned', False),
                    'eagerly_scrub': getattr(dev.backing, 'eagerlyScrub', False),
                    'disk_mode': getattr(dev.backing, 'diskMode', None),
                }
                disks.append(disk_info)
        
        details['disks'] = disks
        return details

    def wait_for_task(self, task, timeout: int = 3600) -> None:
        """Enhanced task waiter with timeout and progress monitoring."""
        start_time = time.time()
        last_progress = None
        
        while task.info.state not in (vim.TaskInfo.State.success, vim.TaskInfo.State.error):
            if time.time() - start_time > timeout:
                raise VMwareError(f"Task timeout after {timeout} seconds")
            
            # Monitor progress for longer tasks
            if hasattr(task.info, 'progress') and task.info.progress != last_progress:
                self.logger.debug(f"Task progress: {task.info.progress}%")
                last_progress = task.info.progress
            
            time.sleep(1)
        
        if task.info.state == vim.TaskInfo.State.error:
            error_msg = str(task.info.error)
            if hasattr(task.info.error, 'faultMessage'):
                for msg in task.info.error.faultMessage:
                    error_msg += f"\n- {msg.message}"
            raise VMwareError(f"Task failed: {error_msg}")

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

    @staticmethod
    def _is_text_descriptor(p: Path) -> bool:
        # Prefer shared parser if available.
        if VMDK_PARSER_AVAILABLE:
            return VMDK._is_text_descriptor(p)  # type: ignore[attr-defined]
        try:
            st = p.stat()
        except Exception:
            return False
        if st.st_size > 8 * 1024 * 1024:
            return False
        try:
            head = p.open("rb").read(4096)
        except Exception:
            return False
        if b"\x00" in head:
            return False
        return True

    def _parse_parents_and_extents(self, desc: Path) -> Tuple[Optional[str], List[str]]:
        """Return (parent_rel, extent_rel_list) from a local descriptor."""
        if not self._is_text_descriptor(desc):
            return None, []
        parent: Optional[str] = None
        extents: List[str] = []

        if VMDK_PARSER_AVAILABLE:
            parent = VMDK.parse_parent(self.logger, desc)  # type: ignore[attr-defined]
            for e in VMDK.parse_extents(self.logger, desc):  # type: ignore[attr-defined]
                extents.append(e.filename)
            return parent, extents

        # Fallback: basic regex parsing.
        try:
            for line in desc.read_text(encoding="utf-8", errors="ignore").splitlines():
                if not parent and line.strip().startswith("parentFileNameHint"):
                    try:
                        parent = line.split("=", 1)[1].strip().strip('"')
                    except Exception:
                        pass
                m = re.match(r'^\s*(RW|RDONLY|NOACCESS)\s+\d+\s+\S+\s+"([^"]+)"', line, re.IGNORECASE)
                if m:
                    extents.append(m.group(2))
        except Exception as e:
            self.logger.debug(f"descriptor parse failed: {e}")
        return parent, extents

    def _download_http(self, *, url: str, headers: dict, local_path: Path, chunk_size: int, verify: bool, resume: bool = True) -> None:
        """Download url -> local_path with optional Range resume, enhanced with retries and larger connection pool."""
        assert requests is not None
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Silence urllib3's InsecureRequestWarning if we're intentionally insecure.
        if not verify:
            try:
                if URLLIB3_AVAILABLE:
                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # type: ignore[attr-defined]
                else:
                    warnings.filterwarnings("ignore", message="Unverified HTTPS request")
            except Exception:
                pass

        # Enhance with session for retries and pool
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=3, pool_maxsize=20)
        session.mount('https://', adapter)
        session.mount('http://', adapter)

        # HEAD for content-length (best-effort)
        total = 0
        try:
            h = session.head(url, headers=headers, verify=verify, allow_redirects=True, timeout=60)
            if h.status_code in (200, 302, 303, 307, 308):
                total = int(h.headers.get("content-length", "0") or "0")
        except Exception:
            total = 0

        got = 0
        mode = "wb"
        req_headers = dict(headers)

        if resume and local_path.exists() and total:
            try:
                have = local_path.stat().st_size
            except Exception:
                have = 0
            if 0 < have < total:
                req_headers["Range"] = f"bytes={have}-"
                mode = "ab"
                got = have
                self.logger.info(f"Resuming download at {have/(1024**2):.1f} MiB ({have}/{total} bytes)")
            elif have == total:
                self.logger.info("File already fully downloaded; skipping")
                return

        last_log = time.time()
        last_bytes = got

        with session.get(url, headers=req_headers, stream=True, verify=verify) as r:
            # 206 expected when resuming, 200 when fresh.
            r.raise_for_status()
            # If HEAD didn't give us total, try GET headers.
            if not total:
                try:
                    total = int(r.headers.get("content-length", "0") or "0")
                    # When resuming, content-length is remaining; adjust.
                    if "Range" in req_headers and total:
                        total = got + total
                except Exception:
                    total = 0

            with open(local_path, mode) as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    f.write(chunk)
                    got += len(chunk)
                    now = time.time()
                    if now - last_log >= 2.0:
                        # log every ~2s
                        mb = got / (1024**2)
                        if total:
                            tmb = total / (1024**2)
                            pct = (got / total) * 100.0
                            rate = (got - last_bytes) / max(now - last_log, 1e-6)
                            self.logger.info(
                                f"Download progress: {mb:.1f} MiB / {tmb:.1f} MiB ({pct:.1f}%) @ {rate/(1024**2):.1f} MiB/s"
                            )
                        else:
                            rate = (got - last_bytes) / max(now - last_log, 1e-6)
                            self.logger.info(f"Download progress: {mb:.1f} MiB @ {rate/(1024**2):.1f} MiB/s")
                        last_log = now
                        last_bytes = got

        # Post-check: if total known, ensure size matches.
        if total:
            try:
                size = local_path.stat().st_size
            except Exception:
                size = 0
            if size != total:
                raise VMwareError(f"Download incomplete: expected {total} bytes, got {size} bytes")
    
    def download_datastore_file(
        self,
        *,
        datastore: str,
        ds_path: str,
        local_path: Path,
        dc_name: str = "ha-datacenter",
        chunk_size: int = 1024 * 1024,
        download_chain: bool = True,
        resume: bool = True,
    ) -> None:
        if not REQUESTS_AVAILABLE:
            raise VMwareError("requests not installed. Install: pip install requests")
    
        # Clean up and properly encode the parameters
        import urllib.parse
    
        # Clean the datastore name (remove any brackets if present)
        if datastore.startswith('[') and datastore.endswith(']'):
            datastore = datastore[1:-1]
    
        # Clean the ds_path (remove any leading/trailing spaces)
        ds_path = ds_path.strip()
    
        # Ensure dc_name doesn't have leading/trailing slashes and is properly encoded
        dc_name = dc_name.strip('/')
    
        # Remove datastore prefix if present in ds_path
        if ds_path.startswith(f"[{datastore}]"):
            ds_path = ds_path[len(f"[{datastore}]"):].strip()
    
        # URL encode the path components
        encoded_ds_path = urllib.parse.quote(ds_path.lstrip('/'))
        encoded_dc_path = urllib.parse.quote(dc_name)
        encoded_datastore = urllib.parse.quote(datastore)
    
        # Build URL - try different formats
        url = f"https://{self.host}/folder/{encoded_ds_path}?dcPath={encoded_dc_path}&dsName={encoded_datastore}"
    
        self.logger.debug(f"Download URL: {url}")
        self.logger.info(f"Downloading datastore file: [{datastore}] {ds_path} -> {local_path}")
        self.logger.info(f"Using dcPath: {dc_name}")
    
        headers = {"Cookie": self._session_cookie()}
        verify = not self.insecure
    
        try:
            # First, try to access the file to check if it exists
            test_url = f"https://{self.host}/folder/?dcPath={encoded_dc_path}&dsName={encoded_datastore}"
            test_headers = {"Cookie": self._session_cookie()}
        
            # List files in the directory to debug
            try:
                dir_path = os.path.dirname(ds_path)
                if dir_path:
                    list_url = f"https://{self.host}/folder/{urllib.parse.quote(dir_path)}?dcPath={encoded_dc_path}&dsName={encoded_datastore}"
                    self.logger.debug(f"Trying to list directory: {list_url}")
                    with requests.get(list_url, headers=test_headers, verify=verify, timeout=30) as r:
                        if r.status_code == 200:
                            self.logger.debug(f"Directory listing successful")
                        else:
                            self.logger.warning(f"Directory listing failed: {r.status_code}")
            except Exception as e:
                self.logger.debug(f"Directory listing attempt failed: {e}")
        
            # Try the actual download
            self._download_http(url=url, headers=headers, local_path=local_path, 
                               chunk_size=chunk_size, verify=verify, resume=resume)
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # Provide more detailed error information
                self.logger.error(f"File not found (404): {url}")
                self.logger.error(f"Parameters used:")
                self.logger.error(f"  - datastore: {datastore}")
                self.logger.error(f"  - ds_path: {ds_path}")
                self.logger.error(f"  - dc_name: {dc_name}")
                self.logger.error(f"  - host: {self.host}")
            
                # Try alternative URL format (without folder prefix)
                alt_url = f"https://{self.host}/{encoded_ds_path}?dcPath={encoded_dc_path}&dsName={encoded_datastore}"
                self.logger.error(f"Trying alternative URL format: {alt_url}")
            
                raise VMwareError(
                    f"File not found on vSphere server. Please check:\n"
                    f"1. The VM disk file exists in datastore '{datastore}'\n"
                    f"2. The path '{ds_path}' is correct\n"
                    f"3. The datacenter path '{dc_name}' is correct\n"
                    f"4. You have read permissions on the datastore\n\n"
                    f"Full error: {e}"
                )
            else:
                raise VMwareError(f"HTTP error during download: {e}")
        except Exception as e:
            # Don't delete partially downloaded files when resume is enabled; they may be useful.
            if not resume and local_path.exists():
                try:
                    local_path.unlink()
                except Exception:
                    pass
            raise VMwareError(f"Download failed: {e}")

        # Automatically download chain (parents and extents)
        if download_chain and self._is_text_descriptor(local_path):
            parent_rel, extent_rels = self._parse_parents_and_extents(local_path)

            # Parent recursion (snapshot chain)
            if parent_rel:
                parent_ds_path = os.path.normpath(os.path.join(os.path.dirname(ds_path), parent_rel))
                parent_local = local_path.parent / Path(parent_rel).name
                if not parent_local.exists():
                    self.logger.info(f"Downloading parent VMDK: [{datastore}] {parent_ds_path} -> {parent_local}")
                    self.download_datastore_file(
                        datastore=datastore,
                        ds_path=parent_ds_path,
                        local_path=parent_local,
                        dc_name=dc_name,
                        chunk_size=chunk_size,
                        download_chain=True,
                        resume=resume,
                    )

            # Extents: download all referenced files (non-recursive)
            if not extent_rels:
                # common convention fallback
                extent_rels = [f"{local_path.stem}-flat.vmdk"]
                self.logger.warning(
                    "No extent lines parsed from descriptor; falling back to '-flat.vmdk' convention. "
                    "If this is a sparse/stream-optimized disk, consider exporting as OVA/OVF instead."
                )

            for extent_rel in extent_rels:
                extent_ds_path = os.path.normpath(os.path.join(os.path.dirname(ds_path), extent_rel))
                extent_local = local_path.parent / Path(extent_rel).name
                if extent_local.exists() and extent_local.stat().st_size > 0:
                    continue
                self.logger.info(f"Downloading extent: [{datastore}] {extent_ds_path} -> {extent_local}")
                self.download_datastore_file(
                    datastore=datastore,
                    ds_path=extent_ds_path,
                    local_path=extent_local,
                    dc_name=dc_name,
                    chunk_size=chunk_size,
                    download_chain=False,
                    resume=resume,
                )
    def download_vm_files(self, 
                         vm,
                         output_dir: Path,
                         include_disks: bool = True,
                         include_config: bool = True,
                         include_logs: bool = False) -> Dict[str, Path]:
        """Download all files associated with a VM."""
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        downloaded_files = {}
        
        # Download VM configuration
        if include_config:
            config_path = output_dir / f"{vm.name}.vmx"
            with open(config_path, 'w') as f:
                f.write(vm.config)
            downloaded_files['config'] = config_path
        
        # Download disks
        if include_disks:
            for device in vm.config.hardware.device:
                if isinstance(device, vim.vm.device.VirtualDisk):
                    backing = device.backing
                    if backing and backing.fileName:
                        datastore, ds_path = self.parse_backing_filename(backing.fileName)
                        
                        disk_filename = Path(ds_path).name
                        disk_path = output_dir / disk_filename
                        
                        dc_name = self.get_dc_path(vm)
                        
                        self.download_datastore_file(
                            datastore=datastore,
                            ds_path=ds_path,
                            local_path=disk_path,
                            dc_name=dc_name,
                            chunk_size=1024 * 1024,
                            download_chain=True,
                            resume=True
                        )
                        
                        downloaded_files[f"disk_{device.key}"] = disk_path
        
        # Download logs (if any)
        if include_logs and hasattr(vm, 'layoutEx') and vm.layoutEx.file:
            for file in vm.layoutEx.file:
                if file.type == 'logFile':
                    datastore, ds_path = self.parse_backing_filename(file.name)
                    log_filename = Path(ds_path).name
                    log_path = output_dir / log_filename
                    
                    dc_name = self.get_dc_path(vm)
                    
                    self.download_datastore_file(
                        datastore=datastore,
                        ds_path=ds_path,
                        local_path=log_path,
                        dc_name=dc_name,
                        chunk_size=1024 * 1024,
                        download_chain=False,
                        resume=True
                    )
                    
                    downloaded_files[f"log_{log_filename}"] = log_path
        
        return downloaded_files

    def create_snapshot(self, vm, name: str, *, quiesce: bool = True, memory: bool = False, description: str = "Created by vmdk2kvm") -> Any:
        task = vm.CreateSnapshot_Task(name=name, description=description, memory=memory, quiesce=quiesce)
        self.wait_for_task(task)
        return task.info.result

    def get_snapshots(self, vm):
        """Get all snapshots for a VM."""
        if not vm.snapshot:
            return []
        
        snapshots = []
        
        def traverse_snapshots(snapshot_tree):
            snapshots.append(snapshot_tree.snapshot)
            for child in snapshot_tree.childSnapshotList:
                traverse_snapshots(child)
        
        traverse_snapshots(vm.snapshot.rootSnapshotList[0])
        return snapshots
    
    def get_snapshot_by_name(self, vm, name: str):
        """Get snapshot by name."""
        for snapshot in self.get_snapshots(vm):
            if snapshot.name == name:
                return snapshot
        return None
    
    def revert_to_snapshot(self, vm, snapshot) -> None:
        """Revert VM to a specific snapshot."""
        task = snapshot.RevertToSnapshot_Task()
        self.wait_for_task(task)
    
    def revert_to_current_snapshot(self, vm) -> None:
        """Revert VM to current snapshot."""
        task = vm.RevertToCurrentSnapshot_Task()
        self.wait_for_task(task)
    
    def remove_all_snapshots(self, vm) -> None:
        """Remove all snapshots from a VM."""
        task = vm.RemoveAllSnapshots_Task()
        self.wait_for_task(task)
    
    def consolidate_snapshots(self, vm) -> None:
        """Consolidate snapshots to improve VM performance."""
        task = vm.ConsolidateVMDisks_Task()
        self.wait_for_task(task)

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

    # Enhancement: Automatically discover dcPath by traversing VM hierarchy
    def get_dc_path(self, vm) -> str:
        obj = vm.parent
        while obj and not isinstance(obj, vim.Datacenter):
            obj = obj.parent
        dc_path = obj.name if obj else "ha-datacenter"
        self.logger.debug(f"Discovered dc_path for VM '{vm.name}': {dc_path}")
        return dc_path
    
    # ==================== VM POWER OPERATIONS ====================
    
    def power_on_vm(self, vm) -> None:
        """Power on a VM."""
        task = vm.PowerOnVM_Task()
        self.wait_for_task(task)
    
    def power_off_vm(self, vm, force: bool = False) -> None:
        """Power off a VM."""
        if force:
            task = vm.PowerOffVM_Task()
        else:
            task = vm.ShutdownGuest()
        self.wait_for_task(task)
    
    def reset_vm(self, vm) -> None:
        """Reset a VM."""
        task = vm.ResetVM_Task()
        self.wait_for_task(task)
    
    def suspend_vm(self, vm) -> None:
        """Suspend a VM."""
        task = vm.SuspendVM_Task()
        self.wait_for_task(task)
    
    def reboot_vm_guest(self, vm) -> None:
        """Reboot VM guest OS (requires VMware Tools)."""
        if not vm.guest or not vm.guest.toolsRunningStatus == "guestToolsRunning":
            raise VMwareError("VMware Tools not running")
        task = vm.RebootGuest()
        self.wait_for_task(task)
    
    # ==================== DISK MANAGEMENT ====================
    
    def add_disk(self, 
                vm, 
                size_gb: int, 
                thin_provision: bool = True,
                disk_type: str = "thin") -> None:
        """Add a new disk to VM."""
        
        spec = vim.vm.ConfigSpec()
        device_changes = []
        
        # Find SCSI controller
        scsi_ctl = None
        for dev in vm.config.hardware.device:
            if isinstance(dev, vim.vm.device.VirtualSCSIController):
                scsi_ctl = dev
                break
        
        if not scsi_ctl:
            raise VMwareError("No SCSI controller found")
        
        # Find next available unit number
        used_unit_numbers = []
        for dev in vm.config.hardware.device:
            if hasattr(dev, 'unitNumber'):
                used_unit_numbers.append(dev.unitNumber)
        
        unit_number = 0
        while unit_number in used_unit_numbers:
            unit_number += 1
        
        # Create disk spec
        disk_spec = vim.vm.device.VirtualDeviceSpec()
        disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        
        disk_spec.device = vim.vm.device.VirtualDisk()
        disk_spec.device.key = -1
        disk_spec.device.unitNumber = unit_number
        disk_spec.device.capacityInKB = size_gb * 1024 * 1024
        disk_spec.device.controllerKey = scsi_ctl.key
        
        # Set backing info based on disk type
        if disk_type == "thin":
            disk_spec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
            disk_spec.device.backing.thinProvisioned = thin_provision
            disk_spec.device.backing.diskMode = vim.vm.device.VirtualDiskOption.DiskMode.persistent
        elif disk_type == "eagerzeroedthick":
            disk_spec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
            disk_spec.device.backing.eagerlyScrub = True
            disk_spec.device.backing.thinProvisioned = False
            disk_spec.device.backing.diskMode = vim.vm.device.VirtualDiskOption.DiskMode.persistent
        elif disk_type == "thick":
            disk_spec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
            disk_spec.device.backing.thinProvisioned = False
            disk_spec.device.backing.eagerlyScrub = False
            disk_spec.device.backing.diskMode = vim.vm.device.VirtualDiskOption.DiskMode.persistent
        
        disk_spec.device.backing.fileName = f"[{vm.datastore[0].name}]"
        
        device_changes.append(disk_spec)
        spec.deviceChange = device_changes
        
        task = vm.ReconfigVM_Task(spec=spec)
        self.wait_for_task(task)
    
    def remove_disk(self, vm, disk) -> None:
        """Remove a disk from VM."""
        spec = vim.vm.ConfigSpec()
        
        disk_spec = vim.vm.device.VirtualDeviceSpec()
        disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.remove
        disk_spec.device = disk
        
        spec.deviceChange = [disk_spec]
        
        task = vm.ReconfigVM_Task(spec=spec)
        self.wait_for_task(task)
    
    def extend_disk(self, vm, disk, new_size_gb: int) -> None:
        """Extend an existing disk."""
        spec = vim.vm.ConfigSpec()
        
        disk_spec = vim.vm.device.VirtualDeviceSpec()
        disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
        disk_spec.device = disk
        disk_spec.device.capacityInKB = new_size_gb * 1024 * 1024
        
        spec.deviceChange = [disk_spec]
        
        task = vm.ReconfigVM_Task(spec=spec)
        self.wait_for_task(task)
    
    # ==================== DATACENTER & DATASTORE OPERATIONS ====================
    
    def get_datacenters(self):
        """Get all datacenters."""
        if not self.content:
            raise VMwareError("Not connected")
        
        return [obj for obj in self.content.rootFolder.childEntity 
                if isinstance(obj, vim.Datacenter)]
    
    def get_datastores(self, datacenter=None):
        """Get all datastores."""
        if not self.content:
            raise VMwareError("Not connected")
        
        datastores = []
        datacenters = self.get_datacenters() if not datacenter else [datacenter]
        
        for dc in datacenters:
            for ds in dc.datastoreFolder.childEntity:
                if isinstance(ds, vim.Datastore):
                    datastores.append(ds)
        
        return datastores
    
    def get_datastore_info(self, datastore) -> Dict[str, Any]:
        """Get detailed datastore information."""
        summary = datastore.summary
        return {
            'name': summary.name,
            'url': summary.url,
            'capacity_gb': summary.capacity / (1024**3),
            'free_space_gb': summary.freeSpace / (1024**3),
            'used_space_gb': (summary.capacity - summary.freeSpace) / (1024**3),
            'type': summary.type,
            'accessible': summary.accessible,
            'multiple_host_access': summary.multipleHostAccess,
            'maintenance_mode': summary.maintenanceMode,
        }
    
    def browse_datastore(self, 
                        datastore, 
                        path: str = "") -> List[Dict[str, Any]]:
        """Browse datastore files and directories."""
        browser = datastore.browser
        
        search_spec = vim.host.DatastoreBrowser.SearchSpec()
        search_spec.details = vim.host.DatastoreBrowser.FileInfo.Details()
        search_spec.details.fileType = True
        search_spec.details.fileSize = True
        search_spec.details.modification = True
        
        task = browser.Search(path, search_spec)
        self.wait_for_task(task)
        
        results = []
        for file_info in task.info.result:
            results.append({
                'path': file_info.path,
                'name': file_info.friendlyName,
                'type': 'directory' if file_info.fileType == 'directory' else 'file',
                'size': file_info.fileSize if hasattr(file_info, 'fileSize') else None,
                'modified': file_info.modification if hasattr(file_info, 'modification') else None,
            })
        
        return results
    
    def delete_datastore_file(self, datastore, path: str) -> None:
        """Delete a file from datastore."""
        browser = datastore.browser
        
        file_query = [vim.host.DatastoreBrowser.FileQuery()]
        search_spec = vim.host.DatastoreBrowser.SearchSpec(query=file_query)
        
        # First check if file exists
        task = browser.Search(path, search_spec)
        self.wait_for_task(task)
        
        if not task.info.result:
            raise VMwareError(f"File not found: {path}")
        
        # Delete the file
        task = browser.DeleteFile(path)
        self.wait_for_task(task)
    
    # ==================== HOST & CLUSTER OPERATIONS ====================
    
    def get_hosts(self, cluster=None):
        """Get all hosts or hosts in a cluster."""
        if not self.content:
            raise VMwareError("Not connected")
        
        hosts = []
        
        if cluster:
            hosts.extend(cluster.host)
        else:
            for dc in self.get_datacenters():
                for host in dc.hostFolder.childEntity:
                    if isinstance(host, vim.ClusterComputeResource):
                        hosts.extend(host.host)
                    elif isinstance(host, vim.HostSystem):
                        hosts.append(host)
        
        return hosts
    
    def get_clusters(self):
        """Get all clusters."""
        if not self.content:
            raise VMwareError("Not connected")
        
        clusters = []
        for dc in self.get_datacenters():
            for obj in dc.hostFolder.childEntity:
                if isinstance(obj, vim.ClusterComputeResource):
                    clusters.append(obj)
        
        return clusters
    
    def get_host_details(self, host) -> Dict[str, Any]:
        """Get detailed host information."""
        summary = host.summary
        hardware = host.hardware
        
        return {
            'name': host.name,
            'connection_state': host.runtime.connectionState,
            'power_state': host.runtime.powerState,
            'maintenance_mode': host.runtime.inMaintenanceMode,
            'model': hardware.systemInfo.model,
            'vendor': hardware.systemInfo.vendor,
            'cpu_model': hardware.cpuPkg[0].description if hardware.cpuPkg else None,
            'cpu_cores': summary.hardware.numCpuCores,
            'cpu_threads': summary.hardware.numCpuThreads,
            'memory_gb': summary.hardware.memorySize / (1024**3),
            'cpu_usage_mhz': summary.quickStats.overallCpuUsage,
            'memory_usage_mb': summary.quickStats.overallMemoryUsage,
            'uptime': host.summary.quickStats.uptime,
            'vm_count': len(host.vm),
        }
    
    # ==================== NETWORK OPERATIONS ====================
    
    def get_networks(self):
        """Get all networks."""
        if not self.content:
            raise VMwareError("Not connected")
        
        networks = []
        for dc in self.get_datacenters():
            for network in dc.networkFolder.childEntity:
                if isinstance(network, vim.Network):
                    networks.append(network)
        
        return networks
    
    def get_port_groups(self, distributed_switch=None):
        """Get port groups."""
        if not self.content:
            raise VMwareError("Not connected")
        
        port_groups = []
        
        if distributed_switch:
            port_groups.extend(distributed_switch.portgroup)
        else:
            for dc in self.get_datacenters():
                for obj in dc.networkFolder.childEntity:
                    if isinstance(obj, vim.DistributedVirtualSwitch):
                        port_groups.extend(obj.portgroup)
        
        return port_groups
    
    # ==================== TASK & EVENT MANAGEMENT ====================
    
    def get_recent_tasks(self, max_tasks: int = 100) -> List[Dict[str, Any]]:
        """Get recent tasks from the vCenter."""
        if not self.content:
            raise VMwareError("Not connected")
        
        task_manager = self.content.taskManager
        collector = task_manager.CreateCollectorForTasks(
            filter=vim.TaskFilterSpec()
        )
        
        try:
            tasks = collector.ReadNextTasks(max_tasks)
            recent_tasks = []
            
            for task in tasks:
                recent_tasks.append({
                    'name': task.info.name,
                    'description_id': task.info.descriptionId,
                    'entity_name': task.info.entityName,
                    'state': task.info.state,
                    'progress': getattr(task.info, 'progress', None),
                    'queued_time': task.info.queueTime,
                    'start_time': task.info.startTime,
                    'complete_time': task.info.completeTime,
                    'cancelled': task.info.cancelled,
                })
            
            return recent_tasks
        finally:
            collector.DestroyCollector()
    
    def get_recent_events(self, max_events: int = 100) -> List[Dict[str, Any]]:
        """Get recent events from the vCenter."""
        if not self.content:
            raise VMwareError("Not connected")
        
        event_manager = self.content.eventManager
        collector = event_manager.CreateCollectorForEvents(
            filter=vim.EventFilterSpec()
        )
        
        try:
            events = collector.ReadNextEvents(max_events)
            recent_events = []
            
            for event in events:
                recent_events.append({
                    'type': type(event).__name__,
                    'created_time': event.createdTime,
                    'user_name': event.userName,
                    'full_formatted_message': event.fullFormattedMessage,
                    'vm_name': getattr(event, 'vm', None).name if hasattr(event, 'vm') and event.vm else None,
                    'host_name': getattr(event, 'host', None).name if hasattr(event, 'host') and event.host else None,
                    'datacenter_name': getattr(event, 'datacenter', None).name if hasattr(event, 'datacenter') and event.datacenter else None,
                })
            
            return recent_events
        finally:
            collector.DestroyCollector()

# Add this method to the VMwareClient class in vmware_client.py:

def validate_file_location(self, datastore: str, ds_path: str, dc_name: str = "ha-datacenter") -> Dict[str, Any]:
    """Validate that a file exists on the datastore and return information about it."""
    import urllib.parse
    
    if not REQUESTS_AVAILABLE:
        raise VMwareError("requests not installed")
    
    # Clean parameters
    if datastore.startswith('[') and datastore.endswith(']'):
        datastore = datastore[1:-1]
    
    ds_path = ds_path.strip()
    dc_name = dc_name.strip('/')
    
    if ds_path.startswith(f"[{datastore}]"):
        ds_path = ds_path[len(f"[{datastore}]"):].strip()
    
    # Build test URLs
    encoded_ds_path = urllib.parse.quote(ds_path.lstrip('/'))
    encoded_dc_path = urllib.parse.quote(dc_name)
    encoded_datastore = urllib.parse.quote(datastore)
    
    test_urls = [
        f"https://{self.host}/folder/{encoded_ds_path}?dcPath={encoded_dc_path}&dsName={encoded_datastore}",
        f"https://{self.host}/{encoded_ds_path}?dcPath={encoded_dc_path}&dsName={encoded_datastore}",
    ]
    
    headers = {"Cookie": self._session_cookie()}
    verify = not self.insecure
    
    results = {}
    
    for i, url in enumerate(test_urls):
        self.logger.debug(f"Testing URL format {i+1}: {url}")
        try:
            # Try HEAD request first
            response = requests.head(url, headers=headers, verify=verify, timeout=30, allow_redirects=True)
            results[f"format_{i+1}"] = {
                "url": url,
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "success": response.status_code == 200
            }
            
            # If HEAD works, try GET for more info
            if response.status_code == 200:
                # Try a small GET to verify
                get_response = requests.get(url, headers=headers, verify=verify, timeout=30, stream=True)
                get_response.close()
                results[f"format_{i+1}"]["get_status"] = get_response.status_code
                results[f"format_{i+1}"]["content_length"] = get_response.headers.get('content-length')
            
        except Exception as e:
            results[f"format_{i+1}"] = {
                "url": url,
                "error": str(e),
                "success": False
            }
    
    # Also try to list the parent directory
    if ds_path:
        parent_dir = os.path.dirname(ds_path)
        if parent_dir:
            encoded_parent = urllib.parse.quote(parent_dir.lstrip('/'))
            dir_url = f"https://{self.host}/folder/{encoded_parent}?dcPath={encoded_dc_path}&dsName={encoded_datastore}"
            try:
                dir_response = requests.get(dir_url, headers=headers, verify=verify, timeout=30)
                if dir_response.status_code == 200:
                    results["directory_listing"] = "Available"
                else:
                    results["directory_listing"] = f"Failed: {dir_response.status_code}"
            except Exception as e:
                results["directory_listing"] = f"Error: {e}"
    
    return results

# In vsphere_mode.py, update the _handle_download_action method:

def _handle_download_action(self, client: VMwareClient) -> int:
    """Handle download action with pre-validation."""
    if not self.args.vm_name:
        raise Fatal(2, "vsphere: --vm-name is required for download")
    
    vm = client.get_vm_by_name(self.args.vm_name)
    if not vm:
        raise Fatal(2, f"vsphere: VM not found: {self.args.vm_name}")

    disk = client.select_disk(vm, self.args.disk)
    label = getattr(getattr(disk, "deviceInfo", None), "label", "disk")
    backing = getattr(disk, "backing", None)
    file_name = getattr(backing, "fileName", None)
    if not file_name:
        raise Fatal(2, "vsphere: could not read disk backing filename")

    datastore, ds_path = client.parse_backing_filename(file_name)

    out = Path(self.args.out).expanduser() if self.args.out else Path(f"{vm.name}-{label}.vmdk")
    out = out.resolve()

    # Enhancement: Use automatic dc_path if not provided by user
    dc_name = getattr(self.args, "dc_name", None)
    if not dc_name:
        dc_name = client.get_dc_path(vm)
    else:
        self.logger.info(f"Using user-provided dc_name: {dc_name} (overriding automatic discovery)")
    
    # Validate file location before attempting download
    self.logger.info("Validating file location on vSphere...")
    try:
        validation = client.validate_file_location(datastore, ds_path, dc_name)
        
        # Check if any URL format worked
        working_formats = []
        for key, result in validation.items():
            if key.startswith("format_") and result.get("success"):
                working_formats.append(key)
        
        if not working_formats:
            self.logger.error("File validation failed. Available information:")
            for key, result in validation.items():
                if key.startswith("format_"):
                    self.logger.error(f"  {key}: Status {result.get('status_code', 'N/A')}")
                    if "error" in result:
                        self.logger.error(f"    Error: {result['error']}")
            
            raise Fatal(2, 
                f"File not found or inaccessible:\n"
                f"  Datastore: {datastore}\n"
                f"  Path: {ds_path}\n"
                f"  Datacenter path: {dc_name}\n"
                f"  VM: {vm.name}\n\n"
                f"Try:\n"
                f"  1. Check if the VM is powered off\n"
                f"  2. Verify you have read permissions on the datastore\n"
                f"  3. Try a different dcPath (use --dc-name)\n"
                f"  4. Manually verify the file exists in vSphere Client"
            )
        
        self.logger.info(f"File validation successful ({len(working_formats)} URL formats work)")
        
    except Exception as e:
        self.logger.warning(f"File validation skipped: {e}")

    # Proceed with download
    client.download_datastore_file(
        datastore=datastore,
        ds_path=ds_path,
        local_path=out,
        dc_name=dc_name,
        chunk_size=int(getattr(self.args, "chunk_size", 1024 * 1024)),
        resume=bool(getattr(self.args, "resume", True)),
    )

    # Post-flight sanity: if descriptor, ensure at least one extent exists locally.
    try:
        if client._is_text_descriptor(out):
            parent_rel, extent_rels = client._parse_parents_and_extents(out)
            # if parser didn't find extents, fall back to flat convention (client does this too)
            if not extent_rels:
                extent_rels = [f"{out.stem}-flat.vmdk"]

            missing = []
            for er in extent_rels:
                ep = out.parent / Path(er).name
                if not ep.exists() or ep.stat().st_size == 0:
                    missing.append(ep.name)
            if missing:
                raise Fatal(
                    2,
                    "vsphere: downloaded descriptor but missing extent file(s): "
                    + ", ".join(missing)
                    + ". This usually means parsing failed or permissions prevented fetching the extent. "
                    "Try again with a larger --chunk-size, ensure datastore path is accessible, or export as OVA/OVF.",
                )
    except Fatal:
        raise
    except Exception as e:
        self.logger.debug(f"Post-download validation skipped/failed: {e}")

    return 0