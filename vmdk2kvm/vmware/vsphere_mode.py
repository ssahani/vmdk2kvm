from __future__ import annotations

import argparse
import os
from pathlib import Path
import logging
import json

from ..core.exceptions import Fatal
from .vmware_client import VMwareClient, REQUESTS_AVAILABLE
from ..core.exceptions import VMwareError
import requests
from pyVmomi import vim, vmodl


class VsphereMode:
    """CLI entry for vSphere actions: scan / download / cbt-sync."""

    def __init__(self, logger: logging.Logger, args: argparse.Namespace):
        self.logger = logger
        self.args = args

    # ✅ ADDITIVE: centralized dc_name resolution (no behavior change)
    def _dc_name(self) -> str:
        """
        Resolve datacenter name safely.

        Behavior (unchanged):
          - If user supplied --dc-name, use it
          - Else default to 'ha-datacenter'

        This avoids scattered getattr(...) usage and prevents blank dcPath bugs.
        """
        v = getattr(self.args, "dc_name", None)
        return v if v else "ha-datacenter"

    def run(self) -> int:
        vc_host = self.args.vcenter
        vc_user = self.args.vc_user
        vc_pass = self.args.vc_password

        if not vc_pass and getattr(self.args, "vc_password_env", None):
            vc_pass = os.environ.get(self.args.vc_password_env)
        
        # ✅ normalize whitespace to prevent accidental InvalidLogin
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

            if action == "list_vm_names":
                try:
                    content = client._content()
                    # Create container view
                    container = content.rootFolder
                    viewType = [vim.VirtualMachine]
                    recursive = True
                    containerView = content.viewManager.CreateContainerView(container, viewType, recursive)
                    try:
                        # Traversal spec
                        traversal = vmodl.query.PropertyCollector.TraversalSpec(
                            name="traverseEntities",
                            type=vim.view.ContainerView,
                            path="view",
                            skip=False,
                        )
                        # Object spec
                        obj_spec = vmodl.query.PropertyCollector.ObjectSpec(
                            obj=containerView,
                            skip=True,
                            selectSet=[traversal],
                        )
                        # Property specs
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
                        # Filter spec
                        filter_spec = vmodl.query.PropertyCollector.FilterSpec(
                            propSet=[property_spec],
                            objectSet=[obj_spec],
                        )
                        # Retrieve
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

                # Find snapshot by name
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
                change_id = self.args.change_id
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

                    # NOTE: your argparse must define change_id for cbt_sync, or this will throw.
                    # We preserve existing behavior here; if missing, you'll get AttributeError.
                    changed = client.query_changed_disk_areas(
                        vm,
                        snapshot=snap,
                        device_key=device_key,
                        start_offset=0,
                        change_id=self.args.change_id,
                    )

                    if not getattr(changed, "changedDiskAreas", None):
                        self.logger.info("No changed blocks reported by CBT")
                        num_ranges = 0
                        done = 0
                    else:
                        num_ranges = len(changed.changedDiskAreas)

                        # apply ranges via HTTP Range requests
                        if not REQUESTS_AVAILABLE:
                            raise Fatal(2, "requests not installed. Install: pip install requests")

                        dc_name = self._dc_name()  # ✅ NEW: safe default
                        url = f"https://{vc_host}/folder/{ds_path}?dcPath={dc_name}&dsName={datastore}"
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
                        # best-effort cleanup
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
