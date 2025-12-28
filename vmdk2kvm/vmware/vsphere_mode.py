from __future__ import annotations
import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path
import logging

from ..core.exceptions import Fatal
from .vmware_client import VMwareClient, REQUESTS_AVAILABLE
from ..core.exceptions import VMwareError
import requests

class VsphereMode:
    """Enhanced CLI entry for vSphere actions with comprehensive operations."""
    
    def __init__(self, logger: logging.Logger, args: argparse.Namespace):
        self.logger = logger
        self.args = args
    
    def run(self) -> int:
        vc_host = self.args.vcenter
        vc_user = self.args.vc_user
        vc_pass = self.args.vc_password

        if not vc_pass and getattr(self.args, "vc_password_env", None):
            vc_pass = os.environ.get(self.args.vc_password_env)
            if not vc_pass:
                self.logger.error(
                    f"vsphere: environment variable '{self.args.vc_password_env}' is not set or empty"
                )

        if not vc_host or not vc_user or not vc_pass:
            raise Fatal(2, "vsphere: --vcenter, --vc-user, and --vc-password (or --vc-password-env) are required")

        # Add timeout parameter if provided
        timeout = getattr(self.args, 'timeout', 30)
        client = VMwareClient(self.logger, vc_host, vc_user, vc_pass, 
                             port=self.args.vc_port, 
                             insecure=self.args.vc_insecure,
                             timeout=timeout)
        client.connect()
        
        try:
            action = (self.args.vs_action or "scan").lower()
            
            # ==================== SCAN ACTION ====================
            if action == "scan":
                return self._handle_scan_action(client)
            
            # ==================== LIST ACTION ====================
            elif action == "list":
                return self._handle_list_action(client)
            
            # ==================== INFO ACTION ====================
            elif action == "info":
                return self._handle_info_action(client)
            
            # ==================== DOWNLOAD ACTION ====================
            elif action == "download":
                return self._handle_download_action(client)
            
            # ==================== CBT-SYNC ACTION ====================
            elif action == "cbt-sync":
                return self._handle_cbt_sync_action(client)
            
            # ==================== POWER OPERATIONS ====================
            elif action in ["power-on", "power-off", "reset", "suspend", "reboot"]:
                return self._handle_power_action(client, action)
            
            # ==================== SNAPSHOT OPERATIONS ====================
            elif action in ["snapshot-create", "snapshot-list", "snapshot-revert", 
                          "snapshot-remove", "snapshot-consolidate"]:
                return self._handle_snapshot_action(client, action)
            
            # ==================== DISK OPERATIONS ====================
            elif action in ["disk-add", "disk-remove", "disk-extend"]:
                return self._handle_disk_action(client, action)
            
            # ==================== DATASTORE OPERATIONS ====================
            elif action in ["datastore-list", "datastore-browse", "datastore-info"]:
                return self._handle_datastore_action(client, action)
            
            # ==================== HOST OPERATIONS ====================
            elif action in ["host-list", "host-info"]:
                return self._handle_host_action(client, action)
            
            # ==================== TASK OPERATIONS ====================
            elif action in ["task-list", "event-list"]:
                return self._handle_task_event_action(client, action)
            
            # ==================== DOWNLOAD VM FILES ====================
            elif action == "download-vm-files":
                return self._handle_download_vm_files_action(client)
            
            else:
                raise Fatal(2, f"vsphere: unknown action: {action}")
        
        finally:
            client.disconnect()
    
    # ==================== ACTION HANDLERS ====================
    
    def _handle_scan_action(self, client: VMwareClient) -> int:
        """Handle scan action - list VMs."""
        names = client.list_vm_names()
        self.logger.info(f"VMs found: {len(names)}")
        for n in names:
            print(n)
        return 0
    
    def _handle_list_action(self, client: VMwareClient) -> int:
        """Handle list action - comprehensive VM listing."""
        list_type = getattr(self.args, "list_type", "vms").lower()
        
        if list_type == "vms":
            vms = client.get_all_vms()
            self.logger.info(f"VMs found: {len(vms)}")
            for vm in vms:
                power_state = vm.runtime.powerState if vm.runtime else "unknown"
                guest_ip = vm.guest.ipAddress if vm.guest and vm.guest.ipAddress else "N/A"
                print(f"{vm.name:<40} {power_state:<10} {guest_ip:<15}")
        
        elif list_type == "datacenters":
            datacenters = client.get_datacenters()
            self.logger.info(f"Datacenters found: {len(datacenters)}")
            for dc in datacenters:
                print(f"{dc.name}")
        
        elif list_type == "datastores":
            datastores = client.get_datastores()
            self.logger.info(f"Datastores found: {len(datastores)}")
            for ds in datastores:
                info = client.get_datastore_info(ds)
                print(f"{ds.name:<30} {info['capacity_gb']:.1f} GB {info['free_space_gb']:.1f} GB free")
        
        elif list_type == "hosts":
            hosts = client.get_hosts()
            self.logger.info(f"Hosts found: {len(hosts)}")
            for host in hosts:
                details = client.get_host_details(host)
                print(f"{host.name:<30} {details['cpu_cores']:<4} cores {details['memory_gb']:.1f} GB")
        
        elif list_type == "clusters":
            clusters = client.get_clusters()
            self.logger.info(f"Clusters found: {len(clusters)}")
            for cluster in clusters:
                print(f"{cluster.name}")
        
        else:
            raise Fatal(2, f"Unknown list type: {list_type}")
        
        return 0
    
    def _handle_info_action(self, client: VMwareClient) -> int:
        """Handle info action - detailed VM information."""
        if not self.args.vm_name:
            raise Fatal(2, "vsphere: --vm-name is required for info action")
        
        vm = client.get_vm_by_name(self.args.vm_name)
        if not vm:
            raise Fatal(2, f"vsphere: VM not found: {self.args.vm_name}")
        
        details = client.get_vm_details(vm)
        
        print(f"\n=== VM Information: {details['name']} ===")
        print(f"UUID: {details['uuid']}")
        print(f"Guest OS: {details['guest_id']}")
        print(f"Power State: {details['power_state']}")
        print(f"Connection State: {details['connection_state']}")
        print(f"Tools Status: {details['tools_status']}")
        print(f"IP Address: {details['ip_address']}")
        print(f"Host Name: {details['host_name']}")
        print(f"Memory: {details['memory_mb']} MB")
        print(f"vCPUs: {details['num_cpu']}")
        print(f"Folder: {details['folder']}")
        print(f"Resource Pool: {details['resource_pool']}")
        print(f"Annotation: {details['annotation']}")
        
        print(f"\n=== Datastores ===")
        for ds in details['datastore']:
            print(f"  - {ds}")
        
        print(f"\n=== Networks ===")
        for net in details['network']:
            print(f"  - {net}")
        
        print(f"\n=== Disks ===")
        for i, disk in enumerate(details['disks']):
            print(f"  Disk {i}:")
            print(f"    Label: {disk['label']}")
            print(f"    Capacity: {disk['capacity_mb']:.1f} MB")
            print(f"    Backing Type: {disk['backing_type']}")
            print(f"    Thin Provisioned: {disk['thin_provisioned']}")
            print(f"    File: {disk['file_name']}")
        
        return 0
    
    def _handle_download_action(self, client: VMwareClient) -> int:
        """Handle download action."""
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
    
    def _handle_cbt_sync_action(self, client: VMwareClient) -> int:
        """Handle CBT sync action."""
        if not self.args.vm_name:
            raise Fatal(2, "vsphere: --vm-name is required for cbt-sync")
        
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

        local_disk = out
        if not local_disk.exists():
            raise Fatal(2, f"vsphere: local disk file does not exist for cbt-sync: {local_disk}")

        if self.args.enable_cbt:
            client.enable_cbt(vm)

        snap_name = self.args.snapshot_name or "vmdk2kvm-cbt"
        snap = client.create_snapshot(vm, snap_name, quiesce=True, memory=False)
        
        try:
            changed = client.query_changed_disk_areas(vm, snapshot=snap, device_key=disk.key, start_offset=0, change_id="*")
            if not getattr(changed, "diskAreas", None):
                self.logger.info("No changed blocks reported by CBT")
                return 0

            # apply ranges via HTTP Range requests
            if not REQUESTS_AVAILABLE:
                raise Fatal(2, "requests not installed. Install: pip install requests")

            url = f"https://{client.host}/folder/{ds_path}?dcPath={dc_name}&dsName={datastore}"
            headers = {"Cookie": client._session_cookie()}
            verify = not client.insecure

            total = sum(int(a.length) for a in changed.diskAreas)
            done = 0
            self.logger.info(f"Syncing {len(changed.diskAreas)} ranges ({total/(1024**2):.1f} MiB)")

            with open(local_disk, "rb+") as f:
                for a in changed.diskAreas:
                    start = int(a.start)
                    length = int(a.length)
                    end = start + length - 1
                    h = dict(headers)
                    h["Range"] = f"bytes={start}-{end}"
                    # Stream to avoid holding large ranges in memory.
                    with requests.get(url, headers=h, verify=verify, stream=True, timeout=120) as r:
                        r.raise_for_status()
                        f.seek(start)
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if not chunk:
                                continue
                            f.write(chunk)
                    done += length
                    if total:
                        self.logger.debug(f"CBT sync: {done/(1024**2):.1f} MiB / {total/(1024**2):.1f} MiB ({(done/total)*100:.1f}%)")
            self.logger.info("CBT sync completed")
            return 0
        finally:
            try:
                # best-effort cleanup
                task = snap.RemoveSnapshot_Task(removeChildren=False)
                client.wait_for_task(task)
            except Exception:
                pass
    
    def _handle_power_action(self, client: VMwareClient, action: str) -> int:
        """Handle power operations."""
        if not self.args.vm_name:
            raise Fatal(2, f"vsphere: --vm-name is required for {action}")
        
        vm = client.get_vm_by_name(self.args.vm_name)
        if not vm:
            raise Fatal(2, f"vsphere: VM not found: {self.args.vm_name}")
        
        if action == "power-on":
            client.power_on_vm(vm)
            self.logger.info(f"Powered on VM: {vm.name}")
        
        elif action == "power-off":
            force = getattr(self.args, "force", False)
            client.power_off_vm(vm, force=force)
            self.logger.info(f"Powered off VM: {vm.name}")
        
        elif action == "reset":
            client.reset_vm(vm)
            self.logger.info(f"Reset VM: {vm.name}")
        
        elif action == "suspend":
            client.suspend_vm(vm)
            self.logger.info(f"Suspended VM: {vm.name}")
        
        elif action == "reboot":
            client.reboot_vm_guest(vm)
            self.logger.info(f"Rebooted VM guest: {vm.name}")
        
        return 0
    
    def _handle_snapshot_action(self, client: VMwareClient, action: str) -> int:
        """Handle snapshot operations."""
        if not self.args.vm_name:
            raise Fatal(2, f"vsphere: --vm-name is required for {action}")
        
        vm = client.get_vm_by_name(self.args.vm_name)
        if not vm:
            raise Fatal(2, f"vsphere: VM not found: {self.args.vm_name}")
        
        if action == "snapshot-create":
            name = getattr(self.args, "snapshot_name", f"snapshot-{datetime.now().strftime('%Y%m%d-%H%M%S')}")
            description = getattr(self.args, "snapshot_description", "Created by vmdk2kvm")
            quiesce = getattr(self.args, "quiesce", True)
            memory = getattr(self.args, "memory", False)
            
            snapshot = client.create_snapshot(vm, name, quiesce=quiesce, memory=memory, description=description)
            self.logger.info(f"Created snapshot '{name}' for VM: {vm.name}")
        
        elif action == "snapshot-list":
            snapshots = client.get_snapshots(vm)
            self.logger.info(f"Snapshots for VM '{vm.name}': {len(snapshots)}")
            for snapshot in snapshots:
                print(f"{snapshot.name} - {snapshot.description}")
        
        elif action == "snapshot-revert":
            snapshot_name = getattr(self.args, "snapshot_name", None)
            if not snapshot_name:
                raise Fatal(2, "vsphere: --snapshot-name is required for snapshot-revert")
            
            snapshot = client.get_snapshot_by_name(vm, snapshot_name)
            if not snapshot:
                raise Fatal(2, f"Snapshot not found: {snapshot_name}")
            
            client.revert_to_snapshot(vm, snapshot)
            self.logger.info(f"Reverted VM '{vm.name}' to snapshot '{snapshot_name}'")
        
        elif action == "snapshot-remove":
            if getattr(self.args, "remove_all", False):
                client.remove_all_snapshots(vm)
                self.logger.info(f"Removed all snapshots from VM: {vm.name}")
            else:
                snapshot_name = getattr(self.args, "snapshot_name", None)
                if not snapshot_name:
                    raise Fatal(2, "vsphere: --snapshot-name is required for snapshot-remove (unless --remove-all)")
                
                snapshot = client.get_snapshot_by_name(vm, snapshot_name)
                if not snapshot:
                    raise Fatal(2, f"Snapshot not found: {snapshot_name}")
                
                task = snapshot.RemoveSnapshot_Task(removeChildren=getattr(self.args, "remove_children", False))
                client.wait_for_task(task)
                self.logger.info(f"Removed snapshot '{snapshot_name}' from VM: {vm.name}")
        
        elif action == "snapshot-consolidate":
            client.consolidate_snapshots(vm)
            self.logger.info(f"Consolidated snapshots for VM: {vm.name}")
        
        return 0
    
    def _handle_disk_action(self, client: VMwareClient, action: str) -> int:
        """Handle disk operations."""
        if not self.args.vm_name:
            raise Fatal(2, f"vsphere: --vm-name is required for {action}")
        
        vm = client.get_vm_by_name(self.args.vm_name)
        if not vm:
            raise Fatal(2, f"vsphere: VM not found: {self.args.vm_name}")
        
        if action == "disk-add":
            size_gb = getattr(self.args, "size_gb", 20)
            disk_type = getattr(self.args, "disk_type", "thin")
            thin_provision = getattr(self.args, "thin_provision", True)
            
            client.add_disk(vm, size_gb, thin_provision=thin_provision, disk_type=disk_type)
            self.logger.info(f"Added {size_gb}GB {disk_type} disk to VM: {vm.name}")
        
        elif action == "disk-remove":
            disk_index = getattr(self.args, "disk_index", None)
            if disk_index is None:
                raise Fatal(2, "vsphere: --disk-index is required for disk-remove")
            
            disks = client.vm_disks(vm)
            if disk_index < 0 or disk_index >= len(disks):
                raise Fatal(2, f"Disk index out of range: {disk_index} (found {len(disks)})")
            
            disk = disks[disk_index]
            client.remove_disk(vm, disk)
            self.logger.info(f"Removed disk {disk_index} from VM: {vm.name}")
        
        elif action == "disk-extend":
            disk_index = getattr(self.args, "disk_index", None)
            new_size_gb = getattr(self.args, "new_size_gb", None)
            
            if disk_index is None or new_size_gb is None:
                raise Fatal(2, "vsphere: --disk-index and --new-size-gb are required for disk-extend")
            
            disks = client.vm_disks(vm)
            if disk_index < 0 or disk_index >= len(disks):
                raise Fatal(2, f"Disk index out of range: {disk_index} (found {len(disks)})")
            
            disk = disks[disk_index]
            client.extend_disk(vm, disk, new_size_gb)
            self.logger.info(f"Extended disk {disk_index} to {new_size_gb}GB on VM: {vm.name}")
        
        return 0
    
    def _handle_datastore_action(self, client: VMwareClient, action: str) -> int:
        """Handle datastore operations."""
        if action == "datastore-list":
            datastores = client.get_datastores()
            self.logger.info(f"Datastores found: {len(datastores)}")
            for ds in datastores:
                info = client.get_datastore_info(ds)
                print(f"{ds.name:<30} Capacity: {info['capacity_gb']:.1f} GB, "
                      f"Free: {info['free_space_gb']:.1f} GB, "
                      f"Type: {info['type']}, "
                      f"Accessible: {info['accessible']}")
        
        elif action == "datastore-info":
            datastore_name = getattr(self.args, "datastore_name", None)
            if not datastore_name:
                raise Fatal(2, "vsphere: --datastore-name is required for datastore-info")
            
            datastores = client.get_datastores()
            target_ds = None
            for ds in datastores:
                if ds.name == datastore_name:
                    target_ds = ds
                    break
            
            if not target_ds:
                raise Fatal(2, f"Datastore not found: {datastore_name}")
            
            info = client.get_datastore_info(target_ds)
            print(f"\n=== Datastore Information: {info['name']} ===")
            for key, value in info.items():
                print(f"{key.replace('_', ' ').title()}: {value}")
        
        elif action == "datastore-browse":
            datastore_name = getattr(self.args, "datastore_name", None)
            path = getattr(self.args, "path", "")
            
            if not datastore_name:
                raise Fatal(2, "vsphere: --datastore-name is required for datastore-browse")
            
            datastores = client.get_datastores()
            target_ds = None
            for ds in datastores:
                if ds.name == datastore_name:
                    target_ds = ds
                    break
            
            if not target_ds:
                raise Fatal(2, f"Datastore not found: {datastore_name}")
            
            items = client.browse_datastore(target_ds, path)
            self.logger.info(f"Found {len(items)} items in [{datastore_name}] {path}")
            for item in items:
                type_icon = "📁" if item['type'] == 'directory' else "📄"
                size_str = f"{item['size']/(1024**2):.1f} MB" if item['size'] else "N/A"
                print(f"{type_icon} {item['name']:<50} {size_str:<10} {item['modified']}")
        
        return 0
    
    def _handle_host_action(self, client: VMwareClient, action: str) -> int:
        """Handle host operations."""
        if action == "host-list":
            hosts = client.get_hosts()
            self.logger.info(f"Hosts found: {len(hosts)}")
            for host in hosts:
                details = client.get_host_details(host)
                print(f"{host.name:<30} {details['cpu_cores']:<4} cores "
                      f"{details['memory_gb']:.1f} GB RAM "
                      f"VMs: {details['vm_count']} "
                      f"State: {details['connection_state']}")
        
        elif action == "host-info":
            host_name = getattr(self.args, "host_name", None)
            if not host_name:
                raise Fatal(2, "vsphere: --host-name is required for host-info")
            
            hosts = client.get_hosts()
            target_host = None
            for host in hosts:
                if host.name == host_name:
                    target_host = host
                    break
            
            if not target_host:
                raise Fatal(2, f"Host not found: {host_name}")
            
            details = client.get_host_details(target_host)
            print(f"\n=== Host Information: {details['name']} ===")
            for key, value in details.items():
                print(f"{key.replace('_', ' ').title()}: {value}")
        
        return 0
    
    def _handle_task_event_action(self, client: VMwareClient, action: str) -> int:
        """Handle task and event operations."""
        max_items = getattr(self.args, "max_items", 100)
        
        if action == "task-list":
            tasks = client.get_recent_tasks(max_tasks=max_items)
            self.logger.info(f"Recent tasks: {len(tasks)}")
            for task in tasks:
                print(f"{task['name']:<40} {task['entity_name']:<30} "
                      f"{task['state']:<10} {task['progress'] or 'N/A':<5}% "
                      f"{task['queued_time']}")
        
        elif action == "event-list":
            events = client.get_recent_events(max_events=max_items)
            self.logger.info(f"Recent events: {len(events)}")
            for event in events:
                print(f"{event['created_time']} {event['type']:<30} "
                      f"{event['user_name']:<20} {event['full_formatted_message'][:50]}...")
        
        return 0
    
    def _handle_download_vm_files_action(self, client: VMwareClient) -> int:
        """Handle downloading all VM files."""
        if not self.args.vm_name:
            raise Fatal(2, "vsphere: --vm-name is required for download-vm-files")
        
        vm = client.get_vm_by_name(self.args.vm_name)
        if not vm:
            raise Fatal(2, f"vsphere: VM not found: {self.args.vm_name}")
        
        output_dir = Path(self.args.out).expanduser() if self.args.out else Path(f"{vm.name}-files")
        include_disks = getattr(self.args, "include_disks", True)
        include_config = getattr(self.args, "include_config", True)
        include_logs = getattr(self.args, "include_logs", False)
        
        self.logger.info(f"Downloading files for VM '{vm.name}' to {output_dir}")
        
        downloaded_files = client.download_vm_files(
            vm=vm,
            output_dir=output_dir,
            include_disks=include_disks,
            include_config=include_config,
            include_logs=include_logs
        )
        
        self.logger.info(f"Downloaded {len(downloaded_files)} files:")
        for file_type, file_path in downloaded_files.items():
            print(f"  {file_type}: {file_path}")
        
        return 0