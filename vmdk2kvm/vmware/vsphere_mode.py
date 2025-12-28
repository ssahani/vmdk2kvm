from __future__ import annotations
import argparse
import os
from pathlib import Path
import logging

from ..core.exceptions import Fatal
from .vmware_client import VMwareClient, REQUESTS_AVAILABLE
from ..core.exceptions import VMwareError
import requests
class VsphereMode:
    """CLI entry for vSphere actions: scan / download / cbt-sync."""

    def __init__(self, logger: logging.Logger, args: argparse.Namespace):
        self.logger = logger
        self.args = args

    def run(self) -> int:
        vc_host = self.args.vcenter
        vc_user = self.args.vc_user
        vc_pass = self.args.vc_password

        if not vc_pass and getattr(self.args, "vc_password_env", None):
            vc_pass = os.environ.get(self.args.vc_password_env)

        if not vc_host or not vc_user or not vc_pass:
            raise Fatal(2, "vsphere: --vcenter, --vc-user, and --vc-password (or --vc-password-env) are required")

        client = VMwareClient(self.logger, vc_host, vc_user, vc_pass, port=self.args.vc_port, insecure=self.args.vc_insecure)
        client.connect()
        try:
            action = (self.args.vs_action or "scan").lower()

            if action == "scan":
                # list VMs (names only, stable + fast)
                names = client.list_vm_names()
                self.logger.info(f"VMs found: {len(names)}")
                for n in names:
                    print(n)
                return 0

            if action in ("download", "cbt-sync"):
                if not self.args.vm_name:
                    raise Fatal(2, "vsphere: --vm-name is required for download/cbt-sync")
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
                dc_name = getattr(self.args, "dc_name", None) or "ha-datacenter"

                # download descriptor + extents: simplest approach is download just backing path (descriptor)
                # Many VMDKs are descriptor + -flat. Caller can run flatten (local mode) after download.
                if action == "download":
                    client.download_datastore_file(datastore=datastore, ds_path=ds_path, local_path=out, dc_name=dc_name, chunk_size=int(getattr(self.args, "chunk_size", 1024*1024)))
                    return 0

                # cbt-sync: create snapshot, optionally enable cbt, then sync changed blocks onto an existing local disk file
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

                    url = f"https://{vc_host}/folder/{ds_path}?dcPath={dc_name}&dsName={datastore}"
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
                            r = requests.get(url, headers=h, verify=verify)
                            r.raise_for_status()
                            data = r.content
                            f.seek(start)
                            f.write(data)
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

            raise Fatal(2, f"vsphere: unknown action: {action}")
        finally:
            client.disconnect()
