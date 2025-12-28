from __future__ import annotations

import argparse
from typing import Any, Dict, Optional, Tuple

from ..core.logger import c
from ..config.config_loader import Config
from .. import __version__
from ..config.systemd_template import SYSTEMD_UNIT_TEMPLATE
from ..fixers.fstab_rewriter import FstabMode


# --------------------------------------------------------------------------------------
# Rich YAML Examples (shown in --help epilog)
#   - Keep this string as a raw literal (r"""...""") so it prints cleanly.
#   - The parser below already supports config merging; these examples show common patterns.
# --------------------------------------------------------------------------------------

YAML_EXAMPLE = r"""# vmdk2kvm configuration examples (YAML)
#
# Run:
#   sudo ./vmdk2kvm.py --config example.yaml <command>
#
# Merge multiple configs (later overrides earlier):
#   sudo ./vmdk2kvm.py --config base.yaml --config overrides.yaml <command>
#
# NOTE: Required CLI args can come from YAML because vmdk2kvm uses a 2-phase parse:
#   Phase 0: reads only --config / logging
#   Phase 1: loads+merges YAML and applies defaults to argparse
#   Phase 2: parses full args (so required args can be satisfied by config)
#
# --------------------------------------------------------------------------------------
# 0) Common keys (apply to ALL modes)
# --------------------------------------------------------------------------------------
# output_dir: ./out
# workdir: ./out/work
# dry_run: false                 # preview changes, don't modify guest/outputs
# verbose: 0|1|2                 # or CLI: -v/-vv
# log_file: ./vmdk2kvm.log
# report: report.md              # relative to output_dir if not absolute
# checksum: true                 # SHA256 output
# enable_recovery: true          # checkpoints for long ops
# parallel_processing: true      # batch mode concurrency
#
# Fix policy:
# fstab_mode: stabilize-all      # stabilize-all | bypath-only | noop
# print_fstab: true
# no_backup: false               # keep backups in guest unless explicitly disabled
# no_grub: false                 # set true to skip grub root=/device.map cleanup
# regen_initramfs: true          # best-effort initramfs+grub regen
# remove_vmware_tools: true      # linux guests only
#
# Convert policy:
# flatten: true
# flatten_format: qcow2          # qcow2|raw
# to_output: final.qcow2
# out_format: qcow2              # qcow2|raw|vdi
# compress: true
# compress_level: 6              # 1-9
# resize: +10G                   # enlarge only: +10G or set total: 50G
#
# Windows extras:
# virtio_drivers_dir: /path/to/virtio-win
#
# virt-v2v integration:
# use_v2v: false                 # use virt-v2v primarily
# post_v2v: true                 # run v2v after internal fixes
#
# Tests:
# libvirt_test: true
# qemu_test: true
# vm_name: my-test-vm
# memory: 4096
# vcpus: 4
# uefi: true
# timeout: 90
# keep_domain: false
# headless: true
#
# --------------------------------------------------------------------------------------
# 1) LOCAL (offline local VMDK conversion)
# --------------------------------------------------------------------------------------
# Basic local mode config: fix + flatten + convert to qcow2 (Linux guest)
command: local
vmdk: /path/to/vm.vmdk
output_dir: ./out
workdir: ./out/work
flatten: true
flatten_format: qcow2
to_output: vm-fixed.qcow2
out_format: qcow2
compress: true
compress_level: 6
fstab_mode: stabilize-all
print_fstab: true
regen_initramfs: true
remove_vmware_tools: true
checksum: true
report: local-report.md
verbose: 1

# --- Local: "minimal safe" dry-run preview (no changes performed) ---
# command: local
# vmdk: /path/to/vm.vmdk
# dry_run: true
# print_fstab: true
# fstab_mode: stabilize-all
# regen_initramfs: false
# flatten: false
# verbose: 2

# --- Local: Windows virtio injection + BCD scan + convert ---
# command: local
# vmdk: /path/to/windows-vm.vmdk
# virtio_drivers_dir: /path/to/virtio-win
# # optional flags your orchestrator can map:
# # enable_virtio_gpu: true
# # enable_virtio_input: true
# # enable_virtio_fs: true
# flatten: true
# to_output: windows-kvm.qcow2
# out_format: qcow2
# compress: true
# checksum: true
# report: windows-report.md
# verbose: 2

# --- Local: disk growth + cloud-init injection (Linux) ---
# command: local
# vmdk: /path/to/linux-vm.vmdk
# resize: +20G
# cloud_init_config: /path/to/cloud-config.yaml
# fstab_mode: stabilize-all
# regen_initramfs: true
# flatten: true
# to_output: linux-grown.qcow2
# out_format: qcow2
# compress: true

# --- Local: produce RAW image for dd or imaging pipelines ---
# command: local
# vmdk: /path/to/vm.vmdk
# flatten: true
# flatten_format: raw
# to_output: vm.raw
# out_format: raw
# compress: false

# --- Local: batch multiple VMs (shared defaults + per-VM overrides) ---
# vms:
#   - vmdk: /path/to/vm1.vmdk
#     to_output: vm1.qcow2
#     resize: +10G
#   - vmdk: /path/to/vm2.vmdk
#     to_output: vm2.qcow2
#     remove_vmware_tools: false
# flatten: true
# out_format: qcow2
# compress: true
# parallel_processing: true
# enable_recovery: true
#
# Run:
#   sudo ./vmdk2kvm.py --config batch.yaml local
#
# --------------------------------------------------------------------------------------
# 2) LIVE-FIX (apply fixes to a running VM via SSH)
# --------------------------------------------------------------------------------------
# Basic live-fix: rewrite fstab + regen initramfs/grub + optionally remove VMware tools
# command: live-fix
# host: 192.168.1.100
# user: root
# port: 22
# sudo: true
# print_fstab: true
# fstab_mode: stabilize-all
# regen_initramfs: true
# remove_vmware_tools: true
# no_backup: false
# verbose: 2
#
# --- Live-fix: custom identity and SSH options, skip grub updates ---
# command: live-fix
# host: vm.example.com
# user: admin
# identity: ~/.ssh/custom_key
# ssh_opt:
#   - "-o StrictHostKeyChecking=no"
#   - "-o ConnectTimeout=30"
# sudo: true
# no_grub: true
# fstab_mode: bypath-only
# dry_run: true
# log_file: live-fix.log
#
# --- Live-fix: multi-VM sequential list ---
# vms:
#   - host: vm1.example.com
#     user: root
#     sudo: true
#     regen_initramfs: true
#   - host: vm2.example.com
#     user: admin
#     identity: ~/.ssh/key
#     remove_vmware_tools: false
# print_fstab: true
# fstab_mode: stabilize-all
#
# Run:
#   sudo ./vmdk2kvm.py --config live-batch.yaml live-fix
#
# --------------------------------------------------------------------------------------
# 3) FETCH-AND-FIX (fetch from ESXi over SSH/SCP and fix offline)
# --------------------------------------------------------------------------------------
# Basic fetch-and-fix: fetch descriptor then fix+convert
# command: fetch-and-fix
# host: esxi.example.com
# user: root
# port: 22
# remote: /vmfs/volumes/datastore1/vm/vm.vmdk
# fetch_dir: ./downloads
# flatten: true
# to_output: esxi-vm-fixed.qcow2
# out_format: qcow2
# compress: true
# verbose: 1
#
# --- Fetch: full snapshot chain (recursive parent descriptors) ---
# command: fetch-and-fix
# host: esxi-host
# identity: ~/.ssh/esxi_key
# remote: /path/to/snapshot-vm.vmdk
# fetch_all: true
# flatten: true
# resize: 50G
# regen_initramfs: true
# libvirt_test: true
# vm_name: esxi-test-vm
# uefi: true
# timeout: 90
# report: esxi-report.md
#
# --- Fetch: multi-VM batch (parallel fetch + fix) ---
# vms:
#   - host: esxi1.example.com
#     remote: /vmfs/volumes/ds1/vm1/vm1.vmdk
#     fetch_all: true
#   - host: esxi2.example.com
#     remote: /vmfs/volumes/ds2/vm2/vm2.vmdk
#     identity: ~/.ssh/key2
# flatten: true
# out_format: qcow2
# parallel_processing: true
# enable_recovery: true
#
# --------------------------------------------------------------------------------------
# 4) OVA / OVF (offline extract/parse)
# --------------------------------------------------------------------------------------
# OVA:
# command: ova
# ova: /path/to/appliance.ova
# flatten: true
# to_output: appliance.qcow2
#
# OVF (disks in same dir):
# command: ovf
# ovf: /path/to/appliance.ovf
# flatten: true
# to_output: appliance.qcow2
#
# --------------------------------------------------------------------------------------
# 5) DAEMON (watch directory)
# --------------------------------------------------------------------------------------
# CLI:
#   sudo ./vmdk2kvm.py --daemon --watch-dir /incoming local
#
# YAML:
# command: daemon
# daemon: true
# watch_dir: /incoming
# output_dir: /out
#
# --------------------------------------------------------------------------------------
# 6) vSphere/vCenter (pyvmomi) - discovery, downloads, CBT sync
# --------------------------------------------------------------------------------------
# NOTE: This uses the "vsphere" subcommand with nested actions.
#
# List VMs:
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: list_vm_names
# json: true
#
# Get VM details:
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: get_vm_by_name
# name: myVM
# json: true
#
# List VM disks:
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: vm_disks
# vm_name: myVM
# json: true
#
# Download a datastore file (e.g. descriptor, extent, vmx, nvram):
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# dc_name: ha-datacenter
# vs_action: download_datastore_file
# datastore: datastore1
# ds_path: "[datastore1] myVM/myVM.vmdk"
# local_path: ./downloads/myVM.vmdk
# chunk_size: 1048576
#
# Download a specific VM disk (choosing disk by index/label):
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: download_vm_disk
# vm_name: myVM
# disk: 0
# local_path: ./downloads/myVM-disk0.vmdk
#
# Create a quiesced snapshot for safer reads:
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: create_snapshot
# vm_name: myVM
# name: vmdk2kvm-pre-migration
# quiesce: true
# memory: false
#
# Enable CBT (Changed Block Tracking):
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: enable_cbt
# vm_name: myVM
#
# Query CBT changed disk areas:
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: query_changed_disk_areas
# vm_name: myVM
# snapshot_name: vmdk2kvm-cbt
# disk: 0
# start_offset: 0
# change_id: "*"
# json: true
#
# CBT delta sync (working theory / scaffold): download base once, then patch deltas:
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: cbt_sync
# vm_name: myVM
# disk: 0
# local_path: ./downloads/myVM-disk0.vmdk
# enable_cbt: true
# snapshot_name: vmdk2kvm-cbt
# json: true
"""


class CLI:
    @staticmethod
    def build_parser() -> argparse.ArgumentParser:
        epilog = (
            c("YAML examples:\n", "cyan", ["bold"])
            + c(YAML_EXAMPLE, "cyan")
            + "\n"
            + c("Feature summary:\n", "cyan", ["bold"])
            + c(" • Inputs: local VMDK, remote ESXi fetch, OVA/OVF extract, live SSH fix, vSphere pyvmomi\n", "cyan")
            + c(" • Snapshot: flatten convert, recursive parent descriptor fetch, vSphere snapshots/CBT hooks\n", "cyan")
            + c(
                " • Fixes: fstab UUID/PARTUUID/LABEL, btrfs canonicalization, grub root=, crypttab, mdraid checks\n",
                "cyan",
            )
            + c(" • Windows: virtio injection, registry service + CriticalDeviceDatabase, BCD store scan/backup\n", "cyan")
            + c(" • Cloud: cloud-init injection\n", "cyan")
            + c(" • Outputs: qcow2/raw/vdi, compression levels, validation, checksums\n", "cyan")
            + c(" • Tests: libvirt and qemu smoke tests, BIOS/UEFI modes\n", "cyan")
            + c(" • Safety: dry-run, backups, report generation, verbose logs, recovery checkpoints\n", "cyan")
            + c(" • Performance: parallel batch processing\n", "cyan")
            + c("\nSystemd Service Example:\n", "cyan", ["bold"])
            + c(SYSTEMD_UNIT_TEMPLATE, "cyan")
        )

        p = argparse.ArgumentParser(
            description=c("vmdk2kvm: Ultimate VMware → KVM/QEMU Converter + Fixer", "green", ["bold"]),
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=epilog,
        )

        # Global config/logging (two-phase parse relies on these)
        p.add_argument(
            "--config",
            action="append",
            default=[],
            help="YAML/JSON config file (repeatable; later overrides earlier).",
        )
        p.add_argument("--dump-config", action="store_true", help="Print merged normalized config and exit.")
        p.add_argument("--dump-args", action="store_true", help="Print final parsed args and exit.")
        p.add_argument("--version", action="version", version=__version__)
        p.add_argument("-v", "--verbose", action="count", default=0, help="Verbosity: -v, -vv")
        p.add_argument("--log-file", dest="log_file", default=None, help="Write logs to file.")

        # Global operation flags
        p.add_argument("--output-dir", dest="output_dir", default="./out", help="Output directory root.")
        p.add_argument("--dry-run", dest="dry_run", action="store_true", help="Do not modify guest/convert output.")
        p.add_argument("--no-backup", dest="no_backup", action="store_true", help="Skip backups inside guest (dangerous).")
        p.add_argument("--print-fstab", dest="print_fstab", action="store_true", help="Print /etc/fstab before+after.")
        p.add_argument("--workdir", default=None, help="Working directory for intermediate files (default: <output-dir>/work).")

        # Flatten/convert
        p.add_argument("--flatten", action="store_true", help="Flatten snapshot chain into a single working image first.")
        p.add_argument("--flatten-format", dest="flatten_format", default="qcow2", choices=["qcow2", "raw"], help="Flatten output format.")
        p.add_argument("--to-output", dest="to_output", default=None, help="Convert final working image to this path (relative to output-dir if not absolute).")
        p.add_argument("--out-format", dest="out_format", default="qcow2", choices=["qcow2", "raw", "vdi"], help="Output format.")
        p.add_argument("--compress", action="store_true", help="Compression (qcow2 only).")
        p.add_argument("--compress-level", dest="compress_level", type=int, choices=range(1, 10), default=None, help="Compression level 1-9.")
        p.add_argument("--checksum", action="store_true", help="Compute SHA256 checksum of output.")

        # Fixing behavior
        p.add_argument(
            "--fstab-mode",
            dest="fstab_mode",
            default=FstabMode.STABILIZE_ALL.value,
            choices=[m.value for m in FstabMode],
            help="fstab rewrite mode: stabilize-all (recommended), bypath-only, noop",
        )
        p.add_argument("--no-grub", dest="no_grub", action="store_true", help="Skip GRUB root= update and device.map cleanup.")
        p.add_argument("--regen-initramfs", dest="regen_initramfs", action="store_true", help="Regenerate initramfs + grub config (best-effort).")
        p.add_argument("--no-regen-initramfs", dest="regen_initramfs", action="store_false", help="Disable initramfs/grub regen.")
        p.set_defaults(regen_initramfs=True)
        p.add_argument("--remove-vmware-tools", dest="remove_vmware_tools", action="store_true", help="Remove VMware tools from guest (Linux only).")
        p.add_argument("--cloud-init-config", dest="cloud_init_config", default=None, help="Cloud-init config (YAML/JSON) to inject.")
        p.add_argument("--enable-recovery", dest="enable_recovery", action="store_true", help="Enable checkpoint recovery for long operations.")
        p.add_argument("--parallel-processing", dest="parallel_processing", action="store_true", help="Process multiple disks in parallel.")
        p.add_argument("--resize", default=None, help="Resize root filesystem (enlarge only, e.g., +10G or 50G)")
        p.add_argument("--report", default=None, help="Write Markdown report (relative to output-dir if not absolute).")
        p.add_argument("--virtio-drivers-dir", dest="virtio_drivers_dir", default=None, help="Path to virtio-win drivers directory for Windows injection.")
        p.add_argument("--post-v2v", dest="post_v2v", action="store_true", help="Run virt-v2v after internal fixes.")
        p.add_argument("--use-v2v", dest="use_v2v", action="store_true", help="Use virt-v2v for conversion if available.")

        # Tests
        p.add_argument("--libvirt-test", dest="libvirt_test", action="store_true", help="Libvirt smoke test after conversion.")
        p.add_argument("--qemu-test", dest="qemu_test", action="store_true", help="QEMU smoke test after conversion.")
        p.add_argument("--vm-name", dest="vm_name", default="converted-vm", help="VM name for libvirt test.")
        p.add_argument("--memory", type=int, default=2048, help="Memory MiB for tests.")
        p.add_argument("--vcpus", type=int, default=2, help="vCPUs for tests.")
        p.add_argument("--uefi", action="store_true", help="Use UEFI for tests (default BIOS if unset).")
        p.add_argument("--timeout", type=int, default=60, help="Timeout seconds for libvirt state check.")
        p.add_argument("--keep-domain", dest="keep_domain", action="store_true", help="Keep libvirt domain after test.")
        p.add_argument("--headless", action="store_true", help="Headless libvirt domain (no graphics).")

        # Daemon flags (global)
        p.add_argument("--daemon", action="store_true", help="Run in daemon mode (for systemd service).")
        p.add_argument("--watch-dir", dest="watch_dir", default=None, help="Directory to watch for new VMDK files in daemon mode.")

        # Subcommands
        sub = p.add_subparsers(dest="cmd", required=True)

        pl = sub.add_parser("local", help="Offline: local VMDK")
        pl.add_argument("--vmdk", required=True, help="Local VMDK path (descriptor OR monolithic/binary VMDK)")

        pf = sub.add_parser("fetch-and-fix", help="Fetch from remote ESXi over SSH/SCP and fix offline")
        pf.add_argument("--host", required=True)
        pf.add_argument("--user", default="root")
        pf.add_argument("--port", type=int, default=22)
        pf.add_argument("--identity", default=None)
        pf.add_argument("--ssh-opt", action="append", default=None, help="Extra ssh/scp options (repeatable).")
        pf.add_argument("--remote", required=True, help="Remote path to VMDK descriptor")
        pf.add_argument("--fetch-dir", dest="fetch_dir", default=None, help="Where to store fetched files (default: <output-dir>/downloaded)")
        pf.add_argument("--fetch-all", dest="fetch_all", action="store_true", help="Fetch full snapshot descriptor chain recursively.")

        po = sub.add_parser("ova", help="Offline: extract from OVA")
        po.add_argument("--ova", required=True)

        povf = sub.add_parser("ovf", help="Offline: parse OVF (disks in same dir)")
        povf.add_argument("--ovf", required=True)

        plive = sub.add_parser("live-fix", help="LIVE: fix running VM over SSH")
        plive.add_argument("--host", required=True)
        plive.add_argument("--user", default="root")
        plive.add_argument("--port", type=int, default=22)
        plive.add_argument("--identity", default=None)
        plive.add_argument("--ssh-opt", action="append", default=None)
        plive.add_argument("--sudo", action="store_true", help="Run remote commands through sudo -n")

        sub.add_parser("daemon", help="Daemon mode to watch directory")

        pgen = sub.add_parser("generate-systemd", help="Generate systemd unit file")
        pgen.add_argument("--output", default=None, help="Write to file instead of stdout")

        # vSphere / vCenter (pyvmomi) mode
        pvs = sub.add_parser("vsphere", help="vSphere/vCenter: scan VMs, download VMDK, CBT delta sync")
        pvs.add_argument("--vcenter", required=True, help="vCenter/ESXi hostname or IP")
        pvs.add_argument("--vc-user", dest="vc_user", required=True, help="vCenter username")
        pvs.add_argument("--vc-password", dest="vc_password", default=None, help="vCenter password (or use --vc-password-env)")
        pvs.add_argument("--vc-password-env", dest="vc_password_env", default=None, help="Env var containing vCenter password")
        pvs.add_argument("--vc-port", dest="vc_port", type=int, default=443, help="vCenter HTTPS port (default: 443)")
        pvs.add_argument("--vc-insecure", dest="vc_insecure", action="store_true", help="Disable TLS verification")
        pvs.add_argument("--dc-name", dest="dc_name", default="ha-datacenter", help="Datacenter name for /folder URL (default: ha-datacenter)")

        vs_sub = pvs.add_subparsers(dest="vs_action", required=True, help="vSphere actions")

        plist = vs_sub.add_parser("list_vm_names", help="List all VM names")
        plist.add_argument("--json", action="store_true", help="Output in JSON format")

        pget = vs_sub.add_parser("get_vm_by_name", help="Get VM by name")
        pget.add_argument("--name", required=True, help="VM name")
        pget.add_argument("--json", action="store_true", help="Output in JSON format")

        pvm_disks = vs_sub.add_parser("vm_disks", help="List disks for VM")
        pvm_disks.add_argument("--vm_name", required=True, help="VM name")
        pvm_disks.add_argument("--json", action="store_true", help="Output in JSON format")

        pselect = vs_sub.add_parser("select_disk", help="Select disk")
        pselect.add_argument("--vm_name", required=True, help="VM name")
        pselect.add_argument("--label_or_index", default=None, help="Disk label or index")
        pselect.add_argument("--json", action="store_true", help="Output in JSON format")

        pdownload = vs_sub.add_parser("download_datastore_file", help="Download datastore file")
        pdownload.add_argument("--datastore", required=True, help="Datastore name")
        pdownload.add_argument("--ds_path", required=True, help="Datastore path")
        pdownload.add_argument("--local_path", required=True, help="Local output path")
        pdownload.add_argument("--chunk_size", type=int, default=1024 * 1024, help="Download chunk size (bytes)")
        pdownload.add_argument("--json", action="store_true", help="Output in JSON format")

        pcreate = vs_sub.add_parser("create_snapshot", help="Create snapshot")
        pcreate.add_argument("--vm_name", required=True, help="VM name")
        pcreate.add_argument("--name", required=True, help="Snapshot name")
        pcreate.add_argument("--quiesce", action="store_true", default=True, help="Quiesce filesystem")
        pcreate.add_argument("--no_quiesce", action="store_false", dest="quiesce", help="Disable quiesce")
        pcreate.add_argument("--memory", action="store_true", default=False, help="Include memory")
        pcreate.add_argument("--description", default="Created by vmdk2kvm", help="Snapshot description")
        pcreate.add_argument("--json", action="store_true", help="Output in JSON format")

        penable = vs_sub.add_parser("enable_cbt", help="Enable CBT")
        penable.add_argument("--vm_name", required=True, help="VM name")
        penable.add_argument("--json", action="store_true", help="Output in JSON format")

        pquery = vs_sub.add_parser("query_changed_disk_areas", help="Query changed disk areas")
        pquery.add_argument("--vm_name", required=True, help="VM name")
        pquery.add_argument("--snapshot_name", required=True, help="Snapshot name")
        pquery.add_argument("--device_key", type=int, required=False, help="Device key")
        pquery.add_argument("--disk", default=None, help="Disk index or label (alternative to device_key)")
        pquery.add_argument("--start_offset", type=int, default=0, help="Start offset")
        pquery.add_argument("--change_id", default="*", help="Change ID")
        pquery.add_argument("--json", action="store_true", help="Output in JSON format")

        pdownload_vm = vs_sub.add_parser("download_vm_disk", help="Download VM disk")
        pdownload_vm.add_argument("--vm_name", required=True, help="VM name")
        pdownload_vm.add_argument("--disk", default=None, help="Disk index or label")
        pdownload_vm.add_argument("--local_path", required=True, help="Local output path")
        pdownload_vm.add_argument("--chunk_size", type=int, default=1024 * 1024, help="Download chunk size (bytes)")
        pdownload_vm.add_argument("--json", action="store_true", help="Output in JSON format")

        pcbt_sync = vs_sub.add_parser("cbt_sync", help="CBT delta sync")
        pcbt_sync.add_argument("--vm_name", required=True, help="VM name")
        pcbt_sync.add_argument("--disk", default=None, help="Disk index or label")
        pcbt_sync.add_argument("--local_path", required=True, help="Local output path")
        pcbt_sync.add_argument("--enable_cbt", action="store_true", help="Enable CBT")
        pcbt_sync.add_argument("--snapshot_name", default="vmdk2kvm-cbt", help="Snapshot name")
        pcbt_sync.add_argument("--json", action="store_true", help="Output in JSON format")

        return p


def parse_args_with_config(argv=None, logger=None):
    """
    Two-phase parse that preserves the monolith's behavior.

      Phase 0: parse ONLY global flags needed to find config/logging (no subcommand/required args)
      Phase 1: load+merge config files and apply as argparse defaults
      Phase 2: full parse_args with defaults applied (so required args can come from config)

    Returns: (args, merged_config_dict, logger)
    """
    parser = CLI.build_parser()

    # Phase 0: tiny pre-parser that *cannot* trip over subcommand required args.
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--config", action="append", default=[])
    pre.add_argument("-v", "--verbose", action="count", default=0)
    pre.add_argument("--log-file", dest="log_file", default=None)
    pre.add_argument("--dump-config", action="store_true")
    pre.add_argument("--dump-args", action="store_true")
    args0, _rest = pre.parse_known_args(argv)

    # Setup a logger early if caller didn't provide one (used for config merge diagnostics).
    if logger is None:
        from ..core.logger import Log  # local import to avoid cycles
        logger = Log.setup(getattr(args0, "verbose", 0), getattr(args0, "log_file", None))

    conf: Dict[str, Any] = {}
    cfgs = getattr(args0, "config", None) or []
    if cfgs:
        cfgs = Config.expand_configs(logger, list(cfgs))
        conf = Config.load_many(logger, cfgs)

        # Apply config values as argparse defaults (so required args can come from config)
        Config.apply_as_defaults(logger, parser, conf)

    # Phase 2: full parse with defaults applied.
    args = parser.parse_args(argv)

    # Convenience: allow --dump-config / --dump-args to work even if config supplies required args.
    if getattr(args0, "dump_config", False):
        print(U.json_dump(conf))
        raise SystemExit(0)

    if getattr(args0, "dump_args", False):
        print(U.json_dump(vars(args)))
        raise SystemExit(0)

    return args, conf, logger
