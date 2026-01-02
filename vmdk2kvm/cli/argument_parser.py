from __future__ import annotations
import argparse
import os
from typing import Any, Dict, Optional, Tuple
from .. import __version__
from ..config.config_loader import Config
from ..config.systemd_template import SYSTEMD_UNIT_TEMPLATE
from ..core.logger import c
from ..core.utils import U
from ..fixers.fstab_rewriter import FstabMode
from .help_texts import YAML_EXAMPLE, FEATURE_SUMMARY, SYSTEMD_EXAMPLE  # New import

def build_parser() -> argparse.ArgumentParser:
    epilog = (
        c("YAML examples:\n", "cyan", ["bold"])
        + c(YAML_EXAMPLE, "cyan")
        + "\n"
        + c("Feature summary:\n", "cyan", ["bold"])
        + c(FEATURE_SUMMARY, "cyan")
        + c("\nSystemd Service Example:\n", "cyan", ["bold"])
        + c(SYSTEMD_UNIT_TEMPLATE + SYSTEMD_EXAMPLE, "cyan")  # Combined for brevity
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
    p.add_argument(
        "--workdir",
        default=None,
        help="Working directory for intermediate files (default: <output-dir>/work).",
    )
    # Flatten/convert
    p.add_argument("--flatten", action="store_true", help="Flatten snapshot chain into a single working image first.")
    p.add_argument(
        "--flatten-format",
        dest="flatten_format",
        default="qcow2",
        choices=["qcow2", "raw"],
        help="Flatten output format.",
    )
    p.add_argument(
        "--to-output",
        dest="to_output",
        default=None,
        help="Convert final working image to this path (relative to output-dir if not absolute).",
    )
    p.add_argument(
        "--out-format",
        dest="out_format",
        default="qcow2",
        choices=["qcow2", "raw", "vdi"],
        help="Output format.",
    )
    p.add_argument("--compress", action="store_true", help="Compression (qcow2 only).")
    p.add_argument(
        "--compress-level",
        dest="compress_level",
        type=int,
        choices=range(1, 10),
        default=None,
        help="Compression level 1-9.",
    )
    p.add_argument("--checksum", action="store_true", help="Compute SHA256 checksum of output.")
    # Fixing behavior
    p.add_argument(
        "--fstab-mode",
        dest="fstab_mode",
        default=FstabMode.STABILIZE_ALL.value,
        choices=[m.value for m in FstabMode],
        help="fstab rewrite mode: stabilize-all (recommended), bypath-only, noop",
    )
    p.add_argument(
        "--no-grub",
        dest="no_grub",
        action="store_true",
        help="Skip GRUB root= update and device.map cleanup.",
    )
    p.add_argument(
        "--regen-initramfs",
        dest="regen_initramfs",
        action="store_true",
        help="Regenerate initramfs + grub config (best-effort).",
    )
    p.add_argument(
        "--no-regen-initramfs",
        dest="regen_initramfs",
        action="store_false",
        help="Disable initramfs/grub regen.",
    )
    p.set_defaults(regen_initramfs=True)
    p.add_argument(
        "--remove-vmware-tools",
        dest="remove_vmware_tools",
        action="store_true",
        help="Remove VMware tools from guest (Linux only).",
    )
    p.add_argument(
        "--cloud-init-config",
        dest="cloud_init_config",
        default=None,
        help="Cloud-init config (YAML/JSON) to inject.",
    )
    p.add_argument(
        "--enable-recovery",
        dest="enable_recovery",
        action="store_true",
        help="Enable checkpoint recovery for long operations.",
    )
    p.add_argument(
        "--parallel-processing",
        dest="parallel_processing",
        action="store_true",
        help="Process multiple disks in parallel.",
    )
    p.add_argument("--resize", default=None, help="Resize root filesystem (enlarge only, e.g., +10G or 50G)")
    p.add_argument("--report", default=None, help="Write Markdown report (relative to output-dir if not absolute).")
    p.add_argument(
        "--virtio-drivers-dir",
        dest="virtio_drivers_dir",
        default=None,
        help="Path to virtio-win drivers directory for Windows injection.",
    )
    p.add_argument("--post-v2v", dest="post_v2v", action="store_true", help="Run virt-v2v after internal fixes.")
    p.add_argument("--use-v2v", dest="use_v2v", action="store_true", help="Use virt-v2v for conversion if available.")
    p.add_argument(
        "--v2v-parallel",
        dest="v2v_parallel",
        action="store_true",
        help="Run multiple virt-v2v jobs in parallel when multiple disks/images are provided (experimental).",
    )
    p.add_argument(
        "--v2v-concurrency",
        dest="v2v_concurrency",
        type=int,
        default=2,
        help="Max concurrent virt-v2v jobs when --v2v-parallel is set (default: 2).",
    )
    p.add_argument(
        "--luks-passphrase",
        dest="luks_passphrase",
        default=os.environ.get("VMDK2KVM_LUKS_PASSPHRASE"),
        help="Passphrase for LUKS-encrypted disks (or set VMDK2KVM_LUKS_PASSPHRASE env var).",
    )
    p.add_argument(
        "--luks-passphrase-env",
        dest="luks_passphrase_env",
        default=None,
        help="Env var containing LUKS passphrase (overrides --luks-passphrase if set at runtime).",
    )
    p.add_argument(
        "--luks-keyfile",
        dest="luks_keyfile",
        default=None,
        help="Path to LUKS keyfile (binary/text). Overrides passphrase if provided.",
    )
    p.add_argument(
        "--luks-mapper-prefix",
        dest="luks_mapper_prefix",
        default="vmdk2kvm-crypt",
        help="Mapper name prefix for opened LUKS devices (default: vmdk2kvm-crypt).",
    )
    p.add_argument(
        "--luks-enable",
        dest="luks_enable",
        action="store_true",
        help="Explicitly enable LUKS unlocking (otherwise inferred from passphrase/keyfile).",
    )
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
    # Your existing global OVF/OVA knobs (kept exactly, only shown here because you included them)
    p.add_argument(
        "--log-virt-filesystems",
        dest="log_virt_filesystems",
        action="store_true",
        help="For OVA/OVF inputs, log `virt-filesystems --all --long -h` for each disk.",
    )
    p.add_argument(
        "--ova-convert-to-qcow2",
        dest="ova_convert_to_qcow2",
        action="store_true",
        help="For OVA/OVF inputs, convert extracted VMDK(s) to qcow2 before continuing pipeline.",
    )
    p.add_argument(
        "--ova-qcow2-dir",
        dest="ova_qcow2_dir",
        default=None,
        help="Output directory for qcow2 images created from OVA/OVF disks (default: <output-dir>/qcow2).",
    )
    p.add_argument(
        "--ova-convert-compress",
        dest="ova_convert_compress",
        action="store_true",
        help="When converting OVA/OVF disks to qcow2, enable compression.",
    )
    p.add_argument(
        "--ova-convert-compress-level",
        dest="ova_convert_compress_level",
        type=int,
        choices=range(1, 10),
        default=None,
        help="Compression level 1-9 for qcow2 conversion of OVA/OVF disks.",
    )
    # ✅ NEW: generic AMI/cloud tarball extraction knobs
    p.add_argument(
        "--extract-nested-tar",
        dest="extract_nested_tar",
        action="store_true",
        help="For AMI/cloud tarballs: extract one level of nested tarballs (tar-in-tar).",
    )
    p.add_argument(
        "--no-extract-nested-tar",
        dest="extract_nested_tar",
        action="store_false",
        help="Disable nested tar extraction for AMI/cloud tarballs.",
    )
    p.set_defaults(extract_nested_tar=True)
    p.add_argument(
        "--convert-payload-to-qcow2",
        dest="convert_payload_to_qcow2",
        action="store_true",
        help="For AMI/cloud tarballs: convert extracted payload disk(s) to qcow2 before continuing pipeline.",
    )
    p.add_argument(
        "--payload-qcow2-dir",
        dest="payload_qcow2_dir",
        default=None,
        help="Output directory for qcow2 created from AMI/cloud payload disks (default: <output-dir>/qcow2).",
    )
    p.add_argument(
        "--payload-convert-compress",
        dest="payload_convert_compress",
        action="store_true",
        help="When converting AMI/cloud payload disks to qcow2, enable compression.",
    )
    p.add_argument(
        "--payload-convert-compress-level",
        dest="payload_convert_compress_level",
        type=int,
        choices=range(1, 10),
        default=None,
        help="Compression level 1-9 for qcow2 conversion of AMI/cloud payload disks.",
    )
    # Subcommands
    sub = p.add_subparsers(dest="cmd", required=True)
    pl = sub.add_parser("local", help="Offline: local VMDK")
    pl.add_argument("--vmdk", required=False, default=None, help="Local VMDK path (descriptor OR monolithic/binary VMDK)")
    pf = sub.add_parser("fetch-and-fix", help="Fetch from remote ESXi over SSH/SCP and fix offline")
    pf.add_argument("--host", required=False, default=None)
    pf.add_argument("--user", default="root")
    pf.add_argument("--port", type=int, default=22)
    pf.add_argument("--identity", default=None)
    pf.add_argument("--ssh-opt", action="append", default=None, help="Extra ssh/scp options (repeatable).")
    pf.add_argument("--remote", required=False, default=None, help="Remote path to VMDK descriptor")
    pf.add_argument(
        "--fetch-dir",
        dest="fetch_dir",
        default=None,
        help="Where to store fetched files (default: <output-dir>/downloaded)",
    )
    pf.add_argument("--fetch-all", dest="fetch_all", action="store_true", help="Fetch full snapshot descriptor chain recursively.")
    po = sub.add_parser("ova", help="Offline: extract from OVA")
    po.add_argument("--ova", required=False, default=None)
    povf = sub.add_parser("ovf", help="Offline: parse OVF (disks in same dir)")
    povf.add_argument("--ovf", required=False, default=None)
    pvhd = sub.add_parser("vhd", help="Offline: VHD input (.vhd or archive containing .vhd)")
    pvhd.add_argument(
        "--vhd",
        required=False,
        default=None,
        help="Path to .vhd OR tarball containing a .vhd (e.g. .tar/.tar.gz/.tgz).",
    )
    pami = sub.add_parser("ami", help="Offline: AMI/cloud-image tarball (extract payload disk(s) from tar archives)")
    pami.add_argument(
        "--ami",
        required=False,
        default=None,
        help="Path to tar/tar.gz/tgz/tar.xz containing a disk payload (raw/img/qcow2/vmdk/vhd/...).",
    )
    plive = sub.add_parser("live-fix", help="LIVE: fix running VM over SSH")
    plive.add_argument("--host", required=False, default=None)
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
    pvs.add_argument("--vcenter", required=False, default=None, help="vCenter/ESXi hostname or IP")
    pvs.add_argument("--vc-user", dest="vc_user", required=False, default=None, help="vCenter username")
    pvs.add_argument("--vc-password", dest="vc_password", default=None, help="vCenter password (or use --vc-password-env)")
    pvs.add_argument("--vc-password-env", dest="vc_password_env", default=None, help="Env var containing vCenter password")
    pvs.add_argument("--vc-port", dest="vc_port", type=int, default=443, help="vCenter HTTPS port (default: 443)")
    pvs.add_argument("--vc-insecure", dest="vc_insecure", action="store_true", help="Disable TLS verification")
    pvs.add_argument(
        "--dc-name",
        dest="dc_name",
        default="ha-datacenter",
        help="Datacenter name for /folder URL (default: ha-datacenter)",
    )
    pvs.add_argument(
        "--vs-v2v",
        dest="vs_v2v",
        action="store_true",
        help="EXPERIMENTAL: export VM(s) directly from vSphere via virt-v2v (VDDK/SSH) and then run normal pipeline.",
    )
    pvs.add_argument("--vs-vm", dest="vs_vm", default=None, help="VM name to export (alternative to --vm-name).")
    pvs.add_argument("--vs-vms", dest="vs_vms", nargs="*", default=None, help="Multiple VM names to export.")
    pvs.add_argument("--vs-datacenter", dest="vs_datacenter", default="ha-datacenter", help="Datacenter name (default: ha-datacenter)")
    pvs.add_argument("--vs-transport", dest="vs_transport", default="vddk", choices=["vddk", "ssh"], help="virt-v2v input transport (default: vddk)")
    pvs.add_argument("--vs-vddk-libdir", dest="vs_vddk_libdir", default=None, help="Path to VDDK libdir (if using vddk transport)")
    pvs.add_argument("--vs-vddk-thumbprint", dest="vs_vddk_thumbprint", default=None, help="vCenter TLS thumbprint for VDDK verification")
    pvs.add_argument("--vs-snapshot-moref", dest="vs_snapshot_moref", default=None, help="Snapshot MoRef (e.g. snapshot-123) to export from")
    pvs.add_argument("--vs-create-snapshot", dest="vs_create_snapshot", action="store_true", help="Create a quiesced snapshot before export and use it")
    pvs.add_argument(
        "--vs-download-only",
        dest="vs_download_only",
        action="store_true",
        help="vSphere virt-v2v hook: download/export ONLY (skip inspection/fixes/tests in later pipeline).",
    )
    pvs.add_argument(
        "--vs-no-download-only",
        dest="vs_download_only",
        action="store_false",
        help="Disable download-only mode (run normal pipeline after export).",
    )
    pvs.set_defaults(vs_download_only=False)
    pvs.add_argument(
        "--vs-v2v-concurrency",
        dest="vs_v2v_concurrency",
        type=int,
        default=1,
        help="Max concurrent vSphere virt-v2v exports (default: 1).",
    )
    pvs.add_argument(
        "--vs-v2v-extra-args",
        dest="vs_v2v_extra_args",
        action="append",
        default=[],
        help="Extra args passed through to virt-v2v (repeatable).",
    )
    pvs.add_argument(
        "--vs-no-verify",
        dest="vs_no_verify",
        action="store_true",
        help="Disable TLS verification for virt-v2v vpx:// input (use with caution).",
    )
    pvs.add_argument(
        "--include-glob",
        dest="vs_include_glob",
        action="append",
        default=[],
        help="download-only VM folder: include file glob (repeatable). Default is ['*'] if none supplied.",
    )
    pvs.add_argument(
        "--exclude-glob",
        dest="vs_exclude_glob",
        action="append",
        default=[],
        help="download-only VM folder: exclude file glob (repeatable).",
    )
    pvs.add_argument(
        "--concurrency",
        dest="vs_concurrency",
        type=int,
        default=4,
        help="download-only VM folder: concurrent downloads (default: 4).",
    )
    pvs.add_argument(
        "--max-files",
        dest="vs_max_files",
        type=int,
        default=5000,
        help="download-only VM folder: refuse to download more than this many files (default: 5000).",
    )
    pvs.add_argument(
        "--use-async-http",
        dest="vs_use_async_http",
        action="store_true",
        help="download-only VM folder: prefer aiohttp/aiofiles when available.",
    )
    pvs.add_argument(
        "--no-use-async-http",
        dest="vs_use_async_http",
        action="store_false",
        help="download-only VM folder: disable aiohttp/aiofiles (force requests).",
    )
    pvs.set_defaults(vs_use_async_http=True)
    pvs.add_argument(
        "--fail-on-missing",
        dest="vs_fail_on_missing",
        action="store_true",
        help="download-only VM folder: treat any failed/missing download as fatal.",
    )
    pvs.add_argument(
        "--vddk-libdir",
        dest="vs_vddk_libdir2",
        default=None,
        help="VDDK raw download: directory containing libvixDiskLib.so (or a parent that contains it).",
    )
    pvs.add_argument(
        "--vddk-thumbprint",
        dest="vs_vddk_thumbprint2",
        default=None,
        help="VDDK raw download: ESXi/vCenter thumbprint (SHA1 AA:BB:..).",
    )
    pvs.add_argument(
        "--no-verify",
        dest="vs_no_verify2",
        action="store_true",
        help="VDDK raw download: disable TLS verification (insecure).",
    )
    pvs.add_argument(
        "--vddk-transports",
        dest="vs_vddk_transports2",
        default=None,
        help="VDDK raw download: transport modes string (e.g. 'nbdssl:nbd').",
    )
    vs_sub = pvs.add_subparsers(dest="vs_action", required=True, help="vSphere actions")
    plist = vs_sub.add_parser("list_vm_names", help="List all VM names")
    plist.add_argument("--json", action="store_true", help="Output in JSON format")
    pget = vs_sub.add_parser("get_vm_by_name", help="Get VM by name")
    pget.add_argument("--name", required=False, default=None, help="VM name")
    pget.add_argument("--json", action="store_true", help="Output in JSON format")
    pvm_disks = vs_sub.add_parser("vm_disks", help="List disks for VM")
    pvm_disks.add_argument("--vm_name", required=False, default=None, help="VM name")
    pvm_disks.add_argument("--json", action="store_true", help="Output in JSON format")
    pselect = vs_sub.add_parser("select_disk", help="Select disk")
    pselect.add_argument("--vm_name", required=False, default=None, help="VM name")
    pselect.add_argument("--label_or_index", default=None, help="Disk label or index")
    pselect.add_argument("--json", action="store_true", help="Output in JSON format")
    pdownload = vs_sub.add_parser("download_datastore_file", help="Download datastore file")
    pdownload.add_argument("--datastore", required=False, default=None, help="Datastore name")
    pdownload.add_argument("--ds_path", required=False, default=None, help="Datastore path")
    pdownload.add_argument("--local_path", required=False, default=None, help="Local output path")
    pdownload.add_argument("--chunk_size", type=int, default=1024 * 1024, help="Download chunk size (bytes)")
    pdownload.add_argument("--json", action="store_true", help="Output in JSON format")
    pcreate = vs_sub.add_parser("create_snapshot", help="Create snapshot")
    pcreate.add_argument("--vm_name", required=False, default=None, help="VM name")
    pcreate.add_argument("--name", required=False, default=None, help="Snapshot name")
    pcreate.add_argument("--quiesce", action="store_true", default=True, help="Quiesce filesystem")
    pcreate.add_argument("--no_quiesce", action="store_false", dest="quiesce", help="Disable quiesce")
    pcreate.add_argument("--memory", action="store_true", default=False, help="Include memory")
    pcreate.add_argument("--description", default="Created by vmdk2kvm", help="Snapshot description")
    pcreate.add_argument("--json", action="store_true", help="Output in JSON format")
    penable = vs_sub.add_parser("enable_cbt", help="Enable CBT")
    penable.add_argument("--vm_name", required=False, default=None, help="VM name")
    penable.add_argument("--json", action="store_true", help="Output in JSON format")
    pquery = vs_sub.add_parser("query_changed_disk_areas", help="Query changed disk areas")
    pquery.add_argument("--vm_name", required=False, default=None, help="VM name")
    pquery.add_argument("--snapshot_name", required=False, default=None, help="Snapshot name")
    pquery.add_argument("--device_key", type=int, required=False, help="Device key")
    pquery.add_argument("--disk", default=None, help="Disk index or label (alternative to device_key)")
    pquery.add_argument("--start_offset", type=int, default=0, help="Start offset")
    pquery.add_argument("--change_id", default="*", help="Change ID")
    pquery.add_argument("--json", action="store_true", help="Output in JSON format")
    pdownload_vm = vs_sub.add_parser("download_vm_disk", help="Download VM disk")
    pdownload_vm.add_argument("--vm_name", required=False, default=None, help="VM name")
    pdownload_vm.add_argument("--disk", default=None, help="Disk index or label")
    pdownload_vm.add_argument("--local_path", required=False, default=None, help="Local output path")
    pdownload_vm.add_argument("--chunk_size", type=int, default=1024 * 1024, help="Download chunk size (bytes)")
    pdownload_vm.add_argument("--json", action="store_true", help="Output in JSON format")
    pcbt_sync = vs_sub.add_parser("cbt_sync", help="CBT delta sync")
    pcbt_sync.add_argument("--vm_name", required=False, default=None, help="VM name")
    pcbt_sync.add_argument("--disk", default=None, help="Disk index or label")
    pcbt_sync.add_argument("--local_path", required=False, default=None, help="Local output path")
    pcbt_sync.add_argument("--enable_cbt", action="store_true", help="Enable CBT")
    pcbt_sync.add_argument("--snapshot_name", default="vmdk2kvm-cbt", help="Snapshot name")
    pcbt_sync.add_argument("--change_id", default="*", help="Change ID for CBT query (default: '*')")
    pcbt_sync.add_argument("--json", action="store_true", help="Output in JSON format")
    pdl_only = vs_sub.add_parser("download_only_vm", help="Download the entire VM folder from datastore (no virt-v2v)")
    pdl_only.add_argument("--vm_name", required=False, default=None, help="VM name")
    pdl_only.add_argument(
        "--output_dir",
        dest="vs_output_dir",
        default=None,
        help="Local output directory override for this action (defaults to global --output-dir).",
    )
    pdl_only.add_argument("--json", action="store_true", help="Output in JSON format")
    pvddk_dl = vs_sub.add_parser("vddk_download_disk", help="VDDK raw download of a VM disk (no virt-v2v)")
    pvddk_dl.add_argument("--vm_name", required=False, default=None, help="VM name")
    pvddk_dl.add_argument("--disk", default=None, help="Disk index or label (default: first disk)")
    pvddk_dl.add_argument(
        "--local_path",
        required=False,
        default=None,
        help="Local output path for downloaded disk.",
    )
    pvddk_dl.add_argument("--json", action="store_true", help="Output in JSON format")
    return p

def _require(v: Any) -> bool:
    return v is not None and v != ""

def validate_args(args: argparse.Namespace) -> None:
    """
    Enforce "required" semantics AFTER config defaults have been applied.
    This is the only way YAML can satisfy parameters that argparse would otherwise require on the CLI.
    """
    cmd = getattr(args, "cmd", None)
    if cmd == "local":
        if not _require(getattr(args, "vmdk", None)):
            raise SystemExit("local: missing required value: vmdk (set in YAML as `vmdk:` or pass --vmdk)")
    elif cmd == "fetch-and-fix":
        if not _require(getattr(args, "host", None)):
            raise SystemExit("fetch-and-fix: missing required value: host (YAML `host:` or --host)")
        if not _require(getattr(args, "remote", None)):
            raise SystemExit("fetch-and-fix: missing required value: remote (YAML `remote:` or --remote)")
    elif cmd == "ova":
        if not _require(getattr(args, "ova", None)):
            raise SystemExit("ova: missing required value: ova (YAML `ova:` or --ova)")
    elif cmd == "ovf":
        if not _require(getattr(args, "ovf", None)):
            raise SystemExit("ovf: missing required value: ovf (YAML `ovf:` or --ovf)")
    elif cmd == "vhd":
        if not _require(getattr(args, "vhd", None)):
            raise SystemExit("vhd: missing required value: vhd (YAML `vhd:` or --vhd)")
    elif cmd == "ami":
        if not _require(getattr(args, "ami", None)):
            raise SystemExit("ami: missing required value: ami (YAML `ami:` or --ami)")
    elif cmd == "live-fix":
        if not _require(getattr(args, "host", None)):
            raise SystemExit("live-fix: missing required value: host (YAML `host:` or --host)")
    elif cmd == "vsphere":
        if not _require(getattr(args, "vcenter", None)):
            raise SystemExit("vsphere: missing required value: vcenter (YAML `vcenter:` or --vcenter)")
        if not _require(getattr(args, "vc_user", None)):
            raise SystemExit("vsphere: missing required value: vc_user (YAML `vc_user:` or --vc-user)")
        act = getattr(args, "vs_action", None)
        if act in ("vm_disks", "select_disk", "download_vm_disk", "cbt_sync", "create_snapshot", "enable_cbt", "query_changed_disk_areas", "download_only_vm", "vddk_download_disk"):
            if not _require(getattr(args, "vm_name", None)):
                raise SystemExit(f"vsphere {act}: missing required value: vm_name (YAML `vm_name:` or --vm_name)")
        if act == "get_vm_by_name":
            if not _require(getattr(args, "name", None)):
                raise SystemExit("vsphere get_vm_by_name: missing required value: name (YAML `name:` or --name)")
        if act == "download_datastore_file":
            for k in ("datastore", "ds_path", "local_path"):
                if not _require(getattr(args, k, None)):
                    raise SystemExit(f"vsphere download_datastore_file: missing required value: {k}")
        if act == "download_vm_disk":
            if not _require(getattr(args, "local_path", None)):
                raise SystemExit("vsphere download_vm_disk: missing required value: local_path (YAML `local_path:` or --local_path)")
        if act == "vddk_download_disk":
            if not _require(getattr(args, "local_path", None)):
                raise SystemExit("vsphere vddk_download_disk: missing required value: local_path (YAML `local_path:` or --local_path)")
    # other cmds: daemon/generate-systemd etc typically don't need post-validate

def parse_args_with_config(argv=None, logger=None):
    """
    Two-phase parse that preserves the monolith's behavior.
      Phase 0: parse ONLY global flags needed to find config/logging (no subcommand/required args)
      Phase 1: load+merge config files and apply as argparse defaults
      Phase 2: full parse_args with defaults applied (so required args can come from config)
    Returns: (args, merged_config_dict, logger)
    """
    parser = build_parser()
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
        from ..core.logger import Log # local import to avoid cycles
        logger = Log.setup(getattr(args0, "verbose", 0), getattr(args0, "log_file", None))
    conf: Dict[str, Any] = {}
    cfgs = getattr(args0, "config", None) or []
    if cfgs:
        # Enhancement: normalize/expand config paths before load.
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
    # ✅ NEW: validate after config defaults are applied
    validate_args(args)
    return args, conf, logger