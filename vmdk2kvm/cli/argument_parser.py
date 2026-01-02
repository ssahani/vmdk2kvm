# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import os
from typing import Any, Dict, Optional, Sequence, Tuple

from .. import __version__
from ..config.config_loader import Config
from ..config.systemd_template import SYSTEMD_UNIT_TEMPLATE
from ..core.logger import c
from ..core.utils import U
from ..fixers.fstab_rewriter import FstabMode
from .help_texts import FEATURE_SUMMARY, SYSTEMD_EXAMPLE, YAML_EXAMPLE


class HelpFormatter(argparse.RawDescriptionHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    """Combines raw description formatting with default value display in help."""

    pass


def build_parser() -> argparse.ArgumentParser:
    epilog = (
        c("YAML examples:\n", "cyan", ["bold"])
        + c(YAML_EXAMPLE, "cyan")
        + "\n"
        + c("Feature summary:\n", "cyan", ["bold"])
        + c(FEATURE_SUMMARY, "cyan")
        + c("\nSystemd Service Example:\n", "cyan", ["bold"])
        + c(SYSTEMD_UNIT_TEMPLATE + SYSTEMD_EXAMPLE, "cyan")
    )

    p = argparse.ArgumentParser(
        description=c("vmdk2kvm: Ultimate VMware → KVM/QEMU Converter + Fixer", "green", ["bold"]),
        formatter_class=HelpFormatter,
        epilog=epilog,
    )

    # ------------------------------------------------------------------
    # Global config/logging (two-phase parse relies on these)
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # NEW PROJECT CONTROL: YAML-driven operation (no subcommands)
    # ------------------------------------------------------------------
    p.add_argument(
        "--cmd",
        dest="cmd",
        default=None,
        help="Operation (normally from YAML `cmd:`). Examples: local, fetch-and-fix, ova, ovf, vhd, ami, live-fix, vsphere, daemon, generate-systemd",
    )
    p.add_argument(
        "--vs-action",
        dest="vs_action",
        default=None,
        help="vSphere action (normally from YAML `vs_action:`). Examples: list_vm_names, vm_disks, download_vm_disk, cbt_sync, ...",
    )

    # ------------------------------------------------------------------
    # Global operation flags
    # ------------------------------------------------------------------
    p.add_argument("--output-dir", dest="output_dir", default="./out", help="Output directory root.")
    p.add_argument("--dry-run", dest="dry_run", action="store_true", help="Do not modify guest/convert output.")
    p.add_argument("--no-backup", dest="no_backup", action="store_true", help="Skip backups inside guest (dangerous).")
    p.add_argument("--print-fstab", dest="print_fstab", action="store_true", help="Print /etc/fstab before+after.")
    p.add_argument("--workdir", default=None, help="Working directory for intermediate files (default: <output-dir>/work).")

    # ------------------------------------------------------------------
    # Flatten/convert
    # ------------------------------------------------------------------
    p.add_argument("--flatten", action="store_true", help="Flatten snapshot chain into a single working image first.")
    p.add_argument(
        "--flatten-format", dest="flatten_format", default="qcow2", choices=["qcow2", "raw"], help="Flatten output format."
    )
    p.add_argument(
        "--to-output",
        dest="to_output",
        default=None,
        help="Convert final working image to this path (relative to output-dir if not absolute).",
    )
    p.add_argument("--out-format", dest="out_format", default="qcow2", choices=["qcow2", "raw", "vdi"], help="Output format.")
    p.add_argument("--compress", action="store_true", help="Compression (qcow2 only).")
    p.add_argument("--compress-level", dest="compress_level", type=int, choices=range(1, 10), default=None, help="Compression level 1-9.")
    p.add_argument("--checksum", action="store_true", help="Compute SHA256 checksum of output.")

    # ------------------------------------------------------------------
    # Fixing behavior
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # LUKS knobs
    # ------------------------------------------------------------------
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
    p.add_argument("--luks-keyfile", dest="luks_keyfile", default=None, help="Path to LUKS keyfile (binary/text). Overrides passphrase if provided.")
    p.add_argument(
        "--luks-mapper-prefix",
        dest="luks_mapper_prefix",
        default="vmdk2kvm-crypt",
        help="Mapper name prefix for opened LUKS devices (default: vmdk2kvm-crypt).",
    )
    p.add_argument("--luks-enable", dest="luks_enable", action="store_true", help="Explicitly enable LUKS unlocking (otherwise inferred from passphrase/keyfile).")

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------
    p.add_argument("--libvirt-test", dest="libvirt_test", action="store_true", help="Libvirt smoke test after conversion.")
    p.add_argument("--qemu-test", dest="qemu_test", action="store_true", help="QEMU smoke test after conversion.")
    p.add_argument("--vm-name", dest="vm_name", default="converted-vm", help="VM name for libvirt test.")
    p.add_argument("--memory", type=int, default=2048, help="Memory MiB for tests.")
    p.add_argument("--vcpus", type=int, default=2, help="vCPUs for tests.")
    p.add_argument("--uefi", action="store_true", help="Use UEFI for tests (default BIOS if unset).")
    p.add_argument("--timeout", type=int, default=60, help="Timeout seconds for libvirt state check.")
    p.add_argument("--keep-domain", dest="keep_domain", action="store_true", help="Keep libvirt domain after test.")
    p.add_argument("--headless", action="store_true", help="Headless libvirt domain (no graphics).")

    # ------------------------------------------------------------------
    # Daemon flags
    # ------------------------------------------------------------------
    p.add_argument("--daemon", action="store_true", help="Run in daemon mode (for systemd service).")
    p.add_argument("--watch-dir", dest="watch_dir", default=None, help="Directory to watch for new VMDK files in daemon mode.")

    # ------------------------------------------------------------------
    # OVF/OVA knobs
    # ------------------------------------------------------------------
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
    p.add_argument("--ova-convert-compress", dest="ova_convert_compress", action="store_true", help="When converting OVA/OVF disks to qcow2, enable compression.")
    p.add_argument(
        "--ova-convert-compress-level",
        dest="ova_convert_compress_level",
        type=int,
        choices=range(1, 10),
        default=None,
        help="Compression level 1-9 for qcow2 conversion of OVA/OVF disks.",
    )

    # ------------------------------------------------------------------
    # AMI/cloud tarball extraction knobs
    # ------------------------------------------------------------------
    p.add_argument("--extract-nested-tar", dest="extract_nested_tar", action="store_true", help="For AMI/cloud tarballs: extract one level of nested tarballs (tar-in-tar).")
    p.add_argument("--no-extract-nested-tar", dest="extract_nested_tar", action="store_false", help="Disable nested tar extraction for AMI/cloud tarballs.")
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
    p.add_argument("--payload-convert-compress", dest="payload_convert_compress", action="store_true", help="When converting AMI/cloud payload disks to qcow2, enable compression.")
    p.add_argument(
        "--payload-convert-compress-level",
        dest="payload_convert_compress_level",
        type=int,
        choices=range(1, 10),
        default=None,
        help="Compression level 1-9 for qcow2 conversion of AMI/cloud payload disks.",
    )

    # ------------------------------------------------------------------
    # Former subcommand args, promoted to globals (YAML-driven; CLI overrides)
    # ------------------------------------------------------------------
    p.add_argument("--vmdk", default=None, help="Local VMDK path (descriptor OR monolithic/binary VMDK)")
    p.add_argument("--ova", default=None, help="Path to .ova")
    p.add_argument("--ovf", default=None, help="Path to .ovf (disks in same dir)")
    p.add_argument("--vhd", default=None, help="Path to .vhd OR tarball containing a .vhd (e.g. .tar/.tar.gz/.tgz).")
    p.add_argument("--ami", default=None, help="Path to tar/tar.gz/tgz/tar.xz containing a disk payload (raw/img/qcow2/vmdk/vhd/...).")

    # fetch-and-fix + live-fix common SSH knobs:
    p.add_argument("--host", default=None, help="Remote host for fetch-and-fix/live-fix")
    p.add_argument("--user", default="root", help="Remote user (fetch-and-fix/live-fix)")
    p.add_argument("--port", type=int, default=22, help="SSH port (fetch-and-fix/live-fix)")
    p.add_argument("--identity", default=None, help="SSH identity key path (fetch-and-fix/live-fix)")
    p.add_argument("--ssh-opt", action="append", default=None, help="Extra ssh/scp options (repeatable).")
    p.add_argument("--remote", default=None, help="Remote path to VMDK descriptor (fetch-and-fix)")
    p.add_argument("--fetch-dir", dest="fetch_dir", default=None, help="Where to store fetched files (default: <output-dir>/downloaded)")
    p.add_argument("--fetch-all", dest="fetch_all", action="store_true", help="Fetch full snapshot descriptor chain recursively.")
    p.add_argument("--sudo", action="store_true", help="Run remote commands through sudo -n (live-fix)")

    # generate-systemd:
    p.add_argument("--systemd-output", dest="systemd_output", default=None, help="Write systemd unit to file instead of stdout")

    # ------------------------------------------------------------------
    # vSphere / vCenter knobs (promoted to globals)
    # ------------------------------------------------------------------
    p.add_argument("--vcenter", default=None, help="vCenter/ESXi hostname or IP")
    p.add_argument("--vc-user", dest="vc_user", default=None, help="vCenter username")
    p.add_argument("--vc-password", dest="vc_password", default=None, help="vCenter password (or use --vc-password-env)")
    p.add_argument("--vc-password-env", dest="vc_password_env", default=None, help="Env var containing vCenter password")
    p.add_argument("--vc-port", dest="vc_port", type=int, default=443, help="vCenter HTTPS port (default: 443)")
    p.add_argument("--vc-insecure", dest="vc_insecure", action="store_true", help="Disable TLS verification")
    p.add_argument("--dc-name", dest="dc_name", default="ha-datacenter", help="Datacenter name for /folder URL (default: ha-datacenter)")

    # ------------------------------------------------------------------
    # vSphere control-plane selection: govc vs pyvmomi
    # ------------------------------------------------------------------
    p.add_argument(
        "--vs-control-plane",
        dest="vs_control_plane",
        default=None,
        choices=["auto", "govc", "pyvmomi"],
        help="vSphere control-plane backend: auto (prefer govc), govc, or pyvmomi.",
    )

    # govc context knobs (CLI overrides; YAML can carry same keys)
    p.add_argument("--govc-url", dest="govc_url", default=None, help="govc URL (e.g. https://vcenter/sdk or https://esxi/sdk).")
    p.add_argument("--govc-user", dest="govc_user", default=None, help="govc username (defaults to vc_user if unset).")
    p.add_argument("--govc-password", dest="govc_password", default=None, help="govc password (defaults to vc_password if unset).")
    p.add_argument("--govc-password-env", dest="govc_password_env", default=None, help="Env var containing govc password.")
    p.add_argument("--govc-insecure", dest="govc_insecure", action="store_true", help="govc: disable TLS verification.")
    p.add_argument("--govc-datacenter", dest="govc_datacenter", default=None, help="govc datacenter (GOVC_DATACENTER).")
    p.add_argument("--govc-cluster", dest="govc_cluster", default=None, help="govc cluster (optional).")
    p.add_argument("--govc-folder", dest="govc_folder", default=None, help="govc inventory folder root (optional).")
    p.add_argument("--govc-ds", dest="govc_ds", default=None, help="govc datastore default (optional).")
    p.add_argument("--govc-resource-pool", dest="govc_resource_pool", default=None, help="govc resource pool (optional).")
    p.add_argument("--govc-stdout-json", dest="govc_stdout_json", action="store_true", help="Prefer govc JSON output where supported.")

    # ------------------------------------------------------------------
    # Existing virt-v2v vSphere export knobs, download-only knobs, VDDK knobs...
    # ------------------------------------------------------------------
    p.add_argument(
        "--vs-v2v",
        dest="vs_v2v",
        action="store_true",
        help="EXPERIMENTAL: export VM(s) directly from vSphere via virt-v2v (VDDK/SSH) and then run normal pipeline.",
    )
    p.add_argument("--vs-vm", dest="vs_vm", default=None, help="VM name to export (alternative to --vm-name).")
    p.add_argument("--vs-vms", dest="vs_vms", nargs="*", default=None, help="Multiple VM names to export.")
    p.add_argument("--vs-datacenter", dest="vs_datacenter", default="ha-datacenter", help="Datacenter name (default: ha-datacenter)")
    p.add_argument("--vs-transport", dest="vs_transport", default="vddk", choices=["vddk", "ssh"], help="virt-v2v input transport (default: vddk)")
    p.add_argument("--vs-vddk-libdir", dest="vs_vddk_libdir", default=None, help="Path to VDDK libdir (if using vddk transport)")
    p.add_argument("--vs-vddk-thumbprint", dest="vs_vddk_thumbprint", default=None, help="vCenter TLS thumbprint for VDDK verification")
    p.add_argument("--vs-snapshot-moref", dest="vs_snapshot_moref", default=None, help="Snapshot MoRef (e.g. snapshot-123) to export from")
    p.add_argument("--vs-create-snapshot", dest="vs_create_snapshot", action="store_true", help="Create a quiesced snapshot before export and use it")

    p.add_argument("--vs-download-only", dest="vs_download_only", action="store_true", help="vSphere virt-v2v hook: download/export ONLY (skip inspection/fixes/tests in later pipeline).")
    p.add_argument("--vs-no-download-only", dest="vs_download_only", action="store_false", help="Disable download-only mode (run normal pipeline after export).")
    p.set_defaults(vs_download_only=False)

    p.add_argument("--vs-v2v-concurrency", dest="vs_v2v_concurrency", type=int, default=1, help="Max concurrent vSphere virt-v2v exports (default: 1).")
    p.add_argument("--vs-v2v-extra-args", dest="vs_v2v_extra_args", action="append", default=[], help="Extra args passed through to virt-v2v (repeatable).")
    p.add_argument("--vs-no-verify", dest="vs_no_verify", action="store_true", help="Disable TLS verification for virt-v2v vpx:// input (use with caution).")

    p.add_argument(
        "--include-glob",
        dest="vs_include_glob",
        action="append",
        default=[],
        help="download-only VM folder: include file glob (repeatable). Default is ['*'] if none supplied.",
    )
    p.add_argument("--exclude-glob", dest="vs_exclude_glob", action="append", default=[], help="download-only VM folder: exclude file glob (repeatable).")
    p.add_argument("--concurrency", dest="vs_concurrency", type=int, default=4, help="download-only VM folder: concurrent downloads (default: 4).")
    p.add_argument("--max-files", dest="vs_max_files", type=int, default=5000, help="download-only VM folder: refuse to download more than this many files (default: 5000).")

    p.add_argument("--use-async-http", dest="vs_use_async_http", action="store_true", help="download-only VM folder: prefer aiohttp/aiofiles when available.")
    p.add_argument("--no-use-async-http", dest="vs_use_async_http", action="store_false", help="download-only VM folder: disable aiohttp/aiofiles (force requests).")
    p.set_defaults(vs_use_async_http=True)

    p.add_argument("--fail-on-missing", dest="vs_fail_on_missing", action="store_true", help="download-only VM folder: treat any failed/missing download as fatal.")

    p.add_argument("--vddk-libdir", dest="vs_vddk_libdir2", default=None, help="VDDK raw download: directory containing libvixDiskLib.so (or a parent that contains it).")
    p.add_argument("--vddk-thumbprint", dest="vs_vddk_thumbprint2", default=None, help="VDDK raw download: ESXi/vCenter thumbprint (SHA1 AA:BB:..).")
    p.add_argument("--no-verify", dest="vs_no_verify2", action="store_true", help="VDDK raw download: disable TLS verification (insecure).")
    p.add_argument("--vddk-transports", dest="vs_vddk_transports2", default=None, help="VDDK raw download: transport modes string (e.g. 'nbdssl:nbd').")

    # vSphere action-scoped params (now global)
    p.add_argument("--json", dest="json", action="store_true", help="Output in JSON format (where supported).")
    p.add_argument("--vm_name", dest="vm_name_vsphere", default=None, help="vSphere VM name for actions that require it")
    p.add_argument("--name", dest="name_vsphere", default=None, help="VM name for get_vm_by_name")
    p.add_argument("--label_or_index", dest="label_or_index", default=None, help="Disk label or index for select_disk")
    p.add_argument("--datastore", dest="datastore", default=None, help="Datastore name (download_datastore_file)")
    p.add_argument("--ds_path", dest="ds_path", default=None, help="Datastore path (download_datastore_file)")
    p.add_argument("--local_path", dest="local_path", default=None, help="Local output path (download_*)")
    p.add_argument("--chunk_size", dest="chunk_size", type=int, default=1024 * 1024, help="Download chunk size bytes (default 1MiB)")

    p.add_argument("--snapshot_name", dest="snapshot_name", default=None, help="Snapshot name (create_snapshot/query_changed_disk_areas/cbt_sync)")
    p.add_argument("--quiesce", dest="quiesce", action="store_true", default=True, help="Quiesce filesystem (create_snapshot)")
    p.add_argument("--no_quiesce", dest="quiesce", action="store_false", help="Disable quiesce (create_snapshot)")
    p.add_argument("--snapshot_memory", dest="snapshot_memory", action="store_true", default=False, help="Include memory in snapshot (create_snapshot)")
    p.add_argument("--description", dest="snapshot_description", default="Created by vmdk2kvm", help="Snapshot description (create_snapshot)")

    p.add_argument("--enable_cbt", dest="enable_cbt", action="store_true", help="Enable CBT (cbt_sync)")
    p.add_argument("--device_key", dest="device_key", type=int, default=None, help="Device key (query_changed_disk_areas)")
    p.add_argument("--disk", dest="disk", default=None, help="Disk index/label (query_changed_disk_areas/download_vm_disk/cbt_sync/vddk_download_disk)")
    p.add_argument("--start_offset", dest="start_offset", type=int, default=0, help="Start offset (query_changed_disk_areas)")
    p.add_argument("--change_id", dest="change_id", default="*", help="Change ID (query_changed_disk_areas/cbt_sync)")

    p.add_argument("--vs_output_dir", dest="vs_output_dir", default=None, help="Local output dir override for download_only_vm (defaults to --output-dir)")

    return p


def _require(v: Any) -> bool:
    """True if v is meaningfully present (treats empty/whitespace-only strings as missing)."""
    if v is None:
        return False
    if isinstance(v, str):
        return v.strip() != ""
    return True


def _merged_get(args: argparse.Namespace, conf: Dict[str, Any], key: str) -> Any:
    """
    Prefer CLI override if present (non-empty), else config.
    Supports both snake_case keys in conf and argparse dest keys.
    """
    v = getattr(args, key, None)
    if _require(v):
        return v
    return conf.get(key)


def _merged_secret(args: argparse.Namespace, conf: Dict[str, Any], value_key: str, env_key: str) -> Optional[str]:
    """
    Resolve a secret from (CLI value) or (CLI env var name) or (YAML value) or (YAML env var name).
    Example: (vc_password, vc_password_env)
    """
    direct = _merged_get(args, conf, value_key)
    if _require(direct):
        return str(direct)

    envname = _merged_get(args, conf, env_key)
    if _require(envname):
        return os.environ.get(str(envname), None)

    return None


def _merged_cmd(args: argparse.Namespace, conf: Dict[str, Any]) -> Optional[str]:
    v = getattr(args, "cmd", None)
    if _require(v):
        return str(v).strip()
    v = conf.get("cmd", None)
    if _require(v):
        return str(v).strip()
    v = conf.get("command", None)
    if _require(v):
        return str(v).strip()
    return None


def _merged_vs_action(args: argparse.Namespace, conf: Dict[str, Any]) -> Optional[str]:
    v = getattr(args, "vs_action", None)
    if _require(v):
        return str(v).strip()
    v = conf.get("vs_action", None)
    if _require(v):
        return str(v).strip()
    v = conf.get("action", None)
    if _require(v):
        return str(v).strip()
    return None


def validate_args(args: argparse.Namespace, conf: Dict[str, Any]) -> None:
    """
    New-project policy:
      - No CLI subcommands.
      - YAML drives the operation (cmd / vs_action), CLI can override.

    Validation uses merged view:
      - command selection: cmd from (CLI --cmd) else YAML (cmd/command)
      - required per-cmd keys can come from YAML or CLI overrides
    """
    cmd = _merged_cmd(args, conf)
    if not _require(cmd):
        raise SystemExit(
            "Missing required YAML key: `cmd:` (or `command:`). "
            "Examples: local, fetch-and-fix, ova, ovf, vhd, ami, live-fix, vsphere, daemon, generate-systemd."
        )

    cmd_l = str(cmd).strip().lower()

    if cmd_l == "local":
        if not _require(_merged_get(args, conf, "vmdk")):
            raise SystemExit("cmd=local: missing required `vmdk:` (YAML) or CLI override --vmdk")

    elif cmd_l == "fetch-and-fix":
        if not _require(_merged_get(args, conf, "host")):
            raise SystemExit("cmd=fetch-and-fix: missing required `host:` (YAML) or CLI --host")
        if not _require(_merged_get(args, conf, "remote")):
            raise SystemExit("cmd=fetch-and-fix: missing required `remote:` (YAML) or CLI --remote")

    elif cmd_l == "ova":
        if not _require(_merged_get(args, conf, "ova")):
            raise SystemExit("cmd=ova: missing required `ova:` (YAML) or CLI --ova")

    elif cmd_l == "ovf":
        if not _require(_merged_get(args, conf, "ovf")):
            raise SystemExit("cmd=ovf: missing required `ovf:` (YAML) or CLI --ovf")

    elif cmd_l == "vhd":
        if not _require(_merged_get(args, conf, "vhd")):
            raise SystemExit("cmd=vhd: missing required `vhd:` (YAML) or CLI --vhd")

    elif cmd_l == "ami":
        if not _require(_merged_get(args, conf, "ami")):
            raise SystemExit("cmd=ami: missing required `ami:` (YAML) or CLI --ami")

    elif cmd_l == "live-fix":
        if not _require(_merged_get(args, conf, "host")):
            raise SystemExit("cmd=live-fix: missing required `host:` (YAML) or CLI --host")

    elif cmd_l == "generate-systemd":
        pass

    elif cmd_l == "daemon":
        pass

    elif cmd_l == "vsphere":
        # Core identity
        vcenter = _merged_get(args, conf, "vcenter")
        vc_user = _merged_get(args, conf, "vc_user")
        vc_password = _merged_secret(args, conf, "vc_password", "vc_password_env")

        if not _require(vcenter):
            raise SystemExit("cmd=vsphere: missing required `vcenter:` (YAML) or CLI --vcenter")
        if not _require(vc_user):
            raise SystemExit("cmd=vsphere: missing required `vc_user:` (YAML) or CLI --vc-user")
        if not _require(vc_password):
            raise SystemExit("cmd=vsphere: missing vCenter password. Set `vc_password:` or `vc_password_env:` (or CLI equivalents).")

        # Control-plane selection (auto/govc/pyvmomi)
        vs_cp = _merged_get(args, conf, "vs_control_plane")
        if not _require(vs_cp):
            vs_cp = conf.get("vs_control_plane", None) or "auto"
        vs_cp = str(vs_cp).strip().lower()

        # govc derived identity (only required if backend=govc)
        govc_url = _merged_get(args, conf, "govc_url")
        govc_user = _merged_get(args, conf, "govc_user") or vc_user
        govc_password = _merged_secret(args, conf, "govc_password", "govc_password_env") or vc_password

        if not _require(govc_url) and _require(vcenter):
            govc_url = f"https://{str(vcenter).strip()}/sdk"

        if vs_cp == "govc":
            if not _require(govc_url):
                raise SystemExit("cmd=vsphere: vs_control_plane=govc requires `govc_url:` (or it must be derivable).")
            if not _require(govc_user):
                raise SystemExit("cmd=vsphere: vs_control_plane=govc requires `govc_user:` (or `vc_user:`).")
            if not _require(govc_password):
                raise SystemExit("cmd=vsphere: vs_control_plane=govc requires `govc_password:`/`govc_password_env:` (or `vc_password:`).")

        elif vs_cp in ("pyvmomi", "auto"):
            # pyvmomi uses vc_* keys which are already validated above
            pass
        else:
            raise SystemExit(f"cmd=vsphere: invalid vs_control_plane={vs_cp!r} (use auto|govc|pyvmomi)")

        act = _merged_vs_action(args, conf)
        if not _require(act):
            raise SystemExit("cmd=vsphere: missing required `vs_action:` (YAML) or CLI --vs-action")
        act = str(act).strip()

        # Action-specific args
        vm_name = conf.get("vm_name", None)
        if not _require(vm_name):
            vm_name = getattr(args, "vm_name_vsphere", None)
        if not _require(vm_name):
            vm_name = getattr(args, "vs_vm", None) or (getattr(args, "vs_vms", None)[0] if getattr(args, "vs_vms", None) else None)

        name = conf.get("name", None)
        if not _require(name):
            name = getattr(args, "name_vsphere", None)

        label_or_index = conf.get("label_or_index", None)
        if not _require(label_or_index):
            label_or_index = getattr(args, "label_or_index", None)

        datastore = conf.get("datastore", None) if _require(conf.get("datastore", None)) else getattr(args, "datastore", None)
        ds_path = conf.get("ds_path", None) if _require(conf.get("ds_path", None)) else getattr(args, "ds_path", None)
        local_path = conf.get("local_path", None) if _require(conf.get("local_path", None)) else getattr(args, "local_path", None)

        needs_vm = {
            "vm_disks",
            "select_disk",
            "download_vm_disk",
            "cbt_sync",
            "create_snapshot",
            "enable_cbt",
            "query_changed_disk_areas",
            "download_only_vm",
            "vddk_download_disk",
        }
        if act in needs_vm and not _require(vm_name):
            raise SystemExit(f"cmd=vsphere vs_action={act}: missing required `vm_name:` (YAML) or CLI --vm_name (or --vs-vm)")

        if act == "get_vm_by_name" and not _require(name):
            raise SystemExit("cmd=vsphere vs_action=get_vm_by_name: missing required `name:` (YAML) or CLI --name")

        if act == "select_disk" and not _require(label_or_index):
            raise SystemExit("cmd=vsphere vs_action=select_disk: missing required `label_or_index:` (YAML) or CLI --label_or_index")

        if act == "download_datastore_file":
            for k, vv in (("datastore", datastore), ("ds_path", ds_path), ("local_path", local_path)):
                if not _require(vv):
                    raise SystemExit(f"cmd=vsphere vs_action=download_datastore_file: missing required `{k}:` (YAML) or CLI --{k}")

        if act in ("download_vm_disk", "vddk_download_disk", "cbt_sync"):
            if not _require(local_path):
                raise SystemExit(f"cmd=vsphere vs_action={act}: missing required `local_path:` (YAML) or CLI --local_path")

        if act == "download_only_vm":
            outd = conf.get("vs_output_dir", None)
            if not _require(outd):
                outd = getattr(args, "vs_output_dir", None) or getattr(args, "output_dir", None)
            if not _require(outd):
                raise SystemExit("cmd=vsphere vs_action=download_only_vm: missing `vs_output_dir:` (or set --output-dir).")

        if act == "query_changed_disk_areas":
            device_key = conf.get("device_key", None) if _require(conf.get("device_key", None)) else getattr(args, "device_key", None)
            disk = conf.get("disk", None) if _require(conf.get("disk", None)) else getattr(args, "disk", None)
            if not (_require(device_key) or _require(disk)):
                raise SystemExit("cmd=vsphere vs_action=query_changed_disk_areas: must set `device_key:` OR `disk:` in YAML (or CLI overrides).")

    else:
        raise SystemExit(f"Unknown cmd={cmd!r}. Set YAML `cmd:` to a supported operation.")


def parse_args_with_config(
    argv: Optional[Sequence[str]] = None,
    logger: Any = None,
) -> Tuple[argparse.Namespace, Dict[str, Any], Any]:
    """
    New-project policy:
      - No CLI subcommands.
      - YAML drives `cmd` and (for vsphere) `vs_action`.
      - CLI provides overrides/toggles.

    Flow:
      Phase 0: parse ONLY global flags needed to locate config/logging
      Phase 1: load+merge config files
      Phase 2: apply config as defaults onto the parser
      Phase 3: full parse to get final args
      Phase 4: validate using merged config + args
    """
    import sys

    if argv is None:
        argv = sys.argv[1:]
    argv = list(argv)

    parser = build_parser()

    # Phase 0: pre-parser (must not depend on any "required" semantics)
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--config", action="append", default=[])
    pre.add_argument("-v", "--verbose", action="count", default=0)
    pre.add_argument("--log-file", dest="log_file", default=None)
    pre.add_argument("--dump-config", action="store_true")
    pre.add_argument("--dump-args", action="store_true")
    args0, _rest = pre.parse_known_args(argv)

    if logger is None:
        from ..core.logger import Log  # local import to avoid cycles

        logger = Log.setup(getattr(args0, "verbose", 0), getattr(args0, "log_file", None))

    conf: Dict[str, Any] = {}
    cfgs = getattr(args0, "config", None) or []
    if cfgs:
        cfgs = Config.expand_configs(logger, list(cfgs))
        conf = Config.load_many(logger, cfgs)

    if getattr(args0, "dump_config", False):
        print(U.json_dump(conf))
        raise SystemExit(0)

    # Apply config as defaults so CLI can override.
    Config.apply_as_defaults(logger, parser, conf)

    args = parser.parse_args(argv)

    if getattr(args0, "dump_args", False):
        print(U.json_dump(vars(args)))
        raise SystemExit(0)

    validate_args(args, conf)
    return args, conf, logger
