from __future__ import annotations
import argparse

from ..core.logger import c
from ..config.config_loader import Config
from .. import __version__
from ..config.systemd_template import SYSTEMD_UNIT_TEMPLATE
from ..fixers.fstab_rewriter import FstabMode

YAML_EXAMPLE = r"""# vmdk2kvm config (offline/local mode)
# Run:
# sudo ./vmdk2kvm.py --config config.yaml local
# or merge configs:
# sudo ./vmdk2kvm.py --config base.yaml --config overrides.yaml local
#
# For multiple VMs:
# vms:
# - vmdk: vm1.vmdk
# to_output: vm1.qcow2
# - vmdk: vm2.vmdk
# to_output: vm2.qcow2
#
# What it will do:
# - Open the VMDK offline with libguestfs
# - Mount root safely (never using /dev/disk/by-path for mounting)
# - Rewrite /etc/fstab to stable identifiers (UUID preferred; fallback PARTUUID then LABEL/PARTLABEL)
# - Canonicalize btrfs subvolume entries (removes btrfsvol: pseudo-specs)
# - Ensure /tmp exists (fixes virt-v2v random seed stage)
# - Optionally flatten snapshot chain first (recommended if snapshots exist)
# - Optionally convert to qcow2/raw/vdi output
command: local
vmdk: /home/ssahani/by-path/openSUSE_Leap_15.4_VM_LinuxVMImages.COM.vmdk
output_dir: /home/ssahani/by-path/out
dry_run: false
print_fstab: true
flatten: true
flatten_format: qcow2
workdir: /home/ssahani/by-path/out/work # Optional work directory
to_output: opensuse-leap-15.4-fixed.qcow2
out_format: qcow2
compress: true
compress_level: 6 # Optional: 1-9 compression level
checksum: true
fstab_mode: stabilize-all # stabilize-all | bypath-only | noop
no_backup: false
grub: true
regen_initramfs: true
remove_vmware_tools: true # Remove VMware tools from guest
enable_recovery: true # Enable checkpoint recovery
parallel_processing: true # Process multiple disks in parallel
resize: +10G # Resize root (enlarge only, +10G or 50G)
post_v2v: true # Run virt-v2v after internal fixes
# Cloud-init configuration (optional)
cloud_init_config: /path/to/cloud-init.yaml
verbose: 2
# Optional tests:
# libvirt_test: true
# vm_name: vmdk2kvm-opensuse154
# memory: 2048
# vcpus: 2
# uefi: true
# timeout: 60
# keep_domain: false
# headless: true
"""

class CLI:
    @staticmethod
    def build_parser() -> argparse.ArgumentParser:
        epilog = (
            c("YAML example:\n", "cyan", ["bold"]) +
            c(YAML_EXAMPLE, "cyan") +
            "\n" +
            c("Feature summary:\n", "cyan", ["bold"]) +
            c(" • Inputs: local VMDK, remote ESXi fetch, OVA/OVF extract, live SSH fix\n", "cyan") +
            c(" • Snapshot: flatten convert, recursive parent descriptor fetch\n", "cyan") +
            c(" • Fixes: fstab UUID/PARTUUID/LABEL, btrfs canonicalization, grub root=, crypttab, mdraid checks\n", "cyan") +
            c(" • Windows: BCD store fixes\n", "cyan") +
            c(" • Network: Configuration updates for KVM\n", "cyan") +
            c(" • VMware: Tools removal\n", "cyan") +
            c(" • Cloud: Cloud-init integration\n", "cyan") +
            c(" • Outputs: qcow2/raw/vdi, compression with levels, validation, checksum\n", "cyan") +
            c(" • Tests: libvirt and qemu smoke tests, BIOS/UEFI modes\n", "cyan") +
            c(" • Safety: dry-run, backups, report generation, verbose logs, recovery checkpoints\n", "cyan") +
            c(" • Performance: Parallel disk processing\n", "cyan") +
            c("\nSystemd Service Example:\n", "cyan", ["bold"]) +
            c(SYSTEMD_UNIT_TEMPLATE, "cyan")
        )
        p = argparse.ArgumentParser(
            description=c("vmdk2kvm: Ultimate VMware → KVM/QEMU Converter + Fixer", "green", ["bold"]),
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=epilog,
        )
        p.add_argument("--config", action="append", default=[], help="YAML/JSON config file (repeatable; later overrides earlier).")
        p.add_argument("--dump-config", action="store_true", help="Print merged normalized config and exit.")
        p.add_argument("--dump-args", action="store_true", help="Print final parsed args and exit.")
        p.add_argument("--version", action="version", version=__version__)
        p.add_argument("-v", "--verbose", action="count", default=0, help="Verbosity: -v, -vv")
        p.add_argument("--log-file", default=None, help="Write logs to file.")
        p.add_argument("--output-dir", default="./out", help="Output directory root.")
        p.add_argument("--dry-run", action="store_true", help="Do not modify guest or convert output; show what would happen.")
        p.add_argument("--no-backup", action="store_true", help="Skip backups inside guest (dangerous).")
        p.add_argument("--print-fstab", action="store_true", help="Print /etc/fstab before+after (even if already UUID).")
        p.add_argument("--workdir", default=None, help="Working directory for intermediate files (default: <output-dir>/work).")
        p.add_argument("--flatten", action="store_true", help="Flatten snapshot chain into a single working image first.")
        p.add_argument("--flatten-format", default="qcow2", choices=["qcow2", "raw"], help="Flatten output format.")
        p.add_argument("--to-output", default=None, help="Convert final working image to this path (relative to output-dir if not absolute).")
        p.add_argument("--out-format", default="qcow2", choices=["qcow2", "raw", "vdi"], help="Output format.")
        p.add_argument("--compress", action="store_true", help="Compression (qcow2 only).")
        p.add_argument("--compress-level", type=int, choices=range(1, 10), default=None, help="Compression level 1-9 (default: 6).")
        p.add_argument("--checksum", action="store_true", help="Compute SHA256 checksum of output.")
        p.add_argument("--fstab-mode", default=FstabMode.STABILIZE_ALL.value, choices=[m.value for m in FstabMode],
                       help="fstab rewrite mode: stabilize-all (recommended), bypath-only, noop")
        p.add_argument("--no-grub", action="store_true", help="Skip GRUB root= update and device.map cleanup.")
        p.add_argument("--regen-initramfs", action="store_true", help="Regenerate initramfs + grub config (best-effort).")
        p.add_argument("--no-regen-initramfs", dest="regen_initramfs", action="store_false", help="Disable initramfs/grub regen.")
        p.set_defaults(regen_initramfs=True)
        p.add_argument("--remove-vmware-tools", action="store_true", help="Remove VMware tools from guest (Linux only).")
        p.add_argument("--cloud-init-config", default=None, help="Cloud-init configuration file (YAML/JSON) to inject.")
        p.add_argument("--enable-recovery", action="store_true", help="Enable checkpoint recovery for long operations.")
        p.add_argument("--parallel-processing", action="store_true", help="Process multiple disks in parallel.")
        p.add_argument("--resize", default=None, help="Resize root filesystem (enlarge only, e.g., +10G or 50G)")
        p.add_argument("--report", default=None, help="Write Markdown report (relative to output-dir if not absolute).")
        p.add_argument("--virtio-drivers-dir", default=None, help="Path to virtio-win drivers directory for Windows injection.")
        p.add_argument("--post-v2v", action="store_true", help="Run virt-v2v after internal fixes.")
        p.add_argument("--libvirt-test", action="store_true", help="Libvirt smoke test after conversion.")
        p.add_argument("--qemu-test", action="store_true", help="QEMU smoke test after conversion.")
        p.add_argument("--vm-name", default="converted-vm", help="VM name for libvirt test.")
        p.add_argument("--memory", type=int, default=2048, help="Memory MiB for tests.")
        p.add_argument("--vcpus", type=int, default=2, help="vCPUs for tests.")
        p.add_argument("--uefi", action="store_true", help="Use UEFI for tests (default BIOS if unset).")
        p.add_argument("--timeout", type=int, default=60, help="Timeout seconds for libvirt state check.")
        p.add_argument("--keep-domain", action="store_true", help="Keep libvirt domain after test.")
        p.add_argument("--headless", action="store_true", help="Headless libvirt domain (no graphics).")
        p.add_argument("--daemon", action="store_true", help="Run in daemon mode (for systemd service)")
        p.add_argument("--watch-dir", default=None, help="Directory to watch for new VMDK files in daemon mode")
        p.add_argument("--use-v2v", action="store_true", help="Use virt-v2v for conversion if available.")
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
        pf.add_argument("--fetch-dir", default=None, help="Where to store fetched files (default: <output-dir>/downloaded)")
        pf.add_argument("--fetch-all", action="store_true", help="Fetch full snapshot descriptor chain recursively.")
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
        pdaemon = sub.add_parser("daemon", help="Daemon mode to watch directory")
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
        pvs.add_argument("--action", dest="vs_action", choices=["scan","download","cbt-sync"], default="scan", help="Action to run")
        pvs.add_argument("--vm-name", dest="vm_name", default=None, help="VM name (required for download/cbt-sync)")
        pvs.add_argument("--disk", dest="disk", default=None, help="Disk index (0..) or label substring (default: first disk)")
        pvs.add_argument("--out", dest="out", default=None, help="Output path for downloaded VMDK or local disk for cbt-sync")
        pvs.add_argument("--chunk-size", dest="chunk_size", type=int, default=1024*1024, help="Download chunk size (bytes)")
        pvs.add_argument("--enable-cbt", dest="enable_cbt", action="store_true", help="Enable CBT before cbt-sync")
        pvs.add_argument("--snapshot-name", dest="snapshot_name", default="vmdk2kvm-cbt", help="Snapshot name for CBT sync")

        return p


def parse_args_with_config(argv=None, logger=None):
    """Two-phase parse that preserves the monolith's behavior.

    Phase 0: parse ONLY global flags needed to find config/logging (no subcommand/required args)
    Phase 1: load+merge config files and apply as argparse defaults
    Phase 2: full parse_args with defaults applied (so required args can come from config)

    Returns: (args, merged_config_dict, logger)
    """
    # Full parser (with subcommands + required args)
    parser = CLI.build_parser()

    # Phase 0: tiny pre-parser that *cannot* trip over subcommand required args.
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--config", action="append", default=[])
    pre.add_argument("-v", "--verbose", action="count", default=0)
    pre.add_argument("--log-file", dest="log_file", default=None)
    args0, _rest = pre.parse_known_args(argv)

    # Setup a logger early if caller didn't provide one (used for config merge diagnostics).
    if logger is None:
        from ..core.logger import Log  # local import to avoid cycles
        logger = Log.setup(getattr(args0, "verbose", 0), getattr(args0, "log_file", None))

    conf = {}
    cfgs = getattr(args0, "config", None) or []
    if cfgs:
        cfgs = Config.expand_configs(logger, list(cfgs))
        conf = Config.load_many(logger, cfgs)
        Config.apply_as_defaults(logger, parser, conf)

    # Phase 2: full parse with defaults applied.
    args = parser.parse_args(argv)
    return args, conf, logger
