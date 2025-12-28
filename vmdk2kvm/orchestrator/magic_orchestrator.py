from __future__ import annotations
import argparse
import logging
from pathlib import Path
from typing import Any, Dict, Optional, List

from ..core.utils import U
from ..core.sanity_checker import SanityChecker
from ..core.recovery_manager import RecoveryManager
from ..fixers.offline_fixer import OfflineFSFix
from ..fixers.live_fixer import LiveFixer
from ..converters.flatten import Flatten
from ..converters.qemu_converter import Convert
from ..converters.ovf_extractor import OVF
from ..testers.libvirt_tester import LibvirtTest
from ..testers.qemu_tester import QemuTest
from ..vmware.vmdk_parser import VMDK
from ..vmware.vsphere_mode import VsphereMode
import os
import shutil
import json
import concurrent.futures as concurrent
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from ..config.config_loader import YAML_AVAILABLE, yaml
from ..core.exceptions import Fatal
from ..converters.fetch import Fetch
from ..ssh.ssh_client import SSHClient
from ..ssh.ssh_config import SSHConfig
from ..vmware.vmware_client import REQUESTS_AVAILABLE, PYVMOMI_AVAILABLE
class Magic:
    @staticmethod
    def v2v_convert(logger: logging.Logger, disks: List[Path], out_root: Path, out_format: str, compress: bool) -> List[Path]:
        if U.which("virt-v2v") is None:
            logger.warning("virt-v2v not found; falling back to internal fixer")
            return []
        cmd = ["virt-v2v"]
        for d in disks:
            cmd += ["-i", "disk", str(d)]
        cmd += ["-o", "local", "-os", str(out_root), "-of", out_format]
        if compress:
            cmd += ["--compressed"]
        U.banner(logger, "Using virt-v2v for conversion")
        U.run_cmd(logger, cmd, check=True, capture=False)
        out_images = list(out_root.glob("*.qcow2")) # assume qcow2
        logger.info("virt-v2v conversion completed.")
        return out_images
    def __init__(self, logger: logging.Logger, args: argparse.Namespace):
        self.logger = logger
        self.args = args
        self.recovery_manager: Optional[RecoveryManager] = None
        self.disks: List[Path] = []
    def log_input_layout(self, vmdk: Path) -> None:
        try:
            st = vmdk.stat()
            self.logger.info(f"Input VMDK: {vmdk} ({U.human_bytes(st.st_size)})")
        except Exception:
            self.logger.info(f"Input VMDK: {vmdk}")
        layout, extent = VMDK.guess_layout(self.logger, vmdk)
        if layout == "monolithic":
            self.logger.info("VMDK layout: monolithic/binary (no separate extent) ✅")
        else:
            if extent and extent.exists():
                self.logger.info(f"VMDK layout: descriptor + extent ✅ ({extent})")
            else:
                self.logger.warning(f"VMDK layout: descriptor (extent missing?) ⚠️ ({extent})")
    def process_single_disk(self, disk: Path, out_root: Path, disk_index: int = 0) -> Path:
        self.log_input_layout(disk)
        working = disk
        if self.args.flatten:
            workdir = Path(self.args.workdir).expanduser().resolve() if getattr(self.args, "workdir", None) else (out_root / "work")
            U.ensure_dir(workdir)
            working = Flatten.to_working(self.logger, disk, workdir, fmt=self.args.flatten_format)
        report_path = None
        if self.args.report:
            rp = Path(self.args.report)
            if len(self.disks) > 1:
                report_path = (out_root / f"{rp.stem}_disk{disk_index}{rp.suffix}") if not rp.is_absolute() else rp
            else:
                report_path = rp if rp.is_absolute() else (out_root / rp)
        cloud_init_data = None
        if getattr(self.args, "cloud_init_config", None):
            try:
                config_path = Path(self.args.cloud_init_config).expanduser().resolve()
                if config_path.suffix.lower() == ".json":
                    cloud_init_data = json.loads(config_path.read_text(encoding="utf-8"))
                elif YAML_AVAILABLE:
                    cloud_init_data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
                else:
                    self.logger.warning("YAML not available, cannot load cloud-init config")
            except Exception as e:
                self.logger.warning(f"Failed to load cloud-init config: {e}")
        fixer = OfflineFSFix(
            self.logger,
            working,
            dry_run=self.args.dry_run,
            no_backup=self.args.no_backup,
            print_fstab=self.args.print_fstab,
            update_grub=not self.args.no_grub,
            regen_initramfs=self.args.regen_initramfs,
            fstab_mode=self.args.fstab_mode,
            report_path=report_path,
            remove_vmware_tools=getattr(self.args, "remove_vmware_tools", False),
            inject_cloud_init=cloud_init_data,
            recovery_manager=self.recovery_manager,
            resize=getattr(self.args, "resize", None),
            virtio_drivers_dir=getattr(self.args, "virtio_drivers_dir", None),
        )
        fixer.run()
        out_image: Optional[Path] = None
        if self.args.to_output and not self.args.dry_run:
            if len(self.disks) > 1:
                base_output = Path(self.args.to_output)
                out_image = base_output.parent / f"{base_output.stem}_disk{disk_index}{base_output.suffix}"
            else:
                out_image = Path(self.args.to_output)
            if not out_image.is_absolute():
                out_image = out_root / out_image
            out_image = out_image.expanduser().resolve()
            def progress_callback(progress: float):
                if progress < 1.0:
                    self.logger.info(f"Conversion progress: {progress:.1%}")
                else:
                    self.logger.info("Conversion complete")
            Convert.convert_image_with_progress(
                self.logger,
                working,
                out_image,
                out_format=self.args.out_format,
                compress=self.args.compress,
                compress_level=getattr(self.args, "compress_level", None),
                progress_callback=progress_callback,
            )
            Convert.validate(self.logger, out_image)
            if self.args.checksum:
                cs = U.checksum(out_image)
                self.logger.info(f"SHA256 checksum: {cs}")
        fixed_image = out_image if out_image else working
        return fixed_image
    def process_disks_parallel(self, disks: List[Path], out_root: Path) -> List[Path]:
        self.logger.info(f"Processing {len(disks)} disks in parallel")
        results: List[Path] = []
        max_workers = min(4, len(disks), (os.cpu_count() or 1))
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
            task = progress.add_task("Processing disks", total=len(disks))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self.process_single_disk, disk, out_root, idx): (idx, disk)
                    for idx, disk in enumerate(disks)
                }
                for future in concurrent.futures.as_completed(futures):
                    idx, disk = futures[future]
                    try:
                        result = future.result()
                        results.append(result)
                        self.logger.info(f"Completed processing disk {idx}: {disk.name}")
                    except Exception as e:
                        self.logger.error(f"Failed processing disk {idx} ({disk.name}): {e}")
                    progress.update(task, advance=1)
        return results
    def run(self) -> None:
        out_root = Path(self.args.output_dir).expanduser().resolve()
        U.ensure_dir(out_root)
        if getattr(self.args, "enable_recovery", False):
            recovery_dir = out_root / "recovery"
            self.recovery_manager = RecoveryManager(self.logger, recovery_dir)
            self.logger.info(f"Recovery mode enabled: {recovery_dir}")
        sanity = SanityChecker(self.logger, self.args)
        sanity.check_all()
        U.banner(self.logger, f"Mode: {self.args.cmd}")
        write_actions = (not self.args.dry_run) or bool(self.args.to_output) or bool(self.args.flatten)
        U.require_root_if_needed(self.logger, write_actions)
        if self.args.cmd == "live-fix":
            sshc = SSHClient(self.logger, SSHConfig(
                host=self.args.host, user=self.args.user, port=self.args.port,
                identity=getattr(self.args, "identity", None),
                ssh_opt=getattr(self.args, "ssh_opt", None),
                sudo=getattr(self.args, "sudo", False)
            ))
            LiveFixer(
                self.logger, sshc,
                dry_run=self.args.dry_run,
                no_backup=self.args.no_backup,
                print_fstab=self.args.print_fstab,
                update_grub=not self.args.no_grub,
                regen_initramfs=self.args.regen_initramfs,
                remove_vmware_tools=getattr(self.args, "remove_vmware_tools", False),
            ).run()
            self.logger.info("Live fix done.")
            return
        temp_dir: Optional[Path] = None
        if self.args.cmd == "vsphere":
            if not PYVMOMI_AVAILABLE:
                raise Fatal(2, "pyvmomi not installed. Install: pip install pyvmomi")
            if not REQUESTS_AVAILABLE and (getattr(self.args, "vs_action", "scan") in ("download","cbt-sync")):
                raise Fatal(2, "requests not installed. Install: pip install requests")
            VsphereMode(self.logger, self.args).run()
            return

        if self.args.cmd == "local":
            self.disks = [Path(self.args.vmdk).expanduser().resolve()]
        elif self.args.cmd == "fetch-and-fix":
            sshc = SSHClient(self.logger, SSHConfig(
                host=self.args.host, user=self.args.user, port=self.args.port,
                identity=getattr(self.args, "identity", None),
                ssh_opt=getattr(self.args, "ssh_opt", None),
                sudo=False
            ))
            fetch_dir = Path(self.args.fetch_dir).expanduser().resolve() if getattr(self.args, "fetch_dir", None) else (out_root / "downloaded")
            U.ensure_dir(fetch_dir)
            desc = Fetch.fetch_descriptor_and_extent(self.logger, sshc, self.args.remote, fetch_dir, getattr(self.args, "fetch_all", False))
            self.disks = [desc]
        elif self.args.cmd == "ova":
            temp_dir = out_root / "extracted"
            self.disks = OVF.extract_ova(self.logger, Path(self.args.ova).expanduser().resolve(), temp_dir)
        elif self.args.cmd == "ovf":
            temp_dir = out_root / "extracted"
            self.disks = OVF.extract_ovf(self.logger, Path(self.args.ovf).expanduser().resolve(), temp_dir)
        else:
            U.die(self.logger, f"Unknown command: {self.args.cmd}", 1)
        if self.recovery_manager:
            self.recovery_manager.save_checkpoint("disks_discovered", {
                "count": len(self.disks),
                "disks": [str(d) for d in self.disks]
            })
        fixed_images: List[Path] = []
        if self.args.use_v2v:
            v2v_images = Magic.v2v_convert(self.logger, self.disks, out_root, self.args.out_format, self.args.compress)
            if v2v_images:
                fixed_images = v2v_images
            else:
                # fallback to internal
                if len(self.disks) > 1 and getattr(self.args, "parallel_processing", False):
                    fixed_images = self.process_disks_parallel(self.disks, out_root)
                else:
                    for idx, disk in enumerate(self.disks):
                        if not disk.exists():
                            U.die(self.logger, f"Disk not found: {disk}", 1)
                        fixed_image = self.process_single_disk(disk, out_root, idx)
                        fixed_images.append(fixed_image)
        else:
            if len(self.disks) > 1 and getattr(self.args, "parallel_processing", False):
                fixed_images = self.process_disks_parallel(self.disks, out_root)
            else:
                for idx, disk in enumerate(self.disks):
                    if not disk.exists():
                        U.die(self.logger, f"Disk not found: {disk}", 1)
                    fixed_image = self.process_single_disk(disk, out_root, idx)
                    fixed_images.append(fixed_image)
        out_images = fixed_images
        if getattr(self.args, "post_v2v", False):
            v2v_dir = out_root / "post-v2v"
            U.ensure_dir(v2v_dir)
            v2v_images = Magic.v2v_convert(self.logger, fixed_images, v2v_dir, self.args.out_format, self.args.compress)
            if v2v_images:
                out_images = v2v_images
        if out_images:
            test_image = out_images[0]
            if self.args.libvirt_test:
                LibvirtTest.run(
                    self.logger,
                    test_image,
                    name=self.args.vm_name,
                    memory_mib=self.args.memory,
                    vcpus=self.args.vcpus,
                    uefi=self.args.uefi,
                    timeout_s=self.args.timeout,
                    keep=self.args.keep_domain,
                    headless=self.args.headless,
                )
            if self.args.qemu_test:
                QemuTest.run(
                    self.logger,
                    test_image,
                    memory_mib=self.args.memory,
                    vcpus=self.args.vcpus,
                    uefi=self.args.uefi,
                )
        if self.recovery_manager:
            self.recovery_manager.cleanup_old_checkpoints()
        if temp_dir and temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
        U.banner(self.logger, "Done")
        self.logger.info(f"Output directory: {out_root} 📦")
        if out_images:
            self.logger.info("Generated images:")
            for img in out_images:
                self.logger.info(f" - {img}")
