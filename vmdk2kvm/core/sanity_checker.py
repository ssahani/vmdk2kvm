from __future__ import annotations
import argparse
import logging
import shutil
from pathlib import Path
from typing import Any

from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from .utils import U
class SanityChecker:
    def __init__(self, logger: logging.Logger, args: argparse.Namespace):
        self.logger = logger
        self.args = args
        self.out_root = Path(args.output_dir).expanduser().resolve()
    def check_tools(self):
        required_tools = ["qemu-img"]
        optional_tools = ["rsync", "virsh", "qemu-system-x86_64", "sgdisk"]
        missing_required = []
        missing_optional = []
        for tool in required_tools:
            if U.which(tool) is None:
                missing_required.append(tool)
        for tool in optional_tools:
            if U.which(tool) is None:
                missing_optional.append(tool)
        if missing_required:
            U.die(self.logger, f"Missing required tools: {', '.join(missing_required)}", 1)
        if missing_optional:
            self.logger.warning(f"Missing optional tools: {', '.join(missing_optional)}")
        try:
            import guestfs
            g = guestfs.GuestFS()
            g.close()
        except Exception as e:
            U.die(self.logger, f"libguestfs test failed: {e}", 1)
        self.logger.info("Tools sanity check passed.")
    def check_disk_space(self):
        if self.args.dry_run:
            self.logger.info("DRY-RUN: skipping disk space check")
            return
        try:
            usage = shutil.disk_usage(self.out_root)
            free_gb = usage.free / (1024 ** 3)
            input_size = sum(Path(d).stat().st_size for d in self.args.disks) if hasattr(self.args, 'disks') else 0
            estimated_needed = input_size * 2 # Rough estimate
            estimated_needed_gb = estimated_needed / (1024 ** 3)
            if free_gb < estimated_needed_gb:
                U.die(self.logger, f"Insufficient disk space: {free_gb:.2f} GB free, estimated needed {estimated_needed_gb:.2f} GB", 1)
            else:
                self.logger.info(f"Disk space OK: {free_gb:.2f} GB free")
        except Exception as e:
            self.logger.warning(f"Disk space check failed: {e}")
    def check_permissions(self):
        try:
            test_file = self.out_root / ".permission_test"
            test_file.touch()
            test_file.unlink()
            self.logger.debug("Permissions OK")
        except Exception as e:
            U.die(self.logger, f"Permission check failed: {e}", 1)
    def check_network(self):
        try:
            import socket
            socket.gethostbyname("www.google.com")
            self.logger.debug("Network OK")
        except Exception:
            self.logger.warning("Network check failed: no internet connection")
    def check_all(self):
        checks = [self.check_tools, self.check_disk_space, self.check_permissions, self.check_network]
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
            task = progress.add_task("Running sanity checks", total=len(checks))
            for check in checks:
                check()
                progress.update(task, advance=1)
        # Add more sanity checks as needed
        self.logger.info("All sanity checks passed.")
