from __future__ import annotations
import logging
import os
import tarfile
import tempfile
from pathlib import Path
from typing import Optional, Tuple
import xml.etree.ElementTree as ET
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from ..core.utils import U
class OVF:
    @staticmethod
    def extract_ova(logger: logging.Logger, ova: Path, outdir: Path) -> List[Path]:
        U.banner(logger, "Extract OVA")
        U.ensure_dir(outdir)
        logger.info(f"OVA: {ova}")
        with tarfile.open(ova) as tar:
            members = tar.getmembers()
            with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
                task = progress.add_task("Extracting OVA", total=len(members))
                for member in members:
                    tar.extract(member, outdir)
                    progress.update(task, advance=1)
        ovfs = list(outdir.glob("*.ovf"))
        if not ovfs:
            U.die(logger, "No OVF found inside OVA.", 1)
        return OVF.extract_ovf(logger, ovfs[0], outdir)
    @staticmethod
    def extract_ovf(logger: logging.Logger, ovf: Path, outdir: Path) -> List[Path]:
        U.banner(logger, "Parse OVF")
        logger.info(f"OVF: {ovf}")
        tree = ET.parse(ovf)
        ns = {"ovf": "http://schemas.dmtf.org/ovf/envelope/1"}
        disks: List[Path] = []
        for disk in tree.findall(".//ovf:Disk", ns):
            file_id = disk.get("ovf:fileRef")
            if not file_id:
                continue
            for f in tree.findall(".//ovf:File", ns):
                if f.get("ovf:id") == file_id:
                    href = f.get("ovf:href")
                    if href:
                        disks.append(outdir / href)
        if not disks:
            U.die(logger, "No disks found in OVF.", 1)
        logger.info("Disks referenced by OVF:")
        for d in disks:
            logger.info(f" - {d}")
        return disks
