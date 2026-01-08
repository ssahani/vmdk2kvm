# SPDX-License-Identifier: LGPL-3.0-or-later
# vmdk2kvm/converters/vhd_extractor.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import tarfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    DownloadColumn,
    TransferSpeedColumn,
)

from ..core.utils import U


class VHD:
    @staticmethod
    def extract_vhd_or_tar(
        logger: logging.Logger,
        src: Path,
        outdir: Path,
        *,
        # --- Enhancement (non-breaking): optional convert stage right after extract ---
        convert_to_qcow2: bool = False,
        convert_outdir: Optional[Path] = None,
        convert_compress: bool = False,
        convert_compress_level: Optional[int] = None,
        # --- Enhancement: optional host-side debug logging ---
        log_virt_filesystems: bool = False,
    ) -> List[Path]:
        """
        Accepts either:
          - a plain .vhd
          - a tar/tar.gz/tgz containing .vhd(s)

        Returns:
          - extracted .vhd paths (if no conversion), OR
          - converted .qcow2 paths (if convert_to_qcow2=True)
        """
        src = Path(src)
        outdir = Path(outdir)
        U.ensure_dir(outdir)

        if not src.exists():
            U.die(logger, f"Source not found: {src}", 1)
        if not src.is_file():
            U.die(logger, f"Source is not a file: {src}", 1)

        # Case A: direct VHD
        if VHD._looks_like_vhd(src):
            logger.info(f"VHD: {src}")
            vhds = [src.expanduser().resolve()]
            if log_virt_filesystems:
                VHD._log_virt_filesystems(logger, vhds[0])

            if convert_to_qcow2:
                out_conv = Path(convert_outdir) if convert_outdir else (outdir / "qcow2")
                U.ensure_dir(out_conv)
                return VHD._convert_disks_to_qcow2(
                    logger,
                    vhds,
                    out_conv,
                    compress=convert_compress,
                    compress_level=convert_compress_level,
                    log_virt_filesystems=log_virt_filesystems,
                )
            return vhds

        # Case B: tarball
        if VHD._looks_like_tar(src):
            return VHD._extract_vhd_tar(
                logger,
                src,
                outdir,
                convert_to_qcow2=convert_to_qcow2,
                convert_outdir=convert_outdir,
                convert_compress=convert_compress,
                convert_compress_level=convert_compress_level,
                log_virt_filesystems=log_virt_filesystems,
            )

        # Unknown extension: try tar open anyway; if it fails, error nicely.
        try:
            with tarfile.open(src, mode="r:*"):
                pass
            return VHD._extract_vhd_tar(
                logger,
                src,
                outdir,
                convert_to_qcow2=convert_to_qcow2,
                convert_outdir=convert_outdir,
                convert_compress=convert_compress,
                convert_compress_level=convert_compress_level,
                log_virt_filesystems=log_virt_filesystems,
            )
        except Exception:
            U.die(logger, f"Unsupported source type (expected .vhd or tarball): {src}", 1)
            raise  # unreachable


    @staticmethod
    def _extract_vhd_tar(
        logger: logging.Logger,
        vhd_tar: Path,
        outdir: Path,
        *,
        convert_to_qcow2: bool = False,
        convert_outdir: Optional[Path] = None,
        convert_compress: bool = False,
        convert_compress_level: Optional[int] = None,
        log_virt_filesystems: bool = False,
    ) -> List[Path]:
        U.banner(logger, "Extract VHD tarball")
        logger.info(f"VHD tarball: {vhd_tar}")

        with tarfile.open(vhd_tar, mode="r:*") as tar:
            members = tar.getmembers()

            total_bytes = 0
            for m in members:
                try:
                    total_bytes += int(getattr(m, "size", 0) or 0)
                except Exception:
                    pass

            with Progress(
                TextColumn("{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                TextColumn("{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            ) as progress:
                task = progress.add_task("Extracting VHD tarball", total=total_bytes or len(members))

                for member in members:
                    VHD._safe_extract_one(tar, member, outdir)
                    advance = int(getattr(member, "size", 0) or 0)
                    progress.update(task, advance=advance if total_bytes else 1)

        vhds = sorted(list(outdir.rglob("*.vhd")) + list(outdir.rglob("*.VHD")))
        if not vhds:
            U.die(logger, "No .vhd found inside tarball after extraction.", 1)

        logger.info("VHD(s) extracted:")
        for d in vhds:
            logger.info(f" - {d}")

        if log_virt_filesystems:
            for d in vhds:
                if d.exists():
                    VHD._log_virt_filesystems(logger, d)

        if convert_to_qcow2:
            out_conv = Path(convert_outdir) if convert_outdir else (outdir / "qcow2")
            U.ensure_dir(out_conv)
            return VHD._convert_disks_to_qcow2(
                logger,
                vhds,
                out_conv,
                compress=convert_compress,
                compress_level=convert_compress_level,
                log_virt_filesystems=log_virt_filesystems,
            )

        return vhds


    @staticmethod
    def _convert_disks_to_qcow2(
        logger: logging.Logger,
        disks: List[Path],
        outdir: Path,
        *,
        compress: bool = False,
        compress_level: Optional[int] = None,
        log_virt_filesystems: bool = False,
    ) -> List[Path]:
        try:
            from ..converters.qemu_converter import Convert  # type: ignore
        except Exception as e:
            U.die(logger, f"QCOW2 conversion requested but Convert could not be imported: {e}", 1)
            raise

        U.banner(logger, "Convert extracted VHD(s) to QCOW2")
        U.ensure_dir(outdir)

        outputs: List[Path] = []
        for idx, disk in enumerate(disks, 1):
            if not disk.exists():
                logger.warning(f"Skipping missing disk: {disk}")
                continue

            if log_virt_filesystems:
                VHD._log_virt_filesystems(logger, disk)

            out = (outdir / f"{disk.stem}.qcow2").expanduser().resolve()

            last_bucket = {"b": -1}

            def progress_callback(progress: float) -> None:
                b = int(progress * 20)  # 0..20
                if b != last_bucket["b"]:
                    last_bucket["b"] = b
                    if progress < 1.0:
                        logger.info(f"QCOW2 convert [{idx}/{len(disks)}] {disk.name}: {progress:.1%}")
                    else:
                        logger.info(f"QCOW2 convert [{idx}/{len(disks)}] {disk.name}: complete")

            logger.info(
                f"Converting [{idx}/{len(disks)}]: {disk} -> {out} "
                f"(compress={compress}, level={compress_level})"
            )

            # VHD is "vpc" in qemu-img terms; Convert wrapper should auto-detect.
            Convert.convert_image_with_progress(
                logger,
                disk,
                out,
                out_format="qcow2",
                compress=compress,
                compress_level=compress_level,
                progress_callback=progress_callback,
            )
            Convert.validate(logger, out)
            outputs.append(out)

            if log_virt_filesystems:
                VHD._log_virt_filesystems(logger, out)

        # De-dup preserving order
        seen: set[str] = set()
        uniq: List[Path] = []
        for p in outputs:
            s = str(p)
            if s not in seen:
                uniq.append(p)
                seen.add(s)

        if not uniq:
            U.die(logger, "QCOW2 conversion produced no outputs.", 1)

        logger.info("QCOW2 outputs:")
        for p in uniq:
            logger.info(f" - {p}")
        return uniq

    @staticmethod
    def _log_virt_filesystems(logger: logging.Logger, image: Path) -> Dict[str, Any]:
        cmd = ["virt-filesystems", "-a", str(image), "--all", "--long", "-h"]
        try:
            cp = U.run_cmd(logger, cmd, capture=True, check=False)
            out = (cp.stdout or "").strip()
            if out:
                logger.info(f"virt-filesystems -a {image} --all --long -h\n{out}")
            else:
                logger.info(f"virt-filesystems -a {image}: (empty)")
            return {"ok": True, "stdout": out, "cmd": cmd, "rc": getattr(cp, "returncode", 0)}
        except Exception as e:
            logger.warning(f"virt-filesystems failed for {image}: {e}")
            return {"ok": False, "error": str(e), "cmd": cmd}


    @staticmethod
    def _looks_like_vhd(p: Path) -> bool:
        s = p.name.lower()
        return s.endswith(".vhd") or s.endswith(".vhdx")

    @staticmethod
    def _looks_like_tar(p: Path) -> bool:
        s = p.name.lower()
        return s.endswith(".tar") or s.endswith(".tar.gz") or s.endswith(".tgz") or s.endswith(".tar.xz") or s.endswith(".txz")

    @staticmethod
    def _safe_extract_one(tar: tarfile.TarFile, member: tarfile.TarInfo, outdir: Path) -> None:
        outdir = Path(outdir).resolve()
        target_path = (outdir / member.name).resolve()
        if outdir != target_path and outdir not in target_path.parents:
            raise RuntimeError(f"Blocked unsafe tar path traversal: {member.name}")
        tar.extract(member, outdir)
