# SPDX-License-Identifier: LGPL-3.0-or-later
# vmdk2kvm/converters/ami_extractor.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import tarfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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


class AMI:
    """
    Generic AMI/cloud-image tarball extractor.

    The term "AMI tarball" is loosely used in the wild: it's often just a tar/tar.gz
    containing a disk image payload (raw/img/qcow2/vmdk/vhd/...), plus metadata.

    This extractor:
      - safely extracts tar/tar.gz/tgz/tar.xz
      - discovers disk payloads by extension
      - optionally extracts one level of nested tarballs
      - optionally converts discovered disks to qcow2
    """

    DISK_EXTS = (
        ".raw",
        ".img",
        ".qcow2",
        ".vmdk",
        ".vhd",
        ".vhdx",
        ".vdi",
    )

    # metadata we don't treat as disks (but may exist in bundles)
    META_EXTS = (
        ".json",
        ".mf",
        ".manifest",
        ".ovf",
        ".xml",
        ".txt",
        ".sha256",
        ".sha512",
        ".sig",
        ".asc",
    )

    @staticmethod
    def extract_ami_or_tar(
        logger: logging.Logger,
        src: Path,
        outdir: Path,
        *,
        convert_to_qcow2: bool = False,
        convert_outdir: Optional[Path] = None,
        convert_compress: bool = False,
        convert_compress_level: Optional[int] = None,
        log_virt_filesystems: bool = False,
        # Enhancement: handle tar-within-tar (one level)
        extract_nested_tar: bool = True,
    ) -> List[Path]:
        """
        Accepts:
          - tar/tar.gz/tgz/tar.xz containing disk payload(s)

        Returns:
          - extracted disk paths (if no conversion), OR
          - converted qcow2 paths (if convert_to_qcow2=True)
        """
        src = Path(src)
        outdir = Path(outdir)
        U.ensure_dir(outdir)

        if not src.exists():
            U.die(logger, f"Source not found: {src}", 1)
        if not src.is_file():
            U.die(logger, f"Source is not a file: {src}", 1)

        # Try tar open; allow "unknown extension" as long as tar can open it.
        try:
            with tarfile.open(src, mode="r:*"):
                pass
        except Exception:
            U.die(logger, f"Unsupported source type (expected tarball): {src}", 1)

        return AMI._extract_and_find_disks(
            logger,
            src,
            outdir,
            convert_to_qcow2=convert_to_qcow2,
            convert_outdir=convert_outdir,
            convert_compress=convert_compress,
            convert_compress_level=convert_compress_level,
            log_virt_filesystems=log_virt_filesystems,
            extract_nested_tar=extract_nested_tar,
        )

    # extraction + discovery

    @staticmethod
    def _extract_and_find_disks(
        logger: logging.Logger,
        tar_path: Path,
        outdir: Path,
        *,
        convert_to_qcow2: bool,
        convert_outdir: Optional[Path],
        convert_compress: bool,
        convert_compress_level: Optional[int],
        log_virt_filesystems: bool,
        extract_nested_tar: bool,
    ) -> List[Path]:
        U.banner(logger, "Extract AMI/cloud-image tarball")
        logger.info(f"Tarball: {tar_path}")

        AMI._extract_tar(logger, tar_path, outdir)

        if extract_nested_tar:
            AMI._extract_one_level_nested_tars(logger, outdir)

        disks = AMI._find_disk_payloads(logger, outdir)

        if not disks:
            hint = AMI._debug_top_level(outdir)
            U.die(logger, f"No disk payload found inside tarball. Top-level files: {hint}", 1)

        logger.info("Disk payload(s) found (largest-first):")
        for d in disks:
            logger.info(f" - {d}")

        if log_virt_filesystems:
            for d in disks:
                if d.exists():
                    AMI._log_virt_filesystems(logger, d)

        if convert_to_qcow2:
            out_conv = Path(convert_outdir) if convert_outdir else (outdir / "qcow2")
            U.ensure_dir(out_conv)
            return AMI._convert_disks_to_qcow2(
                logger,
                disks,
                out_conv,
                compress=convert_compress,
                compress_level=convert_compress_level,
                log_virt_filesystems=log_virt_filesystems,
            )

        return disks

    @staticmethod
    def _extract_tar(logger: logging.Logger, tar_path: Path, outdir: Path) -> None:
        with tarfile.open(tar_path, mode="r:*") as tar:
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
                task = progress.add_task("Extracting tarball", total=total_bytes or len(members))

                for member in members:
                    AMI._safe_extract_one(tar, member, outdir)
                    advance = int(getattr(member, "size", 0) or 0)
                    progress.update(task, advance=advance if total_bytes else 1)

    @staticmethod
    def _extract_one_level_nested_tars(logger: logging.Logger, outdir: Path) -> None:
        """
        One-level nested tar extraction: if the bundle contains a tarball,
        extract it into a sibling folder to avoid filename collisions.
        """
        nested = sorted([p for p in outdir.rglob("*") if p.is_file() and AMI._looks_like_tar(p)])
        if not nested:
            return

        U.banner(logger, "Extract nested tarball(s)")
        for t in nested:
            # Try opening to confirm it's a real tar, not random .tar in name
            try:
                with tarfile.open(t, mode="r:*"):
                    pass
            except Exception:
                continue

            nested_out = t.parent / f"{t.stem}.extracted"
            U.ensure_dir(nested_out)
            logger.info(f"Nested tarball: {t} -> {nested_out}")
            try:
                AMI._extract_tar(logger, t, nested_out)
            except Exception as e:
                logger.warning(f"Failed extracting nested tar {t}: {e}")

    @staticmethod
    def _find_disk_payloads(logger: logging.Logger, outdir: Path) -> List[Path]:
        exts = set(e.lower() for e in AMI.DISK_EXTS)

        hits: List[Path] = []
        for p in outdir.rglob("*"):
            if not p.is_file():
                continue
            # Skip obvious metadata
            if p.suffix.lower() in AMI.META_EXTS:
                continue
            if p.suffix.lower() in exts:
                hits.append(p.resolve())

        # Some bundles ship raw disks with no extension. Best-effort heuristic:
        # if there are no hits, look for large files (>64MB) that are not metadata-like.
        if not hits:
            candidates: List[Path] = []
            for p in outdir.rglob("*"):
                if not p.is_file():
                    continue
                if p.suffix.lower() in AMI.META_EXTS:
                    continue
                try:
                    sz = p.stat().st_size
                except Exception:
                    continue
                if sz >= 64 * 1024 * 1024:
                    candidates.append(p.resolve())

            # If we found a few, pick the largest-first as likely disk(s)
            candidates.sort(key=lambda x: x.stat().st_size if x.exists() else 0, reverse=True)
            if candidates:
                logger.info("No known disk extensions found; using large-file heuristic.")
                return candidates

        # De-dup + sort largest-first (often the real root disk)
        seen: set[str] = set()
        uniq: List[Path] = []
        for p in hits:
            s = str(p)
            if s not in seen:
                uniq.append(p)
                seen.add(s)

        uniq.sort(key=lambda x: x.stat().st_size if x.exists() else 0, reverse=True)
        return uniq

    @staticmethod
    def _debug_top_level(outdir: Path) -> str:
        top = sorted([p for p in outdir.iterdir() if p.is_file()])
        return ", ".join(p.name for p in top[:20]) or "(none)"

    # --------------------------
    # conversion + logging
    # --------------------------

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

        U.banner(logger, "Convert extracted disk(s) to QCOW2")
        U.ensure_dir(outdir)

        outputs: List[Path] = []
        for idx, disk in enumerate(disks, 1):
            if not disk.exists():
                logger.warning(f"Skipping missing disk: {disk}")
                continue

            if log_virt_filesystems:
                AMI._log_virt_filesystems(logger, disk)

            out = (outdir / f"{disk.stem}.qcow2").expanduser().resolve()

            last_bucket = {"b": -1}

            def progress_callback(progress: float) -> None:
                b = int(progress * 20)
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
                AMI._log_virt_filesystems(logger, out)

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

    # --------------------------
    # helpers
    # --------------------------

    @staticmethod
    def _looks_like_tar(p: Path) -> bool:
        s = p.name.lower()
        return (
            s.endswith(".tar")
            or s.endswith(".tar.gz")
            or s.endswith(".tgz")
            or s.endswith(".tar.xz")
            or s.endswith(".txz")
        )

    @staticmethod
    def _safe_extract_one(tar: tarfile.TarFile, member: tarfile.TarInfo, outdir: Path) -> None:
        outdir = Path(outdir).resolve()
        target_path = (outdir / member.name).resolve()
        if outdir != target_path and outdir not in target_path.parents:
            raise RuntimeError(f"Blocked unsafe tar path traversal: {member.name}")
        tar.extract(member, outdir)
