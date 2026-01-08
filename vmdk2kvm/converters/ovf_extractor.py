# SPDX-License-Identifier: LGPL-3.0-or-later
from __future__ import annotations

import logging
import os
import tarfile
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import xml.etree.ElementTree as ET

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


class OVF:
    @staticmethod
    def extract_ova(
        logger: logging.Logger,
        ova: Path,
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
        Extract an OVA (tar) into outdir, then parse OVF(s) inside and return referenced disk paths.

        Enhancements (non-breaking):
          - Optional conversion to QCOW2 immediately after extraction (convert_to_qcow2=True)
          - Optional "virt-filesystems -a ..." logging for each disk

        Returns:
            List[Path]: Disk file paths (in outdir) referenced by the OVF (or converted qcow2 outputs if enabled).
        """
        U.banner(logger, "Extract OVA")
        ova = Path(ova)
        outdir = Path(outdir)
        U.ensure_dir(outdir)

        if not ova.exists():
            U.die(logger, f"OVA not found: {ova}", 1)
        if not ova.is_file():
            U.die(logger, f"OVA is not a file: {ova}", 1)

        logger.info(f"OVA: {ova}")

        with tarfile.open(ova, mode="r:*") as tar:
            members = tar.getmembers()

            # Total bytes for progress (some tar members may have 0/None size)
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
                task = progress.add_task("Extracting OVA", total=total_bytes or len(members))

                for member in members:
                    OVF._safe_extract_one(tar, member, outdir)

                    # Advance by bytes if we can, otherwise by 1
                    advance = int(getattr(member, "size", 0) or 0)
                    progress.update(task, advance=advance if total_bytes else 1)

        ovfs = sorted(outdir.glob("*.ovf"))
        if not ovfs:
            U.die(logger, "No OVF found inside OVA.", 1)

        # Many OVAs have one OVF; if multiple, parse them all and union disk references.
        disks: List[Path] = []
        for ovf in ovfs:
            disks.extend(
                OVF.extract_ovf(
                    logger,
                    ovf,
                    outdir,
                    log_virt_filesystems=log_virt_filesystems,
                )
            )

        # De-dup while preserving order
        seen: set[Path] = set()
        uniq: List[Path] = []
        for d in disks:
            if d not in seen:
                uniq.append(d)
                seen.add(d)

        # Validate existence and warn (don’t hard-fail; OVFs can reference missing disks in broken exports)
        missing = [d for d in uniq if not d.exists()]
        if missing:
            logger.warning("Some OVF-referenced disks were not found after extraction:")
            for m in missing:
                logger.warning(f" - {m}")
            # Keep behavior: still return what we found (or die? historically you didn’t check)
            uniq = [d for d in uniq if d.exists()]
            if not uniq:
                U.die(logger, "OVF referenced disks but none were found on disk after extraction.", 1)

        # Optional conversion
        if convert_to_qcow2:
            out_conv = Path(convert_outdir) if convert_outdir else (outdir / "qcow2")
            U.ensure_dir(out_conv)
            return OVF._convert_disks_to_qcow2(
                logger,
                uniq,
                out_conv,
                compress=convert_compress,
                compress_level=convert_compress_level,
                log_virt_filesystems=log_virt_filesystems,
            )

        return uniq

    @staticmethod
    def extract_ovf(
        logger: logging.Logger,
        ovf: Path,
        outdir: Path,
        *,
        # --- Enhancement: optional host-side debug logging ---
        log_virt_filesystems: bool = False,
    ) -> List[Path]:
        """
        Parse an OVF file and return disk paths referenced via <File ... ovf:href="..."> used by <Disk ovf:fileRef="...">.

        Enhancement (non-breaking):
          - Optionally logs host-side disk layout via virt-filesystems (if disks exist).
        """
        U.banner(logger, "Parse OVF")
        ovf = Path(ovf)
        outdir = Path(outdir)

        if not ovf.exists():
            U.die(logger, f"OVF not found: {ovf}", 1)

        logger.info(f"OVF: {ovf}")

        try:
            tree = ET.parse(ovf)
        except ET.ParseError as e:
            U.die(logger, f"Failed to parse OVF XML: {ovf}: {e}", 1)

        root = tree.getroot()

        # Try to detect OVF namespace dynamically, fallback to common one.
        ns_uri = None
        if root.tag.startswith("{") and "}" in root.tag:
            ns_uri = root.tag.split("}", 1)[0][1:]
        if not ns_uri:
            ns_uri = "http://schemas.dmtf.org/ovf/envelope/1"

        ns = {"ovf": ns_uri}

        # Build fileRef -> href map from <File ovf:id="..." ovf:href="...">
        file_map: Dict[str, str] = {}
        for f in root.findall(".//ovf:File", ns):
            fid = f.get(f"{{{ns_uri}}}id") or f.get("ovf:id") or f.get("id")
            href = f.get(f"{{{ns_uri}}}href") or f.get("ovf:href") or f.get("href")
            if fid and href:
                file_map[fid] = href

        disks: List[Path] = []
        for disk in root.findall(".//ovf:Disk", ns):
            file_id = disk.get(f"{{{ns_uri}}}fileRef") or disk.get("ovf:fileRef") or disk.get("fileRef")
            if not file_id:
                continue

            href = file_map.get(file_id)
            if not href:
                # Some OVFs are weird; don’t hard-fail, but warn loudly.
                logger.warning(f"OVF disk references fileRef={file_id} but no matching <File> entry was found")
                continue

            # Normalize (OVF hrefs are usually relative, but can include directories)
            href_norm = href.replace("\\", "/").lstrip("/")
            disks.append(outdir / href_norm)

        if not disks:
            U.die(logger, "No disks found in OVF.", 1)

        logger.info("Disks referenced by OVF:")
        for d in disks:
            logger.info(f" - {d}")

        # Optional: log host-side disk layout for each disk that exists
        if log_virt_filesystems:
            for d in disks:
                if d.exists():
                    OVF._log_virt_filesystems(logger, d)

        return disks

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
        """
        Convert extracted disks to qcow2 outputs. Keeps order and de-dups.
        Uses the project Convert wrapper if available.
        """
        # Lazy import to avoid circular deps at import time.
        try:
            from ..converters.qemu_converter import Convert  # type: ignore
        except Exception as e:
            U.die(logger, f"QCOW2 conversion requested but Convert could not be imported: {e}", 1)
            raise  # unreachable

        U.banner(logger, "Convert extracted disks to QCOW2")
        U.ensure_dir(outdir)

        outputs: List[Path] = []
        for idx, disk in enumerate(disks, 1):
            if not disk.exists():
                logger.warning(f"Skipping missing disk: {disk}")
                continue

            # Optional: log layout before conversion
            if log_virt_filesystems:
                OVF._log_virt_filesystems(logger, disk)

            # Name outputs deterministically
            stem = disk.name
            # Keep descriptor naming nicer
            if stem.lower().endswith(".vmdk"):
                stem = stem[:-5]
            out = (outdir / f"{stem}.qcow2").expanduser().resolve()

            # Throttle log spam; report conversion % in 5% buckets
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

        # De-dup while preserving order
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
        """
        Host-side introspection:
          virt-filesystems -a <image> --all --long -h

        Logs output into normal logs (exactly what you asked).
        """
        cmd = ["virt-filesystems", "-a", str(image), "--all", "--long", "-h"]
        try:
            cp = U.run_cmd(logger, cmd, capture=True)
            out = (cp.stdout or "").strip()
            if out:
                logger.info(f"virt-filesystems -a {image} --all --long -h\n{out}")
            else:
                logger.info(f"virt-filesystems -a {image}: (empty)")
            return {"ok": True, "stdout": out, "cmd": cmd}
        except Exception as e:
            logger.warning(f"virt-filesystems failed for {image}: {e}")
            return {"ok": False, "error": str(e), "cmd": cmd}

    @staticmethod
    def _safe_extract_one(tar: tarfile.TarFile, member: tarfile.TarInfo, outdir: Path) -> None:
        """
        Extract a single tar member safely, preventing path traversal.
        """
        outdir = Path(outdir).resolve()

        # member.name can be absolute or contain .. components
        target_path = (outdir / member.name).resolve()

        # Ensure the target is within outdir
        if outdir != target_path and outdir not in target_path.parents:
            raise RuntimeError(f"Blocked unsafe tar path traversal: {member.name}")

        # Extract (tarfile handles dirs/files/links; we rely on path check above)
        tar.extract(member, outdir)
