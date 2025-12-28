from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class Extent:
    """One extent declaration from a VMDK descriptor.

    Example line:
      RW 41943040 VMFS "vm-flat.vmdk"
    """

    access: str  # RW|RDONLY|NOACCESS
    sectors: int
    kind: str    # VMFS|FLAT|SPARSE|...
    filename: str


class VMDK:
    """Small, robust-ish parser for VMDK *text* descriptors.

    This intentionally does NOT try to fully model the VMDK spec.
    It only extracts the pieces we need for automation:
      - extent filenames (may be 1+)
      - parentFileNameHint (optional)
    """

    # Extent format is: <access> <sectors> <type> "<filename>" [optional extra tokens]
    # Some descriptors include extra tokens at the end, so we allow them.
    EXTENT_RE = re.compile(
        r"^\s*(RW|RDONLY|NOACCESS)\s+(\d+)\s+(\S+)\s+\"([^\"]+)\"\s*(?:#.*)?$",
        re.IGNORECASE,
    )

    PARENT_RE = re.compile(r"^\s*parentFileNameHint\s*=\s*(.+?)\s*$", re.IGNORECASE)

    @staticmethod
    def _is_text_descriptor(p: Path) -> bool:
        """Heuristic: descriptor files are small and contain no NUL bytes."""
        try:
            st = p.stat()
        except Exception:
            return False
        # Descriptors are typically a few KB. 8 MiB is a safe upper bound.
        if st.st_size > 8 * 1024 * 1024:
            return False
        try:
            head = p.open("rb").read(4096)
        except Exception:
            return False
        if b"\x00" in head:
            return False
        return True

    @staticmethod
    def _iter_lines(desc: Path) -> Iterable[str]:
        # errors="ignore" handles stray bytes; splitlines() handles CRLF.
        txt = desc.read_text(encoding="utf-8", errors="ignore")
        for line in txt.splitlines():
            yield line.strip("\r\n")

    @staticmethod
    def parse_extents(logger: logging.Logger, desc: Path) -> List[Extent]:
        """Return 0+ extents referenced by a descriptor."""
        if not VMDK._is_text_descriptor(desc):
            return []
        extents: List[Extent] = []
        try:
            for line in VMDK._iter_lines(desc):
                if not line or line.lstrip().startswith("#"):
                    continue
                m = VMDK.EXTENT_RE.match(line)
                if not m:
                    continue
                access, sectors, kind, filename = m.group(1), m.group(2), m.group(3), m.group(4)
                extents.append(
                    Extent(
                        access=access.upper(),
                        sectors=int(sectors),
                        kind=kind,
                        filename=filename,
                    )
                )
        except Exception as e:
            logger.debug(f"extent parse failed: {e}")
        return extents

    @staticmethod
    def parse_parent(logger: logging.Logger, desc: Path) -> Optional[str]:
        if not VMDK._is_text_descriptor(desc):
            return None
        try:
            for line in VMDK._iter_lines(desc):
                m = VMDK.PARENT_RE.match(line)
                if not m:
                    continue
                rhs = m.group(1).strip()
                # parentFileNameHint="foo.vmdk"   (quotes optional in some wild descriptors)
                return rhs.strip().strip('"')
        except Exception as e:
            logger.debug(f"parent parse failed: {e}")
        return None

    @staticmethod
    def guess_layout(logger: logging.Logger, vmdk: Path) -> Tuple[str, List[Path]]:
        """Returns (layout, extent_paths).

        layout is:
          - 'descriptor' when vmdk is a text descriptor
          - 'monolithic' otherwise

        extent_paths are *guesses*; they may not exist locally.
        """
        if VMDK._is_text_descriptor(vmdk):
            extents = VMDK.parse_extents(logger, vmdk)
            if extents:
                return "descriptor", [vmdk.parent / Path(e.filename).name for e in extents]
            # common convention
            cand = vmdk.parent / f"{vmdk.stem}-flat.vmdk"
            return "descriptor", [cand]
        return "monolithic", []