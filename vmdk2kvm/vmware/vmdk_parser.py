from __future__ import annotations
import logging
import re
from pathlib import Path
from typing import Optional, Tuple
class VMDK:
    EXTENT_RE = re.compile(r'^\s*RW\s+\d+\s+\S+\s+"([^"]+)"\s*$', re.IGNORECASE)
    @staticmethod
    def _is_text_descriptor(p: Path) -> bool:
        try:
            st = p.stat()
        except Exception:
            return False
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
    def parse_extent(logger: logging.Logger, desc: Path) -> Optional[str]:
        if not VMDK._is_text_descriptor(desc):
            return None
        try:
            for line in desc.read_text(encoding="utf-8", errors="ignore").splitlines():
                m = VMDK.EXTENT_RE.match(line)
                if m:
                    return m.group(1)
        except Exception as e:
            logger.debug(f"extent parse failed: {e}")
        return None
    @staticmethod
    def parse_parent(logger: logging.Logger, desc: Path) -> Optional[str]:
        if not VMDK._is_text_descriptor(desc):
            return None
        try:
            for line in desc.read_text(encoding="utf-8", errors="ignore").splitlines():
                if line.startswith("parentFileNameHint="):
                    return line.split("=", 1)[1].strip().strip('"')
        except Exception as e:
            logger.debug(f"parent parse failed: {e}")
        return None
    @staticmethod
    def guess_layout(logger: logging.Logger, vmdk: Path) -> Tuple[str, Optional[Path]]:
        """Returns ('monolithic'|'descriptor', extent_path_or_None)."""
        if VMDK._is_text_descriptor(vmdk):
            extent_rel = VMDK.parse_extent(logger, vmdk)
            if extent_rel:
                extent = vmdk.parent / extent_rel
                return "descriptor", extent
            cand = vmdk.parent / (vmdk.stem + "-flat.vmdk")
            return "descriptor", cand
        return "monolithic", None
