from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union


class VMDKType(Enum):
    """Enum for VMDK layout types."""
    MONOLITHIC = "monolithic"   # single file (binary sparse/flat)
    DESCRIPTOR = "descriptor"   # small text descriptor referencing one/more extents
    UNKNOWN = "unknown"


class VMDKError(RuntimeError):
    pass


@dataclass(frozen=True)
class Extent:
    access: str           # RW / RDONLY / NOACCESS
    size_sectors: int     # sector count from descriptor
    type: str             # SPARSE / FLAT / ...
    file: str             # raw string as in descriptor
    line_number: int

    @property
    def file_name(self) -> str:
        # Normalize to just the file name component (descriptor often uses relative)
        # We intentionally keep basename because vSphere export sometimes embeds subdirs.
        return Path(self.file.replace("\\", "/")).name


@dataclass
class DescriptorInfo:
    extents: List[Extent]
    parent: Optional[str] = None
    cid: Optional[str] = None
    parent_cid: Optional[str] = None
    create_type: Optional[str] = None
    version: Optional[str] = None
    encoding: Optional[str] = None
    adapter_type: Optional[str] = None
    size_sectors: Optional[int] = None
    # Keep raw kvs if you want to inspect odd descriptors later
    kv: Dict[str, str] = None  # type: ignore[assignment]


class VMDK:
    """
    VMDK helper with:
      - robust descriptor detection (safe size cap, null-byte rejection, text heuristics)
      - structured parsing (DescriptorInfo + Extent)
      - safer path resolution for extent/parent references
      - layout guessing that returns VMDKType (plus extent if descriptor)
      - multi-extent support (common with split extents)
    """

    # Extent line examples:
    #   RW 41922560 SPARSE "disk-s001.vmdk"
    #   RW 41922560 FLAT   "disk-flat.vmdk"
    EXTENT_RE = re.compile(
        r'^\s*(?P<access>RW|RDONLY|NOACCESS)\s+'
        r'(?P<size>\d+)\s+'
        r'(?P<type>SPARSE|FLAT|ZERO|VMFS|VMFSSPARSE|VMFSRDM|VMFSRAW|SESPARSE)\s+'
        r'"(?P<file>[^"]+)"\s*$',
        re.IGNORECASE,
    )

    # Descriptor kv-ish lines:
    #   parentFileNameHint="foo.vmdk"
    #   createType="monolithicSparse"
    #   encoding="UTF-8"
    #   ddb.adapterType = "lsilogic"
    _KV_QUOTED_RE = re.compile(r'^\s*(?P<k>[^=:#]+?)\s*=\s*"(?P<v>[^"]*)"\s*$', re.IGNORECASE)
    _KV_BARE_RE = re.compile(r'^\s*(?P<k>[^=:#]+?)\s*=\s*(?P<v>[^#;]+?)\s*$', re.IGNORECASE)

    # CID / parentCID lines often appear as:
    #   CID=fffffffe
    #   parentCID=ffffffff
    _CID_EQ_RE = re.compile(r'^\s*(?P<k>cid|parentcid)\s*=\s*(?P<v>[0-9a-fA-F]+)\s*$', re.IGNORECASE)
    _CID_COLON_RE = re.compile(r'^\s*(?P<k>cid|parentcid)\s*:\s*(?P<v>[0-9a-fA-F]+)\s*$', re.IGNORECASE)

    # file size cap to avoid reading huge binaries as “maybe text”
    MAX_DESCRIPTOR_SIZE = 8 * 1024 * 1024  # 8MB

    # quick “this is definitely a descriptor” markers
    _TEXT_MARKERS = (
        "# Disk DescriptorFile",
        "version=",
        "encoding=",
        "CID",
        "parentFileNameHint",
        "createType",
        "ddb.adapterType",
    )

    @staticmethod
    def _safe_stat(p: Path) -> Optional[os.stat_result]:
        try:
            return p.stat()
        except Exception:
            return None

    @staticmethod
    def _read_head(p: Path, n: int) -> Optional[bytes]:
        try:
            with p.open("rb") as f:
                return f.read(n)
        except Exception:
            return None

    @staticmethod
    def _is_text_descriptor(p: Path) -> bool:
        """
        Determine if `p` looks like a text VMDK descriptor.

        Rules (conservative):
          - exists, file, non-empty, <= MAX_DESCRIPTOR_SIZE
          - head has no NUL bytes
          - head decodes to mostly-printable text
          - contains common descriptor markers (fast accept), otherwise still accept if printable ratio is high
        """
        try:
            if not p.exists() or not p.is_file():
                return False

            st = VMDK._safe_stat(p)
            if not st:
                return False

            if st.st_size == 0:
                return False
            if st.st_size > VMDK.MAX_DESCRIPTOR_SIZE:
                return False

            head = VMDK._read_head(p, min(8192, st.st_size))
            if not head:
                return False

            if b"\x00" in head:
                return False

            # printable ratio heuristic
            printable = sum(32 <= b <= 126 or b in (9, 10, 13) for b in head)
            ratio = printable / max(1, len(head))
            if ratio < 0.80:
                return False

            text_head = head.decode("utf-8", errors="ignore").lstrip()
            # “fast accept” if we see marker tokens
            for m in VMDK._TEXT_MARKERS:
                if m.lower() in text_head.lower():
                    return True

            # If it’s extremely small and printable, still might be a descriptor
            # (some tools produce minimal descriptors)
            return True

        except Exception:
            return False

    @staticmethod
    def _norm_ref(ref: str) -> str:
        # Normalize weird slashes; keep raw string separate when returning.
        return ref.strip().strip('"').replace("\\", "/")

    @staticmethod
    def _resolve_ref(base_dir: Path, ref: str) -> Path:
        """
        Resolve extent/parent reference path safely.

        Descriptor references are typically:
          - relative filenames ("disk-flat.vmdk")
          - occasionally subpaths ("subdir/disk-flat.vmdk")
          - occasionally absolute paths (rare in exports; but handle)
        We resolve relative to descriptor directory. Then we also try basename-only
        fallback because exports often flatten directories.
        """
        norm = VMDK._norm_ref(ref)

        # absolute?
        candidate = Path(norm)
        if candidate.is_absolute():
            return candidate

        # relative
        rel = base_dir / candidate
        if rel.exists():
            return rel

        # fallback: basename in same directory
        base = base_dir / candidate.name
        return base

    @staticmethod
    def parse_descriptor(logger: logging.Logger, desc: Path) -> Optional[Dict[str, Any]]:
        """
        Backward-compatible API: returns a dict, but internally uses DescriptorInfo.
        (Your other code likely expects a Dict[str, Any].)
        """
        info = VMDK.parse_descriptor_info(logger, desc)
        if not info:
            return None

        out: Dict[str, Any] = {
            "extents": [
                {
                    "access": e.access,
                    "size": e.size_sectors,
                    "type": e.type,
                    "file": e.file,
                    "line_number": e.line_number,
                }
                for e in info.extents
            ],
            "parent": info.parent,
            "cid": info.cid,
            "parent_cid": info.parent_cid,
            "create_type": info.create_type,
            "version": info.version,
            "encoding": info.encoding,
            "adapter_type": info.adapter_type,
            "size": info.size_sectors,
        }
        # keep kv for debugging/forensics
        out["kv"] = dict(info.kv or {})
        return out

    @staticmethod
    def parse_descriptor_info(logger: logging.Logger, desc: Path) -> Optional[DescriptorInfo]:
        """
        Parse a text descriptor and return strongly-typed DescriptorInfo.
        """
        if not VMDK._is_text_descriptor(desc):
            return None

        try:
            content = desc.read_text(encoding="utf-8", errors="ignore")
        except Exception as e:
            logger.debug(f"Failed to read descriptor {desc}: {e}")
            return None

        info = DescriptorInfo(extents=[], kv={})

        for line_num, raw in enumerate(content.splitlines(), 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            # extents
            m = VMDK.EXTENT_RE.match(line)
            if m:
                info.extents.append(
                    Extent(
                        access=m.group("access").upper(),
                        size_sectors=int(m.group("size")),
                        type=m.group("type").upper(),
                        file=m.group("file"),
                        line_number=line_num,
                    )
                )
                continue

            # cid / parentcid (support both ":" and "=" forms)
            m = VMDK._CID_EQ_RE.match(line) or VMDK._CID_COLON_RE.match(line)
            if m:
                k = m.group("k").lower()
                v = m.group("v")
                if k == "cid":
                    info.cid = v
                else:
                    info.parent_cid = v
                info.kv[k] = v
                continue

            # key="value"
            m = VMDK._KV_QUOTED_RE.match(line)
            if m:
                k = m.group("k").strip().lower()
                v = m.group("v")
                info.kv[k] = v

                if k == "parentfilenamehint":
                    info.parent = v
                elif k == "createtype":
                    info.create_type = v
                elif k == "encoding":
                    info.encoding = v
                elif k == "ddb.adaptertype" or k == "ddb.adaptertype ":
                    info.adapter_type = v
                continue

            # key=value (bare)
            m = VMDK._KV_BARE_RE.match(line)
            if m:
                k = m.group("k").strip().lower()
                v = m.group("v").strip().strip('"')
                info.kv[k] = v

                if k == "version":
                    info.version = v
                elif k == "parentfilenamehint":
                    info.parent = v
                elif k == "createtype":
                    info.create_type = v
                elif k == "encoding":
                    info.encoding = v
                elif k == "ddb.adaptertype":
                    info.adapter_type = v
                continue

        if info.extents:
            info.size_sectors = sum(e.size_sectors for e in info.extents)

        # A “descriptor” with no extents is suspicious; keep it but callers can decide.
        if not info.extents:
            logger.debug(f"Descriptor {desc} parsed but contains no extents")

        return info

    # --- Backward-compatible helpers ---

    @staticmethod
    def parse_extent(logger: logging.Logger, desc: Path) -> Optional[str]:
        info = VMDK.parse_descriptor_info(logger, desc)
        if info and info.extents:
            return info.extents[0].file
        return None

    @staticmethod
    def parse_parent(logger: logging.Logger, desc: Path) -> Optional[str]:
        info = VMDK.parse_descriptor_info(logger, desc)
        return info.parent if info else None

    # --- Smarter layout detection ---

    @staticmethod
    def guess_layout(logger: logging.Logger, vmdk: Path) -> Tuple[str, Optional[Path]]:
        """
        Backward-compatible return:
          ("descriptor"|"monolithic", extent_path_or_None)

        Enhancement: we use VMDKType, and we resolve the first extent properly.
        """
        t, extent = VMDK.guess_layout_typed(logger, vmdk)
        if t == VMDKType.DESCRIPTOR:
            return "descriptor", extent
        return "monolithic", None

    @staticmethod
    def guess_layout_typed(logger: logging.Logger, vmdk: Path) -> Tuple[VMDKType, Optional[Path]]:
        """
        New API: returns (VMDKType, first_extent_path_or_None).
        """
        if VMDK._is_text_descriptor(vmdk):
            info = VMDK.parse_descriptor_info(logger, vmdk)
            if info and info.extents:
                first = info.extents[0]
                extent = VMDK._resolve_ref(vmdk.parent, first.file)
                if extent.exists() and extent != vmdk:
                    return VMDKType.DESCRIPTOR, extent

            # Fall back to common patterns when extent parsing fails / missing
            patterns = [
                vmdk.parent / (vmdk.stem + "-flat.vmdk"),
                vmdk.parent / (vmdk.stem + "-delta.vmdk"),
                vmdk.parent / (vmdk.stem + "-sesparse.vmdk"),
                vmdk.parent / (vmdk.stem + "-s001.vmdk"),  # split sparse
            ]
            for p in patterns:
                if p.exists() and p != vmdk:
                    return VMDKType.DESCRIPTOR, p

            return VMDKType.DESCRIPTOR, None

        # Not a descriptor: likely monolithic (binary)
        if vmdk.suffix.lower() == ".vmdk" and vmdk.exists():
            # Common extent-like naming patterns
            name = vmdk.name.lower()
            if name.endswith(("-flat.vmdk", "-delta.vmdk", "-sesparse.vmdk")):
                return VMDKType.MONOLITHIC, None

            # Light signature probing (don’t overfit; just avoid false positives)
            # Many monolithic sparse VMDKs start with "KDMV" (VMDK magic, little-endian variants exist)
            head = VMDK._read_head(vmdk, 4)
            if head in (b"KDMV", b"COWD"):
                return VMDKType.MONOLITHIC, None

        return VMDKType.UNKNOWN, None

    # --- Multi-extent support ---

    @staticmethod
    def get_all_extents(logger: logging.Logger, desc: Path) -> List[Path]:
        """
        Returns list of resolved absolute extent paths (may include non-existing paths).
        """
        info = VMDK.parse_descriptor_info(logger, desc)
        if not info or not info.extents:
            return []

        return [VMDK._resolve_ref(desc.parent, e.file) for e in info.extents]

    @staticmethod
    def get_existing_extents(logger: logging.Logger, desc: Path) -> List[Path]:
        """
        Like get_all_extents(), but filters to only those that exist.
        Useful for “is this descriptor usable here?” checks.
        """
        return [p for p in VMDK.get_all_extents(logger, desc) if p.exists()]

    @staticmethod
    def is_sparse_vmdk(logger: logging.Logger, vmdk: Path) -> Optional[bool]:
        """
        Returns True for sparse/growable, False for flat/preallocated, None unknown.
        Improvements:
          - handles more createType variants
          - handles multi-extent (if any extent is SPARSE/SESPARSE, treat as sparse)
        """
        if VMDK._is_text_descriptor(vmdk):
            info = VMDK.parse_descriptor_info(logger, vmdk)
            if info:
                ct = (info.create_type or "").lower()
                # common sparse-ish createTypes: monolithicSparse, twoGbMaxExtentSparse, streamOptimized, sesparse
                if any(tok in ct.lower() for tok in ("sparse", "streamoptimized", "sesparse")):
                    return True
                if "flat" in ct.lower():
                    return False

                # fall back to extents types
                if info.extents:
                    any_sparse = any(e.type.upper() in ("SPARSE", "SESPARSE", "VMFSSPARSE") for e in info.extents)
                    any_flat = any(e.type.upper() in ("FLAT", "VMFSRAW") for e in info.extents)
                    if any_sparse and not any_flat:
                        return True
                    if any_flat and not any_sparse:
                        return False

        # name heuristics for non-descriptor vmdks
        name = vmdk.name.lower()
        if name.endswith("-flat.vmdk"):
            return False
        if name.endswith(("-delta.vmdk", "-sesparse.vmdk", "-sparse.vmdk")):
            return True

        return None

    @staticmethod
    def validate_vmdk_pair(logger: logging.Logger, descriptor: Path, extent: Path) -> bool:
        """
        Validate descriptor references the given extent.
        Improvement: uses proper ref resolution (relative/subdir/basename fallback).
        """
        info = VMDK.parse_descriptor_info(logger, descriptor)
        if not info:
            return False
        if not extent.exists():
            return False

        for e in info.extents:
            resolved = VMDK._resolve_ref(descriptor.parent, e.file)
            if resolved.resolve() == extent.resolve():
                return True

        return False

    @staticmethod
    def resolve_parent_path(logger: logging.Logger, desc: Path) -> Optional[Path]:
        """
        Resolve parentFileNameHint to an actual Path if possible.
        This is gold for snapshot-chain walking.
        """
        info = VMDK.parse_descriptor_info(logger, desc)
        if not info or not info.parent:
            return None
        p = VMDK._resolve_ref(desc.parent, info.parent)
        return p if p.exists() else p  # return even if missing; caller may fetch it

    @staticmethod
    def walk_parent_chain(logger: logging.Logger, desc: Path, max_depth: int = 64) -> List[Path]:
        """
        Follow parentFileNameHint recursively (best-effort). Returns list starting at `desc`,
        then parent, grandparent, ... until missing/no-parent or max_depth.
        """
        chain: List[Path] = []
        cur = desc
        for _ in range(max_depth):
            chain.append(cur)
            parent = VMDK.resolve_parent_path(logger, cur)
            if not parent:
                break
            if parent in chain:
                logger.debug(f"Detected parent cycle at {parent}")
                break
            cur = parent
            if not cur.exists():
                break
        return chain
