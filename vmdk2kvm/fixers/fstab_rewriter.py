from __future__ import annotations
import re
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from ..core.utils import U

if TYPE_CHECKING:  # pragma: no cover
    import guestfs  # type: ignore
_BYPATH_PREFIX = "/dev/disk/by-path/"
RE_BTRFSVOL = re.compile(r"^btrfsvol:(.+?)(?://@/|//@/|//)?(.*)$")
IGNORE_MOUNTPOINTS = {"/proc", "/sys", "/dev", "/run", "/dev/pts", "/dev/shm", "/sys/fs/cgroup"}
def parse_btrfsvol_spec(spec: str) -> Tuple[str, Optional[str]]:
    """
    Parse libguestfs inspection btrfsvol: hints.
    Examples:
      btrfsvol:/dev/sda2//@
      btrfsvol:/dev/sda2//@/var
      btrfsvol:/dev/sda2//@/.snapshots/1/snapshot
    Return (device, subvol) where subvol is something like "@", "@/var", "@/.snapshots/..."
    """
    if not spec.startswith("btrfsvol:"):
        return spec, None
    s = spec[len("btrfsvol:"):]
    if "//" not in s:
        return s.strip(), None
    dev, rest = s.split("//", 1)
    dev = dev.strip()
    rest = rest.strip().lstrip("/")
    if not rest:
        return dev, None
    # normalize: libguestfs frequently encodes root subvol as "@"
    if not rest.startswith("@"):
        # If it’s not @-style, still allow (btrfs allows arbitrary subvol names)
        return dev, rest
    return dev, rest
class Ident:
    @staticmethod
    def is_stable(spec: str) -> bool:
        u = spec.upper()
        return u.startswith(("UUID=", "PARTUUID=", "LABEL=", "PARTLABEL="))
    @staticmethod
    def g_blkid_map(g: guestfs.GuestFS, dev: str) -> Dict[str, str]:
        try:
            d = g.blkid(dev)
            return {str(k).upper(): str(v) for k, v in d.items() if v is not None}
        except Exception:
            return {}
    @staticmethod
    def choose_stable(blk: Dict[str, str]) -> Optional[str]:
        if blk.get("UUID"):
            return f"UUID={blk['UUID']}"
        if blk.get("PARTUUID"):
            return f"PARTUUID={blk['PARTUUID']}"
        if blk.get("LABEL"):
            return f"LABEL={blk['LABEL']}"
        if blk.get("PARTLABEL"):
            return f"PARTLABEL={blk['PARTLABEL']}"
        return None
    @staticmethod
    def root_dev_base(root_dev: Optional[str]) -> Optional[str]:
        if not root_dev:
            return None
        m = re.match(r"^(/dev/(?:nvme\d+n\d+|mmcblk\d+))p\d+$", root_dev)
        if m:
            return m.group(1)
        m = re.match(r"^(/dev/[a-zA-Z]+)\d+$", root_dev)
        if m:
            return m.group(1)
        return None
    @staticmethod
    def infer_partition_from_bypath(spec: str, root_dev: Optional[str]) -> Optional[str]:
        if not root_dev or not spec.startswith(_BYPATH_PREFIX):
            return None
        m = re.search(r"-part(\d+)$", spec)
        if not m:
            return None
        partno = int(m.group(1))
        base = Ident.root_dev_base(root_dev)
        if not base:
            return None
        if re.match(r"^/dev/(nvme\d+n\d+|mmcblk\d+)$", base):
            return f"{base}p{partno}"
        return f"{base}{partno}"
class FstabMode(str, Enum):
    STABILIZE_ALL = "stabilize-all"
    BYPATH_ONLY = "bypath-only"
    NOOP = "noop"
@dataclass
class Change:
    line_no: int
    mountpoint: str
    old: str
    new: str
    reason: str
