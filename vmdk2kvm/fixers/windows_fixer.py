# vmdk2kvm/fixers/windows_fixer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import guestfs  # type: ignore

from ..core.utils import U


# ---------------------------
# Windows Constants & Enums
# ---------------------------

class DriverType(Enum):
    STORAGE = "storage"
    NETWORK = "network"
    BALLOON = "balloon"
    INPUT = "input"
    GPU = "gpu"
    FILESYSTEM = "filesystem"


class WindowsEdition(Enum):
    SERVER_2022 = "server_2022"
    SERVER_2019 = "server_2019"
    SERVER_2016 = "server_2016"
    SERVER_2012 = "server_2012"
    SERVER_2008 = "server_2008"
    WINDOWS_11 = "windows_11"
    WINDOWS_10 = "windows_10"
    WINDOWS_8 = "windows_8"
    WINDOWS_7 = "windows_7"
    WINDOWS_VISTA = "vista"
    WINDOWS_XP = "xp"
    UNKNOWN = "unknown"


class DriverStartType(Enum):
    BOOT = 0
    SYSTEM = 1
    AUTO = 2
    MANUAL = 3
    DISABLED = 4


# ---------------------------
# Windows Configuration
# ---------------------------

@dataclass(frozen=True)
class WindowsVirtioPlan:
    """
    A high-level plan for what we intend to inject.

    Key philosophy:
      - For Windows boot reliability, storage drivers are special.
      - We inject *both* viostor and vioscsi when available and set them BOOT.
      - This avoids a very common BSOD: INACCESSIBLE_BOOT_DEVICE when the VM
        controller type changes (virtio-blk vs virtio-scsi).
    """
    arch_dir: str  # "amd64" | "x86" | "arm64" | "ia64"
    os_bucket: str  # bucket HINT only; discovery will try fallbacks (w11->w10->w8...)
    edition: WindowsEdition
    drivers_needed: Set[DriverType]

    @classmethod
    def default_needed(cls) -> Set[DriverType]:
        return {DriverType.STORAGE, DriverType.NETWORK, DriverType.BALLOON}


@dataclass
class DriverFile:
    """
    Represents a driver we found on the host and intend to inject into guest.
    """
    name: str
    type: DriverType
    src_path: Path
    dest_name: str

    # Registry service configuration
    start_type: DriverStartType
    service_name: str

    # PCI IDs used for CriticalDeviceDatabase mapping (boot-critical for storage)
    pci_ids: List[str]

    # Class GUID for device class
    class_guid: str

    # Optional: directory containing the full driver package (INF/CAT/SYS/etc.)
    package_dir: Optional[Path] = None

    # Optional: INF path (if discovered)
    inf_path: Optional[Path] = None

    # For reporting: which bucket/layout matched
    bucket_used: Optional[str] = None
    match_pattern: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type.value,
            "src_path": str(self.src_path),
            "dest_name": self.dest_name,
            "start_type": self.start_type.value,
            "service_name": self.service_name,
            "pci_ids": list(self.pci_ids),
            "class_guid": self.class_guid,
            "package_dir": str(self.package_dir) if self.package_dir else None,
            "inf_path": str(self.inf_path) if self.inf_path else None,
            "bucket_used": self.bucket_used,
            "match_pattern": self.match_pattern,
        }


def _plan_to_dict(plan: WindowsVirtioPlan) -> Dict[str, Any]:
    return {
        "arch_dir": plan.arch_dir,
        "os_bucket": plan.os_bucket,
        "edition": plan.edition.value,
        "drivers_needed": sorted([d.value for d in plan.drivers_needed]),
    }


# ---------------------------
# Logging + Safe helpers
# ---------------------------

def _safe_logger(self) -> logging.Logger:
    lg = getattr(self, "logger", None)
    if isinstance(lg, logging.Logger):
        return lg
    return logging.getLogger("vmdk2kvm.windows_fixer")


def _to_int(v: Any, default: int = 0) -> int:
    if isinstance(v, int):
        return v
    try:
        return int(float(v)) if isinstance(v, (float, str)) else default
    except Exception:
        return default


def _normalize_product_name(name: str) -> str:
    if not name:
        return ""
    normalized = name.lower()
    normalized = re.sub(r"\([^)]*\)", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _json_safe(obj: Any) -> str:
    try:
        return json.dumps(obj, indent=2, sort_keys=True, default=str)
    except Exception:
        return str(obj)


# ---------------------------
# Registry encoding helpers (CRITICAL)
# ---------------------------
# Windows registry strings in hives are generally UTF-16LE, NUL-terminated.
# Writing these as UTF-8 works "sometimes" but is not reliable.
# These helpers make the encoding explicit and correct.

def _reg_sz(s: str) -> bytes:
    return (s + "\0").encode("utf-16le", errors="ignore")


def _set_sz(h, node, key: str, s: str) -> None:
    # REG_SZ => t=1 in hivex
    h.node_set_value(node, {"key": key, "t": 1, "value": _reg_sz(s)})


def _set_expand_sz(h, node, key: str, s: str) -> None:
    # REG_EXPAND_SZ => t=2 in hivex
    h.node_set_value(node, {"key": key, "t": 2, "value": _reg_sz(s)})


def _set_dword(h, node, key: str, v: int) -> None:
    # REG_DWORD => t=4 in hivex
    h.node_set_value(node, {"key": key, "t": 4, "value": int(v).to_bytes(4, "little", signed=False)})


def _ensure_child(h, parent, name: str):
    ch = h.node_get_child(parent, name)
    if ch is None:
        ch = h.node_add_child(parent, name)
    return ch


def _delete_child_if_exists(h, parent, name: str) -> bool:
    """
    Remove a child key node if present (commonly StartOverride).
    """
    try:
        n = h.node_get_child(parent, name)
        if n is None:
            return False
        h.node_delete_child(n)
        return True
    except Exception:
        return False


def _hivex_read_sz(h, node, key: str) -> Optional[str]:
    """
    Read REG_SZ/REG_EXPAND_SZ-ish as text (best-effort).
    hivex returns bytes often UTF-16LE for string values.
    """
    try:
        v = h.node_get_value(node, key)
        if not v or "value" not in v:
            return None
        raw = v["value"]
        if isinstance(raw, (bytes, bytearray)):
            try:
                s = raw.decode("utf-16le", errors="ignore").rstrip("\x00")
                return s.strip() or None
            except Exception:
                try:
                    s = raw.decode("utf-8", errors="ignore").rstrip("\x00")
                    return s.strip() or None
                except Exception:
                    return None
        return str(raw).strip() or None
    except Exception:
        return None


def _hivex_read_dword(h, node, key: str) -> Optional[int]:
    try:
        v = h.node_get_value(node, key)
        if not v or "value" not in v:
            return None
        raw = v["value"]
        if isinstance(raw, (bytes, bytearray)) and len(raw) >= 4:
            return int.from_bytes(raw[:4], "little", signed=False)
        if isinstance(raw, int):
            return raw
        return None
    except Exception:
        return None


# ---------------------------
# GuestFS helpers
# ---------------------------

def _guest_download_bytes(g: guestfs.GuestFS, guest_path: str, max_bytes: Optional[int] = None) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        lp = Path(td) / "dl"
        g.download(guest_path, str(lp))
        b = lp.read_bytes()
        if max_bytes is not None:
            return b[:max_bytes]
        return b


def _guest_sha256(g: guestfs.GuestFS, guest_path: str) -> Optional[str]:
    try:
        b = _guest_download_bytes(g, guest_path)
        return hashlib.sha256(b).hexdigest()
    except Exception:
        return None


# ---------------------------
# Windows detection
# ---------------------------

def is_windows(self, g: guestfs.GuestFS) -> bool:
    """
    Multi-signal detection:
      1) libguestfs inspection type
      2) presence of common Windows directories
      3) presence of SOFTWARE hive
    """
    logger = _safe_logger(self)

    if not getattr(self, "inspect_root", None):
        logger.debug("Windows detect: inspect_root missing -> not Windows")
        return False

    root = self.inspect_root
    try:
        try:
            os_type = U.to_text(g.inspect_get_type(root))
            if os_type and os_type.lower() == "windows":
                logger.debug("Windows detect: inspect_get_type says windows")
                return True
        except Exception:
            pass

        for dir_path in ["/Windows", "/WINDOWS", "/winnt", "/WINNT", "/Program Files"]:
            try:
                if g.is_dir(dir_path):
                    logger.debug(f"Windows detect: found dir {dir_path}")
                    return True
            except Exception:
                continue

        for reg_file in [
            "/Windows/System32/config/SOFTWARE",
            "/WINDOWS/System32/config/SOFTWARE",
            "/winnt/system32/config/SOFTWARE",
        ]:
            try:
                if g.is_file(reg_file):
                    logger.debug(f"Windows detect: found SOFTWARE hive {reg_file}")
                    return True
            except Exception:
                continue

        logger.debug("Windows detect: no signals -> not Windows")
        return False
    except Exception as e:
        logger.debug(f"Windows detect: exception -> not Windows: {e}")
        return False


def _find_windows_root(self, g: guestfs.GuestFS) -> Optional[str]:
    """
    Find Windows installation root directory inside the guest filesystem.
    """
    logger = _safe_logger(self)

    candidates = ["/Windows", "/WINDOWS", "/winnt", "/WINNT"]
    for p in candidates:
        try:
            if g.is_dir(p):
                logger.debug(f"Windows root: found {p}")
                return p
        except Exception:
            continue

    logger.debug("Windows root: no direct hit")
    return None


# ---------------------------
# Windows version enrichment
# ---------------------------

def _read_software_hive_version(self, g: guestfs.GuestFS, software_hive_path: str) -> Dict[str, Any]:
    """
    Parse SOFTWARE hive offline for:
      SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\ProductName etc.
    """
    logger = _safe_logger(self)
    out: Dict[str, Any] = {}

    try:
        if not g.is_file(software_hive_path):
            return out
    except Exception:
        return out

    with tempfile.TemporaryDirectory() as tmpdir:
        local_hive = Path(tmpdir) / "SOFTWARE"
        try:
            g.download(software_hive_path, str(local_hive))
            h = g.hivex_open(str(local_hive), write=0)
            root = h.root()

            microsoft = h.node_get_child(root, "Microsoft")
            if microsoft is None:
                return out
            windows_nt = h.node_get_child(microsoft, "Windows NT")
            if windows_nt is None:
                return out
            cv = h.node_get_child(windows_nt, "CurrentVersion")
            if cv is None:
                return out

            out.update(
                {
                    "reg_product_name": _hivex_read_sz(h, cv, "ProductName"),
                    "reg_current_build": _hivex_read_sz(h, cv, "CurrentBuild") or _hivex_read_sz(h, cv, "CurrentBuildNumber"),
                    "reg_display_version": _hivex_read_sz(h, cv, "DisplayVersion") or _hivex_read_sz(h, cv, "ReleaseId"),
                    "reg_edition_id": _hivex_read_sz(h, cv, "EditionID"),
                    "reg_installation_type": _hivex_read_sz(h, cv, "InstallationType"),
                    "reg_major": _hivex_read_dword(h, cv, "CurrentMajorVersionNumber"),
                    "reg_minor": _hivex_read_dword(h, cv, "CurrentMinorVersionNumber"),
                }
            )

            try:
                h.hivex_close()
            except Exception:
                pass

            logger.debug(f"SOFTWARE hive: extracted keys: { {k: out.get(k) for k in out.keys()} }")
            return out

        except Exception as e:
            logger.debug(f"SOFTWARE hive parse failed: {e}")
            return out


def _windows_version_info(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    """
    Gather Windows metadata from:
      - libguestfs inspection
      - SOFTWARE hive enrichment (best effort)
    """
    logger = _safe_logger(self)

    info: Dict[str, Any] = {
        "windows": True,
        "edition": WindowsEdition.UNKNOWN,
        "bits": 64,
        "build": None,
        "product_name": None,
        "arch": None,
        "major": None,
        "minor": None,
    }

    root = getattr(self, "inspect_root", None)
    if not root:
        logger.debug("Windows info: inspect_root missing")
        return info

    try:
        info["arch"] = U.to_text(g.inspect_get_arch(root))
        info["major"] = g.inspect_get_major_version(root)
        info["minor"] = g.inspect_get_minor_version(root)
        info["product_name"] = U.to_text(g.inspect_get_product_name(root))
        info["distro"] = U.to_text(g.inspect_get_distro(root))
    except Exception as e:
        logger.debug(f"Windows info: inspect getters failed: {e}")

    arch = (info.get("arch") or "").lower()
    if arch in ("x86_64", "amd64", "arm64", "aarch64"):
        info["bits"] = 64
    elif arch in ("i386", "i686", "x86"):
        info["bits"] = 32
    else:
        info["bits"] = 64

    try:
        windows_root = _find_windows_root(self, g)
        if windows_root:
            software_hive = f"{windows_root}/System32/config/SOFTWARE"
            reg_info = _read_software_hive_version(self, g, software_hive)
            info.update(reg_info)

            if not info.get("product_name") and reg_info.get("reg_product_name"):
                info["product_name"] = reg_info["reg_product_name"]

            if info.get("major") is None and reg_info.get("reg_major") is not None:
                info["major"] = reg_info["reg_major"]
            if info.get("minor") is None and reg_info.get("reg_minor") is not None:
                info["minor"] = reg_info["reg_minor"]

            if not info.get("build") and reg_info.get("reg_current_build"):
                info["build"] = reg_info.get("reg_current_build")
    except Exception as e:
        logger.debug(f"Windows info: enrichment failed: {e}")

    return info


def _detect_windows_edition(self, win_info: Dict[str, Any]) -> WindowsEdition:
    major = _to_int(win_info.get("major"))
    minor = _to_int(win_info.get("minor"))
    product = _normalize_product_name(str(win_info.get("product_name", "") or ""))

    # Servers
    if "server 2022" in product:
        return WindowsEdition.SERVER_2022
    if "server 2019" in product:
        return WindowsEdition.SERVER_2019
    if "server 2016" in product:
        return WindowsEdition.SERVER_2016
    if "server 2012" in product:
        return WindowsEdition.SERVER_2012
    if "server 2008" in product:
        return WindowsEdition.SERVER_2008

    # Clients
    if "windows 11" in product or major >= 11:
        return WindowsEdition.WINDOWS_11
    if "windows 10" in product or major == 10:
        return WindowsEdition.WINDOWS_10
    if major == 6 and minor >= 2:
        return WindowsEdition.WINDOWS_8
    if major == 6 and minor == 1:
        return WindowsEdition.WINDOWS_7
    if major == 6 and minor == 0:
        return WindowsEdition.WINDOWS_VISTA
    if major == 5:
        return WindowsEdition.WINDOWS_XP

    return WindowsEdition.UNKNOWN


def _norm_arch_to_dir(arch: str) -> str:
    a = (arch or "").lower().strip()
    mapping = {
        "x86_64": "amd64",
        "amd64": "amd64",
        "x64": "amd64",
        "i386": "x86",
        "i686": "x86",
        "x86": "x86",
        "ia64": "ia64",
        "arm64": "arm64",
        "aarch64": "arm64",
    }
    return mapping.get(a, "amd64")


# ---------------------------
# Bucket fallback logic (Win11 support + ISO layout variation)
# ---------------------------

def _bucket_candidates(edition: WindowsEdition) -> List[str]:
    """
    Virtio-win ISOs differ by release:
      - Some include w11/
      - Some only include w10/ (even for Windows 11)
      - Older ones include w8/ w7/ vista/ xp
    We therefore try a *sequence* of buckets, not just one.
    """
    if edition == WindowsEdition.WINDOWS_11:
        return ["w11", "w10", "w8", "w7"]
    if edition in (WindowsEdition.WINDOWS_10, WindowsEdition.SERVER_2022, WindowsEdition.SERVER_2019, WindowsEdition.SERVER_2016):
        return ["w10", "w11", "w8", "w7"]
    if edition in (WindowsEdition.WINDOWS_8, WindowsEdition.SERVER_2012):
        return ["w8", "w10", "w7"]
    if edition in (WindowsEdition.WINDOWS_7, WindowsEdition.SERVER_2008):
        return ["w7", "w8", "w10"]
    if edition == WindowsEdition.WINDOWS_VISTA:
        return ["vista", "w7", "w8"]
    if edition == WindowsEdition.WINDOWS_XP:
        return ["xp", "w7"]
    return ["w11", "w10", "w8", "w7"]


def _choose_driver_plan(self, win_info: Dict[str, Any]) -> WindowsVirtioPlan:
    """
    Determine which driver buckets to use and which driver types to inject.

    Updates:
      - Windows 11 gets an os_bucket *hint* of "w11" (but we still fall back).
      - We keep os_bucket primarily as a hint for "first attempt"; discovery tries candidates.
      - We do NOT choose one storage driver; discovery will attempt both viostor + vioscsi.
    """
    logger = _safe_logger(self)

    edition = _detect_windows_edition(self, win_info)
    arch_dir = _norm_arch_to_dir(str(win_info.get("arch") or "amd64"))

    edition_to_bucket = {
        WindowsEdition.SERVER_2022: "w10",
        WindowsEdition.SERVER_2019: "w10",
        WindowsEdition.SERVER_2016: "w10",
        WindowsEdition.SERVER_2012: "w8",
        WindowsEdition.SERVER_2008: "w7",
        WindowsEdition.WINDOWS_11: "w11",
        WindowsEdition.WINDOWS_10: "w10",
        WindowsEdition.WINDOWS_8: "w8",
        WindowsEdition.WINDOWS_7: "w7",
        WindowsEdition.WINDOWS_VISTA: "vista",
        WindowsEdition.WINDOWS_XP: "xp",
        WindowsEdition.UNKNOWN: "w10",
    }
    os_bucket = edition_to_bucket.get(edition, "w10")

    drivers_needed = WindowsVirtioPlan.default_needed()

    # Optional types based on flags
    if getattr(self, "enable_virtio_gpu", False):
        drivers_needed.add(DriverType.GPU)
    if getattr(self, "enable_virtio_input", False):
        drivers_needed.add(DriverType.INPUT)
    if getattr(self, "enable_virtio_fs", False):
        drivers_needed.add(DriverType.FILESYSTEM)

    plan = WindowsVirtioPlan(
        arch_dir=arch_dir,
        os_bucket=os_bucket,
        edition=edition,
        drivers_needed=drivers_needed,
    )

    logger.info(
        "Windows plan:"
        f" edition={plan.edition.value}"
        f" arch={plan.arch_dir}"
        f" bucket_hint={plan.os_bucket}"
        f" bucket_candidates={_bucket_candidates(plan.edition)}"
        f" drivers_needed={sorted([d.value for d in plan.drivers_needed])}"
    )

    return plan


# ---------------------------
# Driver discovery (SYS + package (INF/CAT/...))
# ---------------------------

def _is_probably_driver_payload(p: Path) -> bool:
    # conservative: copy only driver-ish payload files; avoid huge EXEs/MSIs unless user wants them
    ext = p.suffix.lower()
    if ext in (".inf", ".cat", ".sys", ".dll", ".exe", ".mui", ".dat", ".bin", ".ini"):
        return True
    return False


def _find_package_dir_for_file(path: Path) -> Path:
    # In virtio-win, sys/inf are typically in the same directory. Use parent.
    return path.parent


def _discover_virtio_drivers(self, drivers_dir: Path, plan: WindowsVirtioPlan) -> List[DriverFile]:
    """
    Discover drivers from virtio-win directory.

    IMPORTANT:
      - For STORAGE, we discover BOTH viostor and vioscsi (if present).
      - Both will be set BOOT-start in registry edits.
      - We support Win11 by trying bucket fallbacks.
      - We *also* try to locate corresponding .inf in the same package directory.
    """
    logger = _safe_logger(self)
    drivers: List[DriverFile] = []

    buckets = _bucket_candidates(plan.edition)

    # Common PCI IDs and class GUIDs for virtio devices (CDD mapping)
    storage_class_guid = "{4D36E967-E325-11CE-BFC1-08002BE10318}"  # SCSIAdapter
    net_class_guid = "{4D36E972-E325-11CE-BFC1-08002BE10318}"      # Net
    balloon_class_guid = "{4D36E97D-E325-11CE-BFC1-08002BE10318}"  # System

    driver_configs: Dict[DriverType, List[Dict[str, Any]]] = {
        DriverType.STORAGE: [
            {
                "name": "viostor",
                "pattern": "viostor/{bucket}/{arch}/viostor.sys",
                "inf_hint": "viostor.inf",
                "service": "viostor",
                "start": DriverStartType.BOOT,
                "pci_ids": [
                    "pci#ven_1af4&dev_1001&subsys_00081af4",
                    "pci#ven_1af4&dev_1042&subsys_00081af4",
                ],
                "class_guid": storage_class_guid,
            },
            {
                "name": "vioscsi",
                "pattern": "vioscsi/{bucket}/{arch}/vioscsi.sys",
                "inf_hint": "vioscsi.inf",
                "service": "vioscsi",
                "start": DriverStartType.BOOT,
                "pci_ids": [
                    "pci#ven_1af4&dev_1004&subsys_00081af4",
                    "pci#ven_1af4&dev_1048&subsys_00081af4",
                ],
                "class_guid": storage_class_guid,
            },
        ],
        DriverType.NETWORK: [
            {
                "name": "NetKVM",
                "pattern": "NetKVM/{bucket}/{arch}/netkvm.sys",
                "inf_hint": "netkvm.inf",
                "service": "netkvm",
                "start": DriverStartType.AUTO,
                "pci_ids": [
                    "pci#ven_1af4&dev_1000&subsys_00081af4",
                    "pci#ven_1af4&dev_1041&subsys_00081af4",
                ],
                "class_guid": net_class_guid,
            },
        ],
        DriverType.BALLOON: [
            {
                "name": "Balloon",
                "pattern": "Balloon/{bucket}/{arch}/balloon.sys",
                "inf_hint": "balloon.inf",
                "service": "balloon",
                "start": DriverStartType.AUTO,
                "pci_ids": [
                    "pci#ven_1af4&dev_1002&subsys_00051af4",
                    "pci#ven_1af4&dev_1045&subsys_00051af4",
                ],
                "class_guid": balloon_class_guid,
            },
        ],
        DriverType.GPU: [
            {
                "name": "viogpudo",
                "pattern": "viogpudo/{bucket}/{arch}/viogpudo.sys",
                "inf_hint": "viogpudo.inf",
                "service": "viogpudo",
                "start": DriverStartType.MANUAL,
                "pci_ids": ["pci#ven_1af4&dev_1050&subsys_11001af4"],
                "class_guid": "{4D36E968-E325-11CE-BFC1-08002BE10318}",
            },
        ],
        DriverType.INPUT: [
            {
                "name": "vioinput",
                "pattern": "vioinput/{bucket}/{arch}/vioinput.sys",
                "inf_hint": "vioinput.inf",
                "service": "vioinput",
                "start": DriverStartType.MANUAL,
                "pci_ids": ["pci#ven_1af4&dev_1052&subsys_11001af4"],
                "class_guid": "{4D36E96F-E325-11CE-BFC1-08002BE10318}",
            },
        ],
        DriverType.FILESYSTEM: [
            {
                "name": "virtiofs",
                "pattern": "virtiofs/{bucket}/{arch}/virtiofs.sys",
                "inf_hint": "virtiofs.inf",
                "service": "virtiofs",
                "start": DriverStartType.SYSTEM,
                "pci_ids": ["pci#ven_1af4&dev_105a&subsys_11001af4"],
                "class_guid": storage_class_guid,
            },
        ],
    }

    # Layout variations (additive)
    search_patterns = [
        "{pattern}",                       # canonical
        "{driver}/{bucket}/{arch}/*.sys",   # sys name differs
        "{driver}/{arch}/*.sys",            # bucket-less
        "{driver}/*/{arch}/*.sys",          # unknown bucket folder
        "{driver}/*/*/{arch}/*.sys",        # deeper nesting
    ]

    logger.info("Discovering VirtIO drivers...")
    logger.info(f"VirtIO dir: {drivers_dir}")
    logger.info(f"Plan: edition={plan.edition.value} bucket_hint={plan.os_bucket} arch={plan.arch_dir}")
    logger.info(f"Bucket candidates: {buckets}")
    logger.debug(f"Driver types requested: {sorted([d.value for d in plan.drivers_needed])}")

    def _try_candidate_glob(pat: str) -> Optional[Path]:
        # pat is relative to drivers_dir
        try:
            matches = sorted(drivers_dir.glob(pat))
            matches = [m for m in matches if m.is_file()]
            if matches:
                return matches[0]
        except Exception:
            return None
        return None

    def _find_inf_near_sys(sys_path: Path, inf_hint: Optional[str]) -> Optional[Path]:
        pkg = sys_path.parent
        try:
            if inf_hint:
                cand = pkg / inf_hint
                if cand.exists() and cand.is_file():
                    return cand
            # fallback: first .inf in same directory
            infs = sorted([p for p in pkg.glob("*.inf") if p.is_file()])
            if infs:
                return infs[0]
        except Exception:
            return None
        return None

    for driver_type in plan.drivers_needed:
        cfgs = driver_configs.get(driver_type, [])
        if not cfgs:
            logger.debug(f"No config mapping for driver type: {driver_type.value}")
            continue

        for cfg in cfgs:
            driver_name = cfg["name"]
            service = cfg["service"]
            found = False

            for bucket in buckets:
                if found:
                    break

                for tmpl in search_patterns:
                    canonical = cfg["pattern"].format(bucket=bucket, arch=plan.arch_dir)
                    pat = tmpl.format(
                        pattern=canonical,
                        driver=driver_name,
                        bucket=bucket,
                        arch=plan.arch_dir,
                    )

                    src = _try_candidate_glob(pat)
                    if src is None:
                        logger.debug(f"Not found: type={driver_type.value} name={driver_name} bucket={bucket} pat={pat}")
                        continue

                    infp = _find_inf_near_sys(src, cfg.get("inf_hint"))
                    pkg_dir = _find_package_dir_for_file(src)

                    drivers.append(
                        DriverFile(
                            name=driver_name,
                            type=driver_type,
                            src_path=src,
                            dest_name=f"{service}.sys",
                            start_type=cfg["start"],
                            service_name=service,
                            pci_ids=list(cfg["pci_ids"]),
                            class_guid=cfg["class_guid"],
                            package_dir=pkg_dir,
                            inf_path=infp,
                            bucket_used=bucket,
                            match_pattern=pat,
                        )
                    )
                    logger.info(f"Found {driver_type.value} driver: {driver_name} bucket={bucket} -> {src}")
                    if infp:
                        logger.info(f" - INF: {infp}")
                    found = True
                    break

            if not found:
                lvl = logging.WARNING if driver_type == DriverType.STORAGE else logging.INFO
                logger.log(
                    lvl,
                    "Driver not found:"
                    f" type={driver_type.value}"
                    f" name={driver_name}"
                    f" arch={plan.arch_dir}"
                    f" buckets_tried={buckets}",
                )

    logger.info(f"Driver discovery complete: {len(drivers)} driver(s) found.")
    found_storage = sorted([d.service_name for d in drivers if d.type == DriverType.STORAGE])
    if not found_storage:
        logger.warning(
            "No storage drivers found (viostor/vioscsi). "
            "If you boot Windows on virtio storage, this is very likely to BSOD (INACCESSIBLE_BOOT_DEVICE)."
        )
    elif len(found_storage) == 1:
        logger.warning(
            f"Only one storage driver found: {found_storage}. "
            "If the VM uses the other controller model (virtio-blk vs virtio-scsi), Windows may BSOD. "
            "Best practice: ensure both viostor and vioscsi are available in your virtio-win directory."
        )

    for d in drivers:
        logger.debug(
            f" - {d.type.value}:{d.service_name}"
            f" src={d.src_path}"
            f" dest={d.dest_name}"
            f" start={d.start_type.value}"
            f" pci_ids={len(d.pci_ids)}"
            f" bucket={d.bucket_used}"
        )

    return drivers


# ---------------------------
# Registry operations (boot-critical!)
# ---------------------------

def _edit_windows_registry(
    self,
    g: guestfs.GuestFS,
    hive_path: str,
    drivers: List[DriverFile],
    plan: WindowsVirtioPlan,
) -> Dict[str, Any]:
    """
    Edit SYSTEM hive offline to:
      - Create Services\\<driver> keys with correct Types/Start/ImagePath/Group
      - Add CriticalDeviceDatabase entries for STORAGE drivers
      - Remove StartOverride keys that frequently disable boot drivers
    """
    logger = _safe_logger(self)
    dry_run = getattr(self, "dry_run", False)

    results: Dict[str, Any] = {
        "success": False,
        "dry_run": bool(dry_run),
        "registry_modified": False,
        "hive_path": hive_path,
        "errors": [],
        "services": [],
        "cdd": [],
        "startoverride_removed": [],
        "notes": [],
    }

    try:
        if not g.is_file(hive_path):
            results["errors"].append(f"SYSTEM hive not found: {hive_path}")
            return results
    except Exception as e:
        results["errors"].append(f"Failed to stat hive {hive_path}: {e}")
        return results

    with tempfile.TemporaryDirectory() as tmpdir:
        local_hive = Path(tmpdir) / "SYSTEM"

        try:
            logger.info(f"Downloading SYSTEM hive: {hive_path} -> {local_hive}")
            g.download(hive_path, str(local_hive))

            orig_bytes = local_hive.read_bytes()
            original_hash = hashlib.sha256(orig_bytes).hexdigest()
            logger.debug(f"SYSTEM hive baseline sha256={original_hash}")

            write_mode = 0 if dry_run else 1
            h = g.hivex_open(str(local_hive), write=write_mode)
            root = h.root()

            # ---- Determine current ControlSet ----
            select = h.node_get_child(root, "Select")
            if select is None:
                raise RuntimeError("SYSTEM hive missing Select key")

            cur = h.node_get_value(select, "Current")
            if cur is None or "value" not in cur:
                raise RuntimeError("SYSTEM hive missing Select\\Current value")

            # IMPORTANT: slice to 4 bytes to avoid weirdness
            cur_raw = cur["value"]
            if isinstance(cur_raw, (bytes, bytearray)) and len(cur_raw) >= 4:
                current_set = int.from_bytes(cur_raw[:4], "little", signed=False)
            elif isinstance(cur_raw, int):
                current_set = int(cur_raw)
            else:
                current_set = 1

            cs_name = f"ControlSet{current_set:03d}"
            logger.info(f"Using control set: {cs_name}")

            control_set = h.node_get_child(root, cs_name)
            if control_set is None:
                logger.warning(f"{cs_name} missing; falling back to ControlSet001")
                cs_name = "ControlSet001"
                control_set = h.node_get_child(root, cs_name)
                if control_set is None:
                    raise RuntimeError("No usable ControlSet found (001/current)")

            # ---- Ensure Services root ----
            services = _ensure_child(h, control_set, "Services")

            # ---- Create / update Services\\<driver> entries ----
            for drv in drivers:
                try:
                    svc = h.node_get_child(services, drv.service_name)
                    action = "updated" if svc is not None else "created"
                    if svc is None:
                        svc = h.node_add_child(services, drv.service_name)

                    logger.info(f"Registry service {action}: Services\\{drv.service_name}")

                    _set_dword(h, svc, "Type", 1)         # SERVICE_KERNEL_DRIVER
                    _set_dword(h, svc, "ErrorControl", 1) # normal error handling

                    start = drv.start_type.value
                    if drv.type == DriverType.STORAGE:
                        start = DriverStartType.BOOT.value
                    _set_dword(h, svc, "Start", start)

                    if drv.type == DriverType.STORAGE:
                        group = "SCSI miniport"
                    elif drv.type == DriverType.NETWORK:
                        group = "NDIS"
                    else:
                        group = "System Bus Extender"

                    # Use canonical ImagePath style
                    _set_sz(h, svc, "Group", group)
                    _set_sz(h, svc, "ImagePath", fr"\SystemRoot\System32\drivers\{drv.dest_name}")
                    _set_sz(h, svc, "DisplayName", drv.service_name)

                    removed = _delete_child_if_exists(h, svc, "StartOverride")
                    if removed:
                        logger.info(f"Removed StartOverride: Services\\{drv.service_name}\\StartOverride")
                        results["startoverride_removed"].append(drv.service_name)

                    results["services"].append(
                        {
                            "service": drv.service_name,
                            "type": drv.type.value,
                            "start": start,
                            "group": group,
                            "image": fr"\SystemRoot\System32\drivers\{drv.dest_name}",
                            "action": action,
                        }
                    )

                except Exception as e:
                    msg = f"Failed to create/update service {drv.service_name}: {e}"
                    logger.error(msg)
                    results["errors"].append(msg)

            # ---- CriticalDeviceDatabase for storage ----
            control = _ensure_child(h, control_set, "Control")
            cdd = _ensure_child(h, control, "CriticalDeviceDatabase")

            for drv in drivers:
                if drv.type != DriverType.STORAGE:
                    continue

                for pci_id in drv.pci_ids:
                    try:
                        node = h.node_get_child(cdd, pci_id)
                        action = "updated" if node is not None else "created"
                        if node is None:
                            node = h.node_add_child(cdd, pci_id)

                        _set_sz(h, node, "Service", drv.service_name)
                        _set_sz(h, node, "ClassGUID", drv.class_guid)

                        _set_sz(h, node, "Class", "SCSIAdapter")
                        _set_sz(h, node, "DeviceDesc", drv.name)

                        logger.info(f"CDD {action}: {pci_id} -> {drv.service_name}")
                        results["cdd"].append({"pci_id": pci_id, "service": drv.service_name, "action": action})

                    except Exception as e:
                        msg = f"Failed CDD entry {pci_id} -> {drv.service_name}: {e}"
                        logger.error(msg)
                        results["errors"].append(msg)

            # ---- Commit + upload if not dry_run ----
            if not dry_run:
                logger.info("Committing SYSTEM hive changes (hivex_commit)")
                h.hivex_commit(None)
                h.hivex_close()

                logger.info(f"Uploading modified SYSTEM hive back to guest: {hive_path}")
                g.upload(str(local_hive), hive_path)

                # Verify by hash
                with tempfile.TemporaryDirectory() as verify_tmp:
                    verify_path = Path(verify_tmp) / "SYSTEM_verify"
                    g.download(hive_path, str(verify_path))
                    new_hash = hashlib.sha256(verify_path.read_bytes()).hexdigest()

                if new_hash != original_hash:
                    results["registry_modified"] = True
                    logger.info(f"SYSTEM hive changed: sha256 {original_hash} -> {new_hash}")
                else:
                    logger.warning("SYSTEM hive appears unchanged after upload (unexpected)")

            else:
                try:
                    h.hivex_close()
                except Exception:
                    pass
                logger.info("Dry-run: registry edits computed but not committed/uploaded")

            results["success"] = len(results["errors"]) == 0
            results["notes"] += [
                "Storage services forced to BOOT start to prevent INACCESSIBLE_BOOT_DEVICE.",
                "StartOverride keys removed (if present) because they can silently disable drivers.",
                "Registry strings written as UTF-16LE REG_SZ (Windows-correct).",
                "CriticalDeviceDatabase populated for storage PCI IDs.",
                f"Buckets tried in discovery: {_bucket_candidates(plan.edition)}",
            ]
            return results

        except Exception as e:
            msg = f"Registry editing failed: {e}"
            logger.error(msg)
            results["errors"].append(msg)
            return results


def _edit_software_devicepath(
    self,
    g: guestfs.GuestFS,
    software_hive_path: str,
    append_path: str,
) -> Dict[str, Any]:
    """
    Add append_path to:
      HKLM\\SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\DevicePath

    This helps PnP find your staged drivers on first boot (offline-friendly).
    """
    logger = _safe_logger(self)
    dry_run = getattr(self, "dry_run", False)

    out: Dict[str, Any] = {
        "success": False,
        "dry_run": bool(dry_run),
        "hive_path": software_hive_path,
        "modified": False,
        "original": None,
        "new": None,
        "errors": [],
        "notes": [],
    }

    try:
        if not g.is_file(software_hive_path):
            out["errors"].append(f"SOFTWARE hive not found: {software_hive_path}")
            return out
    except Exception as e:
        out["errors"].append(f"Failed to stat hive {software_hive_path}: {e}")
        return out

    with tempfile.TemporaryDirectory() as td:
        local = Path(td) / "SOFTWARE"
        try:
            g.download(software_hive_path, str(local))
            orig_hash = hashlib.sha256(local.read_bytes()).hexdigest()

            h = g.hivex_open(str(local), write=(0 if dry_run else 1))
            root = h.root()

            microsoft = h.node_get_child(root, "Microsoft")
            if microsoft is None:
                out["errors"].append("SOFTWARE hive missing Microsoft key")
                try:
                    h.hivex_close()
                except Exception:
                    pass
                return out

            windows = h.node_get_child(microsoft, "Windows")
            if windows is None:
                out["errors"].append("SOFTWARE hive missing Microsoft\\Windows key")
                try:
                    h.hivex_close()
                except Exception:
                    pass
                return out

            cv = h.node_get_child(windows, "CurrentVersion")
            if cv is None:
                out["errors"].append("SOFTWARE hive missing Microsoft\\Windows\\CurrentVersion key")
                try:
                    h.hivex_close()
                except Exception:
                    pass
                return out

            cur = _hivex_read_sz(h, cv, "DevicePath") or r"%SystemRoot%\inf"
            out["original"] = cur

            # normalize: ensure semicolon-separated
            parts = [p.strip() for p in cur.split(";") if p.strip()]
            if append_path not in parts:
                parts.append(append_path)
            new = ";".join(parts)
            out["new"] = new

            if new != cur:
                logger.info(f"Updating DevicePath: +{append_path}")
                # DevicePath is typically REG_EXPAND_SZ
                _set_expand_sz(h, cv, "DevicePath", new)
                out["modified"] = True
            else:
                logger.info("DevicePath already contains staging path; no change needed")

            if not dry_run:
                h.hivex_commit(None)
                h.hivex_close()
                g.upload(str(local), software_hive_path)

                # verify
                with tempfile.TemporaryDirectory() as vtd:
                    vlocal = Path(vtd) / "SOFTWARE_verify"
                    g.download(software_hive_path, str(vlocal))
                    new_hash = hashlib.sha256(vlocal.read_bytes()).hexdigest()
                if new_hash != orig_hash:
                    out["success"] = True
                else:
                    out["success"] = not out["modified"]
            else:
                try:
                    h.hivex_close()
                except Exception:
                    pass
                out["success"] = True

            out["notes"] += [
                "DevicePath updated to help Windows PnP discover staged INF packages on first boot.",
                "Value written as REG_EXPAND_SZ (UTF-16LE).",
            ]
            return out

        except Exception as e:
            out["errors"].append(f"DevicePath update failed: {e}")
            return out


# ---------------------------
# Public: BCD "actual fix" (kept)
# ---------------------------

def windows_bcd_actual_fix(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    """
    Offline-safe BCD store handling:
      - locate BIOS + UEFI stores
      - backup if not dry-run
    """
    logger = _safe_logger(self)

    if not is_windows(self, g):
        return {"windows": False, "reason": "not_windows"}

    logger.info("Windows detected - checking BCD stores")

    windows_root = _find_windows_root(self, g)
    if not windows_root:
        return {"windows": True, "bcd": "no_windows_directory"}

    bcd_stores = {
        "bios": f"{windows_root}/Boot/BCD",
        "uefi_standard": "/boot/efi/EFI/Microsoft/Boot/BCD",
        "uefi_alternative": "/boot/EFI/Microsoft/Boot/BCD",
        "uefi_fallback": "/efi/EFI/Microsoft/Boot/BCD",
        "uefi_root": "/EFI/Microsoft/Boot/BCD",
    }

    found: Dict[str, Any] = {}
    backups: Dict[str, Any] = {}
    dry_run = getattr(self, "dry_run", False)

    for store_type, store_path in bcd_stores.items():
        try:
            if g.is_file(store_path):
                size = g.filesize(store_path)
                logger.info(f"Found BCD ({store_type}): {store_path} ({size} bytes)")
                found[store_type] = {"path": store_path, "size": size, "exists": True}

                if not dry_run:
                    ts = U.now_ts()
                    backup_path = f"{store_path}.backup.vmdk2kvm.{ts}"
                    try:
                        g.cp(store_path, backup_path)
                        backups[store_type] = {"backup_path": backup_path, "timestamp": ts, "size": size}
                        logger.info(f"BCD backup created: {backup_path}")
                    except Exception as be:
                        logger.warning(f"BCD backup failed for {store_path}: {be}")
                        backups[store_type] = {"error": str(be), "path": store_path}
            else:
                found[store_type] = {"path": store_path, "exists": False}
        except Exception as e:
            logger.debug(f"BCD check failed for {store_path}: {e}")
            found[store_type] = {"path": store_path, "exists": False, "error": str(e)}

    if not any(v.get("exists") for v in found.values()):
        logger.warning("No BCD stores found")
        return {"windows": True, "bcd": "no_bcd_store", "stores": found}

    # Lightweight firmware mismatch hinting (no BCD edits offline)
    notes: List[str] = [
        "Offline-safe: backups created where possible.",
        "Deep BCD edits need Windows tools (bcdedit/bootrec) inside Windows RE.",
    ]
    try:
        has_uefi = any(found.get(k, {}).get("exists") for k in ("uefi_standard", "uefi_alternative", "uefi_fallback", "uefi_root"))
        has_bios = found.get("bios", {}).get("exists")
        if has_uefi and not has_bios:
            notes.append("Hint: UEFI-style BCD present; ensure you boot the converted VM in UEFI mode.")
        if has_bios and not has_uefi:
            notes.append("Hint: BIOS-style BCD present; ensure you boot the converted VM in legacy BIOS mode.")
        if has_bios and has_uefi:
            notes.append("Hint: Both BIOS and UEFI BCD stores found; boot mode must match the installed Windows mode.")
    except Exception:
        pass

    return {
        "windows": True,
        "bcd": "found",
        "stores": found,
        "backups": backups,
        "notes": notes,
    }


# ---------------------------
# Public: VirtIO injection (enhanced: stage INF packages + DevicePath)
# ---------------------------

def inject_virtio_drivers(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    """
    Inject VirtIO drivers into Windows guest.

    Flow:
      1) Validate inputs
      2) Detect Windows + locate Windows root
      3) Identify version/edition/bucket/arch
      4) Discover host driver files (bucket fallbacks, Win11-aware)
      5) Copy .sys into guest System32\\drivers
      6) Stage full driver packages (INF/CAT/...) into guest (C:\\vmdk2kvm\\drivers\\virtio\\...)
      7) Edit SYSTEM hive:
           - Services entries (boot-safe)
           - CriticalDeviceDatabase entries (boot binding)
           - remove StartOverride
      8) Optionally update SOFTWARE\\DevicePath to include staged drivers path
      9) Return structured report data
    """
    logger = _safe_logger(self)
    dry_run = getattr(self, "dry_run", False)
    force_overwrite = bool(getattr(self, "force_virtio_overwrite", False))

    virtio_dir = getattr(self, "virtio_drivers_dir", None)
    if not virtio_dir:
        logger.info("VirtIO inject: virtio_drivers_dir not set -> skip")
        return {"injected": False, "reason": "virtio_drivers_dir_not_set"}

    drivers_dir = Path(virtio_dir)
    if not drivers_dir.exists() or not drivers_dir.is_dir():
        logger.error(f"VirtIO inject: virtio_drivers_dir invalid: {drivers_dir}")
        return {"injected": False, "reason": "virtio_drivers_dir_not_found", "path": str(drivers_dir)}

    if not is_windows(self, g):
        logger.info("VirtIO inject: guest is not Windows -> skip")
        return {"injected": False, "reason": "not_windows"}

    if not getattr(self, "inspect_root", None):
        logger.warning("VirtIO inject: inspect_root missing -> cannot proceed safely")
        return {"injected": False, "reason": "no_inspect_root"}

    windows_root = _find_windows_root(self, g)
    if not windows_root:
        logger.error("VirtIO inject: could not locate Windows directory")
        return {"injected": False, "reason": "no_windows_root"}

    win_info = _windows_version_info(self, g)
    plan = _choose_driver_plan(self, win_info)

    logger.info(
        "VirtIO inject: Windows detected: "
        f"product={win_info.get('product_name')} build={win_info.get('build')} "
        f"arch={plan.arch_dir} bucket_hint={plan.os_bucket} edition={plan.edition.value}"
    )

    drivers = _discover_virtio_drivers(self, drivers_dir, plan)
    if not drivers:
        logger.error("VirtIO inject: no drivers found under virtio_drivers_dir (cannot inject)")
        return {
            "injected": False,
            "reason": "no_drivers_found",
            "virtio_dir": str(drivers_dir),
            "windows_info": win_info,
            "plan": _plan_to_dict(plan),
            "buckets_tried": _bucket_candidates(plan.edition),
        }

    result: Dict[str, Any] = {
        "injected": False,
        "success": False,
        "dry_run": bool(dry_run),
        "force_overwrite": bool(force_overwrite),
        "windows": win_info,
        "plan": _plan_to_dict(plan),
        "virtio_dir": str(drivers_dir),
        "windows_root": windows_root,
        "drivers_found": [d.to_dict() for d in drivers],
        "files_copied": [],
        "packages_staged": [],
        "registry_changes": {},
        "devicepath_changes": {},
        "warnings": [],
        "notes": [],
    }

    # ---- Copy SYS into System32\drivers ----
    drivers_target_dir = f"{windows_root}/System32/drivers"
    logger.info(f"VirtIO inject: target dir inside guest: {drivers_target_dir}")

    try:
        if not g.is_dir(drivers_target_dir):
            logger.warning(f"VirtIO inject: {drivers_target_dir} missing; creating")
            if not dry_run:
                g.mkdir_p(drivers_target_dir)
    except Exception as e:
        logger.error(f"VirtIO inject: cannot ensure driver dir: {e}")
        return {**result, "reason": f"drivers_dir_error: {e}"}

    for drv in drivers:
        dest_path = f"{drivers_target_dir}/{drv.dest_name}"
        try:
            src_size = drv.src_path.stat().st_size

            if g.is_file(dest_path):
                # Compare by sha256 (best-effort) unless force_overwrite
                if not force_overwrite:
                    try:
                        guest_hash = _guest_sha256(g, dest_path)
                        host_hash = hashlib.sha256(drv.src_path.read_bytes()).hexdigest()
                        if guest_hash and guest_hash == host_hash:
                            logger.info(f"VirtIO inject: already present: {drv.dest_name} (hash match), skipping")
                            result["files_copied"].append(
                                {
                                    "name": drv.dest_name,
                                    "action": "skipped",
                                    "reason": "already_exists_same_hash",
                                    "source": str(drv.src_path),
                                    "destination": dest_path,
                                    "size": src_size,
                                    "type": drv.type.value,
                                    "service": drv.service_name,
                                }
                            )
                            continue
                    except Exception:
                        pass

                logger.warning(f"VirtIO inject: overwriting {drv.dest_name} (force={force_overwrite})")

            if not dry_run:
                g.upload(str(drv.src_path), dest_path)

            logger.info(f"VirtIO inject: copied {drv.src_path.name} -> {dest_path}")
            result["files_copied"].append(
                {
                    "name": drv.dest_name,
                    "action": "copied" if not dry_run else "dry_run",
                    "source": str(drv.src_path),
                    "destination": dest_path,
                    "size": src_size,
                    "type": drv.type.value,
                    "service": drv.service_name,
                    "bucket_used": drv.bucket_used,
                    "match_pattern": drv.match_pattern,
                }
            )

        except Exception as e:
            msg = f"VirtIO inject: copy failed {drv.src_path} -> {dest_path}: {e}"
            logger.error(msg)
            result["warnings"].append(msg)

    if not result["files_copied"]:
        logger.error("VirtIO inject: no files copied (all failed?)")
        result["reason"] = "no_files_copied"
        return result

    # ---- Stage full driver packages (INF/CAT/...) ----
    # Guest path: C:\vmdk2kvm\drivers\virtio\<service>\...
    staging_root = f"{windows_root}/vmdk2kvm/drivers/virtio"
    # For DevicePath, use Windows-style variable
    devicepath_append = r"%SystemRoot%\vmdk2kvm\drivers\virtio"

    def _guest_mkdir_p(path: str) -> None:
        try:
            if not g.is_dir(path):
                if not dry_run:
                    g.mkdir_p(path)
        except Exception:
            if not dry_run:
                g.mkdir_p(path)

    logger.info(f"VirtIO stage: staging root: {staging_root}")
    try:
        _guest_mkdir_p(staging_root)
    except Exception as e:
        msg = f"VirtIO stage: failed to create staging root: {e}"
        logger.warning(msg)
        result["warnings"].append(msg)

    for drv in drivers:
        if not drv.package_dir or not drv.package_dir.exists():
            continue

        # Put each driver package under its service name to avoid collisions
        guest_pkg_dir = f"{staging_root}/{drv.service_name}"
        try:
            _guest_mkdir_p(guest_pkg_dir)
        except Exception as e:
            msg = f"VirtIO stage: cannot create {guest_pkg_dir}: {e}"
            logger.warning(msg)
            result["warnings"].append(msg)
            continue

        staged_files: List[Dict[str, Any]] = []
        try:
            # Copy only driver-ish payload; keep it small & relevant
            payload = sorted([p for p in drv.package_dir.iterdir() if p.is_file() and _is_probably_driver_payload(p)])
            if not payload:
                continue

            for p in payload:
                gp = f"{guest_pkg_dir}/{p.name}"
                try:
                    if not dry_run:
                        g.upload(str(p), gp)
                    staged_files.append({"name": p.name, "source": str(p), "dest": gp, "size": p.stat().st_size})
                except Exception as e:
                    result["warnings"].append(f"VirtIO stage: upload failed {p} -> {gp}: {e}")

            if staged_files:
                logger.info(f"VirtIO stage: staged {len(staged_files)} file(s) for {drv.service_name} -> {guest_pkg_dir}")
                result["packages_staged"].append(
                    {
                        "service": drv.service_name,
                        "type": drv.type.value,
                        "package_dir": str(drv.package_dir),
                        "inf": str(drv.inf_path) if drv.inf_path else None,
                        "guest_dir": guest_pkg_dir,
                        "files": staged_files,
                    }
                )

        except Exception as e:
            result["warnings"].append(f"VirtIO stage: failed staging package for {drv.service_name}: {e}")

    # ---- Registry edit (SYSTEM) ----
    registry_path = f"{windows_root}/System32/config/SYSTEM"
    logger.info(f"VirtIO inject: editing registry hive: {registry_path}")

    try:
        reg_res = _edit_windows_registry(self, g, registry_path, drivers, plan)
        result["registry_changes"] = reg_res
    except Exception as e:
        msg = f"VirtIO inject: registry exception: {e}"
        logger.error(msg)
        result["registry_changes"] = {"success": False, "error": str(e)}
        result["warnings"].append(msg)

    # ---- DevicePath update (SOFTWARE) to help PnP find staged INFs ----
    # This is additive; failures shouldn't block storage boot.
    try:
        software_hive = f"{windows_root}/System32/config/SOFTWARE"
        # Only attempt if we staged anything
        if result["packages_staged"]:
            logger.info(f"VirtIO inject: updating SOFTWARE DevicePath to include: {devicepath_append}")
            dp_res = _edit_software_devicepath(self, g, software_hive, devicepath_append)
            result["devicepath_changes"] = dp_res
        else:
            result["devicepath_changes"] = {"skipped": True, "reason": "no_packages_staged"}
    except Exception as e:
        result["devicepath_changes"] = {"success": False, "error": str(e)}
        result["warnings"].append(f"DevicePath update failed: {e}")

    # ---- Success criteria ----
    sys_ok = any(x.get("action") in ("copied", "dry_run", "skipped") for x in result["files_copied"])
    reg_ok = bool(result.get("registry_changes", {}).get("success"))
    if sys_ok and reg_ok:
        result["injected"] = True
        result["success"] = True
        logger.info("VirtIO inject: SUCCESS (SYS + registry); packages staged for PnP assist")
    else:
        result["injected"] = False
        result["success"] = False
        if not reg_ok:
            result["reason"] = "registry_update_failed"
            logger.warning("VirtIO inject: SYS copied but registry update failed")
        else:
            result["reason"] = "sys_copy_failed"
            logger.warning("VirtIO inject: registry ok but sys copy did not succeed")

    # ---- Final notes / guidance ----
    storage_found = sorted({d.service_name for d in drivers if d.type == DriverType.STORAGE})
    storage_missing: List[str] = []
    if "viostor" not in storage_found:
        storage_missing.append("viostor")
    if "vioscsi" not in storage_found:
        storage_missing.append("vioscsi")

    result["notes"] += [
        "Storage: attempts to inject BOTH viostor + vioscsi (if present) and force BOOT start.",
        "Registry: strings written as UTF-16LE; ImagePath set to \\SystemRoot\\System32\\drivers\\<sys>.",
        "Registry: StartOverride removed when found (can silently disable boot drivers).",
        "CDD: CriticalDeviceDatabase populated for virtio storage PCI IDs to ensure early binding.",
        f"Driver discovery bucket candidates: {_bucket_candidates(plan.edition)}",
        f"Storage drivers found: {storage_found} missing: {storage_missing}",
        "Staging: INF/CAT/etc staged under %SystemRoot%\\vmdk2kvm\\drivers\\virtio to help PnP on first boot.",
        "Best practice: attach virtio-win ISO for first boot so Windows can stage INFs properly.",
        "If BSOD persists, ensure the VM disk/controller model matches injected drivers (virtio-blk vs virtio-scsi).",
        "Also ensure firmware mode matches install (BIOS vs UEFI) and BCD points to the right loader.",
    ]

    return result
