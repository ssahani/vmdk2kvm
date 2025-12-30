# vmdk2kvm/fixers/windows_fixer.py
from __future__ import annotations

import hashlib
import json
import logging
import re
import tempfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

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

    # If Windows is nested weirdly, do a minimal breadth guess.
    # We avoid heavy globbing here to keep runtime reasonable.
    logger.debug("Windows root: no direct hit")
    return None


# ---------------------------
# Windows version enrichment
# ---------------------------

def _read_software_hive_version(self, g: guestfs.GuestFS, software_hive_path: str) -> Dict[str, Any]:
    """
    Parse SOFTWARE hive offline for:
      SOFTWARE\Microsoft\Windows NT\CurrentVersion\ProductName etc.
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
# NEW: bucket fallback logic (Win11 support + ISO layout variation)
# ---------------------------

def _bucket_candidates(edition: WindowsEdition) -> List[str]:
    """
    Virtio-win ISOs differ by release:
      - Some include w11/
      - Some only include w10/ (even for Windows 11)
      - Older ones include w8/ w7/ vista/ xp
    We therefore try a *sequence* of buckets, not just one.

    The sequence below is "prefer most specific, then fall back".
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

    IMPORTANT updates (additive, does not break your external API):
      - Windows 11 gets an os_bucket *hint* of "w11" (but we still fall back).
      - We keep os_bucket primarily as a hint for "first attempt"; discovery tries candidates.
      - We do NOT choose one storage driver; discovery will attempt both viostor + vioscsi.
    """
    logger = _safe_logger(self)

    edition = _detect_windows_edition(self, win_info)
    arch_dir = _norm_arch_to_dir(str(win_info.get("arch") or "amd64"))

    # Keep your original mapping logic, but improve Windows 11.
    # NOTE: os_bucket is now a "hint", not a hard constraint.
    edition_to_bucket = {
        WindowsEdition.SERVER_2022: "w10",
        WindowsEdition.SERVER_2019: "w10",
        WindowsEdition.SERVER_2016: "w10",
        WindowsEdition.SERVER_2012: "w8",
        WindowsEdition.SERVER_2008: "w7",
        WindowsEdition.WINDOWS_11: "w11",  # <-- FIXED: was w10; now w11 first, with fallback in discovery
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

    # Verbose plan logs (helps debug "why did it search w10 not w11?")
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
# Driver discovery
# ---------------------------

def _discover_virtio_drivers(self, drivers_dir: Path, plan: WindowsVirtioPlan) -> List[DriverFile]:
    """
    Discover drivers from virtio-win directory.

    IMPORTANT:
      - For STORAGE, we discover BOTH viostor and vioscsi (if present).
      - Both will be set BOOT-start in registry edits, which prevents BSOD when controller differs.
      - We now support Windows 11 by trying bucket fallbacks: w11 -> w10 -> w8 -> w7 -> ...
      - We log *exactly* what we tried, so failures are diagnosable.
    """
    logger = _safe_logger(self)
    drivers: List[DriverFile] = []

    # Bucket candidates (Win11 fix): search multiple buckets rather than only plan.os_bucket
    buckets = _bucket_candidates(plan.edition)

    # Common PCI IDs and class GUIDs for virtio devices.
    # These are used in CriticalDeviceDatabase (boot-critical mapping).
    storage_class_guid = "{4D36E967-E325-11CE-BFC1-08002BE10318}"  # SCSIAdapter
    net_class_guid = "{4D36E972-E325-11CE-BFC1-08002BE10318}"      # Net
    balloon_class_guid = "{4D36E97D-E325-11CE-BFC1-08002BE10318}"  # System

    driver_configs: Dict[DriverType, List[Dict[str, Any]]] = {
        DriverType.STORAGE: [
            {
                "name": "viostor",
                "pattern": "viostor/{bucket}/{arch}/viostor.sys",
                "service": "viostor",
                "start": DriverStartType.BOOT,
                "pci_ids": [
                    # virtio-blk variants
                    "pci#ven_1af4&dev_1001&subsys_00081af4",
                    "pci#ven_1af4&dev_1042&subsys_00081af4",
                ],
                "class_guid": storage_class_guid,
            },
            {
                "name": "vioscsi",
                "pattern": "vioscsi/{bucket}/{arch}/vioscsi.sys",
                "service": "vioscsi",
                "start": DriverStartType.BOOT,
                "pci_ids": [
                    # virtio-scsi variants
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
                "service": "virtiofs",
                "start": DriverStartType.SYSTEM,
                "pci_ids": ["pci#ven_1af4&dev_105a&subsys_11001af4"],
                "class_guid": storage_class_guid,
            },
        ],
    }

    # We try multiple layouts; virtio-win directory structures vary.
    # Added patterns are additive and harmless.
    search_patterns = [
        "{pattern}",                       # canonical
        "{driver}/{bucket}/{arch}/*.sys",   # sometimes sys name differs, pick the first
        "{driver}/{arch}/*.sys",            # bucket-less
        "{driver}/*/{arch}/*.sys",          # nested unknown bucket folder
        "{driver}/*/*/{arch}/*.sys",        # deeper nesting seen in some packs
    ]

    logger.info("Discovering VirtIO drivers...")
    logger.info(f"VirtIO dir: {drivers_dir}")
    logger.info(f"Plan: edition={plan.edition.value} bucket_hint={plan.os_bucket} arch={plan.arch_dir}")
    logger.info(f"Bucket candidates: {buckets}")
    logger.debug(f"Driver types requested: {sorted([d.value for d in plan.drivers_needed])}")

    # Helper: try a candidate path (direct or glob)
    def _try_candidate(pat: str) -> Optional[Path]:
        candidate = drivers_dir / pat
        if "*" in pat:
            try:
                matches = sorted(candidate.parent.glob(candidate.name))
                matches = [m for m in matches if m.is_file()]
                if matches:
                    return matches[0]
            except Exception:
                return None
            return None
        if candidate.exists() and candidate.is_file():
            return candidate
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

            # TRY BUCKETS IN ORDER (Win11 fix)
            for bucket in buckets:
                if found:
                    break

                for tmpl in search_patterns:
                    # Render pattern with this bucket
                    canonical = cfg["pattern"].format(bucket=bucket, arch=plan.arch_dir)
                    pat = tmpl.format(
                        pattern=canonical,
                        driver=driver_name,
                        bucket=bucket,
                        arch=plan.arch_dir,
                    )

                    src = _try_candidate(pat)
                    if src is None:
                        logger.debug(f"Not found: type={driver_type.value} name={driver_name} bucket={bucket} pat={pat}")
                        continue

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
                        )
                    )
                    logger.info(f"Found {driver_type.value} driver: {driver_name} bucket={bucket} -> {src}")
                    found = True
                    break

            if not found:
                # IMPORTANT: storage missing means likely BSOD if VM switches controller types.
                lvl = logging.WARNING if driver_type == DriverType.STORAGE else logging.INFO
                logger.log(
                    lvl,
                    "Driver not found:"
                    f" type={driver_type.value}"
                    f" name={driver_name}"
                    f" arch={plan.arch_dir}"
                    f" buckets_tried={buckets}",
                )

    # Safety: log what we found, and warn if storage incomplete
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

    Why this prevents BSOD:
      - When Windows boots, it needs a working storage miniport *before* it can mount C:\\.
      - CriticalDeviceDatabase + BOOT start ensures Windows binds the right driver early.
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

            current_set = int.from_bytes(cur["value"], "little")
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
            # More Windows-correct values than "minimal":
            #   Type=1 (kernel driver)
            #   Start=0 for storage (BOOT), others as requested
            #   Group="SCSI miniport" for storage (more correct for miniports)
            #   ImagePath uses system32\\drivers\\<sys> (REG_SZ UTF-16LE)
            for drv in drivers:
                try:
                    svc = h.node_get_child(services, drv.service_name)
                    action = "updated" if svc is not None else "created"
                    if svc is None:
                        svc = h.node_add_child(services, drv.service_name)

                    logger.info(f"Registry service {action}: Services\\{drv.service_name}")

                    _set_dword(h, svc, "Type", 1)         # SERVICE_KERNEL_DRIVER
                    _set_dword(h, svc, "ErrorControl", 1) # normal error handling

                    # Storage must be BOOT to avoid INACCESSIBLE_BOOT_DEVICE.
                    start = drv.start_type.value
                    if drv.type == DriverType.STORAGE:
                        start = DriverStartType.BOOT.value
                    _set_dword(h, svc, "Start", start)

                    # Group: storage is a SCSI miniport, network is NDIS, others bus extender-ish.
                    if drv.type == DriverType.STORAGE:
                        group = "SCSI miniport"
                    elif drv.type == DriverType.NETWORK:
                        group = "NDIS"
                    else:
                        group = "System Bus Extender"

                    _set_sz(h, svc, "Group", group)
                    _set_sz(h, svc, "ImagePath", fr"system32\drivers\{drv.dest_name}")
                    _set_sz(h, svc, "DisplayName", drv.service_name)

                    # StartOverride is a common reason boot drivers don't load.
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
                            "image": fr"system32\drivers\{drv.dest_name}",
                            "action": action,
                        }
                    )

                except Exception as e:
                    msg = f"Failed to create/update service {drv.service_name}: {e}"
                    logger.error(msg)
                    results["errors"].append(msg)

            # ---- CriticalDeviceDatabase for storage ----
            # Ensures boot-time binding of PCI device -> Service.
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

                        # Optional helpers: not always required, but can assist older builds.
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

                # Verify change by hash
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

    return {
        "windows": True,
        "bcd": "found",
        "stores": found,
        "backups": backups,
        "notes": [
            "Offline-safe: backups created where possible.",
            "Deep BCD edits need Windows tools (bcdedit/bootrec) inside Windows RE.",
        ],
    }


# ---------------------------
# Public: VirtIO injection (rewritten with detailed logs)
# ---------------------------

def inject_virtio_drivers(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    """
    Inject VirtIO drivers into Windows guest.

    Flow:
      1) Validate inputs
      2) Detect Windows + locate Windows root
      3) Identify version/edition/bucket/arch
      4) Discover host driver files (NOW: tries bucket fallbacks, Win11-aware)
      5) Copy .sys into guest System32\\drivers
      6) Edit SYSTEM hive:
           - Services entries (boot-safe)
           - CriticalDeviceDatabase entries (boot binding)
           - remove StartOverride
      7) Return structured report data

    Why BSOD happens and how this fixes it:
      - When moving VMware -> KVM, storage controller model changes.
      - Windows must load the matching boot storage driver before it can mount C:\\.
      - We inject BOTH viostor + vioscsi and force BOOT start + CDD mapping.
    """
    logger = _safe_logger(self)
    dry_run = getattr(self, "dry_run", False)

    # ---- Validate configuration ----
    virtio_dir = getattr(self, "virtio_drivers_dir", None)
    if not virtio_dir:
        logger.info("VirtIO inject: virtio_drivers_dir not set -> skip")
        return {"injected": False, "reason": "virtio_drivers_dir_not_set"}

    drivers_dir = Path(virtio_dir)
    if not drivers_dir.exists() or not drivers_dir.is_dir():
        logger.error(f"VirtIO inject: virtio_drivers_dir invalid: {drivers_dir}")
        return {"injected": False, "reason": "virtio_drivers_dir_not_found", "path": str(drivers_dir)}

    # ---- Detect Windows ----
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

    # ---- Determine plan ----
    win_info = _windows_version_info(self, g)
    plan = _choose_driver_plan(self, win_info)

    logger.info(
        "VirtIO inject: Windows detected: "
        f"product={win_info.get('product_name')} build={win_info.get('build')} "
        f"arch={plan.arch_dir} bucket_hint={plan.os_bucket} edition={plan.edition.value}"
    )

    # ---- Discover driver files on host (Win11-safe) ----
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

    # ---- Copy into guest ----
    result: Dict[str, Any] = {
        "injected": False,
        "success": False,
        "dry_run": bool(dry_run),
        "windows": win_info,
        "plan": _plan_to_dict(plan),
        "virtio_dir": str(drivers_dir),
        "windows_root": windows_root,
        "drivers_found": [d.to_dict() for d in drivers],
        "files_copied": [],
        "registry_changes": {},
        "warnings": [],
        "notes": [],
    }

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
            # If file already exists with same size, skip
            if g.is_file(dest_path):
                try:
                    existing_size = g.filesize(dest_path)
                except Exception:
                    existing_size = None
                source_size = drv.src_path.stat().st_size

                if existing_size is not None and existing_size == source_size:
                    logger.info(f"VirtIO inject: already present: {drv.dest_name} (size match), skipping")
                    result["files_copied"].append(
                        {
                            "name": drv.dest_name,
                            "action": "skipped",
                            "reason": "already_exists_same_size",
                            "source": str(drv.src_path),
                            "destination": dest_path,
                            "size": source_size,
                        }
                    )
                    continue

                logger.warning(
                    f"VirtIO inject: overwriting {drv.dest_name}: guest_size={existing_size} host_size={source_size}"
                )

            if not dry_run:
                g.upload(str(drv.src_path), dest_path)

            logger.info(f"VirtIO inject: copied {drv.src_path.name} -> {dest_path}")
            result["files_copied"].append(
                {
                    "name": drv.dest_name,
                    "action": "copied" if not dry_run else "dry_run",
                    "source": str(drv.src_path),
                    "destination": dest_path,
                    "size": drv.src_path.stat().st_size,
                    "type": drv.type.value,
                    "service": drv.service_name,
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

    # ---- Registry edit ----
    registry_path = f"{windows_root}/System32/config/SYSTEM"
    logger.info(f"VirtIO inject: editing registry hive: {registry_path}")

    try:
        reg_res = _edit_windows_registry(self, g, registry_path, drivers, plan)
        result["registry_changes"] = reg_res

        if reg_res.get("success"):
            result["injected"] = True
            result["success"] = True
            logger.info("VirtIO inject: SUCCESS (files + registry)")
        else:
            result["injected"] = False
            result["success"] = False
            result["reason"] = "registry_update_failed"
            logger.warning("VirtIO inject: files copied but registry update failed")

    except Exception as e:
        msg = f"VirtIO inject: registry exception: {e}"
        logger.error(msg)
        result["injected"] = False
        result["success"] = False
        result["reason"] = "registry_exception"
        result["registry_changes"] = {"error": str(e)}
        result["warnings"].append(msg)

    # ---- Final notes / guidance ----
    result["notes"] += [
        "Storage: attempts to inject BOTH viostor + vioscsi (if present) and force BOOT start.",
        "Registry: strings written as UTF-16LE REG_SZ (Windows-correct).",
        "Registry: StartOverride removed when found (can silently disable boot drivers).",
        "CDD: CriticalDeviceDatabase populated for virtio storage PCI IDs to ensure early binding.",
        f"Driver discovery bucket candidates: {_bucket_candidates(plan.edition)}",
        "Best practice: attach virtio-win ISO for first boot so Windows can stage INFs properly.",
        "If BSOD persists, ensure the VM disk/controller model matches injected drivers (virtio-blk vs virtio-scsi).",
        "Also ensure firmware mode matches install (BIOS vs UEFI) and BCD points to the right loader.",
    ]

    return result
