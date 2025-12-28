# vmdk2kvm/fixers/windows_fixer.py
from __future__ import annotations

import hashlib
import json
import logging
import re
import tempfile
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

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
    arch_dir: str  # "amd64" | "x86" | "arm64" | "ia64"
    os_bucket: str  # e.g., "w10", "w8", "w7", "xp"
    storage_service: str  # "vioscsi" | "viostor"
    edition: WindowsEdition
    drivers_needed: Set[DriverType]

    @classmethod
    def default_needed(cls) -> Set[DriverType]:
        return {DriverType.STORAGE, DriverType.NETWORK, DriverType.BALLOON}


@dataclass
class DriverFile:
    name: str
    type: DriverType
    src_path: Path
    dest_name: str
    start_type: DriverStartType
    service_name: str
    pci_ids: List[str]
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
        "storage_service": plan.storage_service,
        "edition": plan.edition.value,
        "drivers_needed": sorted([d.value for d in plan.drivers_needed]),
    }


# ---------------------------
# Windows Helpers / Policy
# ---------------------------

def _norm_arch_to_dir(arch: str) -> str:
    """Normalize architecture string to directory name."""
    a = (arch or "").lower().strip()
    mapping = {
        "x86_64": "amd64",
        "amd64": "amd64",
        "x64": "amd64",
        "i386": "x86",
        "i686": "x86",
        "x86": "x86",
        "ia64": "ia64",  # Itanium
        "arm64": "arm64",
        "aarch64": "arm64",
    }
    return mapping.get(a, "amd64")


def _to_int(v: Any, default: int = 0) -> int:
    """Safely convert to integer."""
    if isinstance(v, int):
        return v
    try:
        return int(float(v)) if isinstance(v, (float, str)) else default
    except (ValueError, TypeError):
        return default


def _normalize_product_name(name: str) -> str:
    """Normalize Windows product name for easier matching."""
    if not name:
        return ""
    normalized = name.lower()
    normalized = re.sub(r"\([^)]*\)", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _safe_logger(self) -> logging.Logger:
    lg = getattr(self, "logger", None)
    if isinstance(lg, logging.Logger):
        return lg
    # fallback
    return logging.getLogger("vmdk2kvm.windows_fixer")


def _guest_glob(g: guestfs.GuestFS, pattern: str) -> List[str]:
    """
    Prefer a guest-side shell glob expansion (works even when libguestfs glob()
    is missing/limited). Falls back to g.glob if available.
    """
    # GuestFS sh() returns stdout; it can raise if command fails.
    # We keep it simple and robust.
    try:
        cmd = (
            "sh -c "
            + U.shell_quote(
                # -d: directories only; suppress errors; print 1 per line
                f'for p in {pattern}; do [ -d "$p" ] && printf "%s\\n" "$p"; done 2>/dev/null || true'
            )
        )
        out = U.to_text(g.sh(cmd))  # type: ignore[attr-defined]
        res = [line.strip() for line in out.splitlines() if line.strip()]
        return res
    except Exception:
        # fallback to g.glob if present
        try:
            res = g.glob(pattern)  # type: ignore[attr-defined]
            return [U.to_text(x) for x in res]
        except Exception:
            return []


def _hivex_read_sz(h, node, key: str) -> Optional[str]:
    """Read REG_SZ / REG_EXPAND_SZ-ish as text (best-effort)."""
    try:
        v = h.node_get_value(node, key)
        if not v or "value" not in v:
            return None
        raw = v["value"]
        if isinstance(raw, (bytes, bytearray)):
            # hivex may return UTF-16LE (common for Windows registry strings)
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
    """Read REG_DWORD as int (best-effort)."""
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
# Windows Detection
# ---------------------------

def is_windows(self, g: guestfs.GuestFS) -> bool:
    """
    Check if the guest OS is Windows.

    Args:
        self: The fixer instance
        g: GuestFS instance

    Returns:
        True if Windows, False otherwise
    """
    logger = _safe_logger(self)

    if not getattr(self, "inspect_root", None):
        return False

    try:
        root = self.inspect_root
        if not root:
            return False

        # Method 1: libguestfs inspection type
        try:
            os_type = U.to_text(g.inspect_get_type(root))
            if os_type and os_type.lower() == "windows":
                return True
        except Exception:
            pass

        # Method 2: common Windows directories
        windows_dirs = ["/Windows", "/WINDOWS", "/winnt", "/WINNT", "/Program Files"]
        for dir_path in windows_dirs:
            try:
                if g.is_dir(dir_path):
                    return True
            except Exception:
                continue

        # Method 3: registry file existence
        reg_files = [
            "/Windows/System32/config/SOFTWARE",
            "/WINDOWS/System32/config/SOFTWARE",
            "/winnt/system32/config/SOFTWARE",
        ]
        for reg_file in reg_files:
            try:
                if g.is_file(reg_file):
                    return True
            except Exception:
                continue

        return False

    except Exception as e:
        logger.debug(f"Windows detection failed: {e}")
        return False


def _find_windows_root(self, g: guestfs.GuestFS) -> Optional[str]:
    """Find the Windows installation root directory."""
    candidates = [
        "/Windows",
        "/WINDOWS",
        "/winnt",
        "/WINNT",
        # Sometimes Windows is in a subdirectory
        "/*/Windows",
        "/*/WINDOWS",
    ]

    for pattern in candidates:
        try:
            if "*" in pattern:
                results = _guest_glob(g, pattern)
                for result in results:
                    try:
                        if g.is_dir(result):
                            return result
                    except Exception:
                        continue
            else:
                if g.is_dir(pattern):
                    return pattern
        except Exception:
            continue

    return None


def _read_software_hive_version(self, g: guestfs.GuestFS, software_hive_path: str) -> Dict[str, Any]:
    """
    Best-effort offline parse of SOFTWARE hive for richer Windows version metadata.
    Uses guestfs.hivex_* APIs (no winreg dependency).
    """
    logger = _safe_logger(self)
    out: Dict[str, Any] = {}
    if not g.is_file(software_hive_path):
        return out

    dry_run = getattr(self, "dry_run", False)

    with tempfile.TemporaryDirectory() as tmpdir:
        local_hive = Path(tmpdir) / "SOFTWARE"
        try:
            g.download(software_hive_path, str(local_hive))
            # write=0 even for read; keep consistent
            h = g.hivex_open(str(local_hive), write=0 if dry_run else 0)
            root = h.root()

            # SOFTWARE\Microsoft\Windows NT\CurrentVersion
            microsoft = h.node_get_child(root, "Microsoft")
            if microsoft is None:
                return out
            windows_nt = h.node_get_child(microsoft, "Windows NT")
            if windows_nt is None:
                return out
            cv = h.node_get_child(windows_nt, "CurrentVersion")
            if cv is None:
                return out

            product_name = _hivex_read_sz(h, cv, "ProductName")
            current_build = _hivex_read_sz(h, cv, "CurrentBuild")
            current_build_number = _hivex_read_sz(h, cv, "CurrentBuildNumber")
            display_version = _hivex_read_sz(h, cv, "DisplayVersion")
            release_id = _hivex_read_sz(h, cv, "ReleaseId")
            edition_id = _hivex_read_sz(h, cv, "EditionID")
            install_type = _hivex_read_sz(h, cv, "InstallationType")

            major = _hivex_read_dword(h, cv, "CurrentMajorVersionNumber")
            minor = _hivex_read_dword(h, cv, "CurrentMinorVersionNumber")

            out.update(
                {
                    "reg_product_name": product_name,
                    "reg_current_build": current_build or current_build_number,
                    "reg_display_version": display_version or release_id,
                    "reg_edition_id": edition_id,
                    "reg_installation_type": install_type,
                    "reg_major": major,
                    "reg_minor": minor,
                }
            )
            try:
                h.hivex_close()
            except Exception:
                pass
        except Exception as e:
            logger.debug(f"SOFTWARE hive parse failed: {e}")
            return out

    return out


def _windows_version_info(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    """Get detailed Windows version information."""
    info: Dict[str, Any] = {
        "windows": True,
        "edition": WindowsEdition.UNKNOWN,
        "bits": 64,
        "build": None,
        "sp": None,
    }

    if not getattr(self, "inspect_root", None):
        return info

    root = self.inspect_root

    # Get basic info from libguestfs
    try:
        info["arch"] = U.to_text(g.inspect_get_arch(root))
        info["major"] = g.inspect_get_major_version(root)
        info["minor"] = g.inspect_get_minor_version(root)
        info["product_name"] = U.to_text(g.inspect_get_product_name(root))
        info["distro"] = U.to_text(g.inspect_get_distro(root))
    except Exception:
        info["arch"] = None
        info["major"] = None
        info["minor"] = None
        info["product_name"] = None
        info["distro"] = None

    # Determine bitness
    arch = (info.get("arch") or "").lower()
    if arch in ("x86_64", "amd64", "arm64"):
        info["bits"] = 64
    elif arch in ("i386", "i686", "x86"):
        info["bits"] = 32
    else:
        info["bits"] = 64  # Default assumption

    # Enrich from SOFTWARE hive when possible
    try:
        windows_root = _find_windows_root(self, g)
        if windows_root:
            software_hive = f"{windows_root}/System32/config/SOFTWARE"
            reg_info = _read_software_hive_version(self, g, software_hive)
            info.update(reg_info)

            # If libguestfs product name is missing, use registry ProductName
            if not info.get("product_name") and reg_info.get("reg_product_name"):
                info["product_name"] = reg_info["reg_product_name"]

            # If major/minor missing, use registry dwords
            if not info.get("major") and reg_info.get("reg_major") is not None:
                info["major"] = reg_info["reg_major"]
            if not info.get("minor") and reg_info.get("reg_minor") is not None:
                info["minor"] = reg_info["reg_minor"]

            # Build
            if not info.get("build") and reg_info.get("reg_current_build"):
                info["build"] = reg_info["reg_current_build"]
    except Exception:
        pass

    return info


def _detect_windows_edition(self, win_info: Dict[str, Any]) -> WindowsEdition:
    """Determine Windows edition from version info."""
    major = _to_int(win_info.get("major"))
    minor = _to_int(win_info.get("minor"))
    product = _normalize_product_name(str(win_info.get("product_name", "")))

    # Server editions
    if "server 2022" in product:
        return WindowsEdition.SERVER_2022
    elif "server 2019" in product:
        return WindowsEdition.SERVER_2019
    elif "server 2016" in product:
        return WindowsEdition.SERVER_2016
    elif "server 2012" in product:
        return WindowsEdition.SERVER_2012
    elif "server 2008" in product:
        return WindowsEdition.SERVER_2008

    # Client editions
    # Windows 11 is still major 10 in many APIs; prefer explicit product name.
    if "windows 11" in product or major >= 11:
        return WindowsEdition.WINDOWS_11
    elif "windows 10" in product or major == 10:
        return WindowsEdition.WINDOWS_10
    elif major == 6 and minor >= 2:
        return WindowsEdition.WINDOWS_8
    elif major == 6 and minor == 1:
        return WindowsEdition.WINDOWS_7
    elif major == 6 and minor == 0:
        return WindowsEdition.WINDOWS_VISTA
    elif major == 5:
        return WindowsEdition.WINDOWS_XP

    return WindowsEdition.UNKNOWN


def _choose_driver_plan(self, win_info: Dict[str, Any]) -> WindowsVirtioPlan:
    """Create a driver installation plan based on Windows version."""
    arch_dir = _norm_arch_to_dir(str(win_info.get("arch") or "amd64"))
    edition = _detect_windows_edition(self, win_info)

    # Map edition to driver bucket
    edition_to_bucket = {
        WindowsEdition.SERVER_2022: "w10",
        WindowsEdition.SERVER_2019: "w10",
        WindowsEdition.SERVER_2016: "w10",
        WindowsEdition.SERVER_2012: "w8",
        WindowsEdition.SERVER_2008: "w7",
        WindowsEdition.WINDOWS_11: "w10",
        WindowsEdition.WINDOWS_10: "w10",
        WindowsEdition.WINDOWS_8: "w8",
        WindowsEdition.WINDOWS_7: "w7",
        WindowsEdition.WINDOWS_VISTA: "vista",
        WindowsEdition.WINDOWS_XP: "xp",
        WindowsEdition.UNKNOWN: "w10",
    }

    os_bucket = edition_to_bucket.get(edition, "w10")

    # Choose storage driver
    # (Modern Windows generally prefers vioscsi; older ones are safer with viostor)
    if os_bucket in ("w10", "w8", "w7"):
        storage_service = "vioscsi"
    else:
        storage_service = "viostor"

    # Determine which drivers are needed
    drivers_needed = WindowsVirtioPlan.default_needed()

    # Add optional drivers based on configuration flags
    if getattr(self, "enable_virtio_gpu", False):
        drivers_needed.add(DriverType.GPU)
    if getattr(self, "enable_virtio_input", False):
        drivers_needed.add(DriverType.INPUT)
    if getattr(self, "enable_virtio_fs", False):
        drivers_needed.add(DriverType.FILESYSTEM)

    return WindowsVirtioPlan(
        arch_dir=arch_dir,
        os_bucket=os_bucket,
        storage_service=storage_service,
        edition=edition,
        drivers_needed=drivers_needed,
    )


# ---------------------------
# Driver Discovery
# ---------------------------

def _discover_virtio_drivers(
    self,
    drivers_dir: Path,
    plan: WindowsVirtioPlan,
) -> List[DriverFile]:
    """
    Discover available VirtIO drivers in the driver directory.

    Returns:
        List of DriverFile objects for drivers that exist
    """
    logger = _safe_logger(self)
    drivers: List[DriverFile] = []

    # Driver configuration mapping
    driver_configs: Dict[DriverType, List[Dict[str, Any]]] = {
        DriverType.STORAGE: [
            {
                "name": "viostor",
                "pattern": "{driver}/{bucket}/{arch}/viostor.sys",
                "service": "viostor",
                "start": DriverStartType.BOOT,
                "pci_ids": [
                    "pci#ven_1af4&dev_1001&subsys_00081af4",
                    "pci#ven_1af4&dev_1042&subsys_00081af4",
                ],
                "class_guid": "{4D36E967-E325-11CE-BFC1-08002BE10318}",
            },
            {
                "name": "vioscsi",
                "pattern": "{driver}/{bucket}/{arch}/vioscsi.sys",
                "service": "vioscsi",
                "start": DriverStartType.BOOT,
                "pci_ids": [
                    "pci#ven_1af4&dev_1004&subsys_00081af4",
                    "pci#ven_1af4&dev_1048&subsys_00081af4",
                ],
                "class_guid": "{4D36E967-E325-11CE-BFC1-08002BE10318}",
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
                "class_guid": "{4D36E97B-E325-11CE-BFC1-08002BE10318}",
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
                "class_guid": "{4D36E97D-E325-11CE-BFC1-08002BE10318}",
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
                "class_guid": "{4D36E967-E325-11CE-BFC1-08002BE10318}",
            },
        ],
    }

    # Search patterns in order of preference
    # NOTE: include {pattern} so the "original" mapping works.
    search_patterns = [
        "{pattern}",  # Original pattern
        "{driver}/{arch}/{driver}.sys",  # Flat structure
        "{driver}/{arch}/*.sys",  # Wildcard
        "{driver}/*/{arch}/*.sys",  # Nested
    ]

    for driver_type in plan.drivers_needed:
        if driver_type not in driver_configs:
            continue

        for driver_config in driver_configs[driver_type]:
            driver_name = driver_config["name"]
            found = False

            for pattern_template in search_patterns:
                # Try with bucket
                pattern = pattern_template.format(
                    pattern=driver_config["pattern"],
                    driver=driver_name,
                    bucket=plan.os_bucket,
                    arch=plan.arch_dir,
                )
                candidate = drivers_dir / pattern

                if candidate.exists() and candidate.is_file():
                    driver = DriverFile(
                        name=driver_name,
                        type=driver_type,
                        src_path=candidate,
                        dest_name=f"{driver_config['service']}.sys",
                        start_type=driver_config["start"],
                        service_name=driver_config["service"],
                        pci_ids=driver_config["pci_ids"],
                        class_guid=driver_config["class_guid"],
                    )
                    drivers.append(driver)
                    found = True
                    logger.debug(f"Found driver: {driver_name} at {candidate}")
                    break

                # Try without bucket for generic drivers
                pattern_no_bucket = pattern_template.format(
                    pattern=driver_config["pattern"],
                    driver=driver_name,
                    bucket="",
                    arch=plan.arch_dir,
                ).replace("//", "/").strip("/")

                if pattern_no_bucket != pattern:
                    candidate2 = drivers_dir / pattern_no_bucket

                    # Wildcard patterns: pick the first .sys
                    if "*" in pattern_no_bucket:
                        matches = sorted(candidate2.parent.glob(candidate2.name))
                        matches = [m for m in matches if m.is_file()]
                        if matches:
                            m0 = matches[0]
                            driver = DriverFile(
                                name=driver_name,
                                type=driver_type,
                                src_path=m0,
                                dest_name=f"{driver_config['service']}.sys",
                                start_type=driver_config["start"],
                                service_name=driver_config["service"],
                                pci_ids=driver_config["pci_ids"],
                                class_guid=driver_config["class_guid"],
                            )
                            drivers.append(driver)
                            found = True
                            logger.debug(f"Found driver (glob): {driver_name} at {m0}")
                            break

                    if candidate2.exists() and candidate2.is_file():
                        driver = DriverFile(
                            name=driver_name,
                            type=driver_type,
                            src_path=candidate2,
                            dest_name=f"{driver_config['service']}.sys",
                            start_type=driver_config["start"],
                            service_name=driver_config["service"],
                            pci_ids=driver_config["pci_ids"],
                            class_guid=driver_config["class_guid"],
                        )
                        drivers.append(driver)
                        found = True
                        logger.debug(f"Found driver (no bucket): {driver_name} at {candidate2}")
                        break

            if not found:
                logger.warning(f"Driver not found: {driver_name} for {plan.os_bucket}/{plan.arch_dir}")

    return drivers


# ---------------------------
# Registry Operations
# ---------------------------

def _edit_windows_registry(
    self,
    g: guestfs.GuestFS,
    hive_path: str,
    drivers: List[DriverFile],
    plan: WindowsVirtioPlan,
) -> Dict[str, Any]:
    """
    Edit Windows registry to add VirtIO drivers.

    Args:
        g: GuestFS instance
        hive_path: Path to SYSTEM hive
        drivers: List of drivers to install
        plan: Driver installation plan

    Returns:
        Dictionary with operation results
    """
    logger = _safe_logger(self)

    results: Dict[str, Any] = {
        "registry_modified": False,
        "errors": [],
        "services_added": [],
        "cdd_entries_added": [],
    }

    if not g.is_file(hive_path):
        results["errors"].append(f"Registry hive not found: {hive_path}")
        results["success"] = False
        return results

    dry_run = getattr(self, "dry_run", False)

    with tempfile.TemporaryDirectory() as tmpdir:
        local_hive = Path(tmpdir) / "SYSTEM"

        try:
            # Download the registry hive
            g.download(hive_path, str(local_hive))
            logger.debug(f"Downloaded registry hive to {local_hive}")

            # Calculate hash for backup baseline
            with open(local_hive, "rb") as f:
                original_hash = hashlib.sha256(f.read()).hexdigest()

            # Open hive for editing (write=1 only if not dry-run)
            write_mode = 0 if dry_run else 1
            h = g.hivex_open(str(local_hive), write=write_mode)
            root = h.root()

            # Get current control set
            select = h.node_get_child(root, "Select")
            if select is None:
                raise RuntimeError("No 'Select' key in registry")

            current_val = h.node_get_value(select, "Current")
            if current_val is None:
                raise RuntimeError("No 'Current' value in Select")

            current_set = int.from_bytes(current_val["value"], "little")
            cs_name = f"ControlSet{current_set:03d}"
            control_set = h.node_get_child(root, cs_name)

            if control_set is None:
                # Try ControlSet001 as fallback
                cs_name = "ControlSet001"
                control_set = h.node_get_child(root, cs_name)
                if control_set is None:
                    raise RuntimeError(f"Could not find control set ({cs_name})")

            # Ensure Services key exists
            services = h.node_get_child(control_set, "Services")
            if services is None:
                services = h.node_add_child(control_set, "Services")
                logger.debug("Created Services registry key")

            # Add each driver as a service
            for driver in drivers:
                try:
                    existing_service = h.node_get_child(services, driver.service_name)
                    if existing_service:
                        logger.info(f"Service already exists: {driver.service_name}")
                        results["services_added"].append(
                            {"name": driver.service_name, "action": "skipped", "reason": "already_exists"}
                        )
                        continue

                    service_node = h.node_add_child(services, driver.service_name)

                    # Minimal sane service values
                    h.node_set_value(
                        service_node,
                        {"key": "Type", "t": 4, "value": (1).to_bytes(4, "little")},  # REG_DWORD
                    )
                    h.node_set_value(
                        service_node,
                        {"key": "Start", "t": 4, "value": (driver.start_type.value).to_bytes(4, "little")},
                    )
                    h.node_set_value(
                        service_node,
                        {"key": "ErrorControl", "t": 4, "value": (1).to_bytes(4, "little")},
                    )
                    h.node_set_value(
                        service_node,
                        {"key": "ImagePath", "t": 1, "value": f"system32\\drivers\\{driver.dest_name}\0".encode("utf-8")},
                    )

                    # Group based on driver type
                    if driver.type == DriverType.STORAGE:
                        group = "Boot Bus Extender"
                    elif driver.type == DriverType.NETWORK:
                        group = "NDIS"
                    else:
                        group = "System Bus Extender"

                    h.node_set_value(service_node, {"key": "Group", "t": 1, "value": f"{group}\0".encode("utf-8")})

                    logger.info(f"Added registry service: {driver.service_name}")
                    results["services_added"].append(
                        {
                            "name": driver.service_name,
                            "type": driver.type.value,
                            "start_type": driver.start_type.value,
                            "action": "added",
                        }
                    )

                except Exception as e:
                    error_msg = f"Failed to add service {driver.service_name}: {e}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)

            # Add CriticalDeviceDatabase entries for storage drivers
            control = h.node_get_child(control_set, "Control")
            if control is None:
                control = h.node_add_child(control_set, "Control")

            cdd = h.node_get_child(control, "CriticalDeviceDatabase")
            if cdd is None:
                cdd = h.node_add_child(control, "CriticalDeviceDatabase")

            for driver in drivers:
                if driver.type != DriverType.STORAGE:
                    continue

                for pci_id in driver.pci_ids:
                    try:
                        existing_entry = h.node_get_child(cdd, pci_id)

                        if existing_entry:
                            h.node_set_value(
                                existing_entry,
                                {"key": "Service", "t": 1, "value": f"{driver.service_name}\0".encode("utf-8")},
                            )
                            action = "updated"
                        else:
                            entry_node = h.node_add_child(cdd, pci_id)
                            h.node_set_value(
                                entry_node,
                                {"key": "Service", "t": 1, "value": f"{driver.service_name}\0".encode("utf-8")},
                            )
                            h.node_set_value(
                                entry_node,
                                {"key": "ClassGUID", "t": 1, "value": f"{driver.class_guid}\0".encode("utf-8")},
                            )
                            action = "added"

                        logger.debug(f"CDD entry {action}: {pci_id} -> {driver.service_name}")
                        results["cdd_entries_added"].append({"pci_id": pci_id, "service": driver.service_name, "action": action})

                    except Exception as e:
                        error_msg = f"Failed to add CDD entry {pci_id}: {e}"
                        logger.error(error_msg)
                        results["errors"].append(error_msg)

            # Save changes if not dry run
            if not dry_run:
                h.hivex_commit(None)
                h.hivex_close()

                # Upload modified hive back to guest
                g.upload(str(local_hive), hive_path)
                logger.info("Updated registry hive uploaded to guest")

                # Verify upload changed content
                with tempfile.TemporaryDirectory() as verify_tmp:
                    verify_path = Path(verify_tmp) / "SYSTEM_verify"
                    g.download(hive_path, str(verify_path))
                    with open(verify_path, "rb") as f:
                        new_hash = hashlib.sha256(f.read()).hexdigest()

                    if new_hash != original_hash:
                        results["registry_modified"] = True
                        logger.info("Registry successfully modified")
                    else:
                        logger.warning("Registry file unchanged after modifications")

            else:
                try:
                    h.hivex_close()
                except Exception:
                    pass

            results["success"] = True

        except Exception as e:
            error_msg = f"Registry editing failed: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
            results["success"] = False

    return results


# ---------------------------
# Main Windows Fix Functions
# ---------------------------

def windows_bcd_actual_fix(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    """
    Offline-safe BCD store handling for Windows.

    This function:
      - Detects Windows installation
      - Locates BCD stores (BIOS/UEFI)
      - Creates backups
      - Performs basic validation
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
    backup_details: Dict[str, Any] = {}

    for store_type, store_path in bcd_stores.items():
        try:
            if g.is_file(store_path):
                size = g.filesize(store_path)
                found[store_type] = {"path": store_path, "size": size, "exists": True}

                logger.info(f"Found BCD store ({store_type}): {store_path} ({size} bytes)")

                if not getattr(self, "dry_run", False):
                    timestamp = U.now_ts()
                    backup_path = f"{store_path}.backup.vmdk2kvm.{timestamp}"
                    try:
                        g.cp(store_path, backup_path)
                        backup_details[store_type] = {
                            "backup_path": backup_path,
                            "original_size": size,
                            "timestamp": timestamp,
                        }
                        logger.info(f"Created backup: {backup_path}")
                    except Exception as backup_error:
                        logger.warning(f"Failed to backup {store_path}: {backup_error}")
                        backup_details[store_type] = {"error": str(backup_error)}
        except Exception as e:
            logger.debug(f"BCD store check failed for {store_path}: {e}")
            found[store_type] = {"path": store_path, "exists": False, "error": str(e)}

    if not any(v.get("exists") for v in found.values()):
        logger.warning("No BCD stores found")
        return {"windows": True, "bcd": "no_bcd_store", "stores": found}

    return {
        "windows": True,
        "bcd": "found",
        "stores": found,
        "backups": backup_details,
        "notes": [
            "Offline-safe: Backups created where possible.",
            "Deep BCD editing requires bcdedit/bootrec inside Windows.",
            "Typical recovery commands (Windows RE): bootrec /fixmbr, /fixboot, /scanos, /rebuildbcd",
        ],
    }


def inject_virtio_drivers(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    """
    Inject VirtIO drivers into Windows guest.

    This function:
      - Discovers available VirtIO drivers
      - Copies driver files to guest
      - Updates registry for driver services
      - Configures CriticalDeviceDatabase entries
    """
    logger = _safe_logger(self)

    if not getattr(self, "virtio_drivers_dir", None):
        return {"injected": False, "reason": "virtio_drivers_dir_not_set"}

    drivers_dir = Path(self.virtio_drivers_dir)
    if not drivers_dir.exists() or not drivers_dir.is_dir():
        return {"injected": False, "reason": "virtio_drivers_dir_not_found"}

    if not is_windows(self, g):
        return {"injected": False, "reason": "not_windows"}

    if not getattr(self, "inspect_root", None):
        return {"injected": False, "reason": "no_inspect_root"}

    windows_root = _find_windows_root(self, g)
    if not windows_root:
        return {"injected": False, "reason": "no_windows_root"}

    win_info = _windows_version_info(self, g)
    plan = _choose_driver_plan(self, win_info)

    logger.info(
        f"Windows detected: {win_info.get('product_name')} "
        f"(Edition: {plan.edition.value}, Arch: {plan.arch_dir}, Bucket: {plan.os_bucket})"
    )

    drivers = _discover_virtio_drivers(self, drivers_dir, plan)
    if not drivers:
        return {
            "injected": False,
            "reason": "no_drivers_found",
            "windows_info": win_info,
            "plan": _plan_to_dict(plan),
        }

    result: Dict[str, Any] = {
        "injected": False,
        "windows": win_info,
        "plan": _plan_to_dict(plan),
        "drivers_found": [d.to_dict() for d in drivers],
        "files_copied": [],
        "registry_changes": {},
        "warnings": [],
    }

    drivers_target_dir = f"{windows_root}/System32/drivers"
    try:
        if not g.is_dir(drivers_target_dir):
            if not getattr(self, "dry_run", False):
                g.mkdir_p(drivers_target_dir)
            logger.info(f"Created drivers directory: {drivers_target_dir}")
    except Exception as e:
        return {**result, "reason": f"drivers_dir_error: {e}"}

    dry_run = getattr(self, "dry_run", False)
    files_copied: List[Dict[str, Any]] = []

    for driver in drivers:
        dest_path = f"{drivers_target_dir}/{driver.dest_name}"
        try:
            if g.is_file(dest_path):
                existing_size = g.filesize(dest_path)
                source_size = driver.src_path.stat().st_size

                if existing_size == source_size:
                    logger.info(f"Driver already exists: {driver.dest_name}")
                    files_copied.append(
                        {
                            "name": driver.dest_name,
                            "action": "skipped",
                            "reason": "already_exists",
                            "source": str(driver.src_path),
                            "destination": dest_path,
                        }
                    )
                    continue

            if not dry_run:
                g.upload(str(driver.src_path), dest_path)

            files_copied.append(
                {
                    "name": driver.dest_name,
                    "action": "copied" if not dry_run else "dry_run",
                    "source": str(driver.src_path),
                    "destination": dest_path,
                    "size": driver.src_path.stat().st_size,
                }
            )
            logger.info(f"Copied driver: {driver.src_path.name} -> {dest_path}")

        except Exception as e:
            error_msg = f"Failed to copy {driver.src_path.name}: {e}"
            logger.error(error_msg)
            result["warnings"].append(error_msg)

    if not files_copied:
        result["reason"] = "no_files_copied"
        return result

    result["files_copied"] = files_copied

    registry_path = f"{windows_root}/System32/config/SYSTEM"
    try:
        registry_result = _edit_windows_registry(self, g, registry_path, drivers, plan)
        result["registry_changes"] = registry_result

        if registry_result.get("success", False):
            result["injected"] = True
            result["success"] = True
            logger.info("VirtIO drivers successfully injected")
        else:
            result["injected"] = False
            result["reason"] = "registry_update_failed"
            logger.warning("Driver files copied but registry update failed")

    except Exception as e:
        result["injected"] = False
        result["reason"] = f"registry_exception: {e}"
        result["registry_changes"] = {"error": str(e)}
        logger.error(f"Registry operation failed: {e}")

    result["notes"] = [
        "Storage drivers injected as services (BOOT start).",
        "Network/balloon injected as services (AUTO start).",
        "Windows Plug-and-Play may still install INF metadata on first boot.",
        "For the most reliable results, also attach virtio-win ISO and let Windows finish driver install.",
        "Always test in a safe environment first.",
    ]

    return result


# ---------------------------
# Additional Windows Fixes
# ---------------------------

def fix_windows_services(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    """
    Disable problematic Windows services that might conflict with virtualization.

    NOTE: This function is still "advisory". Actual service changes would require
    SYSTEM hive edits under Services\\<name> (Start value) or doing it inside Windows.
    """
    logger = _safe_logger(self)

    if not is_windows(self, g):
        return {"windows": False, "reason": "not_windows"}

    logger.info("Checking Windows services for virtualization compatibility")

    services_to_disable = [
        "WinDefend",
        "WdNisSvc",
        "Sense",
        "MpsSvc",
        "WSearch",
        "Superfetch",
        "SysMain",
        "DiagTrack",
    ]

    services_to_enable = [
        "VirtIO storage services if installed",
        "Remote Desktop Services for management",
    ]

    return {
        "windows": True,
        "action": "checked",
        "services_checked": services_to_disable,
        "services_recommended_enable": services_to_enable,
        "recommendations": [],
        "notes": "Service changes require registry modification or booting into Windows.",
    }


def enable_kvm_optimizations(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    """
    Apply KVM-specific optimizations to Windows guest.

    NOTE: mostly advisory; real changes are in libvirt domain XML and/or in-guest.
    """
    logger = _safe_logger(self)

    if not is_windows(self, g):
        return {"windows": False, "reason": "not_windows"}

    logger.info("Applying KVM optimizations for Windows")

    optimizations = {
        "recommended": [
            "Install KVM guest drivers (virtio-win).",
            "Enable Hyper-V enlightenments in KVM (libvirt: <features><hyperv>...).",
            "Use paravirtualized clock (kvmclock/hypervclock where applicable).",
            "Use virtio-scsi with multiple queues for storage-heavy workloads.",
            "Enable balloon driver for memory overcommit.",
            "Use modern NIC model (virtio-net) + multiqueue.",
        ],
        "registry_tweaks": [
            r"HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Control\PriorityControl - Win32PrioritySeparation",
            r"HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Multimedia\SystemProfile - NetworkThrottlingIndex",
        ],
        "performance": [
            "Disable unnecessary services if boot time / CPU is critical.",
            "Ensure pagefile is sized sanely for VM memory.",
            "Install QEMU guest agent for clean shutdown + IP reporting.",
        ],
    }

    return {
        "windows": True,
        "optimizations": optimizations,
        "applied": False,
        "notes": "These are recommendations. Apply carefully in production environments.",
    }


# ---------------------------
# Utility Functions
# ---------------------------

def get_windows_info(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    """Get comprehensive Windows information without making changes."""
    if not is_windows(self, g):
        return {"is_windows": False}

    win_info = _windows_version_info(self, g)
    plan = _choose_driver_plan(self, win_info)

    windows_root = _find_windows_root(self, g)
    existing_drivers: List[Dict[str, Any]] = []

    if windows_root:
        drivers_dir = f"{windows_root}/System32/drivers"
        virtio_drivers = ["viostor.sys", "vioscsi.sys", "netkvm.sys", "balloon.sys", "vioinput.sys", "viogpudo.sys", "virtiofs.sys"]

        for driver in virtio_drivers:
            try:
                p = f"{drivers_dir}/{driver}"
                if g.is_file(p):
                    size = g.filesize(p)
                    existing_drivers.append({"name": driver, "size": size, "exists": True})
                else:
                    existing_drivers.append({"name": driver, "exists": False})
            except Exception:
                existing_drivers.append({"name": driver, "exists": False})

    return {
        "is_windows": True,
        "version_info": win_info,
        "driver_plan": _plan_to_dict(plan),
        "existing_virtio_drivers": existing_drivers,
        "windows_root": windows_root,
        "detection_method": "libguestfs_inspection+registry_enrichment",
    }


def create_windows_recovery_script(self, g: guestfs.GuestFS) -> Optional[str]:
    """
    Create a recovery script for Windows guest issues.
    Returns the script content if Windows is detected.
    """
    if not is_windows(self, g):
        return None

    win_info = _windows_version_info(self, g)
    plan = _choose_driver_plan(self, win_info)

    # Keep your original content, but add a couple of practical extras.
    script = f"""@echo off
REM Windows Recovery Script for KVM Migration
REM Generated by vmdk2kvm
REM Windows Version: {win_info.get('product_name', 'Unknown')}
REM Architecture: {plan.arch_dir}
REM Bucket: {plan.os_bucket}
echo ========================================
echo Windows KVM Recovery Script
echo ========================================
echo.

REM 1. Check BCD store
echo Checking BCD store...
bcdedit /enum all
echo.

REM 2. Repair BCD if needed
echo To repair BCD, run as Administrator:
echo   bootrec /fixmbr
echo   bootrec /fixboot
echo   bootrec /scanos
echo   bootrec /rebuildbcd
echo.

REM 3. Check VirtIO drivers
echo Checking VirtIO drivers...
sc query viostor 2>nul || echo viostor driver not installed
sc query vioscsi 2>nul || echo vioscsi driver not installed
sc query netkvm 2>nul || echo netkvm driver not installed
sc query balloon 2>nul || echo balloon driver not installed
echo.

REM 4. Enable drivers if found but not running
echo To enable VirtIO drivers, run as Administrator:
for %%d in (viostor vioscsi netkvm balloon) do (
  sc query %%d >nul 2>&1 && sc config %%d start= auto && sc start %%d
)
echo.

REM 5. QEMU Guest Agent (optional but very useful)
echo QEMU Guest Agent check (optional):
sc query qemu-ga 2>nul || echo qemu-ga service not installed
echo.

REM 6. Network configuration (if needed)
echo Network configuration:
ipconfig /all
netsh interface show interface
echo.

REM 7. General system checks
echo System information:
systeminfo | findstr /B /C:"OS Name" /C:"OS Version" /C:"System Type"
echo.

echo Recovery steps completed.
pause
"""
    return script
