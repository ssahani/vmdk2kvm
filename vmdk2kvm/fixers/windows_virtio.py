# vmdk2kvm/fixers/windows_virtio.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import guestfs  # type: ignore

from ..core.utils import U
from .windows_registry import (
    append_devicepath_software_hive,
    edit_system_hive,
    provision_firstboot_payload_and_service,
    _ensure_windows_root,  # internal helper in same package; ensures correct system volume mounted
)

# Optional ISO extractor
try:
    import pycdlib  # type: ignore
except Exception:  # pragma: no cover
    pycdlib = None


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
    SERIAL = "serial"
    RNG = "rng"


class WindowsEdition(Enum):
    SERVER_2022 = "server_2022"
    SERVER_2019 = "server_2019"
    SERVER_2016 = "server_2016"
    SERVER_2012 = "server_2012"
    SERVER_2008 = "server_2008"
    WINDOWS_12 = "windows_12"
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
# Plan + Driver model
# ---------------------------

@dataclass(frozen=True)
class WindowsVirtioPlan:
    arch_dir: str
    os_bucket: str
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

    package_dir: Optional[Path] = None
    inf_path: Optional[Path] = None

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
# Logging helpers (emoji + steps)
# ---------------------------

def _safe_logger(self) -> logging.Logger:
    lg = getattr(self, "logger", None)
    if isinstance(lg, logging.Logger):
        return lg
    return logging.getLogger("vmdk2kvm.windows_virtio")


def _emoji(level: int) -> str:
    if level >= logging.ERROR:
        return "❌"
    if level >= logging.WARNING:
        return "⚠️"
    if level >= logging.INFO:
        return "✅"
    return "🔍"


def _log(logger: logging.Logger, level: int, msg: str, *args: Any) -> None:
    logger.log(level, f"{_emoji(level)} {msg}", *args)


@contextmanager
def _step(logger: logging.Logger, title: str):
    t0 = time.time()
    _log(logger, logging.INFO, "%s ...", title)
    try:
        yield
        _log(logger, logging.INFO, "%s done (%.2fs)", title, time.time() - t0)
    except Exception as e:
        _log(logger, logging.ERROR, "%s failed (%.2fs): %s", title, time.time() - t0, e)
        raise


# ---------------------------
# Misc helpers
# ---------------------------

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
    s = name.lower()
    s = re.sub(r"\([^)]*\)", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _guest_download_bytes(g: guestfs.GuestFS, guest_path: str, max_bytes: Optional[int] = None) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        lp = Path(td) / "dl"
        g.download(guest_path, str(lp))
        b = lp.read_bytes()
        return b[:max_bytes] if max_bytes is not None else b


def _guest_sha256(g: guestfs.GuestFS, guest_path: str) -> Optional[str]:
    try:
        return hashlib.sha256(_guest_download_bytes(g, guest_path)).hexdigest()
    except Exception:
        return None


def _sha256_path(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _log_mountpoints_best_effort(logger: logging.Logger, g: guestfs.GuestFS) -> None:
    try:
        mps = g.mountpoints()
        _log(logger, logging.DEBUG, "guestfs mountpoints=%r", mps)
    except Exception:
        pass


def _guest_mkdir_p(g: guestfs.GuestFS, path: str, *, dry_run: bool) -> None:
    if dry_run:
        return
    try:
        if not g.is_dir(path):
            g.mkdir_p(path)
    except Exception:
        # Some backends throw if path does not exist; mkdir_p is idempotent anyway.
        g.mkdir_p(path)


def _guest_write_text(g: guestfs.GuestFS, path: str, content: str, *, dry_run: bool) -> None:
    if dry_run:
        return
    g.write(path, content.encode("utf-8", errors="ignore"))


# ---------------------------
# VirtIO source materialization (dir OR ISO)
# ---------------------------

@contextmanager
def _materialize_virtio_source(self, virtio_path: Path):
    logger = _safe_logger(self)

    if virtio_path.is_dir():
        yield virtio_path
        return

    if virtio_path.suffix.lower() != ".iso":
        raise RuntimeError(f"virtio_drivers_dir must be a directory or .iso, got: {virtio_path}")

    if pycdlib is None:
        raise RuntimeError(
            "virtio_drivers_dir is an ISO but pycdlib is not installed. "
            "Install pycdlib or provide an extracted virtio-win directory."
        )

    td = Path(tempfile.mkdtemp(prefix="vmdk2kvm-virtio-iso-"))
    extracted = 0
    tried: List[str] = []
    try:
        _log(logger, logging.INFO, "📀 Extracting VirtIO ISO -> %s", td)
        iso = pycdlib.PyCdlib()
        iso.open(str(virtio_path))

        def _children(iso_dir: str, use_joliet: bool):
            if use_joliet:
                return iso.list_children(joliet_path=iso_dir)
            return iso.list_children(iso_path=iso_dir)

        def _walk(iso_dir: str, use_joliet: bool):
            try:
                kids = _children(iso_dir, use_joliet)
            except Exception:
                return
            for c in kids:
                try:
                    name = c.file_identifier().decode("utf-8", errors="ignore").rstrip(";1")
                except Exception:
                    continue
                if name in (".", "..") or not name:
                    continue
                child = iso_dir.rstrip("/") + "/" + name
                try:
                    if c.is_dir():
                        yield from _walk(child, use_joliet)
                    else:
                        yield child
                except Exception:
                    continue

        for use_joliet in (False, True):
            mode = "joliet" if use_joliet else "iso9660"
            tried.append(mode)
            for iso_file in _walk("/", use_joliet):
                rel = iso_file.lstrip("/").rstrip(";1")
                out = td / rel
                out.parent.mkdir(parents=True, exist_ok=True)
                try:
                    if use_joliet:
                        iso.get_file_from_iso(str(out), joliet_path=iso_file)
                    else:
                        iso.get_file_from_iso(str(out), iso_path=iso_file)
                    extracted += 1
                except Exception as e:
                    _log(logger, logging.DEBUG, "ISO extract failed for %s (%s): %s", iso_file, mode, e)

        try:
            iso.close()
        except Exception:
            pass

        _log(logger, logging.INFO, "📀 ISO extraction complete: %d files (modes tried=%s)", extracted, tried)
        yield td
    finally:
        try:
            shutil.rmtree(td, ignore_errors=True)
        except Exception:
            pass


# ---------------------------
# Windows detection + version
# ---------------------------

def is_windows(self, g: guestfs.GuestFS) -> bool:
    logger = _safe_logger(self)
    if not getattr(self, "inspect_root", None):
        _log(logger, logging.DEBUG, "Windows detect: inspect_root missing -> not Windows")
        return False

    root = self.inspect_root

    try:
        try:
            os_type = U.to_text(g.inspect_get_type(root))
            if os_type and os_type.lower() == "windows":
                _log(logger, logging.DEBUG, "Windows detect: inspect_get_type says windows")
                return True
        except Exception:
            pass

        for dir_path in ["/Windows", "/WINDOWS", "/winnt", "/WINNT", "/Program Files"]:
            try:
                if g.is_dir(dir_path):
                    _log(logger, logging.DEBUG, "Windows detect: found dir %s", dir_path)
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
                    _log(logger, logging.DEBUG, "Windows detect: found SOFTWARE hive %s", reg_file)
                    return True
            except Exception:
                continue

        _log(logger, logging.DEBUG, "Windows detect: no signals -> not Windows")
        return False

    except Exception as e:
        _log(logger, logging.DEBUG, "Windows detect: exception -> not Windows: %s", e)
        return False


def _find_windows_root(self, g: guestfs.GuestFS) -> Optional[str]:
    logger = _safe_logger(self)
    for p in ["/Windows", "/WINDOWS", "/winnt", "/WINNT"]:
        try:
            if g.is_dir(p):
                _log(logger, logging.DEBUG, "Windows root: found %s", p)
                return p
        except Exception:
            continue
    _log(logger, logging.DEBUG, "Windows root: no direct hit")
    return None


def _windows_version_info(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    logger = _safe_logger(self)
    info: Dict[str, Any] = {
        "windows": True,
        "bits": 64,
        "build": None,
        "product_name": None,
        "arch": None,
        "major": None,
        "minor": None,
        "distro": None,
    }

    root = getattr(self, "inspect_root", None)
    if not root:
        _log(logger, logging.DEBUG, "Windows info: inspect_root missing")
        return info

    try:
        info["arch"] = U.to_text(g.inspect_get_arch(root))
        info["major"] = g.inspect_get_major_version(root)
        info["minor"] = g.inspect_get_minor_version(root)
        info["product_name"] = U.to_text(g.inspect_get_product_name(root))
        info["distro"] = U.to_text(g.inspect_get_distro(root))
    except Exception as e:
        _log(logger, logging.DEBUG, "Windows info: inspect getters failed: %s", e)

    arch = (info.get("arch") or "").lower()
    if arch in ("x86_64", "amd64", "arm64", "aarch64"):
        info["bits"] = 64
    elif arch in ("i386", "i686", "x86"):
        info["bits"] = 32
    else:
        info["bits"] = 64

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

    # Clients (future-ish bucket)
    if "windows 12" in product or major >= 12:
        return WindowsEdition.WINDOWS_12
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
# Bucket fallback logic
# ---------------------------

def _bucket_candidates(edition: WindowsEdition) -> List[str]:
    if edition == WindowsEdition.WINDOWS_12:
        return ["w12", "w11", "w10", "w8", "w7"]
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
    logger = _safe_logger(self)

    edition = _detect_windows_edition(self, win_info)
    arch_dir = _norm_arch_to_dir(str(win_info.get("arch") or "amd64"))

    edition_to_bucket = {
        WindowsEdition.SERVER_2022: "w10",
        WindowsEdition.SERVER_2019: "w10",
        WindowsEdition.SERVER_2016: "w10",
        WindowsEdition.SERVER_2012: "w8",
        WindowsEdition.SERVER_2008: "w7",
        WindowsEdition.WINDOWS_12: "w12",
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

    if getattr(self, "enable_virtio_gpu", False):
        drivers_needed.add(DriverType.GPU)
    if getattr(self, "enable_virtio_input", False):
        drivers_needed.add(DriverType.INPUT)
    if getattr(self, "enable_virtio_fs", False):
        drivers_needed.add(DriverType.FILESYSTEM)
    if getattr(self, "enable_virtio_serial", False):
        drivers_needed.add(DriverType.SERIAL)
    if getattr(self, "enable_virtio_rng", False):
        drivers_needed.add(DriverType.RNG)

    plan = WindowsVirtioPlan(
        arch_dir=arch_dir,
        os_bucket=os_bucket,
        edition=edition,
        drivers_needed=drivers_needed,
    )

    _log(
        logger,
        logging.INFO,
        "🧩 Windows plan: edition=%s arch=%s bucket_hint=%s candidates=%s drivers=%s",
        plan.edition.value,
        plan.arch_dir,
        plan.os_bucket,
        _bucket_candidates(plan.edition),
        sorted([d.value for d in plan.drivers_needed]),
    )
    return plan


# ---------------------------
# Driver discovery + staging
# ---------------------------

def _is_probably_driver_payload(p: Path) -> bool:
    ext = p.suffix.lower()
    return ext in (".inf", ".cat", ".sys", ".dll", ".mui")


def _discover_virtio_drivers(self, virtio_src: Path, plan: WindowsVirtioPlan) -> List[DriverFile]:
    logger = _safe_logger(self)
    drivers: List[DriverFile] = []
    buckets = _bucket_candidates(plan.edition)

    storage_class_guid = "{4D36E967-E325-11CE-BFC1-08002BE10318}"   # SCSIAdapter
    net_class_guid = "{4D36E972-E325-11CE-BFC1-08002BE10318}"       # Net
    balloon_class_guid = "{4D36E97D-E325-11CE-BFC1-08002BE10318}"   # System

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
        DriverType.SERIAL: [
            {
                "name": "vioser",
                "pattern": "vioser/{bucket}/{arch}/vioser.sys",
                "inf_hint": "vioser.inf",
                "service": "vioser",
                "start": DriverStartType.MANUAL,
                "pci_ids": [
                    "pci#ven_1af4&dev_1003&subsys_00031af4",
                    "pci#ven_1af4&dev_1043&subsys_00031af4",
                ],
                "class_guid": "{4D36E978-E325-11CE-BFC1-08002BE10318}",
            },
        ],
        DriverType.RNG: [
            {
                "name": "viorng",
                "pattern": "viorng/{bucket}/{arch}/viorng.sys",
                "inf_hint": "viorng.inf",
                "service": "viorng",
                "start": DriverStartType.AUTO,
                "pci_ids": [
                    "pci#ven_1af4&dev_1005&subsys_00041af4",
                    "pci#ven_1af4&dev_1044&subsys_00041af4",
                ],
                "class_guid": balloon_class_guid,
            },
        ],
    }

    search_patterns = [
        "{pattern}",
        "{driver}/{bucket}/{arch}/*.sys",
        "{driver}/{arch}/*.sys",
        "{driver}/*/{arch}/*.sys",
        "{driver}/*/*/{arch}/*.sys",
    ]

    def _try_candidate_glob(base: Path, pat: str) -> Optional[Path]:
        try:
            matches = sorted([p for p in base.glob(pat) if p.is_file()])
            return matches[0] if matches else None
        except Exception:
            return None

    def _find_inf_near_sys(sys_path: Path, inf_hint: Optional[str]) -> Optional[Path]:
        pkg = sys_path.parent
        try:
            if inf_hint:
                cand = pkg / inf_hint
                if cand.exists() and cand.is_file():
                    return cand
            infs = sorted([p for p in pkg.glob("*.inf") if p.is_file()])
            return infs[0] if infs else None
        except Exception:
            return None

    _log(logger, logging.INFO, "🔎 Discovering VirtIO drivers ...")
    _log(logger, logging.INFO, "VirtIO source: %s", virtio_src)
    _log(logger, logging.INFO, "Bucket candidates: %s", buckets)

    with _materialize_virtio_source(self, virtio_src) as base:
        _log(logger, logging.INFO, "VirtIO materialized dir: %s", base)

        for driver_type in sorted(plan.drivers_needed, key=lambda d: d.value):
            cfgs = driver_configs.get(driver_type, [])
            if not cfgs:
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
                        src = _try_candidate_glob(base, pat)
                        if src is None:
                            continue

                        infp = _find_inf_near_sys(src, cfg.get("inf_hint"))
                        pkg_dir = src.parent

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
                        _log(
                            logger,
                            logging.INFO,
                            "📦 Found driver: type=%s name=%s bucket=%s -> %s",
                            driver_type.value,
                            driver_name,
                            bucket,
                            src,
                        )
                        if infp:
                            _log(logger, logging.INFO, "📄 INF: %s", infp)
                        else:
                            _log(logger, logging.WARNING, "📄 INF missing near %s (PnP may still work via SYS only)", src)
                        found = True
                        break

                if not found:
                    lvl = logging.WARNING if driver_type == DriverType.STORAGE else logging.INFO
                    _log(
                        logger,
                        lvl,
                        "Driver not found: type=%s name=%s arch=%s buckets=%s",
                        driver_type.value,
                        driver_name,
                        plan.arch_dir,
                        buckets,
                    )

    return drivers


# ---------------------------
# Public: BCD backup + hints (offline-safe)
# ---------------------------

def windows_bcd_actual_fix(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    logger = _safe_logger(self)

    if not is_windows(self, g):
        return {"windows": False, "reason": "not_windows"}

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
                found[store_type] = {"path": store_path, "size": size, "exists": True}
                if not dry_run:
                    ts = U.now_ts()
                    backup_path = f"{store_path}.backup.vmdk2kvm.{ts}"
                    try:
                        g.cp(store_path, backup_path)
                        backups[store_type] = {"backup_path": backup_path, "timestamp": ts, "size": size}
                    except Exception as be:
                        backups[store_type] = {"error": str(be), "path": store_path}
            else:
                found[store_type] = {"path": store_path, "exists": False}
        except Exception as e:
            found[store_type] = {"path": store_path, "exists": False, "error": str(e)}

    if not any(v.get("exists") for v in found.values()):
        return {"windows": True, "bcd": "no_bcd_store", "stores": found}

    notes: List[str] = [
        "Offline-safe: backups created where possible.",
        "Deep BCD edits need Windows tools (bcdedit/bootrec) inside Windows RE.",
    ]

    has_uefi = any(found.get(k, {}).get("exists") for k in ("uefi_standard", "uefi_alternative", "uefi_fallback", "uefi_root"))
    has_bios = found.get("bios", {}).get("exists")

    if has_uefi and not has_bios:
        notes.append("Hint: UEFI-style BCD present; boot the converted VM in UEFI mode.")
    if has_bios and not has_uefi:
        notes.append("Hint: BIOS-style BCD present; boot the converted VM in legacy BIOS mode.")
    if has_bios and has_uefi:
        notes.append("Hint: Both BIOS+UEFI BCD stores found; boot mode must match installed Windows mode.")

    return {"windows": True, "bcd": "found", "stores": found, "backups": backups, "notes": notes}


# ---------------------------
# Public: VirtIO injection
# ---------------------------

def inject_virtio_drivers(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    logger = _safe_logger(self)

    dry_run = bool(getattr(self, "dry_run", False))
    force_overwrite = bool(getattr(self, "force_virtio_overwrite", False))
    virtio_dir = getattr(self, "virtio_drivers_dir", None)
    export_report = bool(getattr(self, "export_report", False))

    if not virtio_dir:
        _log(logger, logging.INFO, "VirtIO inject: virtio_drivers_dir not set -> skip")
        return {"injected": False, "reason": "virtio_drivers_dir_not_set"}

    virtio_src = Path(str(virtio_dir))
    if not virtio_src.exists():
        return {"injected": False, "reason": "virtio_drivers_dir_not_found", "path": str(virtio_src)}
    if not (virtio_src.is_dir() or virtio_src.suffix.lower() == ".iso"):
        return {"injected": False, "reason": "virtio_drivers_dir_invalid", "path": str(virtio_src)}

    if not is_windows(self, g):
        return {"injected": False, "reason": "not_windows"}
    if not getattr(self, "inspect_root", None):
        return {"injected": False, "reason": "no_inspect_root"}

    _log_mountpoints_best_effort(logger, g)

    # Ensure we're operating on the REAL Windows system volume (C:) mounted at /
    # This prevents "copied to wrong partition" failures.
    with _step(logger, "🧭 Ensure Windows system volume mounted (C: -> /)"):
        # SYSTEM hive existence is the strongest anchor that /Windows is real.
        _ensure_windows_root(logger, g, hint_hive_path="/Windows/System32/config/SYSTEM")

    windows_root = _find_windows_root(self, g) or "/Windows"
    if not windows_root or not g.is_dir(windows_root):
        return {"injected": False, "reason": "no_windows_root"}

    win_info = _windows_version_info(self, g)
    plan = _choose_driver_plan(self, win_info)

    with _step(logger, "🔎 Discover VirtIO drivers"):
        drivers = _discover_virtio_drivers(self, virtio_src, plan)

    if not drivers:
        return {
            "injected": False,
            "reason": "no_drivers_found",
            "virtio_dir": str(virtio_src),
            "windows_info": win_info,
            "plan": _plan_to_dict(plan),
            "buckets_tried": _bucket_candidates(plan.edition),
        }

    # Critical sanity: storage drivers missing is almost always fatal for boot.
    storage_services = sorted({d.service_name for d in drivers if d.type == DriverType.STORAGE})
    if not storage_services:
        _log(logger, logging.ERROR, "No storage drivers discovered (viostor/vioscsi). Boot is likely to fail.")

    result: Dict[str, Any] = {
        "injected": False,
        "success": False,
        "dry_run": bool(dry_run),
        "force_overwrite": bool(force_overwrite),
        "windows": win_info,
        "plan": _plan_to_dict(plan),
        "virtio_dir": str(virtio_src),
        "windows_root": windows_root,
        "drivers_found": [d.to_dict() for d in drivers],
        "files_copied": [],
        "packages_staged": [],
        "registry_changes": {},
        "devicepath_changes": {},
        "bcd_changes": {},
        "firstboot": {},
        "artifacts": [],
        "warnings": [],
        "notes": [],
    }

    # ---- Copy SYS into System32\drivers ----
    drivers_target_dir = f"{windows_root}/System32/drivers"
    with _step(logger, "🧱 Ensure System32\\drivers exists"):
        try:
            if not g.is_dir(drivers_target_dir) and not dry_run:
                g.mkdir_p(drivers_target_dir)
        except Exception as e:
            return {**result, "reason": f"drivers_dir_error: {e}"}

    with _step(logger, "📦 Upload .sys driver binaries"):
        for drv in drivers:
            dest_path = f"{drivers_target_dir}/{drv.dest_name}"
            try:
                src_size = drv.src_path.stat().st_size
                host_hash = _sha256_path(drv.src_path)

                if g.is_file(dest_path) and not force_overwrite:
                    try:
                        guest_hash = _guest_sha256(g, dest_path)
                        if guest_hash and guest_hash == host_hash:
                            result["files_copied"].append(
                                {
                                    "name": drv.dest_name,
                                    "action": "skipped",
                                    "reason": "already_exists_same_hash",
                                    "source": str(drv.src_path),
                                    "destination": dest_path,
                                    "size": src_size,
                                    "sha256": host_hash,
                                    "type": drv.type.value,
                                    "service": drv.service_name,
                                }
                            )
                            result["artifacts"].append(
                                {
                                    "kind": "driver_sys",
                                    "service": drv.service_name,
                                    "type": drv.type.value,
                                    "src": str(drv.src_path),
                                    "dst": dest_path,
                                    "size": src_size,
                                    "sha256": host_hash,
                                    "action": "skipped",
                                }
                            )
                            _log(logger, logging.INFO, "Skip (same hash): %s -> %s", drv.src_path, dest_path)
                            continue
                    except Exception:
                        pass

                if not dry_run:
                    g.upload(str(drv.src_path), dest_path)

                # Optional verify for critical storage drivers (cheap + high value)
                verify = None
                if drv.type == DriverType.STORAGE and not dry_run:
                    try:
                        verify = _guest_sha256(g, dest_path)
                    except Exception:
                        verify = None

                action = "copied" if not dry_run else "dry_run"
                result["files_copied"].append(
                    {
                        "name": drv.dest_name,
                        "action": action,
                        "source": str(drv.src_path),
                        "destination": dest_path,
                        "size": src_size,
                        "sha256": host_hash,
                        "guest_sha256": verify,
                        "type": drv.type.value,
                        "service": drv.service_name,
                        "bucket_used": drv.bucket_used,
                        "match_pattern": drv.match_pattern,
                    }
                )
                result["artifacts"].append(
                    {
                        "kind": "driver_sys",
                        "service": drv.service_name,
                        "type": drv.type.value,
                        "src": str(drv.src_path),
                        "dst": dest_path,
                        "size": src_size,
                        "sha256": host_hash,
                        "guest_sha256": verify,
                        "action": action,
                        "bucket_used": drv.bucket_used,
                        "match_pattern": drv.match_pattern,
                    }
                )
                _log(logger, logging.INFO, "Upload: %s -> %s", drv.src_path, dest_path)
            except Exception as e:
                msg = f"VirtIO inject: copy failed {drv.src_path} -> {dest_path}: {e}"
                result["warnings"].append(msg)
                _log(logger, logging.WARNING, "%s", msg)

    # ---- Stage packages for firstboot pnputil (C:\vmdk2kvm\drivers\virtio\...) ----
    # IMPORTANT: /vmdk2kvm maps to C:\vmdk2kvm (NOT /Windows/vmdk2kvm).
    staging_root = "/vmdk2kvm/drivers/virtio"
    devicepath_append = r"%SystemDrive%\vmdk2kvm\drivers\virtio"  # safer than %SystemRoot%\vmdk2kvm\...

    with _step(logger, "📁 Stage driver packages (INF/CAT/DLL) for PnP"):
        try:
            _guest_mkdir_p(g, staging_root, dry_run=dry_run)
        except Exception as e:
            msg = f"VirtIO stage: failed to create staging root {staging_root}: {e}"
            result["warnings"].append(msg)
            _log(logger, logging.WARNING, "%s", msg)

        for drv in drivers:
            if not drv.package_dir or not drv.package_dir.exists() or not drv.inf_path:
                continue

            guest_pkg_dir = f"{staging_root}/{drv.service_name}"
            try:
                _guest_mkdir_p(g, guest_pkg_dir, dry_run=dry_run)
            except Exception as e:
                msg = f"VirtIO stage: cannot create {guest_pkg_dir}: {e}"
                result["warnings"].append(msg)
                _log(logger, logging.WARNING, "%s", msg)
                continue

            staged_files: List[Dict[str, Any]] = []
            try:
                payload = sorted([p for p in drv.package_dir.iterdir() if p.is_file() and _is_probably_driver_payload(p)])
                for p in payload:
                    gp = f"{guest_pkg_dir}/{p.name}"
                    try:
                        if not dry_run:
                            g.upload(str(p), gp)
                        staged_files.append({"name": p.name, "source": str(p), "dest": gp, "size": p.stat().st_size})
                        result["artifacts"].append(
                            {
                                "kind": "staged_payload",
                                "service": drv.service_name,
                                "type": drv.type.value,
                                "src": str(p),
                                "dst": gp,
                                "size": p.stat().st_size,
                                "action": "copied" if not dry_run else "dry_run",
                            }
                        )
                    except Exception as e:
                        msg = f"VirtIO stage: upload failed {p} -> {gp}: {e}"
                        result["warnings"].append(msg)
                        _log(logger, logging.WARNING, "%s", msg)

                if staged_files:
                    result["packages_staged"].append(
                        {
                            "service": drv.service_name,
                            "type": drv.type.value,
                            "package_dir": str(drv.package_dir),
                            "inf": str(drv.inf_path),
                            "guest_dir": guest_pkg_dir,
                            "files": staged_files,
                        }
                    )
                    _log(logger, logging.INFO, "Staged package: %s -> %s (%d files)", drv.service_name, guest_pkg_dir, len(staged_files))
            except Exception as e:
                msg = f"VirtIO stage: failed staging package for {drv.service_name}: {e}"
                result["warnings"].append(msg)
                _log(logger, logging.WARNING, "%s", msg)

    # Stage a setup.cmd for manual run (optional emergency lever)
    if result["packages_staged"]:
        setup_script = "/vmdk2kvm/setup.cmd"
        script_content = "@echo off\r\n"
        script_content += "echo Installing staged VirtIO drivers...\r\n"
        for staged in result["packages_staged"]:
            inf = staged.get("inf")
            if inf:
                inf_name = Path(str(inf)).name
                # NOTE: setup.cmd is run inside Windows, so use C:\ paths
                script_content += (
                    f'pnputil /add-driver "C:\\vmdk2kvm\\drivers\\virtio\\{staged["service"]}\\{inf_name}" /install\r\n'
                )
        script_content += "echo Done.\r\n"
        try:
            with _step(logger, "🧾 Stage manual setup.cmd (optional)"):
                _guest_write_text(g, setup_script, script_content, dry_run=dry_run)
            result["setup_script"] = {"path": setup_script, "content": script_content}
            result["artifacts"].append({"kind": "setup_cmd", "dst": setup_script, "action": "written" if not dry_run else "dry_run"})
        except Exception as e:
            msg = f"Failed to stage setup.cmd: {e}"
            result["warnings"].append(msg)
            _log(logger, logging.WARNING, "%s", msg)

    # ---- Registry edits (SYSTEM) ----
    system_hive = f"{windows_root}/System32/config/SYSTEM"
    with _step(logger, "🧬 Edit SYSTEM hive (Services + CDD + StartOverride)"):
        try:
            reg_res = edit_system_hive(
                self,
                g,
                system_hive,
                drivers,
                driver_type_storage_value=DriverType.STORAGE.value,
                boot_start_value=DriverStartType.BOOT.value,
            )
            result["registry_changes"] = reg_res
            if not reg_res.get("success"):
                _log(logger, logging.WARNING, "SYSTEM hive edit reported errors: %s", reg_res.get("errors"))
        except Exception as e:
            result["registry_changes"] = {"success": False, "error": str(e)}
            msg = f"Registry edit failed: {e}"
            result["warnings"].append(msg)
            _log(logger, logging.WARNING, "%s", msg)

    # ---- DevicePath append (SOFTWARE) ----
    with _step(logger, "🧩 Update SOFTWARE DevicePath (PnP discovery)"):
        try:
            software_hive = f"{windows_root}/System32/config/SOFTWARE"
            if result["packages_staged"]:
                dp_res = append_devicepath_software_hive(self, g, software_hive, devicepath_append)
                result["devicepath_changes"] = dp_res
                if not dp_res.get("success", True):
                    _log(logger, logging.WARNING, "DevicePath update reported errors: %s", dp_res.get("errors"))
            else:
                result["devicepath_changes"] = {"skipped": True, "reason": "no_packages_staged"}
                _log(logger, logging.INFO, "DevicePath: skipped (no packages staged)")
        except Exception as e:
            result["devicepath_changes"] = {"success": False, "error": str(e)}
            msg = f"DevicePath update failed: {e}"
            result["warnings"].append(msg)
            _log(logger, logging.WARNING, "%s", msg)

    # ---- Firstboot service (preferred over RunOnce) ----
    if result["packages_staged"]:
        with _step(logger, "🛠️ Provision firstboot service (pnputil /install + logging)"):
            try:
                fb = provision_firstboot_payload_and_service(
                    self,
                    g,
                    system_hive_path=system_hive,
                    service_name="vmdk2kvm-firstboot",
                    guest_dir="/vmdk2kvm",
                    log_path="/Windows/Temp/vmdk2kvm-firstboot.log",
                    driver_stage_dir=staging_root,
                    extra_cmd=None,
                )
                result["firstboot"] = fb
                if not fb.get("success", True):
                    msg = f"Firstboot provisioning failed: {fb.get('errors')}"
                    result["warnings"].append(msg)
                    _log(logger, logging.WARNING, "%s", msg)
                else:
                    _log(logger, logging.INFO, "Firstboot installed: service=vmdk2kvm-firstboot log=C:\\Windows\\Temp\\vmdk2kvm-firstboot.log")
            except Exception as e:
                result["firstboot"] = {"success": False, "error": str(e)}
                msg = f"Firstboot provisioning exception: {e}"
                result["warnings"].append(msg)
                _log(logger, logging.WARNING, "%s", msg)
    else:
        result["firstboot"] = {"skipped": True, "reason": "no_packages_staged"}

    # ---- BCD backup/hints ----
    with _step(logger, "🧷 BCD store discovery + backup"):
        try:
            result["bcd_changes"] = windows_bcd_actual_fix(self, g)
        except Exception as e:
            result["bcd_changes"] = {"windows": True, "bcd": "error", "error": str(e)}
            msg = f"BCD check failed: {e}"
            result["warnings"].append(msg)
            _log(logger, logging.WARNING, "%s", msg)

    # ---- Success criteria ----
    sys_ok = any(x.get("action") in ("copied", "dry_run", "skipped") for x in result["files_copied"])
    reg_ok = bool(result.get("registry_changes", {}).get("success"))
    result["injected"] = bool(sys_ok and reg_ok)
    result["success"] = result["injected"]
    if not result["success"]:
        result["reason"] = "registry_update_failed" if not reg_ok else "sys_copy_failed"

    storage_found = sorted({d.service_name for d in drivers if d.type == DriverType.STORAGE})
    storage_missing: List[str] = []
    if "viostor" not in storage_found:
        storage_missing.append("viostor")
    if "vioscsi" not in storage_found:
        storage_missing.append("vioscsi")

    result["notes"] += [
        "Storage: attempts to inject BOTH viostor + vioscsi (if present) and forces BOOT start in SYSTEM hive.",
        "Registry: StartOverride removed when found (can silently disable boot drivers).",
        "CDD: CriticalDeviceDatabase populated for virtio storage PCI IDs to ensure early binding.",
        f"Driver discovery buckets: {_bucket_candidates(plan.edition)}",
        f"Storage drivers found: {storage_found} missing: {storage_missing}",
        r"Staging: payload staged under C:\vmdk2kvm\drivers\virtio and installed via firstboot service (pnputil).",
        r"Logs: C:\Windows\Temp\vmdk2kvm-firstboot.log (firstboot) and service name vmdk2kvm-firstboot.",
    ]

    if storage_missing:
        msg = f"Missing critical storage drivers: {storage_missing} (guest may BSOD INACCESSIBLE_BOOT_DEVICE)"
        result["warnings"].append(msg)
        _log(logger, logging.WARNING, "%s", msg)

    if export_report:
        report_path = "virtio_inject_report.json"
        try:
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, default=str)
            result["report_exported"] = report_path
            _log(logger, logging.INFO, "Report exported: %s", report_path)
        except Exception as e:
            msg = f"Failed to export report: {e}"
            result["warnings"].append(msg)
            _log(logger, logging.WARNING, "%s", msg)

    return result


# ---------------------------
# Optional wrapper (if your framework instantiates a class)
# ---------------------------

class WindowsFixer:
    def is_windows(self, g: guestfs.GuestFS) -> bool:
        return is_windows(self, g)

    def windows_bcd_actual_fix(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        return windows_bcd_actual_fix(self, g)

    def inject_virtio_drivers(self, g: guestfs.GuestFS) -> Dict[str, Any]:
        return inject_virtio_drivers(self, g)
