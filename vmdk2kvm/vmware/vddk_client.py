# SPDX-License-Identifier: LGPL-3.0-or-later
# vmdk2kvm/vsphere/vddk_client.py
# -*- coding: utf-8 -*-
"""
VDDK client for ESXi "download-only" pulls.

This module is intentionally *self-contained* and does NOT depend on pyvmomi.
It focuses on the data-plane: connect to ESXi via VMware VDDK (VixDiskLib),
open a VMDK, read sectors, and stream to a local file.

You can use this from vmware_client.py (pyvmomi control-plane) by:
  - resolving ESXi host + backing.fileName ("[datastore] path/to/disk.vmdk")
  - calling VDDKESXClient.download_vmdk(...)

Runtime requirements:
  - VMware VDDK installed/extracted locally (libvixDiskLib.so present)
  - VDDK dependencies available to dynamic loader (often via LD_LIBRARY_PATH)
  - Network access to ESXi on 443 and VDDK transport ports (NBD/NBDSSL) as needed

Caveats:
  - VDDK API is C; symbol availability can vary by VDDK version.
  - This module binds only the minimal symbols it uses.
  - For best reliability, open the *descriptor* VMDK (not -flat.vmdk).

FIXED: free(): invalid size memory corruption by:
  1. Proper structure alignment with _pack_ = 1
  2. Better library loading with dependency resolution
  3. Safer ConnectEx parameter handling
"""

from __future__ import annotations

import ctypes
import faulthandler
import hashlib
import logging
import os
import random
import signal
import socket
import ssl
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Tuple


class VDDKError(RuntimeError):
    """Generic VDDK client error."""


class VDDKCancelled(VDDKError):
    """Raised when a caller cancels an in-progress download."""


_VIXDISKLIB_API_VERSION_MAJOR = 9
_VIXDISKLIB_API_VERSION_MINOR = 0

# Open flags (stable)
_VIXDISKLIB_FLAG_OPEN_READ_ONLY = 0x00000001

# VDDK types
_VixDiskLibConnection = ctypes.c_void_p
_VixDiskLibHandle = ctypes.c_void_p

_SECTOR_SIZE = 512

# Global list to keep callback references alive
_VDDK_CALLBACK_REFS: list[object] = []

# Enable faulthandler early for better crash diagnostics
faulthandler.enable()


# -----------------------------------------------------------------------------
# ctypes structures - FIXED: Proper alignment to prevent memory corruption
# -----------------------------------------------------------------------------


class _VixDiskLibConnectParams(ctypes.Structure):
    """
    Minimal connect params struct.
    
    FIX: Use 1-byte packing to match VDDK's expected layout exactly.
    Some VDDK versions expect tight packing without padding.
    """
    _pack_ = 1  # CRITICAL FIX: Prevent padding-induced memory corruption
    _fields_ = [
        ("vmxSpec", ctypes.c_char_p),
        ("serverName", ctypes.c_char_p),
        ("thumbPrint", ctypes.c_char_p),
        ("userName", ctypes.c_char_p),
        ("password", ctypes.c_char_p),
        ("port", ctypes.c_uint32),
        # No padding field - let packing handle it
    ]


class _VixDiskLibInfo(ctypes.Structure):
    """
    VixDiskLibInfo from VDDK headers.

    We only rely on 'capacity' (in sectors), but keep the early fields correct.
    """
    _fields_ = [
        ("magic", ctypes.c_uint32),
        ("version", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("capacity", ctypes.c_uint64),  # sectors
        # More fields exist (geometry, adapterType, etc.) but we stop here.
    ]


def _as_cstr(s: Optional[str]) -> Optional[bytes]:
    if s is None:
        return None
    s2 = str(s).strip()
    return s2.encode("utf-8") if s2 else None


def normalize_thumbprint(tp: str) -> str:
    """
    Normalize a SHA1 thumbprint to colon-separated lower-case bytes:
      "AABBCC.." or "aa:bb:cc" -> "aa:bb:cc:..."
    """
    raw = (tp or "").strip().replace(" ", "").replace(":", "").lower()
    if len(raw) != 40 or any(c not in "0123456789abcdef" for c in raw):
        raise VDDKError(f"Invalid thumbprint (expected SHA1 40 hex chars): {tp!r}")
    return ":".join(raw[i : i + 2] for i in range(0, 40, 2))


def compute_server_thumbprint_sha1(host: str, port: int = 443, timeout: float = 10.0) -> str:
    """Fetch the server certificate (DER) and return SHA1 thumbprint (colon-separated)."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with socket.create_connection((host, port), timeout=timeout) as sock:
        with ctx.wrap_socket(sock, server_hostname=host) as ssock:
            der = ssock.getpeercert(binary_form=True)
    sha1 = hashlib.sha1(der).hexdigest()
    return ":".join(sha1[i : i + 2] for i in range(0, 40, 2))


def _peek_tls_cert_sha1(host: str, port: int, timeout: float) -> Tuple[Optional[str], Optional[str]]:
    """
    Best-effort: fetch peer cert and return (sha1_thumbprint, subject_str).
    Returns (None, None) on failure.
    """
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with socket.create_connection((host, port), timeout=timeout) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                der = ssock.getpeercert(binary_form=True)
                info = ssock.getpeercert() or {}

        sha1 = hashlib.sha1(der).hexdigest()
        tp = ":".join(sha1[i : i + 2] for i in range(0, 40, 2))
        subj = str(info.get("subject", "")) if info else ""
        return tp, subj
    except Exception:
        return None, None


def _tcp_probe(host: str, port: int, timeout: float) -> Tuple[bool, str]:
    """Best-effort TCP connect probe. Returns (ok, detail)."""
    try:
        t0 = time.time()
        with socket.create_connection((host, port), timeout=timeout):
            dt = max(0.0, time.time() - t0)
            return True, f"ok ({dt:.3f}s)"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _resolve_host(host: str) -> Tuple[bool, str]:
    """Best-effort DNS resolution. Returns (ok, detail string)."""
    try:
        infos = socket.getaddrinfo(host, None)
        addrs = sorted({i[4][0] for i in infos})
        return True, ", ".join(addrs[:8]) + (" ..." if len(addrs) > 8 else "")
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _atomic_write_replace(tmp_path: Path, final_path: Path) -> None:
    """Atomic-ish replace on POSIX."""
    os.replace(str(tmp_path), str(final_path))


def _fsync_dir(path: Path) -> None:
    """
    Best-effort fsync of a directory to make rename durable on POSIX.
    No-op if not supported.
    """
    try:
        fd = os.open(str(path), os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except Exception:
        pass


def _looks_like_datastore_path(p: str) -> bool:
    # Common form: "[datastore1] folder/disk.vmdk"
    s = (p or "").strip()
    return s.startswith("[") and "]" in s and s.lower().endswith(".vmdk")


def _is_flat_or_delta_vmdk(p: str) -> bool:
    s = (p or "").lower()
    return s.endswith("-flat.vmdk") or s.endswith("-delta.vmdk") or s.endswith("-sesparse.vmdk")


def _fmt_eta(seconds: float) -> str:
    try:
        s = int(max(0.0, seconds))
    except Exception:
        return "?"
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m{s:02d}s"


# -----------------------------------------------------------------------------
# Enhanced crash handler for debugging memory corruption
# -----------------------------------------------------------------------------

def _setup_crash_handler(logger: logging.Logger):
    """Setup comprehensive crash handlers for memory corruption debugging."""
    
    def sigsegv_handler(signum, frame):
        logger.critical("=" * 80)
        logger.critical("SIGSEGV DETECTED - Memory Corruption in VDDK")
        logger.critical("=" * 80)
        
        # Log stack trace
        logger.critical("\nStack trace at crash:")
        for filename, lineno, name, line in traceback.extract_stack(frame):
            logger.critical(f"  {filename}:{lineno} in {name}")
            if line:
                logger.critical(f"    {line.strip()}")
        
        # Dump thread stacks
        logger.critical("\nAll thread stacks:")
        for thread_id, thread_frame in sys._current_frames().items():
            if thread_id != threading.current_thread().ident:
                logger.critical(f"\nThread {thread_id}:")
                for filename, lineno, name, line in traceback.extract_stack(thread_frame):
                    logger.critical(f"  {filename}:{lineno} in {name}")
        
        # Try to dump VDDK state
        try:
            logger.critical(f"\nVDDK Connection state: initialized")
            logger.critical(f"LD_LIBRARY_PATH: {os.environ.get('LD_LIBRARY_PATH')}")
            logger.critical(f"VDDK_HOME: {os.environ.get('VDDK_HOME')}")
        except:
            pass
        
        logger.critical("\n" + "=" * 80)
        logger.critical("Attempting safe termination...")
        
        # Original handler will terminate
        signal.signal(signal.SIGSEGV, signal.SIG_DFL)
        os.kill(os.getpid(), signal.SIGSEGV)
    
    signal.signal(signal.SIGSEGV, sigsegv_handler)


# -----------------------------------------------------------------------------
# Dynamic loading & symbol binding - FIXED: Better dependency handling
# -----------------------------------------------------------------------------


def _candidate_lib_names() -> Tuple[str, ...]:
    return (
        "libvixDiskLib.so",
        "libvixDiskLib.so.9",
        "libvixDiskLib.so.7",
        "libvixDiskLib.so.6",
        "libvixDiskLib.so.5",
    )


def _load_vddk_cdll(
    vddk_libdir: Optional[Path],
    *,
    mutate_env: bool = True,  # CHANGED: Default to True for VDDK
) -> ctypes.CDLL:
    """
    Load libvixDiskLib.so. If vddk_libdir is provided, try it first.
    
    FIXED: VDDK requires proper LD_LIBRARY_PATH setup for its dependencies.
    We now always set up the environment properly.
    """
    # Always set VDDK_HOME for VDDK internal use
    if vddk_libdir:
        vddk_path = Path(vddk_libdir).expanduser().resolve()
        os.environ["VDDK_HOME"] = str(vddk_path)
        
        # Build comprehensive library path
        lib_paths = [
            str(vddk_path),
            str(vddk_path / "lib64"),
            str(vddk_path / "lib"),
            str(vddk_path / "bin"),
            "/usr/lib64",
            "/usr/lib",
            "/lib64", 
            "/lib",
            os.environ.get("LD_LIBRARY_PATH", "")
        ]
        
        # Filter out empty paths and join
        new_ld_path = ":".join([p for p in lib_paths if p and os.path.exists(p.split(':')[0])])
        os.environ["LD_LIBRARY_PATH"] = new_ld_path
        
        logger = logging.getLogger(__name__)
        logger.debug(f"VDDK: Set LD_LIBRARY_PATH={new_ld_path}")
        logger.debug(f"VDDK: Set VDDK_HOME={vddk_path}")
    
    last_error: Optional[Exception] = None
    
    # Try loading from explicit path first
    if vddk_libdir:
        vddk_path = Path(vddk_libdir).expanduser().resolve()
        for n in _candidate_lib_names():
            cand = vddk_path / n
            if cand.exists():
                try:
                    logger = logging.getLogger(__name__)
                    logger.debug(f"VDDK: Trying explicit path: {cand}")
                    # Use RTLD_GLOBAL for VDDK to resolve its internal symbols
                    return ctypes.CDLL(str(cand), mode=ctypes.RTLD_GLOBAL)
                except Exception as e:
                    last_error = e
                    logger.debug(f"VDDK: Failed to load {cand}: {e}")
    
    # Try loading from system paths
    for n in _candidate_lib_names():
        try:
            logger = logging.getLogger(__name__)
            logger.debug(f"VDDK: Trying system load: {n}")
            return ctypes.CDLL(n, mode=ctypes.RTLD_GLOBAL)
        except Exception as e:
            last_error = e
    
    raise VDDKError(
        "Failed to load VDDK library (libvixDiskLib.so).\n"
        "Ensure:\n"
        "  1. VDDK is installed at the path specified by vddk_libdir\n"
        "  2. All VDDK dependencies are available in LD_LIBRARY_PATH\n"
        "  3. You have proper permissions to load the library\n"
        f"Last error: {last_error!r}\n"
        f"LD_LIBRARY_PATH: {os.environ.get('LD_LIBRARY_PATH')}"
    )


def _bind_symbols(lib: ctypes.CDLL) -> None:
    """Bind minimal VDDK symbols used by this module."""
    # Test if InitEx exists with proper signature
    try:
        lib.VixDiskLib_InitEx.argtypes = [
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,  # logFunc
            ctypes.c_void_p,  # warnFunc
            ctypes.c_void_p,  # panicFunc
            ctypes.c_char_p,  # libDir
            ctypes.c_char_p,  # configFile
        ]
        lib.VixDiskLib_InitEx.restype = ctypes.c_int
    except AttributeError:
        # Fallback to older Init
        lib.VixDiskLib_Init.argtypes = [
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,  # logFunc
            ctypes.c_void_p,  # warnFunc
            ctypes.c_void_p,  # panicFunc
        ]
        lib.VixDiskLib_Init.restype = ctypes.c_int

    lib.VixDiskLib_Exit.argtypes = []
    lib.VixDiskLib_Exit.restype = None

    # ConnectEx is preferred but might not exist
    try:
        lib.VixDiskLib_ConnectEx.argtypes = [
            ctypes.POINTER(_VixDiskLibConnectParams),
            ctypes.c_char_p,  # identity
            ctypes.c_char_p,  # snapshotRef
            ctypes.c_char_p,  # transportModes
            ctypes.POINTER(_VixDiskLibConnection),
        ]
        lib.VixDiskLib_ConnectEx.restype = ctypes.c_int
    except AttributeError:
        # Fallback to Connect
        lib.VixDiskLib_Connect.argtypes = [
            ctypes.POINTER(_VixDiskLibConnectParams),
            ctypes.POINTER(_VixDiskLibConnection),
        ]
        lib.VixDiskLib_Connect.restype = ctypes.c_int

    lib.VixDiskLib_Disconnect.argtypes = [_VixDiskLibConnection]
    lib.VixDiskLib_Disconnect.restype = None

    lib.VixDiskLib_Open.argtypes = [
        _VixDiskLibConnection,
        ctypes.c_char_p,
        ctypes.c_uint32,
        ctypes.POINTER(_VixDiskLibHandle),
    ]
    lib.VixDiskLib_Open.restype = ctypes.c_int

    lib.VixDiskLib_Close.argtypes = [_VixDiskLibHandle]
    lib.VixDiskLib_Close.restype = None

    lib.VixDiskLib_GetInfo.argtypes = [
        _VixDiskLibHandle,
        ctypes.POINTER(ctypes.POINTER(_VixDiskLibInfo)),
    ]
    lib.VixDiskLib_GetInfo.restype = ctypes.c_int

    lib.VixDiskLib_FreeInfo.argtypes = [ctypes.c_void_p]
    lib.VixDiskLib_FreeInfo.restype = None

    lib.VixDiskLib_Read.argtypes = [
        _VixDiskLibHandle,
        ctypes.c_uint64,
        ctypes.c_uint64,
        ctypes.c_void_p,  # uint8*
    ]
    lib.VixDiskLib_Read.restype = ctypes.c_int

    lib.VixDiskLib_GetErrorText.argtypes = [ctypes.c_int, ctypes.c_char_p]
    lib.VixDiskLib_GetErrorText.restype = ctypes.c_char_p

    lib.VixDiskLib_FreeErrorText.argtypes = [ctypes.c_void_p]
    lib.VixDiskLib_FreeErrorText.restype = None


def _err_text(lib: ctypes.CDLL, rc: int) -> str:
    """Best-effort conversion of VDDK error codes to text."""
    try:
        p = lib.VixDiskLib_GetErrorText(int(rc), None)
        if not p:
            return f"VDDK rc={rc}"
        raw = ctypes.cast(p, ctypes.c_char_p).value
        msg = raw.decode("utf-8", errors="replace") if raw else f"VDDK rc={rc}"
        try:
            lib.VixDiskLib_FreeErrorText(p)
        except Exception:
            pass
        return msg
    except Exception:
        return f"VDDK rc={rc}"


def _is_likely_transient_error(msg: str) -> bool:
    m = (msg or "").lower()

    hard = (
        "permission",
        "access denied",
        "no such file",
        "not found",
        "invalid",
        "bad parameter",
        "unsupported",
        "authentication",
        "auth failed",
        "thumbprint",
        "certificate",
    )
    if any(x in m for x in hard):
        return False

    transient = (
        "timeout",
        "timed out",
        "connection",
        "connect",
        "network",
        "transport",
        "reset",
        "broken pipe",
        "eof",
        "unavailable",
        "try again",
        "tempor",
    )
    if any(x in m for x in transient):
        return True

    return True


# -----------------------------------------------------------------------------
# VDDK logging callbacks
# -----------------------------------------------------------------------------

_VDDK_LOG_CB_SIMPLE = ctypes.CFUNCTYPE(None, ctypes.c_char_p)

_g_vddk_log_cb: Optional[_VDDK_LOG_CB_SIMPLE] = None
_g_vddk_warn_cb: Optional[_VDDK_LOG_CB_SIMPLE] = None
_g_vddk_panic_cb: Optional[_VDDK_LOG_CB_SIMPLE] = None


def _mk_vddk_log_cb_simple(logger: logging.Logger, level: str) -> _VDDK_LOG_CB_SIMPLE:
    """Simple callback that attempts to decode a char* message safely."""

    def _cb(msg_p: ctypes.c_char_p) -> None:
        try:
            if msg_p:
                msg_bytes = msg_p.value
                if msg_bytes:
                    msg = msg_bytes.decode("utf-8", "replace").rstrip()
                else:
                    msg = "<empty message>"
            else:
                msg = "<NULL pointer>"

            if level == "debug":
                logger.debug("VDDK: %s", msg)
            elif level == "warning":
                logger.warning("VDDK: %s", msg)
            else:
                logger.error("VDDK: %s", msg)
        except Exception as e:
            # Never raise from a C callback.
            try:
                logger.error("VDDK callback error: %s", e)
            except Exception:
                pass

    cb = _VDDK_LOG_CB_SIMPLE(_cb)
    _VDDK_CALLBACK_REFS.append(cb)
    return cb


def _mk_dummy_callback() -> _VDDK_LOG_CB_SIMPLE:
    """Create a dummy callback that does nothing."""

    def _cb(_: ctypes.c_char_p) -> None:
        return

    cb = _VDDK_LOG_CB_SIMPLE(_cb)
    _VDDK_CALLBACK_REFS.append(cb)
    return cb


# -----------------------------------------------------------------------------
# Global init (InitEx once) - FIXED: Safer initialization
# -----------------------------------------------------------------------------

_vddk_lock = threading.Lock()
_vddk_inited = False


def vddk_init_once(logger: logging.Logger, lib: ctypes.CDLL, *, vddk_libdir: Optional[Path]) -> None:
    """
    Initialize VDDK once per process. Thread-safe.
    
    FIXED: Try multiple API versions and handle failures gracefully.
    """
    global _vddk_inited, _g_vddk_log_cb, _g_vddk_warn_cb, _g_vddk_panic_cb

    with _vddk_lock:
        if _vddk_inited:
            return

        libdir_c = _as_cstr(str(Path(vddk_libdir).expanduser().resolve())) if vddk_libdir else None
        
        # Setup crash handler
        _setup_crash_handler(logger)
        
        # Try different initialization strategies
        api_versions = [
            (_VIXDISKLIB_API_VERSION_MAJOR, _VIXDISKLIB_API_VERSION_MINOR),
            (7, 0),
            (6, 7),
            (6, 5),
        ]
        
        for major, minor in api_versions:
            try:
                logger.debug(f"VDDK: Trying InitEx API {major}.{minor}...")
                
                # Try with no callbacks first (safest)
                rc = lib.VixDiskLib_InitEx(
                    major,
                    minor,
                    None,
                    None,
                    None,
                    libdir_c,
                    None,
                )
                
                if rc == 0:
                    logger.debug(f"VDDK: InitEx OK (API {major}.{minor}, no callbacks)")
                    _vddk_inited = True
                    return
                    
            except AttributeError:
                # Try older Init function
                try:
                    logger.debug(f"VDDK: Trying Init API {major}.{minor}...")
                    rc = lib.VixDiskLib_Init(
                        major,
                        minor,
                        None,
                        None,
                        None,
                    )
                    
                    if rc == 0:
                        logger.debug(f"VDDK: Init OK (API {major}.{minor}, no callbacks)")
                        _vddk_inited = True
                        return
                        
                except AttributeError:
                    continue
        
        # If all else fails, try with dummy callbacks
        logger.debug("VDDK: Trying with dummy callbacks...")
        _g_vddk_log_cb = _mk_dummy_callback()
        _g_vddk_warn_cb = _mk_dummy_callback()
        _g_vddk_panic_cb = _mk_dummy_callback()
        
        # Try the highest API version with callbacks
        rc = lib.VixDiskLib_InitEx(
            _VIXDISKLIB_API_VERSION_MAJOR,
            _VIXDISKLIB_API_VERSION_MINOR,
            _g_vddk_log_cb,
            _g_vddk_warn_cb,
            _g_vddk_panic_cb,
            libdir_c,
            None,
        )
        
        if rc != 0:
            raise VDDKError(f"VixDiskLib_InitEx failed with all API versions")
        
        logger.debug("VDDK: InitEx OK (with dummy callbacks)")
        _vddk_inited = True


def vddk_cleanup() -> None:
    """Clean up VDDK resources."""
    global _vddk_inited
    with _vddk_lock:
        if _vddk_inited:
            try:
                # Find and call VixDiskLib_Exit if possible
                import ctypes.util
                lib = ctypes.CDLL("libvixDiskLib.so", mode=ctypes.RTLD_GLOBAL)
                if hasattr(lib, 'VixDiskLib_Exit'):
                    lib.VixDiskLib_Exit()
            except:
                pass
            finally:
                _VDDK_CALLBACK_REFS.clear()
                _vddk_inited = False


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------

ProgressFn = Callable[[int, int, float], None]
CancelFn = Callable[[], bool]


@dataclass(frozen=True)
class VDDKConnectionSpec:
    host: str
    user: str
    password: str
    port: int = 443
    thumbprint: Optional[str] = None
    insecure: bool = False
    transport_modes: Optional[str] = None  # e.g. "nbdssl:nbd"
    vddk_libdir: Optional[Path] = None
    tls_thumbprint_timeout: float = 10.0
    mutate_ld_library_path: bool = True  # CHANGED: Default to True for VDDK

    # Debug / diagnostics
    debug_preflight: bool = True
    preflight_timeout: float = 5.0


class VDDKESXClient:
    """
    Minimal ESXi VDDK reader.
    
    Lifecycle:
      - connect()
      - download_vmdk(...)
      - disconnect()
    """

    def __init__(self, logger: logging.Logger, spec: VDDKConnectionSpec):
        self.logger = logger
        self.spec = spec

        self._lib: Optional[ctypes.CDLL] = None
        self._conn: _VixDiskLibConnection = _VixDiskLibConnection()
        self._connect_strings: dict[str, Optional[bytes]] = {}

    def __enter__(self) -> "VDDKESXClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.disconnect()

    # Setup / Connect

    def _ensure_loaded(self) -> None:
        if self._lib is not None:
            return

        s = self.spec
        if not (s.host and s.user and s.password):
            raise VDDKError("Missing VDDK connection details (host/user/password)")

        self.logger.debug(
            "VDDK: loading lib (vddk_libdir=%r mutate_ld_library_path=%s LD_LIBRARY_PATH=%r)",
            str(s.vddk_libdir) if s.vddk_libdir else None,
            bool(s.mutate_ld_library_path),
            os.environ.get("LD_LIBRARY_PATH", ""),
        )

        lib = _load_vddk_cdll(s.vddk_libdir, mutate_env=bool(s.mutate_ld_library_path))
        self.logger.debug("VDDK: CDLL loaded: %r", getattr(lib, "_name", lib))
        _bind_symbols(lib)
        vddk_init_once(self.logger, lib, vddk_libdir=s.vddk_libdir)

        self._lib = lib

    def _preflight_debug(self) -> None:
        """Extra diagnostics before ConnectEx. Never raises; logs only."""
        s = self.spec
        if not s.debug_preflight:
            return

        self.logger.debug(
            "VDDK: preflight host=%r port=%d user=%r insecure=%s transports=%r",
            s.host,
            s.port,
            s.user,
            s.insecure,
            s.transport_modes,
        )

        ok_res, res = _resolve_host(s.host)
        if ok_res:
            self.logger.debug("VDDK: DNS resolve %s -> %s", s.host, res)
        else:
            self.logger.error("VDDK: DNS resolve failed for %s: %s", s.host, res)

        ok_tcp, tcp = _tcp_probe(s.host, int(s.port), float(s.preflight_timeout))
        if ok_tcp:
            self.logger.debug("VDDK: TCP connect %s:%d -> %s", s.host, s.port, tcp)
        else:
            self.logger.error("VDDK: TCP connect failed %s:%d -> %s", s.host, s.port, tcp)

        tp, subj = _peek_tls_cert_sha1(s.host, int(s.port), float(s.preflight_timeout))
        if tp:
            self.logger.debug("VDDK: TLS peer cert sha1=%s subject=%s", tp, subj)
        else:
            self.logger.debug("VDDK: TLS peer cert fetch failed (may still work if network blocks TLS inspect)")

        if s.thumbprint:
            try:
                self.logger.debug(
                    "VDDK: thumbprint raw=%r normalized=%r",
                    s.thumbprint,
                    normalize_thumbprint(s.thumbprint),
                )
            except Exception as e:
                self.logger.error("VDDK: thumbprint invalid: %r (%s)", s.thumbprint, e)

    def connect(self) -> None:
        """Connect to ESXi host using VDDK."""
        self._ensure_loaded()
        assert self._lib is not None

        s = self.spec
        self._preflight_debug()

        # Use simpler transport for initial connection
        # nbd is more reliable than nbdssl for initial testing
        transport = "nbd"  # Start simple
        if not s.insecure and s.thumbprint:
            transport = "nbdssl,nbd"  # Fallback chain
        
        self.logger.debug("VDDK: Using transport: %s", transport)

        # Prepare connection parameters with proper memory management
        server_bytes = s.host.encode('utf-8')
        user_bytes = s.user.encode('utf-8')
        password_bytes = s.password.encode('utf-8')
        transport_bytes = transport.encode('utf-8')
        
        # Keep references to prevent garbage collection
        self._connect_strings = {
            "server": server_bytes,
            "user": user_bytes,
            "password": password_bytes,
            "transport": transport_bytes,
        }
        
        # Create parameters with explicit NULL for unused fields
        params = _VixDiskLibConnectParams()
        # Zero out the structure first
        ctypes.memset(ctypes.byref(params), 0, ctypes.sizeof(params))
        
        # Set fields
        params.serverName = ctypes.c_char_p(server_bytes)
        params.userName = ctypes.c_char_p(user_bytes)
        params.password = ctypes.c_char_p(password_bytes)
        params.port = ctypes.c_uint32(int(s.port))
        
        # vmxSpec and thumbPrint should be NULL
        params.vmxSpec = None
        params.thumbPrint = None

        conn = _VixDiskLibConnection()
        t0 = time.time()

        self.logger.debug(
            "VDDK: ConnectEx params: serverName=%r port=%d user=%r transport=%r",
            s.host,
            int(s.port),
            s.user,
            transport,
        )

        try:
            # Try ConnectEx first (preferred)
            if hasattr(self._lib, 'VixDiskLib_ConnectEx'):
                rc = self._lib.VixDiskLib_ConnectEx(
                    ctypes.byref(params),
                    None,  # identity
                    None,  # snapshotRef
                    ctypes.c_char_p(transport_bytes),
                    ctypes.byref(conn),
                )
            else:
                # Fallback to Connect
                rc = self._lib.VixDiskLib_Connect(
                    ctypes.byref(params),
                    ctypes.byref(conn),
                )
                
        except Exception as e:
            dt = max(0.0, time.time() - t0)
            self.logger.error("VDDK: ConnectEx EXCEPTION dt=%.3fs: %s", dt, e)
            raise VDDKError(f"VixDiskLib_ConnectEx raised exception: {e}") from e

        dt = max(0.0, time.time() - t0)

        if rc != 0:
            msg = _err_text(self._lib, rc)
            self.logger.error(
                "VDDK: ConnectEx FAILED rc=%d dt=%.3fs msg=%s (host=%s port=%d transport=%s insecure=%s user=%s)",
                int(rc),
                dt,
                msg,
                s.host,
                int(s.port),
                transport,
                bool(s.insecure),
                s.user,
            )
            raise VDDKError(f"VixDiskLib_ConnectEx failed: {msg}")

        self._conn = conn
        self.logger.info(
            "VDDK: connected to ESXi %s:%d (transport=%s) (dt=%.3fs)",
            s.host,
            s.port,
            transport,
            dt,
        )

    def disconnect(self) -> None:
        if self._lib is None:
            return
        try:
            if self._conn:
                self.logger.debug("VDDK: disconnecting")
                self._lib.VixDiskLib_Disconnect(self._conn)
        except Exception as e:
            self.logger.debug("VDDK: disconnect error ignored: %s", e)
        finally:
            self._conn = _VixDiskLibConnection()
            self._connect_strings.clear()

    # Disk ops

    def _require_connected(self) -> None:
        if self._lib is None:
            raise VDDKError("VDDK library not loaded")
        if not self._conn:
            raise VDDKError("VDDK not connected (call connect())")

    def _open_ro(self, remote_vmdk: str) -> _VixDiskLibHandle:
        self._require_connected()
        assert self._lib is not None

        self.logger.debug("VDDK: Open RO %r", remote_vmdk)

        h = _VixDiskLibHandle()
        rc = self._lib.VixDiskLib_Open(
            self._conn,
            _as_cstr(remote_vmdk),
            ctypes.c_uint32(_VIXDISKLIB_FLAG_OPEN_READ_ONLY),
            ctypes.byref(h),
        )
        if rc != 0:
            msg = _err_text(self._lib, rc)
            self.logger.error("VDDK: Open FAILED rc=%d msg=%s path=%r", int(rc), msg, remote_vmdk)
            raise VDDKError(f"VixDiskLib_Open failed for {remote_vmdk!r}: {msg}")

        self.logger.debug("VDDK: Open OK handle=%r", h)
        return h

    def _close(self, h: _VixDiskLibHandle) -> None:
        assert self._lib is not None
        try:
            self._lib.VixDiskLib_Close(h)
        except Exception:
            pass

    def _capacity_sectors(self, h: _VixDiskLibHandle) -> int:
        self._require_connected()
        assert self._lib is not None

        self.logger.debug("VDDK: GetInfo(handle=%r)", h)

        info_p = ctypes.POINTER(_VixDiskLibInfo)()
        rc = self._lib.VixDiskLib_GetInfo(h, ctypes.byref(info_p))
        if rc != 0:
            msg = _err_text(self._lib, rc)
            self.logger.error("VDDK: GetInfo FAILED rc=%d msg=%s", int(rc), msg)
            raise VDDKError(f"VixDiskLib_GetInfo failed: {msg}")

        try:
            cap = int(info_p.contents.capacity)
            self.logger.debug(
                "VDDK: GetInfo OK capacity_sectors=%d (%.2f GiB)",
                cap,
                (cap * _SECTOR_SIZE) / (1024**3),
            )
            return cap
        finally:
            try:
                self._lib.VixDiskLib_FreeInfo(info_p)
            except Exception:
                pass

    def _read_with_retry(
        self,
        h: _VixDiskLibHandle,
        start_sector: int,
        num_sectors: int,
        buf_p: ctypes.c_void_p,
        *,
        max_retries: int,
        base_backoff_s: float,
        max_backoff_s: float,
        jitter_s: float,
        cancel: Optional[CancelFn],
    ) -> None:
        """Read sectors with retry/backoff on likely transient errors."""
        assert self._lib is not None

        attempt = 0
        while True:
            if cancel and cancel():
                raise VDDKCancelled("Download cancelled")

            rc = self._lib.VixDiskLib_Read(
                h,
                ctypes.c_uint64(int(start_sector)),
                ctypes.c_uint64(int(num_sectors)),
                buf_p,
            )
            if rc == 0:
                return

            msg = _err_text(self._lib, rc)
            transient = _is_likely_transient_error(msg)

            attempt += 1
            if (not transient) or attempt > max_retries:
                raise VDDKError(
                    f"VixDiskLib_Read failed at sector={start_sector} count={num_sectors} "
                    f"(attempt={attempt}/{max_retries}, transient={transient}): {msg}"
                )

            backoff = min(max_backoff_s, base_backoff_s * (2 ** (attempt - 1)))
            backoff += random.uniform(0.0, max(0.0, jitter_s))
            self.logger.warning(
                "VDDK: transient read error at sector=%d count=%d: %s (retry %d/%d in %.2fs)",
                start_sector,
                num_sectors,
                msg,
                attempt,
                max_retries,
                backoff,
            )
            time.sleep(backoff)

    def download_vmdk(
        self,
        remote_vmdk: str,
        local_path: Path,
        *,
        sectors_per_read: int = 2048,  # 1 MiB (2048 * 512)
        progress: Optional[ProgressFn] = None,
        progress_interval_s: float = 0.5,
        log_every_bytes: int = 256 * 1024 * 1024,
        resume: bool = True,
        durable: bool = False,
        allow_flat: bool = False,
        cancel: Optional[CancelFn] = None,
        max_read_retries: int = 6,
        base_backoff_s: float = 0.25,
        max_backoff_s: float = 8.0,
        jitter_s: float = 0.25,
        verify_size: bool = True,
        compute_sha256: bool = False,
    ) -> Path:
        """
        Stream a remote VMDK into a local file by reading sectors.

        remote_vmdk should typically be the descriptor:
          "[datastore] vm/vm.vmdk"

        local_path is written atomically: <name>.part then rename.
        """
        self._require_connected()
        assert self._lib is not None

        remote_vmdk = (remote_vmdk or "").strip()
        if not remote_vmdk:
            raise VDDKError("remote_vmdk is empty")

        if not _looks_like_datastore_path(remote_vmdk):
            self.logger.warning("VDDK: remote path doesn't look like datastore form: %r", remote_vmdk)

        if _is_flat_or_delta_vmdk(remote_vmdk) and not allow_flat:
            raise VDDKError(
                f"Refusing to open non-descriptor VMDK {remote_vmdk!r}. "
                "Pass the descriptor .vmdk (not -flat/-delta) or set allow_flat=True."
            )

        local_path = Path(local_path).expanduser().resolve()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = local_path.with_suffix(local_path.suffix + ".part")

        h: Optional[_VixDiskLibHandle] = None
        sha256 = hashlib.sha256() if compute_sha256 else None

        try:
            h = self._open_ro(remote_vmdk)

            cap_sectors = self._capacity_sectors(h)
            total_bytes = cap_sectors * _SECTOR_SIZE
            if cap_sectors <= 0:
                raise VDDKError(f"Invalid capacity from VDDK GetInfo: sectors={cap_sectors}")

            spr = max(1, int(sectors_per_read))
            buf = (ctypes.c_ubyte * (spr * _SECTOR_SIZE))()
            buf_p = ctypes.cast(buf, ctypes.c_void_p)

            # Resume logic
            done = 0
            sector = 0
            mode = "wb"

            if resume and tmp.exists():
                st = tmp.stat()
                if st.st_size > 0 and st.st_size % _SECTOR_SIZE == 0:
                    done = int(st.st_size)
                    sector = done // _SECTOR_SIZE
                    if sector > cap_sectors:
                        self.logger.warning("VDDK: existing .part > remote capacity; restarting: %s", tmp)
                        done = 0
                        sector = 0
                        mode = "wb"
                    else:
                        mode = "r+b"
                        self.logger.info(
                            "VDDK: resuming from %s (%.2f GiB, sector=%d/%d)",
                            tmp,
                            done / (1024**3),
                            sector,
                            cap_sectors,
                        )
                else:
                    self.logger.warning("VDDK: existing .part is not sector-aligned; restarting: %s", tmp)
                    try:
                        tmp.unlink()
                    except Exception:
                        pass
                    done = 0
                    sector = 0
                    mode = "wb"

            self.logger.info(
                "VDDK: download start: %s -> %s (sectors=%d, %.2f GiB)%s",
                remote_vmdk,
                local_path,
                cap_sectors,
                total_bytes / (1024**3),
                " [resume]" if sector else "",
            )

            start = time.time()
            last_log = done
            last_progress_ts = 0.0

            win_bytes = done
            win_ts = time.time()

            with open(tmp, mode) as f:
                if mode == "r+b":
                    f.seek(done, os.SEEK_SET)

                while sector < cap_sectors:
                    if cancel and cancel():
                        raise VDDKCancelled("Download cancelled")

                    n = min(spr, cap_sectors - sector)
                    chunk_bytes = int(n) * _SECTOR_SIZE

                    self._read_with_retry(
                        h,
                        start_sector=int(sector),
                        num_sectors=int(n),
                        buf_p=buf_p,
                        max_retries=int(max_read_retries),
                        base_backoff_s=float(base_backoff_s),
                        max_backoff_s=float(max_backoff_s),
                        jitter_s=float(jitter_s),
                        cancel=cancel,
                    )

                    mv = memoryview(buf)[:chunk_bytes]
                    f.write(mv)
                    if sha256 is not None:
                        sha256.update(mv)

                    sector += int(n)
                    done += chunk_bytes

                    if progress:
                        now = time.time()
                        if (now - last_progress_ts) >= max(0.05, float(progress_interval_s)) or done == total_bytes:
                            last_progress_ts = now
                            pct = (done / total_bytes * 100.0) if total_bytes else 0.0
                            progress(done, total_bytes, pct)

                    if log_every_bytes and (done - last_log) >= int(log_every_bytes):
                        last_log = done

                        now = time.time()
                        w_elapsed = max(0.001, now - win_ts)
                        w_bytes = max(0, done - win_bytes)
                        if w_elapsed >= 1.0:
                            win_ts = now
                            win_bytes = done
                        mib_s = (w_bytes / (1024**2)) / w_elapsed if w_elapsed else 0.0

                        remain = max(0, total_bytes - done)
                        eta_s = remain / (mib_s * (1024**2)) if mib_s > 0 else 0.0

                        self.logger.info(
                            "VDDK: progress %.1f%% (%.1f/%.1f MiB) speed=%.1f MiB/s eta=%s",
                            (done / total_bytes * 100.0) if total_bytes else 0.0,
                            done / (1024**2),
                            total_bytes / (1024**2),
                            mib_s,
                            _fmt_eta(eta_s),
                        )

                if durable:
                    try:
                        f.flush()
                        os.fsync(f.fileno())
                    except Exception as e:
                        self.logger.warning("VDDK: fsync failed (ignored): %s", e)

            if verify_size:
                try:
                    sz = tmp.stat().st_size
                    if sz != total_bytes:
                        raise VDDKError(f"Downloaded size mismatch for {tmp}: got={sz} expected={total_bytes}")
                except FileNotFoundError as e:
                    raise VDDKError(f"Temporary file missing after write: {tmp}") from e

            _atomic_write_replace(tmp, local_path)

            if durable:
                _fsync_dir(local_path.parent)

            if verify_size:
                try:
                    sz2 = local_path.stat().st_size
                    if sz2 != total_bytes:
                        raise VDDKError(f"Final size mismatch for {local_path}: got={sz2} expected={total_bytes}")
                except FileNotFoundError as e:
                    raise VDDKError(f"Final file missing after rename: {local_path}") from e

            elapsed = max(0.0, time.time() - start)
            mib_s_total = (done / (1024**2)) / elapsed if elapsed > 0 else 0.0

            if sha256 is not None:
                self.logger.info("VDDK: sha256 %s  %s", sha256.hexdigest(), local_path)

            self.logger.info(
                "VDDK: download done: %s (%.2f GiB, %.1f MiB/s)",
                local_path,
                done / (1024**3),
                mib_s_total,
            )
            return local_path

        except VDDKCancelled:
            self.logger.warning("VDDK: download cancelled; partial kept at %s", tmp)
            raise
        finally:
            if h:
                self._close(h)