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

Key features:
  ✅ ctypes wrapper around libvixDiskLib.so (no external python deps)
  ✅ global InitEx once + process-safe-ish lock
  ✅ ConnectEx/Open/Read/Close/Disconnect lifecycle
  ✅ capacity discovery via GetInfo
  ✅ atomic output: .part -> rename
  ✅ progress callback hook
  ✅ robust error text extraction via VixDiskLib_GetErrorText
  ✅ optional SHA1 thumbprint generation (TLS) using Python ssl socket

Caveats:
  - VDDK API is C; symbol availability can vary by VDDK version.
  - This module binds only the minimal symbols it uses.
  - For best reliability, open the *descriptor* VMDK (not -flat.vmdk).
"""

from __future__ import annotations

import ctypes
import hashlib
import logging
import os
import socket
import ssl
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union


# -----------------------------------------------------------------------------
# Errors
# -----------------------------------------------------------------------------

class VDDKError(RuntimeError):
    pass


# -----------------------------------------------------------------------------
# Constants / Types
# -----------------------------------------------------------------------------

_VIXDISKLIB_API_VERSION_MAJOR = 7
_VIXDISKLIB_API_VERSION_MINOR = 0

# Open flags (stable)
_VIXDISKLIB_FLAG_OPEN_READ_ONLY = 0x00000001

# VDDK types
_VixDiskLibConnection = ctypes.c_void_p
_VixDiskLibHandle = ctypes.c_void_p


# -----------------------------------------------------------------------------
# ctypes structures
# -----------------------------------------------------------------------------

class _VixDiskLibConnectParams(ctypes.Structure):
    """
    Minimal connect params struct.

    NOTE: Layout must match VDDK headers.

    Fields:
      - vmxSpec: optional "[ds] path/to/vm.vmx" (can be NULL for direct host)
      - serverName: ESXi host
      - thumbPrint: "aa:bb:..." SHA1
      - userName/password
      - port
    """
    _fields_ = [
        ("vmxSpec", ctypes.c_char_p),
        ("serverName", ctypes.c_char_p),
        ("thumbPrint", ctypes.c_char_p),
        ("userName", ctypes.c_char_p),
        ("password", ctypes.c_char_p),
        ("port", ctypes.c_uint32),
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
        ("capacity", ctypes.c_uint64),   # sectors
        # The struct contains more fields (geometry, adapterType, etc.)
        # We do not access them, so we stop here.
    ]


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

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
    return ":".join(raw[i:i+2] for i in range(0, 40, 2))


def compute_server_thumbprint_sha1(host: str, port: int = 443, timeout: float = 10.0) -> str:
    """
    Fetches server certificate (DER) and returns SHA1 thumbprint in colon-separated form.
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with socket.create_connection((host, port), timeout=timeout) as sock:
        with ctx.wrap_socket(sock, server_hostname=host) as ssock:
            der = ssock.getpeercert(binary_form=True)
    sha1 = hashlib.sha1(der).hexdigest()
    return ":".join(sha1[i:i+2] for i in range(0, 40, 2))


def _atomic_write_replace(tmp_path: Path, final_path: Path) -> None:
    """
    Atomic-ish replace on POSIX.
    """
    os.replace(str(tmp_path), str(final_path))


# -----------------------------------------------------------------------------
# Dynamic loading & symbol binding
# -----------------------------------------------------------------------------

def _candidate_lib_names() -> Tuple[str, ...]:
    return (
        "libvixDiskLib.so",
        "libvixDiskLib.so.7",
        "libvixDiskLib.so.6",
        "libvixDiskLib.so.5",
    )


def _load_vddk_cdll(vddk_libdir: Optional[Path]) -> ctypes.CDLL:
    """
    Load libvixDiskLib.so. If vddk_libdir is provided, try it first.
    Uses RTLD_GLOBAL to help resolve transitive deps.
    """
    last: Optional[Exception] = None

    if vddk_libdir:
        p = Path(vddk_libdir).expanduser().resolve()
        # Help the loader for dependent libs.
        os.environ["LD_LIBRARY_PATH"] = f"{str(p)}:{os.environ.get('LD_LIBRARY_PATH','')}".rstrip(":")
        for n in _candidate_lib_names():
            cand = p / n
            if cand.exists():
                try:
                    return ctypes.CDLL(str(cand), mode=ctypes.RTLD_GLOBAL)
                except Exception as e:
                    last = e

    # Fallback to loader path
    for n in _candidate_lib_names():
        try:
            return ctypes.CDLL(n, mode=ctypes.RTLD_GLOBAL)
        except Exception as e:
            last = e

    raise VDDKError(
        "Failed to load VDDK library (libvixDiskLib.so). "
        "Provide vddk_libdir pointing to directory containing libvixDiskLib.so "
        "or set LD_LIBRARY_PATH. "
        f"Last error: {last}"
    )


def _bind_symbols(lib: ctypes.CDLL) -> None:
    """
    Bind minimal VDDK symbols used by this module.
    """
    # int VixDiskLib_InitEx(uint32 major, uint32 minor,
    #   logFunc, warnFunc, panicFunc, const char *libDir, const char *configFile);
    lib.VixDiskLib_InitEx.argtypes = [
        ctypes.c_uint32, ctypes.c_uint32,
        ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
        ctypes.c_char_p, ctypes.c_char_p,
    ]
    lib.VixDiskLib_InitEx.restype = ctypes.c_int

    lib.VixDiskLib_Exit.argtypes = []
    lib.VixDiskLib_Exit.restype = None

    # int VixDiskLib_ConnectEx(const VixDiskLibConnectParams *params,
    #   const char *identity, const char *snapshotRef, const char *transportModes,
    #   VixDiskLibConnection *connection);
    lib.VixDiskLib_ConnectEx.argtypes = [
        ctypes.POINTER(_VixDiskLibConnectParams),
        ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
        ctypes.POINTER(_VixDiskLibConnection),
    ]
    lib.VixDiskLib_ConnectEx.restype = ctypes.c_int

    lib.VixDiskLib_Disconnect.argtypes = [_VixDiskLibConnection]
    lib.VixDiskLib_Disconnect.restype = None

    # int VixDiskLib_Open(VixDiskLibConnection, const char *path, uint32 flags, VixDiskLibHandle *handle);
    lib.VixDiskLib_Open.argtypes = [
        _VixDiskLibConnection,
        ctypes.c_char_p,
        ctypes.c_uint32,
        ctypes.POINTER(_VixDiskLibHandle),
    ]
    lib.VixDiskLib_Open.restype = ctypes.c_int

    lib.VixDiskLib_Close.argtypes = [_VixDiskLibHandle]
    lib.VixDiskLib_Close.restype = None

    # int VixDiskLib_GetInfo(VixDiskLibHandle, VixDiskLibInfo **info);
    lib.VixDiskLib_GetInfo.argtypes = [
        _VixDiskLibHandle,
        ctypes.POINTER(ctypes.POINTER(_VixDiskLibInfo)),
    ]
    lib.VixDiskLib_GetInfo.restype = ctypes.c_int

    # void VixDiskLib_FreeInfo(VixDiskLibInfo *info);
    lib.VixDiskLib_FreeInfo.argtypes = [ctypes.c_void_p]
    lib.VixDiskLib_FreeInfo.restype = None

    # int VixDiskLib_Read(VixDiskLibHandle, uint64 startSector, uint64 numSectors, uint8 *buf);
    lib.VixDiskLib_Read.argtypes = [
        _VixDiskLibHandle,
        ctypes.c_uint64, ctypes.c_uint64,
        ctypes.c_void_p,
    ]
    lib.VixDiskLib_Read.restype = ctypes.c_int

    # const char *VixDiskLib_GetErrorText(int err, const char *locale);
    lib.VixDiskLib_GetErrorText.argtypes = [ctypes.c_int, ctypes.c_char_p]
    lib.VixDiskLib_GetErrorText.restype = ctypes.c_char_p

    # void VixDiskLib_FreeErrorText(char *text);
    lib.VixDiskLib_FreeErrorText.argtypes = [ctypes.c_void_p]
    lib.VixDiskLib_FreeErrorText.restype = None


def _err_text(lib: ctypes.CDLL, rc: int) -> str:
    """
    Best-effort conversion of VDDK error codes to text.
    """
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


# -----------------------------------------------------------------------------
# Global init (InitEx once)
# -----------------------------------------------------------------------------

_vddk_lock = threading.Lock()
_vddk_inited = False


def vddk_init_once(logger: logging.Logger, lib: ctypes.CDLL, *, vddk_libdir: Optional[Path]) -> None:
    """
    Initialize VDDK once per process. Thread-safe.
    """
    global _vddk_inited
    with _vddk_lock:
        if _vddk_inited:
            return
        libdir_c = _as_cstr(str(Path(vddk_libdir).expanduser().resolve())) if vddk_libdir else None
        rc = lib.VixDiskLib_InitEx(
            _VIXDISKLIB_API_VERSION_MAJOR,
            _VIXDISKLIB_API_VERSION_MINOR,
            None, None, None,
            libdir_c,
            None,
        )
        if rc != 0:
            raise VDDKError(f"VixDiskLib_InitEx failed: {_err_text(lib, rc)}")
        _vddk_inited = True
        logger.debug("VDDK InitEx OK")


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------

ProgressFn = Callable[[int, int, float], None]
# args: bytes_done, bytes_total, percent


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


class VDDKESXClient:
    """
    Minimal ESXi VDDK reader.

    Lifecycle:
      - connect()
      - download_vmdk(...)
      - disconnect()

    The same instance can download multiple VMDKs over one connection.
    """

    def __init__(self, logger: logging.Logger, spec: VDDKConnectionSpec):
        self.logger = logger
        self.spec = spec

        self._lib: Optional[ctypes.CDLL] = None
        self._conn: _VixDiskLibConnection = _VixDiskLibConnection()

    # ---------------------------
    # Setup / Connect
    # ---------------------------

    def _ensure_loaded(self) -> None:
        if self._lib is not None:
            return
        s = self.spec
        if not (s.host and s.user and s.password):
            raise VDDKError("Missing VDDK connection details (host/user/password)")
        lib = _load_vddk_cdll(s.vddk_libdir)
        _bind_symbols(lib)
        vddk_init_once(self.logger, lib, vddk_libdir=s.vddk_libdir)
        self._lib = lib

    def connect(self) -> None:
        self._ensure_loaded()
        assert self._lib is not None

        s = self.spec

        tp = (s.thumbprint or "").strip()
        if not tp and not s.insecure:
            self.logger.info("VDDK: computing SHA1 thumbprint for %s:%d", s.host, s.port)
            tp = compute_server_thumbprint_sha1(s.host, s.port, timeout=float(s.tls_thumbprint_timeout))
        if tp:
            tp = normalize_thumbprint(tp)

        if (not tp) and (not s.insecure):
            raise VDDKError("Thumbprint is required unless insecure=True")

        params = _VixDiskLibConnectParams(
            vmxSpec=None,
            serverName=_as_cstr(s.host),
            thumbPrint=_as_cstr(tp) if tp else None,
            userName=_as_cstr(s.user),
            password=_as_cstr(s.password),
            port=ctypes.c_uint32(int(s.port)),
        )

        conn = _VixDiskLibConnection()
        rc = self._lib.VixDiskLib_ConnectEx(
            ctypes.byref(params),
            None,  # identity
            None,  # snapshotRef
            _as_cstr(s.transport_modes) if s.transport_modes else None,
            ctypes.byref(conn),
        )
        if rc != 0:
            raise VDDKError(f"VixDiskLib_ConnectEx failed: {_err_text(self._lib, rc)}")

        self._conn = conn
        self.logger.info("VDDK: connected to ESXi %s:%d", s.host, s.port)

    def disconnect(self) -> None:
        if self._lib is None:
            return
        try:
            if self._conn:
                self._lib.VixDiskLib_Disconnect(self._conn)
        except Exception as e:
            self.logger.debug("VDDK: disconnect error ignored: %s", e)
        finally:
            self._conn = _VixDiskLibConnection()

    # ---------------------------
    # Disk ops
    # ---------------------------

    def _require_connected(self) -> None:
        if self._lib is None:
            raise VDDKError("VDDK library not loaded")
        if not self._conn:
            raise VDDKError("VDDK not connected (call connect())")

    def _open_ro(self, remote_vmdk: str) -> _VixDiskLibHandle:
        self._require_connected()
        assert self._lib is not None
        h = _VixDiskLibHandle()
        rc = self._lib.VixDiskLib_Open(
            self._conn,
            _as_cstr(remote_vmdk),
            ctypes.c_uint32(_VIXDISKLIB_FLAG_OPEN_READ_ONLY),
            ctypes.byref(h),
        )
        if rc != 0:
            raise VDDKError(f"VixDiskLib_Open failed for {remote_vmdk!r}: {_err_text(self._lib, rc)}")
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
        info_p = ctypes.POINTER(_VixDiskLibInfo)()
        rc = self._lib.VixDiskLib_GetInfo(h, ctypes.byref(info_p))
        if rc != 0:
            raise VDDKError(f"VixDiskLib_GetInfo failed: {_err_text(self._lib, rc)}")
        try:
            return int(info_p.contents.capacity)
        finally:
            try:
                self._lib.VixDiskLib_FreeInfo(info_p)
            except Exception:
                pass

    def download_vmdk(
        self,
        remote_vmdk: str,
        local_path: Path,
        *,
        sectors_per_read: int = 2048,   # 1 MiB (2048 * 512)
        progress: Optional[ProgressFn] = None,
        log_every_bytes: int = 256 * 1024 * 1024,
    ) -> Path:
        """
        Stream a remote VMDK into a local file by reading sectors.

        remote_vmdk should typically be the descriptor:
          "[datastore] vm/vm.vmdk"

        local_path is written atomically: <name>.part then rename.
        """
        self._require_connected()
        assert self._lib is not None

        local_path = Path(local_path).expanduser().resolve()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = local_path.with_suffix(local_path.suffix + ".part")

        h = self._open_ro(remote_vmdk)
        try:
            cap_sectors = self._capacity_sectors(h)
            total_bytes = cap_sectors * 512

            self.logger.info(
                "VDDK: download start: %s -> %s (sectors=%d, %.2f GiB)",
                remote_vmdk,
                local_path,
                cap_sectors,
                total_bytes / (1024**3),
            )

            spr = max(1, int(sectors_per_read))
            buf = (ctypes.c_ubyte * (spr * 512))()

            done = 0
            last_log = 0
            start = time.time()

            with open(tmp, "wb") as f:
                sector = 0
                while sector < cap_sectors:
                    n = min(spr, cap_sectors - sector)

                    rc = self._lib.VixDiskLib_Read(
                        h,
                        ctypes.c_uint64(sector),
                        ctypes.c_uint64(n),
                        ctypes.byref(buf),
                    )
                    if rc != 0:
                        raise VDDKError(
                            f"VixDiskLib_Read failed at sector={sector} count={n}: {_err_text(self._lib, rc)}"
                        )

                    chunk_bytes = int(n) * 512
                    f.write(bytes(buf[:chunk_bytes]))
                    sector += int(n)
                    done += chunk_bytes

                    if progress:
                        pct = (done / total_bytes * 100.0) if total_bytes else 0.0
                        progress(done, total_bytes, pct)

                    if log_every_bytes and (done - last_log) >= int(log_every_bytes):
                        last_log = done
                        elapsed = max(0.001, time.time() - start)
                        mib_s = (done / (1024**2)) / elapsed
                        self.logger.info(
                            "VDDK: progress %.1f%% (%.1f/%.1f MiB) speed=%.1f MiB/s",
                            (done / total_bytes * 100.0) if total_bytes else 0.0,
                            done / (1024**2),
                            total_bytes / (1024**2),
                            mib_s,
                        )

            _atomic_write_replace(tmp, local_path)

            elapsed = max(0.001, time.time() - start)
            mib_s = (done / (1024**2)) / elapsed
            self.logger.info(
                "VDDK: download done: %s (%.2f GiB, %.1f MiB/s)",
                local_path,
                done / (1024**3),
                mib_s,
            )
            return local_path
        finally:
            self._close(h)
            if tmp.exists():
                try:
                    tmp.unlink()
                except Exception:
                    pass
