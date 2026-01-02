# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import concurrent.futures
import fnmatch
import hashlib
import logging
import os
import re
import shlex
import shutil
import socket
import ssl
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union
from urllib.parse import quote

# ---------------------------------------------------------------------------
# Errors / creds resolver (robust import fallbacks)
# ---------------------------------------------------------------------------

try:
    # Your repo says: from ..core.exceptions import VMwareError
    from ..core.exceptions import VMwareError  # type: ignore
except Exception:  # pragma: no cover

    class VMwareError(RuntimeError):
        pass


# ✅ shared credential resolver (supports vs_password_env + vc_password_env)
# NOTE: if your file is core/creds.py, change to: from ..core.creds import resolve_vsphere_creds
try:
    from ..core.cred import resolve_vsphere_creds  # type: ignore
except Exception:  # pragma: no cover
    try:
        from ..core.creds import resolve_vsphere_creds  # type: ignore
    except Exception:  # pragma: no cover
        resolve_vsphere_creds = None  # type: ignore


# Optional: vSphere / vCenter integration (pyvmomi)
try:
    from pyVim.connect import Disconnect, SmartConnect  # type: ignore
    from pyVmomi import vim, vmodl  # type: ignore

    PYVMOMI_AVAILABLE = True
except Exception:  # pragma: no cover
    SmartConnect = None  # type: ignore
    Disconnect = None  # type: ignore
    vim = None  # type: ignore
    vmodl = None  # type: ignore
    PYVMOMI_AVAILABLE = False


# Optional: HTTP download (requests)
try:
    import requests  # type: ignore

    REQUESTS_AVAILABLE = True
except Exception:  # pragma: no cover
    requests = None  # type: ignore
    REQUESTS_AVAILABLE = False


# Optional: Async HTTP download (aiohttp + aiofiles)
try:
    import aiohttp  # type: ignore

    AIOHTTP_AVAILABLE = True
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore
    AIOHTTP_AVAILABLE = False

try:
    import aiofiles  # type: ignore

    AIOFILES_AVAILABLE = True
except Exception:  # pragma: no cover
    aiofiles = None  # type: ignore
    AIOFILES_AVAILABLE = False


# ✅ NEW: raw VDDK downloader (data-plane)
try:
    from .vddk_client import VDDKConnectionSpec, VDDKESXClient  # type: ignore

    VDDK_CLIENT_AVAILABLE = True
except Exception:  # pragma: no cover
    VDDKESXClient = None  # type: ignore
    VDDKConnectionSpec = None  # type: ignore
    VDDK_CLIENT_AVAILABLE = False


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------

_BACKING_RE = re.compile(r"\[(.+?)\]\s+(.*)")
_SHA1_40_RE = re.compile(r"^[0-9a-f]{40}$")


def _run_coro_sync(coro: "asyncio.Future[Any] | asyncio.coroutines.Coroutine[Any, Any, Any]") -> Any:
    """
    Run an async coroutine from sync code safely.

    - If no event-loop is running in this thread: asyncio.run(coro)
    - If an event-loop is already running: run asyncio.run(coro) in a worker thread.
      (prevents: RuntimeError: asyncio.run() cannot be called from a running event loop)
    """
    try:
        loop = asyncio.get_running_loop()
        running = loop.is_running()
    except RuntimeError:
        running = False

    if not running:
        return asyncio.run(coro)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(asyncio.run, coro)
        return fut.result()


# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class V2VExportOptions:
    """
    Export / download options.

    Three modes:
      1) export_mode="v2v" (default):
         - Uses virt-v2v (VDDK or SSH) which *may* inspect and modify the guest as part of conversion.
         - Writes local output (qcow2/raw) under output_dir.
      2) export_mode="download_only":
         - Uses pyvmomi control-plane ONLY + HTTPS /folder downloads (session cookie)
         - Downloads VM directory files from datastore (VMDKs, VMX, NVRAM, logs, snapshots, etc.)
         - NO guest inspection, NO virt-v2v, NO libguestfs mutation.
      3) export_mode="vddk_download":
         - Uses pyvmomi control-plane to locate ESXi host + disk backing path
         - Uses a VDDK data-plane client to download one disk (remote VMDK) to local storage
         - NO guest inspection, NO virt-v2v

    IMPORTANT defaults:
      - datacenter defaults to "auto" (never hardcode ha-datacenter for vCenter)
      - compute defaults to "auto" and we resolve a HOST SYSTEM path:
          host/<cluster-or-compute>/<esx-host>
        because libvirt ESX driver rejects cluster-only paths:
          "Path ... does not specify a host system"
    """

    vm_name: str

    # ✅ NEW: knob
    export_mode: str = "v2v"  # "v2v" | "download_only" | "vddk_download"
    datacenter: str = "auto"  # "auto" => resolve from VM parent chain
    compute: str = "auto"  # "auto" => resolve to host/<compute>/<host>

    # v2v (virt-v2v) options
    transport: str = "vddk"  # "vddk" | "ssh"
    no_verify: bool = False
    vddk_libdir: Optional[Path] = None
    vddk_thumbprint: Optional[str] = None
    vddk_snapshot_moref: Optional[str] = None
    vddk_transports: Optional[str] = None

    output_dir: Path = Path("./out")
    output_format: str = "qcow2"  # qcow2|raw
    extra_args: Tuple[str, ...] = ()

    # ✅ Optional: if non-empty, inventory gets printed (otherwise: FAST, no scan)
    print_vm_names: Tuple[str, ...] = ()
    vm_list_limit: int = 120
    vm_list_columns: int = 3
    prefer_cached_vm_lookup: bool = False

    # ------------------------------------------------------------------
    # ✅ NEW: download-only options (NO guest inspection)
    # ------------------------------------------------------------------
    download_only_include_globs: Tuple[str, ...] = ("*",)
    download_only_exclude_globs: Tuple[str, ...] = (
        "*.lck",
        "*.log",
        "*.scoreboard",
        "*.vswp",
        "*.vmem",
        "*.vmsn",
        "*.nvram~",
        "*.tmp",
    )
    download_only_max_files: int = 5000
    download_only_concurrency: int = 4
    download_only_use_async_http: bool = True  # if aiohttp/aiofiles available
    download_only_fail_on_missing: bool = False  # if True, missing file is fatal

    # ------------------------------------------------------------------
    # ✅ NEW: vddk_download options (single-disk raw pull via VDDK client)
    # ------------------------------------------------------------------
    vddk_download_disk: Optional[str] = None
    """
    Disk selector for vddk_download:
      - None => first disk
      - "0", "1", ...
      - or a substring of VirtualDisk label (deviceInfo.label)
    """

    vddk_download_output: Optional[Path] = None
    """
    Local file path for downloaded disk.
    If None, defaults to: <output_dir>/<vm_name>-disk0.vmdk
    """

    vddk_download_sectors_per_read: int = 2048  # 1 MiB chunks (2048 * 512)
    vddk_download_log_every_bytes: int = 256 * 1024 * 1024


# ---------------------------------------------------------------------------
# VMware Client
# ---------------------------------------------------------------------------


class VMwareClient:
    """
    Minimal vSphere/vCenter client:
      - pyvmomi control-plane (inventory, compute path, snapshots)
      - HTTPS /folder downloads via session cookie (requests or aiohttp)
      - virt-v2v command builder + runner
      - ✅ NEW: VDDK raw disk downloader (optional)

    Key fixes:
      ✅ compute path is host-system path (host/<cluster>/<esx>) not cluster-only
      ✅ vddk-libdir validated/auto-resolved to directory containing libvixDiskLib.so
      ✅ inventory printing is OPT-IN (otherwise no expensive scans)
      ✅ avoids repeated CreateContainerView where possible via simple caches
      ✅ NEW: download-only mode (no guest inspection; datastore folder download)
      ✅ NEW: vddk_download mode (single disk direct VDDK pull)
    """

    def __init__(
        self,
        logger: logging.Logger,
        host: str,
        user: str,
        password: str,
        *,
        port: int = 443,
        insecure: bool = False,
        timeout: Optional[float] = None,
    ) -> None:
        self.logger = logger
        self.host = host
        self.user = user
        self.password = password
        self.port = int(port)
        self.insecure = bool(insecure)
        self.timeout = timeout

        self.si: Any = None

        # caches
        self._dc_cache: Optional[List[Any]] = None
        self._dc_name_cache: Optional[List[str]] = None
        self._host_name_cache: Optional[List[str]] = None

        # optional acceleration caches
        self._vm_name_cache: Optional[List[str]] = None
        self._vm_obj_by_name_cache: Dict[str, Any] = {}

    # ---------------------------------------------------------------------
    # build from config using shared resolver (vs_* + vc_* + *_env)
    # ---------------------------------------------------------------------

    @classmethod
    def from_config(
        cls,
        logger: logging.Logger,
        cfg: Dict[str, Any],
        *,
        port: Optional[int] = None,
        insecure: Optional[bool] = None,
        timeout: Optional[float] = None,
    ) -> "VMwareClient":
        if resolve_vsphere_creds is None:
            raise VMwareError(
                "resolve_vsphere_creds not importable. "
                "Fix import: from ..core.cred(s) import resolve_vsphere_creds"
            )

        creds = resolve_vsphere_creds(cfg)
        p = int(port if port is not None else (cfg.get("vc_port") or cfg.get("vs_port") or 443))
        ins = bool(
            insecure
            if insecure is not None
            else (
                cfg.get("vc_insecure")
                if cfg.get("vc_insecure") is not None
                else cfg.get("vs_insecure", False)
            )
        )

        c = cls(logger, creds.host, creds.user, creds.password, port=p, insecure=ins, timeout=timeout)
        if isinstance(c.password, str):
            c.password = c.password.strip()
        return c

    def has_creds(self) -> bool:
        return bool((self.host or "").strip() and (self.user or "").strip() and (self.password or "").strip())

    # ---------------------------
    # Context managers
    # ---------------------------

    def __enter__(self) -> "VMwareClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        try:
            self.disconnect()
        finally:
            if exc_type is not None:
                self.logger.error("Exception in context: %s: %s", getattr(exc_type, "__name__", exc_type), exc_val)
        return False

    async def __aenter__(self) -> "VMwareClient":
        await self.async_connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        try:
            await self.async_disconnect()
        finally:
            if exc_type is not None:
                self.logger.error(
                    "Exception in async context: %s: %s", getattr(exc_type, "__name__", exc_type), exc_val
                )
        return False

    # ---------------------------
    # Connect / Disconnect
    # ---------------------------

    def _require_pyvmomi(self) -> None:
        if not PYVMOMI_AVAILABLE:
            raise VMwareError("pyvmomi not installed. Install: pip install pyvmomi")

    def _ssl_context(self) -> ssl.SSLContext:
        if self.insecure:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            return ctx
        return ssl.create_default_context()

    def connect(self) -> None:
        self._require_pyvmomi()
        ctx = self._ssl_context()

        try:
            if self.timeout is not None:
                old_timeout = socket.getdefaulttimeout()
                socket.setdefaulttimeout(self.timeout)
                try:
                    self.si = SmartConnect(  # type: ignore[misc]
                        host=self.host,
                        user=self.user,
                        pwd=self.password,
                        port=self.port,
                        sslContext=ctx,
                    )
                finally:
                    socket.setdefaulttimeout(old_timeout)
            else:
                self.si = SmartConnect(  # type: ignore[misc]
                    host=self.host,
                    user=self.user,
                    pwd=self.password,
                    port=self.port,
                    sslContext=ctx,
                )

            # warm caches (best-effort, small)
            try:
                self._refresh_datacenter_cache()
            except Exception as e:
                self.logger.debug("Datacenter cache warmup failed (non-fatal): %s", e)
            try:
                self._refresh_host_cache()
            except Exception as e:
                self.logger.debug("Host cache warmup failed (non-fatal): %s", e)

            self.logger.info("Connected to vSphere: %s:%s", self.host, self.port)
        except Exception as e:
            self.si = None
            raise VMwareError(f"Failed to connect to vSphere: {e}")

    async def async_connect(self) -> None:
        self._require_pyvmomi()
        ctx = self._ssl_context()

        def _do_connect() -> Any:
            if self.timeout is not None:
                old_timeout = socket.getdefaulttimeout()
                socket.setdefaulttimeout(self.timeout)
                try:
                    return SmartConnect(  # type: ignore[misc]
                        host=self.host,
                        user=self.user,
                        pwd=self.password,
                        port=self.port,
                        sslContext=ctx,
                    )
                finally:
                    socket.setdefaulttimeout(old_timeout)

            return SmartConnect(  # type: ignore[misc]
                host=self.host,
                user=self.user,
                pwd=self.password,
                port=self.port,
                sslContext=ctx,
            )

        try:
            self.si = await asyncio.to_thread(_do_connect)

            try:
                await asyncio.to_thread(self._refresh_datacenter_cache)
            except Exception as e:
                self.logger.debug("Datacenter cache warmup failed (non-fatal): %s", e)

            try:
                await asyncio.to_thread(self._refresh_host_cache)
            except Exception as e:
                self.logger.debug("Host cache warmup failed (non-fatal): %s", e)

            self.logger.info("Connected to vSphere: %s:%s", self.host, self.port)
        except Exception as e:
            self.si = None
            raise VMwareError(f"Failed to connect to vSphere: {e}")

    def disconnect(self) -> None:
        try:
            if self.si is not None:
                Disconnect(self.si)  # type: ignore[misc]
        except Exception as e:
            self.logger.error("Error during disconnect: %s", e)
        finally:
            self.si = None
            self._dc_cache = None
            self._dc_name_cache = None
            self._host_name_cache = None
            self._vm_name_cache = None
            self._vm_obj_by_name_cache = {}

    async def async_disconnect(self) -> None:
        try:
            if self.si is not None:
                await asyncio.to_thread(Disconnect, self.si)  # type: ignore[misc]
        except Exception as e:
            self.logger.error("Error during async disconnect: %s", e)
        finally:
            self.si = None
            self._dc_cache = None
            self._dc_name_cache = None
            self._host_name_cache = None
            self._vm_name_cache = None
            self._vm_obj_by_name_cache = {}

    # ---------------------------
    # Inventory helpers
    # ---------------------------

    def _content(self) -> Any:
        if not self.si:
            raise VMwareError("Not connected")
        try:
            return self.si.RetrieveContent()
        except Exception as e:
            raise VMwareError(f"Failed to retrieve content: {e}")

    # ---------------------------------------------------------------------
    # Datacenter helpers
    # ---------------------------------------------------------------------

    def _refresh_datacenter_cache(self) -> None:
        self._require_pyvmomi()
        content = self._content()
        view = content.viewManager.CreateContainerView(  # type: ignore[attr-defined]
            content.rootFolder, [vim.Datacenter], True
        )
        try:
            dcs = list(view.view)
            names = sorted([str(getattr(dc, "name", "")) for dc in dcs if getattr(dc, "name", None)])
            self._dc_cache = dcs
            self._dc_name_cache = names
        finally:
            try:
                view.Destroy()
            except Exception:
                pass

    def list_datacenters(self, *, refresh: bool = False) -> List[str]:
        if refresh or self._dc_name_cache is None:
            self._refresh_datacenter_cache()
        return list(self._dc_name_cache or [])

    def get_datacenter_by_name(self, name: str, *, refresh: bool = False) -> Any:
        if refresh or self._dc_cache is None:
            self._refresh_datacenter_cache()
        target = (name or "").strip()
        for dc in (self._dc_cache or []):
            if str(getattr(dc, "name", "")).strip() == target:
                return dc
        return None

    def datacenter_exists(self, name: str, *, refresh: bool = False) -> bool:
        n = (name or "").strip()
        if not n:
            return False
        return self.get_datacenter_by_name(n, refresh=refresh) is not None

    def vm_to_datacenter(self, vm_obj: Any) -> Any:
        self._require_pyvmomi()
        obj = vm_obj
        for _ in range(0, 64):
            if obj is None:
                break
            if isinstance(obj, vim.Datacenter):  # type: ignore[attr-defined]
                return obj
            obj = getattr(obj, "parent", None)
        return None

    def vm_datacenter_name(self, vm_obj: Any) -> Optional[str]:
        dc = self.vm_to_datacenter(vm_obj)
        if dc is None:
            return None
        name = getattr(dc, "name", None)
        return str(name) if name else None

    def guess_default_datacenter(self) -> str:
        dcs = self.list_datacenters(refresh=False)
        if len(dcs) == 1:
            return dcs[0]
        for cand in ("ha-datacenter", "Ha-Datacenter", "HA-Datacenter"):
            if cand in dcs:
                return cand
        if dcs:
            return sorted(dcs)[0]
        raise VMwareError("No datacenters found in inventory (unexpected).")

    # ✅ DC resolution (authoritative: VM parent walk)
    def resolve_datacenter_for_vm(self, vm_name: str, preferred: Optional[str]) -> str:
        pref = (preferred or "").strip()
        if pref and pref.lower() not in ("auto", "detect", "guess") and self.datacenter_exists(pref, refresh=False):
            return pref

        vm_obj = self.get_vm_by_name(vm_name)
        vm_dc = self.vm_datacenter_name(vm_obj) if vm_obj is not None else None
        if vm_dc and self.datacenter_exists(vm_dc, refresh=False):
            return vm_dc

        # refresh caches and retry
        self._refresh_datacenter_cache()

        if pref and pref.lower() not in ("auto", "detect", "guess") and self.datacenter_exists(pref, refresh=False):
            return pref

        if vm_obj is not None:
            vm_dc = self.vm_datacenter_name(vm_obj)
            if vm_dc and self.datacenter_exists(vm_dc, refresh=False):
                return vm_dc

        dcs = self.list_datacenters(refresh=False)
        if len(dcs) == 1:
            return dcs[0]

        raise VMwareError(
            f"Could not resolve datacenter for VM={vm_name!r}. Preferred={pref!r}, VM_dc={vm_dc!r}. "
            f"Available datacenters: {dcs}"
        )

    # ---------------------------------------------------------------------
    # Host helpers (FIXES compute path for libvirt ESX)
    # ---------------------------------------------------------------------

    def _refresh_host_cache(self) -> None:
        self._require_pyvmomi()
        content = self._content()
        view = content.viewManager.CreateContainerView(  # type: ignore[attr-defined]
            content.rootFolder, [vim.HostSystem], True
        )
        try:
            self._host_name_cache = sorted(
                [str(getattr(h, "name", "")) for h in view.view if getattr(h, "name", None)]
            )
        finally:
            try:
                view.Destroy()
            except Exception:
                pass

    def list_host_names(self, *, refresh: bool = False) -> List[str]:
        if refresh or self._host_name_cache is None:
            self._refresh_host_cache()
        return list(self._host_name_cache or [])

    # ---------------------------------------------------------------------
    # VM lookup (cached optional)
    # ---------------------------------------------------------------------

    def get_vm_by_name(self, name: str) -> Any:
        self._require_pyvmomi()
        n = (name or "").strip()
        if not n:
            return None

        if n in self._vm_obj_by_name_cache:
            return self._vm_obj_by_name_cache[n]

        content = self._content()
        view = content.viewManager.CreateContainerView(  # type: ignore[attr-defined]
            content.rootFolder, [vim.VirtualMachine], True
        )
        try:
            for vm_obj in view.view:
                if getattr(vm_obj, "name", None) == n:
                    self._vm_obj_by_name_cache[n] = vm_obj
                    return vm_obj
            return None
        finally:
            try:
                view.Destroy()
            except Exception:
                pass

    def list_vm_names(self) -> List[str]:
        self._require_pyvmomi()
        content = self._content()
        view = content.viewManager.CreateContainerView(  # type: ignore[attr-defined]
            content.rootFolder, [vim.VirtualMachine], True
        )
        try:
            names = sorted([str(vm_obj.name) for vm_obj in view.view if getattr(vm_obj, "name", None)])
            self._vm_name_cache = names
            return names
        finally:
            try:
                view.Destroy()
            except Exception:
                pass

    def _vm_runtime_host(self, vm_obj: Any) -> Any:
        rt = getattr(vm_obj, "runtime", None)
        return getattr(rt, "host", None) if rt else None

    def _host_parent_compute_name(self, host_obj: Any) -> Optional[str]:
        """
        HostSystem.parent is usually a ComputeResource or ClusterComputeResource.
        For cluster hosts, libvirt often expects: host/<ClusterName>/<HostName>
        """
        try:
            parent = getattr(host_obj, "parent", None)
            if parent is None:
                return None
            name = getattr(parent, "name", None)
            if name:
                return str(name).strip()
        except Exception:
            return None
        return None

    def resolve_host_system_for_vm(self, vm_name: str) -> str:
        """
        ✅ FIX:
          Prefer *host system path* (not cluster-only).
          Returns one of:
            - host/<ClusterName>/<HostName>
            - host/<ComputeResourceName>/<HostName>
            - host/<HostName> (fallback)
        """
        vm_obj = self.get_vm_by_name(vm_name)
        if vm_obj is None:
            raise VMwareError(f"VM not found: {vm_name!r}")

        host_obj = self._vm_runtime_host(vm_obj)
        if host_obj is None:
            raise VMwareError(
                f"VM {vm_name!r} has no runtime.host; cannot build vpx compute path. "
                f"Specify opt.compute='host/<cluster>/<host>' or opt.compute='host/<host>'. "
                f"Known hosts: {self.list_host_names(refresh=True)}"
            )

        host_name = str(getattr(host_obj, "name", "") or "").strip()
        if not host_name:
            raise VMwareError(
                f"Could not resolve ESXi host name for VM={vm_name!r}. Known hosts: {self.list_host_names(refresh=True)}"
            )

        cr_name = self._host_parent_compute_name(host_obj)
        if cr_name and cr_name.lower() != host_name.lower():
            return f"host/{cr_name}/{host_name}"

        return f"host/{host_name}"

    def resolve_compute_for_vm(self, vm_name: str, preferred: Optional[str]) -> str:
        """
        Normalize compute:
          - "auto" => host/<cr>/<host> (preferred)
          - "<host>" => host/<host>
          - "host/<x>" => host/<x>
          - "/host/<x>" => host/<x>
        """
        pref = (preferred or "").strip()
        if not pref or pref.lower() in ("auto", "detect", "guess"):
            return self.resolve_host_system_for_vm(vm_name)

        p = pref.strip().lstrip("/")
        if "/" not in p:
            return f"host/{p}"
        return p

    # ---------------------------------------------------------------------
    # FAST printing helpers (OPT-IN)
    # ---------------------------------------------------------------------

    def _vm_to_dc_map(self, *, only: Optional[Iterable[str]] = None) -> Dict[str, Optional[str]]:
        """
        Build VM name -> datacenter in ONE VM view walk.
        This avoids N×CreateContainerView scans (the cause of slow printing).
        """
        self._require_pyvmomi()
        content = self._content()
        only_set = {((n or "").strip()) for n in (only or []) if (n or "").strip()}
        filter_enabled = bool(only_set)

        view = content.viewManager.CreateContainerView(  # type: ignore[attr-defined]
            content.rootFolder, [vim.VirtualMachine], True
        )
        out: Dict[str, Optional[str]] = {}
        try:
            for vm_obj in view.view:
                name = getattr(vm_obj, "name", None)
                if not name:
                    continue
                name = str(name)
                if filter_enabled and name not in only_set:
                    continue
                out[name] = self.vm_datacenter_name(vm_obj)

            if filter_enabled:
                for n in only_set:
                    out.setdefault(n, None)

            return out
        finally:
            try:
                view.Destroy()
            except Exception:
                pass

    @staticmethod
    def _format_vm_table(names: Sequence[str], cols: int = 3, width: int = 44) -> str:
        if not names:
            return "(none)"
        items = [str(n) for n in names]
        rows: List[str] = []
        for i in range(0, len(items), cols):
            row = items[i : i + cols]
            row = row + ([""] * (cols - len(row)))
            rows.append(" " + " ".join(s[:width].ljust(width) for s in row).rstrip())
        return "\n".join(rows)

    def _vm_dc_json(self, vm_name: str, dc_name: Optional[str]) -> Dict[str, Any]:
        # Keep both spellings for legacy consumers.
        return {"name": vm_name, "datacenter": dc_name, "data_centre": dc_name}

    def log_inventory_pretty(
        self,
        *,
        refresh: bool = False,
        vm_list_limit: int = 120,
        vm_list_columns: int = 3,
        selected: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        OPT-IN inventory printing:
          - datacenter names
          - VM names table (limited)
          - selected VMs with datacenter (FAST map)

        Returns JSON list for selected VMs with "data_centre" too.
        """
        dcs = self.list_datacenters(refresh=refresh)
        self.logger.info("Datacenters (%d): %s", len(dcs), dcs)

        vms = self.list_vm_names()
        self.logger.info("VMs: %d total", len(vms))

        show = vms[: max(0, int(vm_list_limit))]
        extra = max(0, len(vms) - len(show))
        if show:
            suffix = f" (showing first {len(show)}; +{extra} more)" if extra else ""
            self.logger.info("VM list%s:\n%s", suffix, self._format_vm_table(show, cols=max(1, int(vm_list_columns))))

        sel = [s.strip() for s in (selected or []) if (s or "").strip()]
        if not sel:
            return []

        dc_map = self._vm_to_dc_map(only=sel)
        lines = ["Selected VMs (with datacenter):"]
        out_json: List[Dict[str, Any]] = []
        for n in sel:
            dc = dc_map.get(n)
            lines.append(f" - {n} (datacenter={dc})")
            out_json.append(self._vm_dc_json(n, dc))
        self.logger.info("\n".join(lines))
        return out_json

    def list_vm_summaries(self, *, include_datacenter: bool = True) -> List[Dict[str, Any]]:
        """
        JSON-friendly VM summaries. Adds 'data_centre' custom field.
        """
        self._require_pyvmomi()
        content = self._content()
        view = content.viewManager.CreateContainerView(  # type: ignore[attr-defined]
            content.rootFolder, [vim.VirtualMachine], True
        )
        out: List[Dict[str, Any]] = []

        try:
            for vm_obj in view.view:
                s = getattr(vm_obj, "summary", None)
                cfg = getattr(s, "config", None) if s else None
                runtime = getattr(s, "runtime", None) if s else None
                guest = getattr(s, "guest", None) if s else None

                dc_name: Optional[str] = None
                if include_datacenter:
                    try:
                        dc_name = self.vm_datacenter_name(vm_obj)
                    except Exception:
                        dc_name = None

                item: Dict[str, Any] = {
                    "name": getattr(vm_obj, "name", None),
                    "moId": getattr(vm_obj, "_moId", None),
                    "runtime.powerState": getattr(runtime, "powerState", None),
                    "guest.guestState": getattr(guest, "guestState", None),
                    "summary.overallStatus": getattr(s, "overallStatus", None),
                    "summary.config.uuid": getattr(cfg, "uuid", None),
                    "summary.config.instanceUuid": getattr(cfg, "instanceUuid", None),
                    "summary.config.memorySizeMB": getattr(cfg, "memorySizeMB", None),
                    "summary.config.numCpu": getattr(cfg, "numCpu", None),
                    "summary.config.vmPathName": getattr(cfg, "vmPathName", None),
                    "summary.guest.guestFullName": getattr(guest, "guestFullName", None),
                }
                if include_datacenter:
                    item["datacenter"] = dc_name
                    item["data_centre"] = dc_name

                out.append(item)

            out.sort(key=lambda d: str(d.get("name") or ""))
            return out
        finally:
            try:
                view.Destroy()
            except Exception:
                pass

    # ---------------------------
    # Tasks / disks / snapshots
    # ---------------------------

    def wait_for_task(self, task: Any) -> None:
        self._require_pyvmomi()
        while task.info.state not in (vim.TaskInfo.State.success, vim.TaskInfo.State.error):  # type: ignore[attr-defined]
            time.sleep(1)
        if task.info.state == vim.TaskInfo.State.error:  # type: ignore[attr-defined]
            raise VMwareError(str(task.info.error))

    def vm_disks(self, vm_obj: Any) -> List[Any]:
        self._require_pyvmomi()
        disks: List[Any] = []
        devices = getattr(getattr(getattr(vm_obj, "config", None), "hardware", None), "device", []) or []
        for dev in devices:
            if isinstance(dev, vim.vm.device.VirtualDisk):  # type: ignore[attr-defined]
                disks.append(dev)
        return disks

    def select_disk(self, vm_obj: Any, label_or_index: Optional[str]) -> Any:
        self._require_pyvmomi()
        disks = self.vm_disks(vm_obj)
        if not disks:
            raise VMwareError("No virtual disks found on VM")

        if label_or_index is None:
            return disks[0]

        s = str(label_or_index).strip()
        if s.isdigit():
            idx = int(s)
            if idx < 0 or idx >= len(disks):
                raise VMwareError(f"Disk index out of range: {idx} (found {len(disks)})")
            return disks[idx]

        sl = s.lower()
        for d in disks:
            label = getattr(getattr(d, "deviceInfo", None), "label", "") or ""
            if sl in str(label).lower():
                return d

        raise VMwareError(f"No disk matching label: {s}")

    def create_snapshot(
        self,
        vm_obj: Any,
        name: str,
        *,
        quiesce: bool = True,
        memory: bool = False,
        description: str = "Created by vmdk2kvm",
    ) -> Any:
        self._require_pyvmomi()
        task = vm_obj.CreateSnapshot_Task(  # type: ignore[attr-defined]
            name=name,
            description=description,
            memory=memory,
            quiesce=quiesce,
        )
        self.wait_for_task(task)
        return task.info.result

    @staticmethod
    def snapshot_moref(snapshot_obj: Any) -> str:
        moid = getattr(snapshot_obj, "_moId", None)
        if not moid:
            raise VMwareError("Could not determine snapshot MoRef (_moId missing)")
        return str(moid)

    def enable_cbt(self, vm_obj: Any) -> None:
        self._require_pyvmomi()
        if not getattr(getattr(vm_obj, "capability", None), "changeTrackingSupported", False):
            raise VMwareError("CBT not supported on this VM")
        if getattr(getattr(vm_obj, "config", None), "changeTrackingEnabled", False):
            return

        spec = vim.vm.ConfigSpec()  # type: ignore[attr-defined]
        spec.changeTrackingEnabled = True
        task = vm_obj.ReconfigVM_Task(spec)  # type: ignore[attr-defined]
        self.wait_for_task(task)

    def query_changed_disk_areas(
        self,
        vm_obj: Any,
        *,
        snapshot: Any,
        device_key: int,
        start_offset: int = 0,
        change_id: str = "*",
    ) -> Any:
        self._require_pyvmomi()
        return vm_obj.QueryChangedDiskAreas(  # type: ignore[attr-defined]
            snapshot=snapshot,
            deviceKey=device_key,
            startOffset=start_offset,
            changeId=change_id,
        )

    # ---------------------------
    # Datastore filename parsing
    # ---------------------------

    @staticmethod
    def parse_backing_filename(file_name: str) -> Tuple[str, str]:
        """
        Parse VMware style backing fileName:
          "[datastore] path/to/file.ext" -> ("datastore", "path/to/file.ext")
        """
        m = _BACKING_RE.match(file_name or "")
        if not m:
            raise VMwareError(f"Could not parse backing filename: {file_name}")
        return m.group(1), m.group(2)

    @staticmethod
    def _split_ds_path(path: str) -> Tuple[str, str, str]:
        """
        "[ds] folder/file" -> (ds, "folder", "file")
        """
        ds, rel = VMwareClient.parse_backing_filename(path)
        rel = (rel or "").lstrip("/")
        folder = rel.rsplit("/", 1)[0] if "/" in rel else ""
        base = rel.rsplit("/", 1)[1] if "/" in rel else rel
        return ds, folder, base

    # ---------------------------
    # Session cookie (for HTTPS /folder downloads)
    # ---------------------------

    def _session_cookie(self) -> str:
        if not self.si:
            raise VMwareError("Not connected")
        stub = getattr(self.si, "_stub", None)
        cookie = getattr(stub, "cookie", None)
        if not cookie:
            raise VMwareError("Could not obtain session cookie")
        return str(cookie)

    # ---------------------------
    # HTTP datastore download (requests)
    # ---------------------------

    def download_datastore_file(
        self,
        *,
        datastore: str,
        ds_path: str,
        local_path: Path,
        dc_name: Optional[str] = None,
        chunk_size: int = 1024 * 1024,
    ) -> None:
        if not REQUESTS_AVAILABLE:
            raise VMwareError("requests not installed. Install: pip install requests")

        dc_use = (dc_name or "").strip()
        if dc_use and not self.datacenter_exists(dc_use, refresh=False):
            self.logger.warning("Requested dc_name=%r not found; will auto-resolve", dc_use)
            dc_use = ""
        if not dc_use:
            dc_use = self.guess_default_datacenter()

        url = f"https://{self.host}/folder/{ds_path}?dcPath={dc_use}&dsName={datastore}"
        headers = {"Cookie": self._session_cookie()}
        verify = not self.insecure

        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info("Downloading datastore file: [%s] %s (dc=%s) -> %s", datastore, ds_path, dc_use, local_path)

        with requests.get(  # type: ignore[union-attr]
            url,
            headers=headers,
            stream=True,
            verify=verify,
            timeout=self.timeout,
        ) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", "0") or "0")
            got = 0

            tmp = local_path.with_suffix(local_path.suffix + ".part")
            try:
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        f.write(chunk)
                        got += len(chunk)
                        if total and got and got % (128 * 1024 * 1024) < chunk_size:
                            self.logger.info(
                                "Download progress: %.1f MiB / %.1f MiB (%.1f%%)",
                                got / (1024**2),
                                total / (1024**2),
                                (got / total) * 100.0,
                            )
                os.replace(tmp, local_path)
            finally:
                if tmp.exists():
                    try:
                        tmp.unlink()
                    except Exception:
                        pass

    # ---------------------------
    # Async datastore download (aiohttp + aiofiles)
    # ---------------------------

    async def async_download_datastore_file(
        self,
        *,
        datastore: str,
        ds_path: str,
        local_path: Path,
        dc_name: Optional[str] = None,
        chunk_size: int = 1024 * 1024,
    ) -> None:
        if not (AIOHTTP_AVAILABLE and AIOFILES_AVAILABLE):
            raise VMwareError("aiohttp and aiofiles not installed. Install: pip install aiohttp aiofiles")

        dc_use = (dc_name or "").strip()
        if dc_use and not self.datacenter_exists(dc_use, refresh=False):
            self.logger.warning("Requested dc_name=%r not found; will auto-resolve", dc_use)
            dc_use = ""
        if not dc_use:
            dc_use = self.guess_default_datacenter()

        url = f"https://{self.host}/folder/{ds_path}?dcPath={dc_use}&dsName={datastore}"
        headers = {"Cookie": self._session_cookie()}

        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info(
            "Async downloading datastore file: [%s] %s (dc=%s) -> %s",
            datastore,
            ds_path,
            dc_use,
            local_path,
        )

        ssl_param: Union[bool, ssl.SSLContext] = True
        if self.insecure:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            ssl_param = ctx

        timeout_param = (
            aiohttp.ClientTimeout(total=self.timeout) if self.timeout is not None else None  # type: ignore[union-attr]
        )

        tmp = local_path.with_suffix(local_path.suffix + ".part")
        try:
            async with aiohttp.ClientSession(timeout=timeout_param) as session:  # type: ignore[union-attr]
                async with session.get(url, headers=headers, ssl=ssl_param) as resp:
                    resp.raise_for_status()
                    total = int(resp.headers.get("content-length", "0") or "0")
                    got = 0

                    async with aiofiles.open(tmp, "wb") as f:  # type: ignore[union-attr]
                        async for chunk in resp.content.iter_chunked(chunk_size):
                            if not chunk:
                                continue
                            await f.write(chunk)
                            got += len(chunk)
                            if total and got and got % (128 * 1024 * 1024) < chunk_size:
                                self.logger.info(
                                    "Download progress: %.1f MiB / %.1f MiB (%.1f%%)",
                                    got / (1024**2),
                                    total / (1024**2),
                                    (got / total) * 100.0,
                                )

            os.replace(tmp, local_path)
        finally:
            if tmp.exists():
                try:
                    await asyncio.to_thread(tmp.unlink)
                except Exception:
                    pass

    # =========================================================================
    # ✅ download-only mode (NO guest inspection)
    # =========================================================================

    def _get_vm_datastore_browser(self, vm_obj: Any) -> Any:
        """
        Returns a DatastoreBrowser for the datastore that contains the VMX (and usually the VM folder).
        """
        self._require_pyvmomi()
        ds = None
        try:
            ds_list = getattr(vm_obj, "datastore", None) or []
            if ds_list:
                ds = ds_list[0]
        except Exception:
            ds = None

        if ds is None:
            raise VMwareError("Could not resolve VM datastore reference (vm.datastore empty)")

        browser = getattr(ds, "browser", None)
        if browser is None:
            raise VMwareError("Datastore has no browser (unexpected)")
        return browser

    def _vmx_pathname(self, vm_obj: Any) -> str:
        """
        Returns the VMX path string: "[ds] folder/vm.vmx"
        """
        s = getattr(vm_obj, "summary", None)
        cfg = getattr(s, "config", None) if s else None
        vmx = getattr(cfg, "vmPathName", None) if cfg else None

        if not vmx:
            try:
                files = getattr(getattr(vm_obj, "config", None), "files", None)
                vmx = getattr(files, "vmPathName", None) if files else None
            except Exception:
                vmx = None

        if not vmx:
            raise VMwareError("Could not determine VMX path (summary.config.vmPathName missing)")

        return str(vmx)

    def _list_vm_directory_files(self, vm_obj: Any) -> Tuple[str, str, List[str]]:
        """
        Returns: (datastore_name, folder_rel, [files...]) where files are *relative to folder_rel*.

        Uses DatastoreBrowser.SearchDatastoreSubFolders_Task against the VM folder.
        """
        self._require_pyvmomi()
        vmx = self._vmx_pathname(vm_obj)
        ds_name, folder_rel, _base = self._split_ds_path(vmx)
        folder_rel = folder_rel.strip("/")

        # Search path must be "[ds] folder"
        search_root = f"[{ds_name}] {folder_rel}" if folder_rel else f"[{ds_name}]"

        browser = self._get_vm_datastore_browser(vm_obj)

        q = vim.HostDatastoreBrowserSearchSpec()  # type: ignore[attr-defined]
        q.matchPattern = ["*"]
        q.details = vim.HostDatastoreBrowserFileInfoDetails()  # type: ignore[attr-defined]
        q.details.fileSize = True
        q.details.modification = True
        q.details.fileType = True

        task = browser.SearchDatastoreSubFolders_Task(search_root, q)  # type: ignore[attr-defined]
        self.wait_for_task(task)

        results = getattr(task.info, "result", None) or []
        files: List[str] = []
        for r in results:
            for fi in (getattr(r, "file", None) or []):
                name = str(getattr(fi, "path", "") or "")
                if name:
                    files.append(name)

        files = sorted(set(files))
        return ds_name, folder_rel, files

    @staticmethod
    def _glob_any(name: str, globs: Sequence[str]) -> bool:
        return any(fnmatch.fnmatch(name, g) for g in globs) if globs else False

    def _filter_download_only_files(
        self,
        files: Sequence[str],
        *,
        include_globs: Sequence[str],
        exclude_globs: Sequence[str],
        max_files: int,
    ) -> List[str]:
        out: List[str] = []
        for f in files:
            if include_globs and not self._glob_any(f, include_globs):
                continue
            if exclude_globs and self._glob_any(f, exclude_globs):
                continue
            out.append(f)

        if max_files and len(out) > int(max_files):
            raise VMwareError(
                f"Refusing to download {len(out)} files (limit={max_files}). "
                "Tune download_only_max_files / include/exclude globs."
            )
        return out

    async def _download_one_file_auto_http(
        self,
        *,
        datastore: str,
        ds_path: str,
        local_path: Path,
        dc_name: str,
        use_async_http: bool,
    ) -> None:
        """
        Use aiohttp if enabled+available; otherwise fall back to requests (in a thread).
        """
        if use_async_http and AIOHTTP_AVAILABLE and AIOFILES_AVAILABLE:
            await self.async_download_datastore_file(
                datastore=datastore,
                ds_path=ds_path,
                local_path=local_path,
                dc_name=dc_name,
            )
            return

        if not REQUESTS_AVAILABLE:
            raise VMwareError("download-only requires aiohttp+aiofiles or requests; install one of them.")

        await asyncio.to_thread(
            self.download_datastore_file,
            datastore=datastore,
            ds_path=ds_path,
            local_path=local_path,
            dc_name=dc_name,
        )

    async def async_download_only_vm(self, opt: V2VExportOptions) -> Path:
        """
        ✅ Download-only: NO virt-v2v, NO guest inspection.

        - Locate VM directory from summary.config.vmPathName (VMX)
        - List files using DatastoreBrowser
        - Download selected files using HTTPS /folder with session cookie
        """
        if not self.si:
            raise VMwareError("Not connected to vSphere; cannot download. Call connect() first.")

        mode = (opt.export_mode or "").strip().lower()
        if mode not in ("download_only", "download-only", "download"):
            raise VMwareError(f"async_download_only_vm() called with export_mode={opt.export_mode!r}")

        vm_obj = self.get_vm_by_name(opt.vm_name)
        if vm_obj is None:
            raise VMwareError(f"VM not found: {opt.vm_name!r}")

        resolved_dc = self.resolve_datacenter_for_vm(opt.vm_name, opt.datacenter)

        ds_name, folder_rel, files = await asyncio.to_thread(self._list_vm_directory_files, vm_obj)

        selected = self._filter_download_only_files(
            files,
            include_globs=tuple(opt.download_only_include_globs or ()),
            exclude_globs=tuple(opt.download_only_exclude_globs or ()),
            max_files=int(opt.download_only_max_files or 0),
        )

        out_dir = Path(opt.output_dir).expanduser().resolve()
        out_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(
            "Download-only VM folder: dc=%s ds=%s folder=%s files=%d (selected=%d)",
            resolved_dc,
            ds_name,
            folder_rel or ".",
            len(files),
            len(selected),
        )

        sem = asyncio.Semaphore(max(1, int(opt.download_only_concurrency or 1)))
        failures: List[str] = []

        async def _dl_one(name: str) -> None:
            ds_path = f"{folder_rel}/{name}" if folder_rel else name
            local_path = out_dir / name
            async with sem:
                try:
                    await self._download_one_file_auto_http(
                        datastore=ds_name,
                        ds_path=ds_path,
                        local_path=local_path,
                        dc_name=resolved_dc,
                        use_async_http=bool(opt.download_only_use_async_http),
                    )
                except Exception as e:
                    msg = f"{name}: {e}"
                    failures.append(msg)
                    if opt.download_only_fail_on_missing:
                        raise
                    self.logger.error("Download failed (non-fatal): %s", msg)

        await asyncio.gather(*(_dl_one(n) for n in selected))

        if failures and opt.download_only_fail_on_missing:
            raise VMwareError("One or more downloads failed:\n" + "\n".join(failures))

        self.logger.info("Download-only completed: %s", out_dir)
        return out_dir

    def download_only_vm(self, opt: V2VExportOptions) -> Path:
        """Sync wrapper for async_download_only_vm()."""
        return Path(_run_coro_sync(self.async_download_only_vm(opt)))

    # =========================================================================
    # ✅ NEW: vddk_download mode (single disk direct pull via VDDK client)
    # =========================================================================

    def _require_vddk_client(self) -> None:
        if not VDDK_CLIENT_AVAILABLE:
            raise VMwareError(
                "VDDK raw download requested but vddk_client is not importable. "
                "Ensure vmdk2kvm/vsphere/vddk_client.py exists and imports cleanly."
            )

    def _vm_disk_backing_filename(self, disk_obj: Any) -> str:
        """Return backing.fileName for a vim.vm.device.VirtualDisk."""
        backing = getattr(disk_obj, "backing", None)
        fn = getattr(backing, "fileName", None) if backing else None
        if not fn:
            raise VMwareError("Selected disk has no backing.fileName (unexpected)")
        return str(fn)

    def _resolve_esx_host_for_vm(self, vm_obj: Any) -> str:
        """Resolve ESXi hostname for runtime.host (used as VDDK endpoint)."""
        host_obj = self._vm_runtime_host(vm_obj)
        if host_obj is None:
            raise VMwareError("VM has no runtime.host; cannot determine ESXi host for VDDK download")
        name = str(getattr(host_obj, "name", "") or "").strip()
        if not name:
            raise VMwareError("Could not resolve ESXi host name for VM runtime.host")
        return name

    def _default_vddk_download_path(self, opt: V2VExportOptions, *, disk_index: int) -> Path:
        out_dir = Path(opt.output_dir).expanduser().resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_vm = re.sub(r"[^A-Za-z0-9_.-]+", "_", opt.vm_name or "vm")
        return out_dir / f"{safe_vm}-disk{disk_index}.vmdk"

    async def async_vddk_download_disk(self, opt: V2VExportOptions) -> Path:
        """
        ✅ export_mode="vddk_download"
          - control-plane: pyvmomi finds ESXi host + disk backing path
          - data-plane: VDDK reads and writes a local file
        """
        self._require_pyvmomi()
        self._require_vddk_client()
        if not self.si:
            raise VMwareError("Not connected to vSphere; cannot download. Call connect() first.")

        vm_obj = self.get_vm_by_name(opt.vm_name)
        if vm_obj is None:
            raise VMwareError(f"VM not found: {opt.vm_name!r}")

        disk_obj = self.select_disk(vm_obj, opt.vddk_download_disk)

        try:
            disks = self.vm_disks(vm_obj)
            disk_index = disks.index(disk_obj)
        except Exception:
            disk_index = 0

        remote_vmdk = self._vm_disk_backing_filename(disk_obj)  # "[ds] folder/disk.vmdk"
        esx_host = self._resolve_esx_host_for_vm(vm_obj)

        vddk_dir = self._resolve_vddk_libdir(opt)
        if not vddk_dir:
            raise VMwareError(
                "vddk_download requires VDDK library directory.\n"
                "Provide opt.vddk_libdir=Path('...') pointing to the directory containing libvixDiskLib.so,\n"
                "or export VDDK_LIBDIR=..., or install/extract VDDK under /opt."
            )

        thumb = (opt.vddk_thumbprint or "").strip() or None
        if (not thumb) and (not opt.no_verify):
            self.logger.info("VDDK: computing TLS thumbprint (SHA1) for ESXi %s:%d ...", esx_host, 443)
            thumb = await asyncio.to_thread(self.compute_server_thumbprint_sha1, esx_host, 443, 10.0)

        local_path = (
            Path(opt.vddk_download_output)
            if opt.vddk_download_output
            else self._default_vddk_download_path(opt, disk_index=disk_index)
        )

        spec = VDDKConnectionSpec(
            host=esx_host,
            user=self.user,
            password=self.password,
            port=443,
            vddk_libdir=vddk_dir,
            transport_modes=opt.vddk_transports or "nbdssl:nbd",
            thumbprint=thumb,
            insecure=bool(opt.no_verify),
        )

        c = VDDKESXClient(self.logger, spec)

        def _progress(done: int, total: int, pct: float) -> None:
            le = int(opt.vddk_download_log_every_bytes or 0)
            if total and done and le > 0:
                # log on boundary-ish; keep it cheap
                if done % le < int(opt.vddk_download_sectors_per_read or 2048) * 512:
                    self.logger.info(
                        "VDDK download progress: %.1f%% (%.1f/%.1f GiB)",
                        pct,
                        done / (1024**3),
                        total / (1024**3),
                    )

        self.logger.info(
            "VDDK download: vm=%s disk=%s esx=%s remote=%s -> %s",
            opt.vm_name,
            opt.vddk_download_disk or str(disk_index),
            esx_host,
            remote_vmdk,
            local_path,
        )

        await asyncio.to_thread(c.connect)
        try:
            out = await asyncio.to_thread(
                c.download_vmdk,
                remote_vmdk,
                Path(local_path),
                sectors_per_read=int(opt.vddk_download_sectors_per_read or 2048),
                progress=_progress,
                log_every_bytes=int(opt.vddk_download_log_every_bytes or 0),
            )
            return Path(out)
        finally:
            await asyncio.to_thread(c.disconnect)

    def vddk_download_disk(self, opt: V2VExportOptions) -> Path:
        """Sync wrapper for async_vddk_download_disk()."""
        return Path(_run_coro_sync(self.async_vddk_download_disk(opt)))

    # =========================================================================
    # VDDK libdir validation / auto-resolution (FIXES your error)
    # =========================================================================

    @staticmethod
    def _is_probably_vddk_libdir(p: Path) -> bool:
        if not p.exists() or not p.is_dir():
            return False
        names = (
            "libvixDiskLib.so",
            "libvixDiskLib.so.7",
            "libvixDiskLib.so.6",
            "libvixDiskLib.so.5",
        )
        return any((p / n).exists() for n in names)

    @classmethod
    def _find_vddk_libdir_under(cls, root: Path, *, max_depth: int = 7) -> Optional[Path]:
        try:
            root = root.expanduser()
        except Exception:
            pass

        if cls._is_probably_vddk_libdir(root):
            return root

        common = (
            root / "lib64",
            root / "lib",
            root / "vddk" / "lib64",
            root / "vddk" / "lib",
            root / "vmware-vix-disklib-distrib" / "lib64",
            root / "vmware-vix-disklib-distrib" / "lib",
        )
        for c in common:
            if cls._is_probably_vddk_libdir(c):
                return c

        try:
            root_res = root.resolve()
        except Exception:
            root_res = root

        base_parts = len(root_res.parts)
        targets = {"libvixDiskLib.so", "libvixDiskLib.so.7", "libvixDiskLib.so.6", "libvixDiskLib.so.5"}

        for dirpath, dirnames, filenames in os.walk(str(root_res)):
            cur = Path(dirpath)
            if (len(cur.parts) - base_parts) > max_depth:
                dirnames[:] = []
                continue
            if targets.intersection(set(filenames)):
                if cls._is_probably_vddk_libdir(cur):
                    return cur

        return None

    def _resolve_vddk_libdir(self, opt: V2VExportOptions) -> Optional[Path]:
        # 1) explicit option
        if opt.vddk_libdir:
            base = Path(opt.vddk_libdir)
            found = self._find_vddk_libdir_under(base)
            if found:
                return found
            raise VMwareError(f"‘-io vddk-libdir={str(base)}’ invalid: no libvixDiskLib.so found under that path")

        # 2) env override
        envp = (os.environ.get("VDDK_LIBDIR") or "").strip()
        if envp:
            base = Path(envp)
            found = self._find_vddk_libdir_under(base)
            if found:
                return found
            raise VMwareError(f"VDDK_LIBDIR={envp!r} invalid: no libvixDiskLib.so found under that path")

        # 3) typical locations
        guesses = (
            Path("/opt/vmware-vix-disklib-distrib"),
            Path("/opt/vmware-vix-disklib-distrib/vmware-vix-disklib-distrib"),
            Path("/usr/lib/vmware-vix-disklib"),
            Path("/usr/local/lib/vmware-vix-disklib"),
        )
        for g in guesses:
            found = self._find_vddk_libdir_under(g)
            if found:
                return found
        return None

    # =========================================================================
    # virt-v2v integration (FIXED compute path + FAST by default)
    # =========================================================================

    @staticmethod
    def _normalize_thumbprint(tp: str) -> str:
        raw = (tp or "").strip().replace(" ", "").replace(":", "").lower()
        if not _SHA1_40_RE.fullmatch(raw):
            raise VMwareError(f"Invalid thumbprint (expected SHA1 40 hex chars): {tp!r}")
        return ":".join(raw[i : i + 2] for i in range(0, 40, 2))

    @staticmethod
    def compute_server_thumbprint_sha1(host: str, port: int = 443, timeout: float = 10.0) -> str:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with ssl.create_connection((host, port), timeout=timeout) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                der = ssock.getpeercert(binary_form=True)
        sha1 = hashlib.sha1(der).hexdigest()
        return ":".join(sha1[i : i + 2] for i in range(0, 40, 2))

    def _vpx_uri(self, *, datacenter: str, compute: str, no_verify: bool) -> str:
        """
        vpx://user@host/<dc>/<compute>?no_verify=1

        compute must be a host system path:
          host/<cluster-or-cr>/<host>
        """
        q = "?no_verify=1" if no_verify else ""
        user_enc = quote(self.user or "", safe="")
        host = (self.host or "").strip()
        dc_enc = quote((datacenter or "").strip(), safe="")
        compute_norm = (compute or "").strip().lstrip("/")
        compute_enc = quote(compute_norm, safe="/-_.")
        return f"vpx://{user_enc}@{host}/{dc_enc}/{compute_enc}{q}"

    def _write_password_file(self, base_dir: Path) -> Path:
        pw = (self.password or "").strip()
        if not pw:
            raise VMwareError(
                "Missing vSphere password for virt-v2v (-ip). "
                "Set vs_password or vs_password_env (or vc_password/vc_password_env as fallback)."
            )

        base_dir.mkdir(parents=True, exist_ok=True)
        pwfile = base_dir / f".v2v-pass-{os.getpid()}.txt"
        pwfile.write_text(pw + "\n", encoding="utf-8")
        try:
            os.chmod(pwfile, 0o600)
        except Exception:
            pass
        return pwfile

    def _build_virt_v2v_cmd(self, opt: V2VExportOptions, *, password_file: Path) -> List[str]:
        if not opt.vm_name:
            raise VMwareError("V2VExportOptions.vm_name is required")
        if not self.si:
            raise VMwareError("Not connected to vSphere; cannot export. Call connect() first.")

        # ✅ FAST DEFAULT: only print inventory when explicitly requested
        if opt.print_vm_names:
            try:
                self.log_inventory_pretty(
                    refresh=False,
                    vm_list_limit=opt.vm_list_limit,
                    vm_list_columns=opt.vm_list_columns,
                    selected=list(opt.print_vm_names),
                )
            except Exception as e:
                self.logger.debug("Inventory print failed (non-fatal): %s", e)

        resolved_dc = self.resolve_datacenter_for_vm(opt.vm_name, opt.datacenter)
        resolved_compute = self.resolve_compute_for_vm(opt.vm_name, opt.compute)

        if resolved_dc != (opt.datacenter or "").strip():
            self.logger.info("Resolved datacenter override for %s: %r -> %r", opt.vm_name, opt.datacenter, resolved_dc)
        if resolved_compute != (opt.compute or "").strip():
            self.logger.info(
                "Resolved compute resource for %s: %r -> %r", opt.vm_name, opt.compute, resolved_compute
            )

        transport = (opt.transport or "").strip().lower()
        if transport not in ("vddk", "ssh"):
            raise VMwareError(f"Unsupported virt-v2v transport: {transport!r} (expected 'vddk' or 'ssh')")

        argv: List[str] = [
            "virt-v2v",
            "-i",
            "libvirt",
            "-ic",
            self._vpx_uri(datacenter=resolved_dc, compute=resolved_compute, no_verify=opt.no_verify),
            "-it",
            transport,
            "-ip",
            str(password_file),
        ]

        if transport == "vddk":
            vddk_dir = self._resolve_vddk_libdir(opt)
            if not vddk_dir:
                raise VMwareError(
                    "VDDK transport selected but no usable vddk-libdir found.\n"
                    "Provide opt.vddk_libdir=Path('...') pointing to the directory containing libvixDiskLib.so,\n"
                    "or export VDDK_LIBDIR=..., or install/extract VDDK under /opt."
                )
            argv += ["-io", f"vddk-libdir={str(vddk_dir)}"]
            if opt.vddk_thumbprint:
                argv += ["-io", f"vddk-thumbprint={self._normalize_thumbprint(opt.vddk_thumbprint)}"]
            if opt.vddk_snapshot_moref:
                argv += ["-io", f"vddk-snapshot={opt.vddk_snapshot_moref}"]
            if opt.vddk_transports:
                argv += ["-io", f"vddk-transports={opt.vddk_transports}"]

        argv.append(opt.vm_name)

        opt.output_dir.mkdir(parents=True, exist_ok=True)
        argv += ["-o", "local", "-os", str(opt.output_dir), "-of", opt.output_format]
        argv += list(opt.extra_args)

        return argv

    async def _run_logged_subprocess(self, argv: Sequence[str], *, env: Optional[Dict[str, str]] = None) -> int:
        # NOTE: never print secrets; argv should not contain passwords (we use -ip file)
        self.logger.info("Running: %s", " ".join(shlex.quote(a) for a in argv))
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )

        async def _pump(stream: Optional[asyncio.StreamReader], level: int, prefix: str) -> None:
            if stream is None:
                return
            while True:
                line = await stream.readline()
                if not line:
                    break
                msg = line.decode(errors="replace").rstrip()
                if msg:
                    self.logger.log(level, "%s%s", prefix, msg)

        await asyncio.gather(
            _pump(proc.stdout, logging.INFO, ""),
            _pump(proc.stderr, logging.INFO, ""),
        )
        return int(await proc.wait())

    async def async_v2v_export_vm(self, opt: V2VExportOptions) -> Path:
        if shutil.which("virt-v2v") is None:
            raise VMwareError("virt-v2v not found in PATH. Install virt-v2v/libguestfs tooling.")
        if not self.si:
            raise VMwareError("Not connected to vSphere; cannot export. Call connect() first.")

        if opt.transport.strip().lower() == "vddk" and (not opt.vddk_thumbprint) and (not opt.no_verify):
            self.logger.info("Computing TLS thumbprint (SHA1) for %s:%s ...", self.host, self.port)
            tp = await asyncio.to_thread(self.compute_server_thumbprint_sha1, self.host, self.port, 10.0)
            opt = V2VExportOptions(**{**opt.__dict__, "vddk_thumbprint": tp})

        pwfile = self._write_password_file(opt.output_dir)
        try:
            argv = await asyncio.to_thread(self._build_virt_v2v_cmd, opt, password_file=pwfile)
            rc = await self._run_logged_subprocess(argv, env=os.environ.copy())
            if rc != 0:
                try:
                    self.logger.error("Available datacenters: %s", self.list_datacenters(refresh=True))
                except Exception:
                    pass
                try:
                    self.logger.error("Available ESXi hosts: %s", self.list_host_names(refresh=True))
                except Exception:
                    pass
                raise VMwareError(f"virt-v2v export failed (rc={rc})")
            self.logger.info("virt-v2v export finished OK -> %s", opt.output_dir)
            return opt.output_dir
        finally:
            try:
                pwfile.unlink()
            except FileNotFoundError:
                pass
            except Exception as e:
                self.logger.warning("Failed to remove password file %s: %s", pwfile, e)

    def v2v_export_vm(self, opt: V2VExportOptions) -> Path:
        """Sync wrapper for async_v2v_export_vm()."""
        return Path(_run_coro_sync(self.async_v2v_export_vm(opt)))

    # ---------------------------------------------------------------------
    # ✅ Unified entrypoint (uses knob export_mode)
    # ---------------------------------------------------------------------

    async def async_export_vm(self, opt: V2VExportOptions) -> Path:
        mode = (opt.export_mode or "v2v").strip().lower()
        if mode in ("download_only", "download-only", "download"):
            return await self.async_download_only_vm(opt)
        if mode in ("vddk_download", "vddk-download", "vddkdownload"):
            return await self.async_vddk_download_disk(opt)
        return await self.async_v2v_export_vm(opt)

    def export_vm(self, opt: V2VExportOptions) -> Path:
        """Sync wrapper for async_export_vm(). Uses opt.export_mode knob."""
        return Path(_run_coro_sync(self.async_export_vm(opt)))
