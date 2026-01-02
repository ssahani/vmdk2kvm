# vmdk2kvm: vSphere Control-Plane + Data-Plane Design

## Overview
`vmdk2kvm` is a specialized tool for integrating with VMware vSphere, treating it in a realistic manner: **inventory and orchestration exist in one domain**, while **disk byte movement operates in another**. This intentional separation ensures the vSphere integration remains fast, predictable, and highly debuggable. By avoiding the mixing of control-plane and data-plane operations, the tool prevents common pitfalls like performance bottlenecks in large inventories or hidden failures during exports.

The philosophy is embodied in two key files:
- **`vmdk2kvm/vsphere/vmware_client.py`**: The reusable engine, designed async-first for multi-mode exports (e.g., conversion, downloads).
- **`vmdk2kvm/vsphere/<vsphere_cli_entry>.py` (VsphereMode)**: The action-driven CLI entrypoint, which orchestrates user commands and delegates to the engine.

This structure allows for modular reuse: the engine can be imported independently for scripting, while the CLI provides a user-friendly interface.

## Design Principles
The core principles guide the tool's behavior to address real-world vSphere challenges:

### Control-Plane ≠ Data-Plane (Don’t Mix Them)
- **Control-Plane** (powered by `pyvmomi` / `pyVim` / `pyVmomi`): Handles resolution of inventory objects, datacenters, hosts, snapshots, Changed Block Tracking (CBT), and datastore browsing.
- **Data-Plane** (using `virt-v2v` / HTTPS `/folder` / VDDK): Focuses solely on moving bytes, such as exporting/converting disks or downloading VM folders.
In `vmdk2kvm`, `pyvmomi` is strictly used to *find and describe* resources (e.g., locating a VM or disk), after which a dedicated data-plane mechanism takes over for efficient byte transfer. This prevents overhead from blending discovery with heavy I/O operations.

### Don’t Scan the Universe Unless Asked
vCenter inventories can be massive, and naive "list everything" approaches lead to sluggish tools. To counter this:
- `VMwareClient` makes inventory printing **opt-in** via `print_vm_names`.
- Caching is applied to small, stable lists like datacenters and hosts for quick access.
- Targeted lookups (e.g., `get_vm_by_name`) are prioritized over repeated `CreateContainerView` traversals, ensuring operations scale well in enterprise environments.

### Correct Compute Paths for Libvirt ESX (Host-System Path)
`vmdk2kvm` resolves a common failure where libvirt rejects cluster-only paths:
- Avoid: `host/<cluster>` ❌ (frequently rejected).
- Resolve to: `host/<cluster-or-compute>/<esx-host>` ✅, or fallback to `host/<esx-host>` ✅.
This fix prevents errors like **“Path … does not specify a host system”** when constructing `vpx://...` URIs for tools like `virt-v2v`.

### Bytes Should Be Explicit (Download ≠ Convert)
Operators need control over operations to avoid surprises. `vmdk2kvm` exposes distinct data-plane modes:
- **virt-v2v Export**: Converts to local `qcow2/raw` formats, potentially inspecting/modifying the guest.
- **HTTP Download-Only**: Pulls exact byte-for-byte VM folder files (e.g., VMDKs, VMX).
- **VDDK Single-Disk Pull**: Raw extraction of one disk via VDDK, without conversion.
No "download" mode accidentally mutates guests—transparency is key.

### Async Where It Matters, Sync Where It’s Safe
- `VMwareClient` is async-first, leveraging `asyncio` for benefits in downloads and subprocess log streaming (e.g., handling large outputs from `virt-v2v`).
- `VsphereMode` remains synchronous but incorporates concurrency via `ThreadPoolExecutor` for parallel tasks like file downloads.

### Never Hide the Real Failure
vSphere and `virt-v2v` failures often involve cryptic issues (e.g., TLS mismatches, invalid thumbprints, path errors, or verbose stderr). `vmware_client.py` addresses this with:
- Stderr tail capture to expose the *actual* root cause in logs/errors.
- Chunk-based stream pumping to prevent `asyncio LimitOverrunError` from tools emitting excessively long lines without newlines.

## Architecture Diagram
The following ASCII diagram illustrates the high-level structure, emphasizing the separation of concerns:

```
+------------------------------------------------------------------------------------------+
|                                   vmdk2kvm: vSphere Integration                          |
|                                                                                          |
|  Philosophy: Control-Plane (Inventory/Orchestration) ≠ Data-Plane (Byte Movement)        |
|  - Fast, Predictable, Debuggable                                                         |
|  - No Universe Scans Unless Opt-In                                                       |
|  - Correct Paths for libvirt ESX (host/<cluster>/<esx-host>)                             |
|  - Explicit Modes: Download ≠ Convert                                                    |
|  - Async-First Engine + Sync CLI                                                         |
|  - Never Hide Failures (Stderr Tail, Chunked Pumping)                                    |
+------------------------------------------------------------------------------------------+
|                                                                                          |
|  +-------------------+     +-------------------+     +-------------------+               |
|  |  VMwareClient.py  |     |  VsphereMode.py   |     |    Data-Plane     |               |
|  | (Reusable Engine) |<--->| (CLI Entrypoint)  |<--->| (Bytes Movement)  |               |
|  +-------------------+     +-------------------+     +-------------------+               |
|         |                           |                           |                        |
|         | (Async-First)             | (Sync w/ Threads)         |                        |
|         v                           v                           v                        |
|  +-------------------+     +-------------------+     +-------------------+               |
|  | Control-Plane     |     | Actions/Flags     |     | Modes:           |               |
|  | - pyvmomi/pyVim   |     | - Wires to Options|     | - virt-v2v Export|               |
|  | - Connect/Session |     | - Calls Engine    |     | - HTTP Download  |               |
|  | - DC/Host Cache   |     |                   |     | - VDDK Disk Pull |               |
|  | - VM Lookup       |     +-------------------+     +-------------------+               |
|  | - Disk Enum       |                                                           |
|  | - Snapshot/CBT    |     +-------------------+                                         |
|  | - DS Browsing     |     | CBT Sync Workflow|                                         |
|  +-------------------+     | (Hybrid)          |                                         |
|                            | 1. Enable CBT     |                                         |
|                            | 2. Quiesced Snap  |                                         |
|                            | 3. Query Changes  |                                         |
|                            | 4. Range HTTP Pull|                                         |
|                            +-------------------+                                         |
|                                                                                          |
+------------------------------------------------------------------------------------------+
| Export Modes Cheatsheet (via V2VExportOptions.export_mode)                               |
|                                                                                          |
|  +-------------------+  +-------------------+  +-------------------+                     |
|  | "v2v" (Default)   |  | "download_only"   |  | "vddk_download"   |                     |
|  | - Converted Output |  | - Exact VM Folder |  | - Single Disk Raw |                     |
|  | - qcow2/raw Local  |  | - Byte-for-Byte   |  | - Fast VDDK Pull  |                     |
|  | - Uses virt-v2v   |  | - HTTPS /folder   |  | - No Conversion   |                     |
|  | - VDDK/SSH Transp.|  | - Globs/Concurrency|  | - Sector Reads    |                     |
|  +-------------------+  +-------------------+  +-------------------+                     |
|                                                                                          |
+------------------------------------------------------------------------------------------+
| Flow: Unified async_export_vm() -> Mode Dispatch                                         |
|                                                                                          |
|  User/CLI --> VsphereMode --> VMwareClient.async_export_vm(opt)                          |
|                                    |                                                     |
|                                    v                                                     |
|                               +---------+                                                |
|                               |  Mode?  |                                                |
|                               +---------+                                                |
|                                 /   |   \                                                |
|                                /    |    \                                               |
|                               v     v     v                                              |
|                    +----------+  +----------+  +----------+                              |
|                    | v2v Export|  |Download |  |VDDK Disk |                              |
|                    | (Convert)|  | Only     |  | Download |                              |
|                    +----------+  +----------+  +----------+                              |
|                                                                                          |
+------------------------------------------------------------------------------------------+
```

## Detailed Architecture Breakdown
### Where pyvmomi Ends and Data-Plane Begins
#### Control-Plane: pyvmomi / pyVim in `vmdk2kvm`
The control-plane leverages `pyvmomi` for non-I/O tasks:
- **Connection + Session Management**: Utilizes `SmartConnect` for establishing connections and retrieving session cookies.
- **Datacenter + Host Discovery**: Caches lists via container views.
- **VM Lookup (by Name)**: Efficient targeted searches.
- **Disk Enumeration + Selection**: Lists virtual disks and selects by index or label.
- **Snapshot + CBT Orchestration**: Creates snapshots, enables CBT, and queries changes.
- **Datastore Browsing**: Lists files in VM folders using browser tasks.

Key Patterns:
- `si.RetrieveContent()` → `content` for root access.
- `CreateContainerView` for scoped views of VMs/hosts/datacenters.
- Property collector in CLI for bulk reads in `list_vm_names`.
- Datastore browser tasks like `SearchDatastore_Task` and `SearchDatastoreSubFolders_Task` for directory listings.

#### Data-Plane Options in `vmdk2kvm`
1. **virt-v2v Export Mode (`export_mode="v2v"`)**:
   - Implemented in `VMwareClient`.
   - Builds correct `vpx://user@host/<dc>/<compute>` URIs.
   - Writes passwords to temp files for `virt-v2v -ip`.
   - Validates/resolves `vddk-libdir` for VDDK transport.
   - Streams subprocess output safely with chunking.
   - Emits local output to `output_dir`.
   - This is the primary "conversion/export" path, supporting transports like `vddk` or `ssh`.

2. **HTTP `/folder` Download-Only Mode (`export_mode="download_only"`)**:
   - Used in both engine and CLI.
   - Mechanics: vCenter exposes `https://<vc>/folder/<ds_path>?dcPath=<dc>&dsName=<ds>`.
   - Authentication via session cookie from `pyvmomi` (`si._stub.cookie`).
   - `ds_path` is URL-encoded with slashes preserved (`quote(..., safe="/")`).
   - In `VMwareClient`: Async downloads with `aiohttp + aiofiles` (if available), controlled concurrency via `download_only_concurrency`, and globs/max files for safety.
   - In `VsphereMode`: File listing via datastore browser, parallel downloads with `ThreadPoolExecutor`, mirrors layout under `--output-dir`.
   - This provides a "byte-for-byte VM directory pull" without guest inspection.

3. **VDDK Single-Disk Pull (`export_mode="vddk_download"`)**:
   - Implemented in `VMwareClient`.
   - Control-plane resolves runtime ESXi host and disk backing filename (`[ds] folder/disk.vmdk`).
   - Data-plane uses `VDDKESXClient` for sector downloads to local files.
   - Handles: `vddk-libdir` validation (must contain `libvixDiskLib.so`), thumbprint normalization/auto-computation (unless `no_verify`), rate-limited progress logging.
   - This is the "get one disk fast, don’t convert" path.

### Why There Are *Two* Download-Only Implementations (Engine + CLI)
Currently, `vmdk2kvm` features dual implementations for download-only:
- `VMwareClient.async_download_only_vm()`: Async, with globs, concurrency, and reuse focus.
- `VsphereMode` action `download_only_vm`: Sync with thread pool, CLI-oriented.
This duplication is temporary for behavior stabilization. Long-term plan:
- CLI (`VsphereMode`) becomes a thin layer.
- Delegates to `VMwareClient.export_vm(V2VExportOptions(export_mode="download_only", ...))`.
- Engine owns correctness, retries, and async HTTP; CLI handles flag mapping.

## CBT Sync in `vmdk2kvm` (Control-Plane + Data-Plane Hybrid)
`cbt_sync` exemplifies the split's value for incremental workflows:
**Control-Plane Steps:**
1. Optionally enable CBT.
2. Create a quiesced snapshot.
3. Query changed disk areas via `QueryChangedDiskAreas(...)`.

**Data-Plane Step:**
4. For each changed extent, fetch byte ranges using HTTP Range requests (`Range: bytes=<start>-<end>`) and write to local disk at the offset.
This transforms vSphere into an efficient incremental block source, avoiding full disk re-downloads.

## Encoding + Typing Choices Used Across `vmdk2kvm`
- `# -*- coding: utf-8 -*-`: Ensures safe handling of logs and VM names in diverse environments.
- `from __future__ import annotations`: Mitigates runtime type-evaluation issues and simplifies optional imports.

## Mode Selection Cheatsheet (for `vmdk2kvm`)
| Need | Mode | Transport/Details |
|------|------|-------------------|
| **Converted qcow2/raw (accept virt-v2v conversion)** | `export_mode="v2v"` | `vddk` or `ssh` |
| **Exact VM folder contents from datastore** | `export_mode="download_only"` | HTTP `/folder` |
| **One disk as raw bytes via VDDK** | `export_mode="vddk_download"` | VDDK client |
| **Incremental updates on local disk** | `cbt_sync` | CBT + ranged HTTP reads |

## Enhancements & Best Practices
- **Error Handling**: Integrates stderr tails for diagnostics; detects transient issues (e.g., connection resets, auth failures).
- **Performance Tips**: Enable `prefer_cached_vm_lookup` for repetitive tasks; tune `download_only_concurrency` to balance load.
- **Security**: Supports `no_verify` but auto-computes thumbprints; passwords use secure temp files.
- **Extensibility**: `V2VExportOptions` dataclass for easy customization; add `extra_args` for `virt-v2v`.
- **Future Directions**: Full consolidation of download logic; multi-disk CBT expansions; enhanced retry mechanisms.

For code-level details, see `vmware_client.py`. If further expansions or examples are needed, provide specifics!
