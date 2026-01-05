# Installation Guide (Fedora)

This document describes a **clean, RPM-first installation** on Fedora for both:

* **Control plane**: vSphere APIs, inventory, orchestration (`pyvmomi`, `govc`)
* **Data plane**: high‑performance disk access via **VMware VDDK** (`libvixDiskLib.so`)

The goal is to keep Python boring and deterministic (RPMs), and isolate proprietary / fast‑moving components where they belong.

---

## 1. Supported Platform

* Fedora 43+ (tested on fc43)
* Python 3 (system Python)
* Root access for system installs

---

## 2. Python Dependencies (Fedora RPMs)

All required Python libraries are available as **official Fedora RPMs** and should be installed system‑wide.

### Install

```bash
sudo dnf install -y \
  python3-rich \
  python3-termcolor \
  python3-watchdog \
  python3-pyyaml \
  python3-requests \
  python3-pyvmomi
```

### What gets installed

* `python3-rich` – structured logging, progress bars, TUI output
* `python3-termcolor` – ANSI color helpers
* `python3-watchdog` – filesystem event monitoring (inotify backend)
* `python3-PyYAML` – YAML parsing
* `python3-requests` – HTTP client
* `python3-pyvmomi` – VMware vSphere API (Python SDK)

Fedora automatically pulls required dependencies such as:

* `python3-markdown-it-py`, `python3-mdurl` – Rich markdown rendering
* `python3-pygments` – syntax highlighting

This avoids `pip`/ABI mismatches and keeps system tooling stable.

---

## 3. (Optional) Python Virtual Environment (pip-based)

While the recommended and default setup uses **Fedora RPMs with system Python**, some scenarios benefit from an isolated Python environment:

* Testing newer `pyvmomi` versions than Fedora ships
* Developing against multiple vSphere API versions
* Avoiding any dependency on system Python for dev workflows

This section documents a **clean virtualenv setup** that coexists safely with the RPM-based install.

### 3.1 Install virtualenv tooling

```bash
sudo dnf install -y python3-virtualenv python3-pip
```

### 3.2 Create and activate a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

Upgrade packaging tools inside the venv:

```bash
pip install --upgrade pip setuptools wheel
```

### 3.3 Install Python dependencies (pip)

```bash
pip install \
  rich \
  termcolor \
  watchdog \
  PyYAML \
  requests \
  pyvmomi
```

> Note:
>
> * This installs **pip wheels**, not Fedora RPMs
> * `pyvmomi` from pip may be newer than the Fedora package
> * VDDK is **not** provided by pip and must still be installed system-wide

### 3.4 Verify virtualenv installation

```bash
python - <<'EOF'
import rich
import termcolor
import watchdog
import yaml
import requests
import pyVmomi

print("Virtualenv imports OK")
print("pyVmomi version:", pyVmomi.__version__)
EOF
```

Deactivate when done:

```bash
deactivate
```

### 3.5 Choosing Between RPM vs Virtualenv

* **Use RPMs** when:

  * Running system services or root-run tools
  * Prioritizing ABI stability and distro integration

* **Use virtualenv** when:

  * Developing or testing
  * Needing a newer `pyvmomi`
  * Working in user-space without root

Both approaches are supported and intentionally documented.

---

## 4. Verify Python Installation

Run the following using **system Python (no virtualenv)**:

```bash
python3 - <<'EOF'
import rich
import termcolor
import watchdog
import yaml
import requests
import pyVmomi

print("All system RPM imports OK")
print("pyVmomi version:", pyVmomi.__version__)
EOF
```

Expected result:

* No tracebacks
* `pyVmomi` reports version `8.0.x`

---

## 4. Control Plane: govc (govmomi CLI)

`govc` is the recommended CLI companion for control‑plane operations:

* Inventory
* Datastores
* VM lifecycle
* Fast, scriptable vSphere access

### Install govc

Download the latest Linux release from:

* [https://github.com/vmware/govmomi/releases](https://github.com/vmware/govmomi/releases)

Example (adjust version as needed):

```bash
curl -LO https://github.com/vmware/govmomi/releases/download/v0.44.0/govc_Linux_x86_64.tar.gz
tar -xzf govc_Linux_x86_64.tar.gz
sudo install -m 0755 govc /usr/local/bin/govc
```

### Verify

```bash
which govc
/usr/local/bin/govc

govc version
```

`govc` and `pyvmomi` complement each other:

* `govc` → CLI / bulk ops / fast listing
* `pyvmomi` → Python orchestration / integration

---

## 5. Data Plane: VMware VDDK (libvixDiskLib)

For **high‑performance disk access** (VMDK reads, snapshots, block‑level conversion), install **VMware Virtual Disk Development Kit (VDDK)**.

> Fedora and other distros **do not ship VDDK**. This is expected.

### Download VDDK

Get the latest Linux VDDK tarball from Broadcom:

* [https://developer.broadcom.com/sdks/vmware-virtual-disk-development-kit-vddk/latest](https://developer.broadcom.com/sdks/vmware-virtual-disk-development-kit-vddk/latest)

(Tested with VDDK 9.0.0.0)

### Install Layout (recommended)

```bash
sudo mkdir -p /opt/vmware
sudo tar -xzf VMware-vix-disklib-*.tar.gz -C /opt/vmware
```

This results in:

```text
/opt/vmware/vmware-vix-disklib/
  ├── bin/
  ├── lib64/
  │   ├── libvixDiskLib.so
  │   ├── libvixDiskLib.so.7
  │   ├── libvixDiskLib.so.6
  │   └── libvixDiskLib.so.5
```

### Register Libraries

```bash
echo "/opt/vmware/vmware-vix-disklib/lib64" | sudo tee /etc/ld.so.conf.d/vmware-vddk.conf
sudo ldconfig
```

### Verify

```bash
ldconfig -p | grep vixDiskLib
```

Expected output shows `libvixDiskLib.so` resolved from `/opt/vmware/...`.

---

## 6. Environment Variables (when required)

Some tools require explicit paths:

```bash
export VIXDISKLIB_DIR=/opt/vmware/vmware-vix-disklib
export LD_LIBRARY_PATH=/opt/vmware/vmware-vix-disklib/lib64:$LD_LIBRARY_PATH
```

Persist if needed via `/etc/profile.d/vddk.sh`.

---

## 7. Design Rationale (Why This Layout)

* **RPMs for Python** → ABI‑safe, reproducible, SELinux‑friendly
* **govc standalone** → fast control plane, no Python dependency
* **VDDK isolated under /opt** → proprietary, versioned, explicit

This mirrors how production vSphere tooling is deployed and avoids mixed‑mode dependency failures.

---

## 8. Summary

✔ Fedora RPMs for all Python dependencies
✔ `pyvmomi` installed system‑wide and verified
✔ `govc` installed for control‑plane operations
✔ VDDK installed and registered for data‑plane disk access

This setup is stable, fast, and suitable for root‑run workflows, automation, and conversion pipelines.
