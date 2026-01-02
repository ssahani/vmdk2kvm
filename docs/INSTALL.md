## Installation

### Quick start (recommended: editable install)

```bash
git clone https://github.com/<you>/vmdk2kvm.git
cd vmdk2kvm

python3 -m venv .venv
source .venv/bin/activate

python -m pip install -U pip wheel setuptools
python -m pip install -r requirements.txt
python -m pip install -e .

# sanity check
python -m vmdk2kvm --help
# or, if you keep the launcher:
python ./vmdk2kvm.py --help
```

### System dependencies (Linux)

`vmdk2kvm` is Python, but it **drives real system tools**. You typically need:

* `qemu-img` (from qemu)
* `libguestfs` + tools (for offline inspection/editing)
* `libvirt` (only if you use `--libvirt-test`)
* `openssh-client` / `scp` (for `fetch-and-fix` and `live-fix`)
* optional: `virt-v2v` (if you use `--use-v2v` / `--post-v2v`)
* optional: `pyvmomi` + `requests` (for `vsphere` downloads/actions)
* optional: `watchdog` (for daemon watch mode)

#### Fedora / RHEL / CentOS Stream

```bash
sudo dnf install -y \
  python3 python3-pip python3-virtualenv \
  qemu-img qemu-kvm \
  libguestfs libguestfs-tools libguestfs-xfs \
  openssh-clients rsync \
  libvirt-client libvirt-daemon-kvm \
  virt-v2v

# For libguestfs on Fedora/RHEL: "libguestfs-test-tool" is handy
sudo dnf install -y libguestfs-test-tool
```

#### Ubuntu / Debian

```bash
sudo apt-get update
sudo apt-get install -y \
  python3 python3-pip python3-venv \
  qemu-utils \
  libguestfs-tools \
  openssh-client rsync \
  libvirt-clients libvirt-daemon-system qemu-system-x86 \
  virt-v2v
```

#### openSUSE / SLES

```bash
sudo zypper install -y \
  python3 python3-pip python3-virtualenv \
  qemu-tools \
  libguestfs libguestfs-tools \
  openssh rsync \
  libvirt-client libvirt-daemon-qemu \
  virt-v2v
```

### Verify libguestfs works (do this once)

If `libguestfs` can’t launch its appliance, everything else becomes sadness.

```bash
sudo libguestfs-test-tool
```

If that fails, it’s usually KVM permissions, missing kernel modules, or a broken appliance setup.

---

## Running

After installation:

```bash
# module entrypoint (preferred)
python -m vmdk2kvm --help

# or your top-level script
python ./vmdk2kvm.py --help
```

Examples:

```bash
sudo python -m vmdk2kvm local --vmdk ./mtv-ubuntu22-4.vmdk --flatten --to-output ubuntu.qcow2 --compress
sudo python -m vmdk2kvm fetch-and-fix --host esxi.example.com --remote /vmfs/volumes/ds/vm/vm.vmdk --fetch-all --flatten --to-output vm.qcow2
sudo python -m vmdk2kvm live-fix --host 192.168.1.50 --sudo --print-fstab
```

---

## Developer install

### Run tests

```bash
python -m pip install -r requirements.txt
python -m pip install -e .
python -m pytest -q
```


