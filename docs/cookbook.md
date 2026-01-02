## Usage cookbook (CLI ↔ YAML side by side)

This section shows **the same operation expressed two ways**:

1. **CLI invocation** — good for exploration and one-offs  
2. **YAML config** — recommended for repeatability, audits, and automation  

If something matters, put it in YAML.

---

## 1. Local mode — Linux VMDK → qcow2

### CLI

```bash
sudo ./vmdk2kvm.py \
  --output-dir ./out \
  local \
  --vmdk /path/to/linux.vmdk \
  --to-output linux-fixed.qcow2 \
  --flatten \
  --fstab-mode stabilize-all \
  --regen-initramfs \
  --remove-vmware-tools \
  --checksum \
  -v
````

### YAML

```yaml
command: local
output_dir: ./out

vmdk: /path/to/linux.vmdk
to_output: linux-fixed.qcow2

flatten: true
fstab_mode: stabilize-all
regen_initramfs: true
remove_vmware_tools: true

checksum: true
verbose: 1
```

---

## 2. Local mode — Windows VMDK with VirtIO pre-staging

### CLI

```bash
sudo ./vmdk2kvm.py \
  --output-dir ./out \
  local \
  --vmdk /path/to/windows.vmdk \
  --to-output windows-fixed.qcow2 \
  --flatten \
  --virtio-drivers-dir /path/to/virtio-win \
  --checksum \
  -v
```

### YAML

```yaml
command: local
output_dir: ./out

vmdk: /path/to/windows.vmdk
to_output: windows-fixed.qcow2

flatten: true
virtio_drivers_dir: /path/to/virtio-win

checksum: true
verbose: 1
```

This injects BOOT_START VirtIO drivers and registry entries **before first KVM boot**.

---

## 3. Dry-run inspection (no writes)

### CLI

```bash
sudo ./vmdk2kvm.py \
  --dry-run \
  --print-fstab \
  local \
  --vmdk /path/to/vm.vmdk \
  -vv
```

### YAML

```yaml
command: local
vmdk: /path/to/vm.vmdk

dry_run: true
print_fstab: true
verbose: 2
```

Use this to understand **exactly what would change**.

---

## 4. Fetch-and-fix — ESXi over SSH

### CLI

```bash
sudo ./vmdk2kvm.py \
  --output-dir ./out \
  fetch-and-fix \
  --host esxi.example.com \
  --user root \
  --remote /vmfs/volumes/datastore1/vm/vm.vmdk \
  --fetch-all \
  --flatten \
  --to-output esxi-fixed.qcow2 \
  -v
```

### YAML

```yaml
command: fetch-and-fix
output_dir: ./out

host: esxi.example.com
user: root
remote: /vmfs/volumes/datastore1/vm/vm.vmdk

fetch_all: true
flatten: true
to_output: esxi-fixed.qcow2

verbose: 1
```

This fetches the **entire snapshot chain**, flattens it, and converts offline.

---

## 5. Live-fix — running Linux VM over SSH

### CLI

```bash
sudo ./vmdk2kvm.py \
  live-fix \
  --host vm.example.com \
  --user root \
  --sudo \
  --fstab-mode stabilize-all \
  --regen-initramfs \
  --remove-vmware-tools \
  -v
```

### YAML

```yaml
command: live-fix

host: vm.example.com
user: root
sudo: true

fstab_mode: stabilize-all
regen_initramfs: true
remove_vmware_tools: true

verbose: 1
```

Live-fix is **post-migration hygiene**, not a replacement for offline repair.

---

## 6. OVA appliance conversion

### CLI

```bash
sudo ./vmdk2kvm.py \
  --output-dir ./out \
  ova \
  --ova appliance.ova \
  --flatten \
  --to-output appliance.qcow2 \
  -v
```

### YAML

```yaml
command: ova
output_dir: ./out

ova: appliance.ova
flatten: true
to_output: appliance.qcow2

verbose: 1
```

---

## 7. OVF descriptor conversion

### CLI

```bash
sudo ./vmdk2kvm.py \
  --output-dir ./out \
  ovf \
  --ovf appliance.ovf \
  --flatten \
  --to-output appliance.qcow2 \
  -v
```

### YAML

```yaml
command: ovf
output_dir: ./out

ovf: appliance.ovf
flatten: true
to_output: appliance.qcow2

verbose: 1
```

---

## 8. vSphere — list VMs (pyvmomi control-plane)

### CLI

```bash
./vmdk2kvm.py vsphere \
  --vcenter vcenter.example.com \
  --vc-user administrator@vsphere.local \
  --vc-password-env VC_PASSWORD \
  --vc-insecure \
  list_vm_names \
  --json
```

### YAML

```yaml
command: vsphere

vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true

vs_action: list_vm_names
json: true
```

---

## 9. vSphere — download a VM disk

### CLI

```bash
./vmdk2kvm.py vsphere \
  --vcenter vcenter.example.com \
  --vc-user administrator@vsphere.local \
  --vc-password-env VC_PASSWORD \
  --vc-insecure \
  download_vm_disk \
  --vm-name myVM \
  --disk 0 \
  --local-path ./downloads/myVM-disk0.vmdk
```

### YAML

```yaml
command: vsphere

vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true

vs_action: download_vm_disk
vm_name: myVM
disk: 0
local_path: ./downloads/myVM-disk0.vmdk
```

---

## 10. vSphere — download entire VM folder (HTTP data-plane)

### CLI

```bash
./vmdk2kvm.py vsphere \
  --vcenter vcenter.example.com \
  --vc-user administrator@vsphere.local \
  --vc-password-env VC_PASSWORD \
  --vc-insecure \
  download_only_vm \
  --vm-name myVM \
  --output-dir ./downloads/myVM
```

### YAML

```yaml
command: vsphere

vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true

vs_action: download_only_vm
vm_name: myVM
output_dir: ./downloads/myVM

vs_include_glob: ["*"]
vs_exclude_glob: ["*.log"]
vs_concurrency: 6
```

This uses:

* pyvmomi for inventory
* HTTPS `/folder` for data transfer
* optional parallel downloads

---

## 11. vSphere — CBT delta sync

### CLI

```bash
./vmdk2kvm.py vsphere \
  --vcenter vcenter.example.com \
  --vc-user administrator@vsphere.local \
  --vc-password-env VC_PASSWORD \
  --vc-insecure \
  cbt_sync \
  --vm-name myVM \
  --disk 0 \
  --local-path ./downloads/myVM-disk0.vmdk \
  --enable-cbt \
  --snapshot-name vmdk2kvm-cbt \
  --change-id "*"
```

### YAML

```yaml
command: vsphere

vcenter: vcenter.example.com
vc_user: administrator@vsphere.local
vc_password_env: VC_PASSWORD
vc_insecure: true

vs_action: cbt_sync
vm_name: myVM
disk: 0
local_path: ./downloads/myVM-disk0.vmdk

enable_cbt: true
snapshot_name: vmdk2kvm-cbt
change_id: "*"
```
