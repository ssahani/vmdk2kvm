# Testing Converted Photon qcow2 on Fedora (GUI, BIOS)

This guide describes how to **validate a converted Photon OS qcow2 image**
using **KVM + libvirt** on Fedora, booting **directly into graphical (GUI) mode**
with **legacy BIOS (SeaBIOS)**.

This is intended as a **post-conversion smoke test** after migrating
a VMware VMDK to qcow2 and applying offline fixes
(fstab, GRUB, initramfs, disk identifiers, etc.).

---

## Host Requirements (Fedora)

Install required virtualization tools:

```bash
sudo dnf install -y \
  libvirt \
  qemu-kvm \
  qemu-img \
  virt-viewer \
  libguestfs-tools
```

Enable libvirt and ensure the default network is active:

```bash
sudo systemctl enable --now libvirtd
sudo virsh net-start default 2>/dev/null || true
```

---

## Image Under Test (Photon OS)

```
/home/ssahani/by-path/out/photon.qcow2
```

**Assumptions:**

* Converted from VMware VMDK
* Legacy BIOS capable (non-UEFI)
* Photon OS (minimal / server-oriented)
* GUI may or may not be installed (console-only is still a valid success)

---

## BIOS + GUI libvirt XML (Most Compatible)

This XML configuration is intentionally conservative and works reliably
for **Photon and other minimal Linux guests**:

* SeaBIOS (`machine='pc'`)
* Virtio disk + network
* VNC graphics
* Simple VGA-compatible video

```bash
cat >/tmp/photon-test.xml <<'EOF'
<domain type='kvm'>
  <name>photon-gui</name>

  <memory unit='MiB'>4096</memory>
  <vcpu>2</vcpu>

  <os>
    <type arch='x86_64' machine='pc'>hvm</type>
    <boot dev='hd'/>
  </os>

  <features>
    <acpi/>
    <apic/>
  </features>

  <cpu mode='host-passthrough'/>

  <devices>
    <!-- Disk -->
    <disk type='file' device='disk'>
      <driver name='qemu' type='qcow2'/>
      <source file='/home/ssahani/by-path/out/photon.qcow2'/>
      <target dev='vda' bus='virtio'/>
    </disk>

    <!-- Network -->
    <interface type='network'>
      <source network='default'/>
      <model type='virtio'/>
    </interface>

    <!-- Graphics -->
    <graphics type='vnc' autoport='yes' listen='127.0.0.1'/>

    <!-- Video -->
    <video>
      <model type='vga'/>
    </video>

    <input type='tablet' bus='usb'/>
  </devices>
</domain>
EOF
```

---

## Define and Start the VM

If a domain with the same name already exists, remove it first:

```bash
sudo virsh destroy photon-gui 2>/dev/null || true
sudo virsh undefine photon-gui 2>/dev/null || true
```

Define and start the VM:

```bash
sudo virsh define /tmp/photon-test.xml
sudo virsh start photon-gui
```

---

## Connect to the Console / GUI

### Option 1: virt-viewer (recommended)

```bash
virt-viewer photon-gui
```

### Option 2: VNC

```bash
sudo virsh vncdisplay photon-gui
```

Connect to the displayed address, for example:

```
127.0.0.1:5901
```

---

## Expected Boot Sequence (Photon)

A successful boot typically shows:

1. SeaBIOS splash
2. GRUB (or Photon bootloader)
3. Photon kernel + initramfs
4. Login prompt (console or GUI, depending on image)

⚠️ **Important:**  
Photon OS is commonly **console-only**.  
A **login prompt is a successful boot**.

---

## Troubleshooting

### Black screen after GRUB

Try an alternative video model:

```xml
<model type='qxl' vram='65536'/>
```

or:

```xml
<model type='virtio'/>
```

---

### “No bootable device”

Some Photon installs expect SATA instead of virtio:

```xml
<target dev='sda' bus='sata'/>
```

---

### Boots but no GUI

Photon OS does **not** install a graphical stack by default.

Inside the guest:

```bash
systemctl get-default
```

If it reports `multi-user.target`, this is **expected behavior**.

---

## Notes

* This test avoids UEFI entirely
* Designed for VMware → KVM Photon migrations
* Suitable for automated or manual post-conversion smoke tests
* Console-only success is still a **pass**

---

**Status:** ✅ Verified boot for Photon OS (BIOS, KVM/libvirt)


