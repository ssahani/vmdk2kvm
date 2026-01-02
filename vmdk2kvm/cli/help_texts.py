from __future__ import annotations

YAML_EXAMPLE = r"""# vmdk2kvm configuration examples (YAML)
#
# Run:
# sudo ./vmdk2kvm.py --config example.yaml <command>
#
# Merge multiple configs (later overrides earlier):
# sudo ./vmdk2kvm.py --config base.yaml --config overrides.yaml <command>
#
# NOTE: Required CLI args can come from YAML because vmdk2kvm uses a 2-phase parse:
# Phase 0: reads only --config / logging
# Phase 1: loads+merges YAML and applies defaults to argparse
# Phase 2: parses full args (so required args can be satisfied by config)
#
# --------------------------------------------------------------------------------------
# 0) Common keys (apply to ALL modes)
# --------------------------------------------------------------------------------------
# output_dir: ./out
# workdir: ./out/work
# dry_run: false # preview changes, don't modify guest/outputs
# verbose: 0|1|2 # or CLI: -v/-vv
# log_file: ./vmdk2kvm.log
# report: report.md # relative to output_dir if not absolute
# checksum: true # SHA256 output
# enable_recovery: true # checkpoints for long ops
# parallel_processing: true # batch mode concurrency
#
# Fix policy:
# fstab_mode: stabilize-all # stabilize-all | bypath-only | noop
# print_fstab: true
# no_backup: false # keep backups in guest unless explicitly disabled
# no_grub: false # set true to skip grub root=/device.map cleanup
# regen_initramfs: true # best-effort initramfs+grub regen
# remove_vmware_tools: true # linux guests only
#
# Convert policy:
# flatten: true
# flatten_format: qcow2 # qcow2|raw
# to_output: final.qcow2
# out_format: qcow2 # qcow2|raw|vdi
# compress: true
# compress_level: 6 # 1-9
# resize: +10G # enlarge only: +10G or set total: 50G
#
# Windows extras:
# virtio_drivers_dir: /path/to/virtio-win
#
# virt-v2v integration:
# use_v2v: false # use virt-v2v primarily
# post_v2v: true # run v2v after internal fixes
#
# Tests:
# libvirt_test: true
# qemu_test: true
# vm_name: my-test-vm
# memory: 4096
# vcpus: 4
# uefi: true
# timeout: 90
# keep_domain: false
# headless: true
#
# --------------------------------------------------------------------------------------
# 1) LOCAL (offline local VMDK conversion)
# --------------------------------------------------------------------------------------
# Basic local mode config: fix + flatten + convert to qcow2 (Linux guest)
command: local
vmdk: /path/to/vm.vmdk
output_dir: ./out
workdir: ./out/work
flatten: true
flatten_format: qcow2
to_output: vm-fixed.qcow2
out_format: qcow2
compress: true
compress_level: 6
fstab_mode: stabilize-all
print_fstab: true
regen_initramfs: true
remove_vmware_tools: true
checksum: true
report: local-report.md
verbose: 1
# --- Local: "minimal safe" dry-run preview (no changes performed) ---
# command: local
# vmdk: /path/to/vm.vmdk
# dry_run: true
# print_fstab: true
# fstab_mode: stabilize-all
# regen_initramfs: false
# flatten: false
# verbose: 2
# --- Local: Windows virtio injection + BCD scan + convert ---
# command: local
# vmdk: /path/to/windows-vm.vmdk
# virtio_drivers_dir: /path/to/virtio-win
# # optional flags your orchestrator can map:
# # enable_virtio_gpu: true
# # enable_virtio_input: true
# # enable_virtio_fs: true
# flatten: true
# to_output: windows-kvm.qcow2
# out_format: qcow2
# compress: true
# checksum: true
# report: windows-report.md
# verbose: 2
# --- Local: disk growth + cloud-init injection (Linux) ---
# command: local
# vmdk: /path/to/linux-vm.vmdk
# resize: +20G
# cloud_init_config: /path/to/cloud-config.yaml
# fstab_mode: stabilize-all
# regen_initramfs: true
# flatten: true
# to_output: linux-grown.qcow2
# out_format: qcow2
# compress: true
# --- Local: produce RAW image for dd or imaging pipelines ---
# command: local
# vmdk: /path/to/vm.vmdk
# flatten: true
# flatten_format: raw
# to_output: vm.raw
# out_format: raw
# compress: false
# --- Local: batch multiple VMs (shared defaults + per-VM overrides) ---
# vms:
# - vmdk: /path/to/vm1.vmdk
# to_output: vm1.qcow2
# resize: +10G
# - vmdk: /path/to/vm2.vmdk
# to_output: vm2.qcow2
# remove_vmware_tools: false
# flatten: true
# out_format: qcow2
# compress: true
# parallel_processing: true
# enable_recovery: true
#
# Run:
# sudo ./vmdk2kvm.py --config batch.yaml local
#
# --------------------------------------------------------------------------------------
# 1b) VHD (Azure/Hyper-V style disks: plain .vhd OR .vhd.tar.gz)
# --------------------------------------------------------------------------------------
# fedora Azure example (plain VHD):
# command: vhd
# vhd: ./fedora-azure-43.0.x86_64.vhd
# output_dir: ./out
# flatten: true
# flatten_format: qcow2
# to_output: fedora-azure-43.0.qcow2
# out_format: qcow2
# compress: true
# compress_level: 6
# fstab_mode: stabilize-all
# print_fstab: true
# regen_initramfs: true
# remove_vmware_tools: false
# qemu_test: true
# headless: true
# uefi: true
# memory: 2048
# vcpus: 2
# timeout: 90
# checksum: true
# report: fedora-azure-report.md
# verbose: 1
#
# azure example (tarball containing VHD):
# command: vhd
# vhd: ./fedora-azure-43.x86_64.vhd.tar.gz
# output_dir: ./out
# flatten: true
# flatten_format: qcow2
# to_output: fedora-azure-43.0.qcow2
# out_format: qcow2
# compress: true
# compress_level: 6
# regen_initramfs: true
# qemu_test: true
# headless: true
# uefi: true
#
# --------------------------------------------------------------------------------------
# 1c) AMI / Generic Cloud Image Tarball (tar/tar.gz/tgz/tar.xz)
# --------------------------------------------------------------------------------------
# These tarballs are typically just archives that contain a disk payload
# (raw/img/qcow2/vmdk/vhd/...) plus metadata. vmdk2kvm will extract disk payload(s),
# optionally convert payload(s) to qcow2, then continue the normal fix/convert pipeline.
#
# Basic example: extract + convert payload disk to qcow2, then proceed with normal pipeline
# command: ami
# ami: ./some-linux-cloud-image.tar.gz
# output_dir: ./out
# flatten: true
# flatten_format: qcow2
# to_output: cloud-image-fixed.qcow2
# out_format: qcow2
# compress: true
# compress_level: 6
# fstab_mode: stabilize-all
# print_fstab: true
# regen_initramfs: true
# checksum: true
# report: cloud-image-report.md
# verbose: 1
#
# If the archive contains nested tarballs (tar-in-tar), enable one-level nested extraction:
# command: ami
# ami: ./vendor-bundle.tar.gz
# extract_nested_tar: true
# convert_payload_to_qcow2: true
# verbose: 2
#
# --------------------------------------------------------------------------------------
# 2) LIVE-FIX (apply fixes to a running VM via SSH)
# --------------------------------------------------------------------------------------
# Basic live-fix: rewrite fstab + regen initramfs/grub + optionally remove VMware tools
# command: live-fix
# host: 192.168.1.100
# user: root
# port: 22
# sudo: true
# print_fstab: true
# fstab_mode: stabilize-all
# regen_initramfs: true
# remove_vmware_tools: true
# no_backup: false
# verbose: 2
#
# --- Live-fix: custom identity and SSH options, skip grub updates ---
# command: live-fix
# host: vm.example.com
# user: admin
# identity: ~/.ssh/custom_key
# ssh_opt:
# - "-o StrictHostKeyChecking=no"
# - "-o ConnectTimeout=30"
# sudo: true
# no_grub: true
# fstab_mode: bypath-only
# dry_run: true
# log_file: live-fix.log
#
# --- Live-fix: multi-VM sequential list ---
# vms:
# - host: vm1.example.com
# user: root
# sudo: true
# regen_initramfs: true
# - host: vm2.example.com
# user: admin
# identity: ~/.ssh/key
# remove_vmware_tools: false
# print_fstab: true
# fstab_mode: stabilize-all
#
# Run:
# sudo ./vmdk2kvm.py --config live-batch.yaml live-fix
#
# --------------------------------------------------------------------------------------
# 3) FETCH-AND-FIX (fetch from ESXi over SSH/SCP and fix offline)
# --------------------------------------------------------------------------------------
# Basic fetch-and-fix: fetch descriptor then fix+convert
# command: fetch-and-fix
# host: esxi.example.com
# user: root
# port: 22
# remote: /vmfs/volumes/datastore1/vm/vm.vmdk
# fetch_dir: ./downloads
# flatten: true
# to_output: esxi-vm-fixed.qcow2
# out_format: qcow2
# compress: true
# verbose: 1
#
# --- Fetch: full snapshot chain (recursive parent descriptors) ---
# command: fetch-and-fix
# host: esxi-host
# identity: ~/.ssh/esxi_key
# remote: /path/to/snapshot-vm.vmdk
# fetch_all: true
# flatten: true
# resize: 50G
# regen_initramfs: true
# libvirt_test: true
# vm_name: esxi-test-vm
# uefi: true
# timeout: 90
# report: esxi-report.md
#
# --- Fetch: multi-VM batch (parallel fetch + fix) ---
# vms:
# - host: esxi1.example.com
# remote: /vmfs/volumes/ds1/vm1/vm1.vmdk
# fetch_all: true
# - host: esxi2.example.com
# remote: /vmfs/volumes/ds2/vm2/vm2.vmdk
# identity: ~/.ssh/key2
# flatten: true
# out_format: qcow2
# parallel_processing: true
# enable_recovery: true
#
# --------------------------------------------------------------------------------------
# 4) OVA / OVF (offline extract/parse)
# --------------------------------------------------------------------------------------
# OVA:
# command: ova
# ova: /path/to/appliance.ova
# flatten: true
# to_output: appliance.qcow2
#
# OVF (disks in same dir):
# command: ovf
# ovf: /path/to/appliance.ovf
# flatten: true
# to_output: appliance.qcow2
#
# --------------------------------------------------------------------------------------
# 5) DAEMON (watch directory)
# --------------------------------------------------------------------------------------
# CLI:
# sudo ./vmdk2kvm.py --daemon --watch-dir /incoming local
#
# YAML:
# command: daemon
# daemon: true
# watch_dir: /incoming
# output_dir: /out
#
# --------------------------------------------------------------------------------------
# 6) vSphere/vCenter (pyvmomi) - discovery, downloads, CBT sync
# --------------------------------------------------------------------------------------
# NOTE: This uses the "vsphere" subcommand with nested actions.
#
# List VMs:
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: list_vm_names
# json: true
#
# Get VM details:
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: get_vm_by_name
# name: myVM
# json: true
#
# List VM disks:
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: vm_disks
# vm_name: myVM
# json: true
#
# Download a datastore file (e.g. descriptor, extent, vmx, nvram):
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# dc_name: ha-datacenter
# vs_action: download_datastore_file
# datastore: datastore1
# ds_path: "[datastore1] myVM/myVM.vmdk"
# local_path: ./downloads/myVM.vmdk
# chunk_size: 1048576
#
# Download a specific VM disk (choosing disk by index/label):
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: download_vm_disk
# vm_name: myVM
# disk: 0
# local_path: ./downloads/myVM-disk0.vmdk
#
# ✅ NEW: Download-only VM folder pull (NO virt-v2v, NO guest inspection):
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: download_only_vm
# vm_name: myVM
# output_dir: ./downloads/myVM-folder
# include_glob: ["*"]
# exclude_glob: ["*.lck", "*.log", "*.vswp", "*.vmem", "*.vmsn"]
# concurrency: 4
#
# ✅ NEW: VDDK raw download (single disk) (requires vddk_client + VDDK libs):
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: vddk_download_disk
# vm_name: myVM
# disk: 0
# local_path: ./downloads/myVM-disk0.vmdk
# vddk_libdir: /opt/vmware-vix-disklib-distrib
# # vddk_thumbprint: "AA:BB:CC:..."
# no_verify: true
#
# Create a quiesced snapshot for safer reads:
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: create_snapshot
# vm_name: myVM
# name: vmdk2kvm-pre-migration
# quiesce: true
# memory: false
#
# vs_control_plane: govc
# govc_url: "https://10.73.213.134/sdk"
# govc_password_env: VC_PASSWORD   # (or govc_password:)
# govc_insecure: true
# govc_datacenter: data

# Enable CBT (Changed Block Tracking):
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: enable_cbt
# vm_name: myVM
#
# Query CBT changed disk areas:
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: query_changed_disk_areas
# vm_name: myVM
# snapshot_name: vmdk2kvm-cbt
# disk: 0
# start_offset: 0
# change_id: "*"
# json: true
#
# CBT delta sync (working theory / scaffold): download base once, then patch deltas:
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
# vs_action: cbt_sync
# vm_name: myVM
# disk: 0
# local_path: ./downloads/myVM-disk0.vmdk
# enable_cbt: true
# snapshot_name: vmdk2kvm-cbt
# change_id: "*"
# json: true
#
# --------------------------------------------------------------------------------------
# 6b) vSphere -> virt-v2v export (EXPERIMENTAL scaffold)
# --------------------------------------------------------------------------------------
# This path is intended for "export a VM (or snapshot) directly from vCenter/ESXi"
# using virt-v2v with VDDK transport. By default we keep concurrency = 1 because
# datastore thrash and session limits are real.
#
# command: vsphere
# vcenter: vcenter.example.com
# vc_user: administrator@vsphere.local
# vc_password_env: VC_PASSWORD
# vc_insecure: true
#
# # Enable vSphere->v2v export hook:
# vs_v2v: true
#
# # Which VM(s) to export:
# # vs_vm: myVM
# # vs_vms: ["vm1", "vm2"]
# vm_name: myVM
#
# # Export settings:
# out_format: qcow2
# compress: true
# vs_datacenter: ha-datacenter
# vs_transport: vddk
# vs_vddk_libdir: /opt/vmware-vix-disklib-distrib
# # vs_vddk_thumbprint: "AA:BB:CC:..." # recommended if TLS verify is enabled
#
# # Snapshot source:
# # vs_snapshot_moref: "snapshot-123"
# # vs_create_snapshot: true
#
# # ✅ NEW: “download-only” handoff (do not run any inspection/fix/test after export)
# vs_download_only: true
#
# # Safety default: keep this 1 unless you REALLY know your datastore can take it
# vs_v2v_concurrency: 1
#
# # Optionally run virt-v2v *after* vmdk2kvm internal conversion/fixes too:
# post_v2v: true
#
# --------------------------------------------------------------------------------------
#
# --------------------------------------------------------------------------------------
# 7) CLI examples (copy/paste)
# --------------------------------------------------------------------------------------
#
# Local conversion (offline):
# sudo ./vmdk2kvm.py --output-dir ./out local --vmdk /path/to/vm.vmdk --flatten --to-output vm.qcow2 --compress --regen-initramfs --print-fstab --fstab-mode stabilize-all --libvirt-test
#
# Fetch from ESXi and fix+test:
# sudo ./vmdk2kvm.py --output-dir ./out fetch-and-fix --host esxi.example.com --user root --remote /vmfs/volumes/datastore1/vm/vm.vmdk --fetch-all --flatten --to-output esxi-vm.qcow2 --compress --regen-initramfs --libvirt-test --vm-name esxi-test --memory 4096 --vcpus 4 --uefi --timeout 90
#
# vSphere list VMs:
# sudo ./vmdk2kvm.py vsphere --vcenter vcenter.example.com --vc-user administrator@vsphere.local --vc-password-env VC_PASSWORD --vc-insecure list_vm_names --json
#
# vSphere download a VM disk by index:
# sudo ./vmdk2kvm.py vsphere --vcenter vcenter.example.com --vc-user administrator@vsphere.local --vc-password-env VC_PASSWORD --vc-insecure --dc-name ha-datacenter download_vm_disk --vm-name myVM --disk 0 --local-path ./downloads/myVM-disk0.vmdk
#
# vSphere download-only VM folder:
# sudo ./vmdk2kvm.py vsphere --vcenter vcenter.example.com --vc-user administrator@vsphere.local --vc-password-env VC_PASSWORD --vc-insecure download_only_vm --vm-name myVM --output-dir ./downloads/myVM-folder --concurrency 4
#
# vSphere VDDK raw disk download:
# sudo ./vmdk2kvm.py vsphere --vcenter vcenter.example.com --vc-user administrator@vsphere.local --vc-password-env VC_PASSWORD --vc-insecure vddk_download_disk --vm-name myVM --disk 0 --local-path ./downloads/myVM-disk0.vmdk --vddk-libdir /opt/vmware-vix-disklib-distrib --no-verify
#
# vSphere -> virt-v2v export scaffold:
# sudo ./vmdk2kvm.py vsphere --vcenter vcenter.example.com --vc-user administrator@vsphere.local --vc-password-env VC_PASSWORD --vc-insecure --vs-v2v --vs-vm myVM --vs-transport vddk --vs-vddk-libdir /opt/vmware-vix-disklib-distrib list_vm_names
#
# vSphere -> virt-v2v download-only (stop after export):
# sudo ./vmdk2kvm.py vsphere --vcenter vcenter.example.com --vc-user administrator@vsphere.local --vc-password-env VC_PASSWORD --vc-insecure --vs-v2v --vs-vm myVM --vs-transport vddk --vs-vddk-libdir /opt/vmware-vix-disklib-distrib --vs-download-only list_vm_names
#
# --------------------------------------------------------------------------------------
"""

FEATURE_SUMMARY = """ • Inputs: local VMDK/VHD, remote ESXi fetch, OVA/OVF extract, AMI/cloud tarball extract, live SSH fix, vSphere pyvmomi\n
 • Snapshot: flatten convert, recursive parent descriptor fetch, vSphere snapshots/CBT hooks\n
 • Fixes: fstab UUID/PARTUUID/LABEL, btrfs canonicalization, grub root=, crypttab, mdraid checks\n
 • Windows: virtio injection, registry service + CriticalDeviceDatabase, BCD store scan/backup\n
 • Cloud: cloud-init injection\n
 • Outputs: qcow2/raw/vdi, compression levels, validation, checksums\n
 • Tests: libvirt and qemu smoke tests, BIOS/UEFI modes\n
 • Safety: dry-run, backups, report generation, verbose logs, recovery checkpoints\n
 • Performance: parallel batch processing\n
 • vSphere export: experimental virt-v2v (VDDK) export hook\n
 • vSphere download-only: VM folder file pull via /folder (no inspection)\n
 • vSphere VDDK raw: single disk direct pull via VDDK client (no inspection)\n"""

SYSTEMD_EXAMPLE = ""  # Empty if not needed; or add if there's more systemd text beyond the template