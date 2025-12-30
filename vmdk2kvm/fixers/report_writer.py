from typing import Any, Dict, List
import datetime as _dt
import os
import sys
from pathlib import Path
from ..core.utils import U
from .. import __version__

def write_report(self) -> None:
    self.report["timestamps"]["end"] = _dt.datetime.now().isoformat()
    if not self.report_path:
        return

    p = self.report_path.expanduser().resolve()
    U.ensure_dir(p.parent)

    def j(x: Any) -> str:
        return U.json_dump(x)

    def _maybe(d: Dict[str, Any], k: str, default: Any = None) -> Any:
        try:
            return d.get(k, default)
        except Exception:
            return default

    # Extract common values
    changes: Dict[str, Any] = self.report.get("changes", {}) or {}
    analysis: Dict[str, Any] = self.report.get("analysis", {}) or {}
    validation_payload: Any = self.report.get("validation")
    error_payload: Any = self.report.get("error")

    # Validation compatibility: payload may be {"results": {...}, "stats": {...}} or flat dict
    validation_results: Dict[str, Any] = {}
    validation_stats: Dict[str, Any] = {}
    if isinstance(validation_payload, dict):
        if isinstance(validation_payload.get("results"), dict):
            validation_results = validation_payload["results"]
            validation_stats = validation_payload.get("stats", {}) or {}
        else:
            # old format
            validation_results = validation_payload

    # Basic run metadata
    run_meta: Dict[str, Any] = {
        "version": __version__,
        "dry_run": self.dry_run,
        "no_backup": self.no_backup,
        "print_fstab": self.print_fstab,
        "update_grub": self.update_grub,
        "regen_initramfs": self.regen_initramfs,
        "fstab_mode": str(self.fstab_mode),
        "remove_vmware_tools": bool(self.remove_vmware_tools),
        "resize": self.resize,
        "virtio_drivers_dir": self.virtio_drivers_dir,
        "image": str(self.image),
        "root_dev": self.root_dev,
        "root_btrfs_subvol": self.root_btrfs_subvol,
        "inspect_root": self.inspect_root,
        "timestamps": self.report.get("timestamps", {}),
    }

    # Helpful: capture a bit of host context (best-effort; keep it non-invasive)
    host_meta: Dict[str, Any] = {"uid": None, "user": None, "cwd": None}
    try:
        host_meta["uid"] = os.geteuid()
    except Exception:
        pass
    try:
        host_meta["user"] = os.environ.get("SUDO_USER") or os.environ.get("USER") or None
    except Exception:
        pass
    try:
        host_meta["cwd"] = str(Path.cwd())
    except Exception:
        pass

    # Tool inventory (best-effort)
    tools = ["qemu-img", "virsh", "qemu-system-x86_64", "sgdisk", "rsync"]
    tool_inv: Dict[str, Any] = {}
    for t in tools:
        tool_inv[t] = {"path": U.which(t)}
    tool_inv["python"] = {"executable": getattr(sys, "executable", None), "version": getattr(sys, "version", None)}

    # Precompute “what changed” lists for nicer report sections
    fstab_changes = analysis.get("fstab_changes", []) or []
    # crypttab changes are not stored line-by-line; show count only unless you store it similarly
    crypttab_count = changes.get("crypttab", 0)
    net = changes.get("network", {}) or {}
    net_files = net.get("updated_files", []) or []
    vmware_rm = changes.get("vmware_tools_removed", {}) or {}
    cloud = changes.get("cloud_init_injected", {}) or {}
    regen = analysis.get("regen", {}) or {}
    disk = analysis.get("disk", {}) or {}
    mdraid = analysis.get("mdraid", {}) or {}
    win = analysis.get("windows", {}) or {}
    virtio = analysis.get("virtio", {}) or {}

    # Validation summary
    critical_failed = []
    failed = []
    for name, r in (validation_results or {}).items():
        if not isinstance(r, dict):
            continue
        if not r.get("passed", False):
            failed.append(name)
            if r.get("critical"):
                critical_failed.append(name)

    # Recovery checkpoints summary
    checkpoints_summary: List[Dict[str, Any]] = []
    if self.recovery_manager and getattr(self.recovery_manager, "checkpoints", None):
        checkpoints_summary = [
            {"stage": cp.stage, "timestamp": cp.timestamp, "completed": cp.completed}
            for cp in self.recovery_manager.checkpoints
        ]

    # Build Markdown
    md: List[str] = []
    md.append("# vmdk2kvm Report")
    md.append("")
    md.append("## Run Metadata")
    md.append("```json")
    md.append(j(run_meta))
    md.append("```")
    md.append("")
    md.append("## Host Context (best-effort)")
    md.append("```json")
    md.append(j(host_meta))
    md.append("```")
    md.append("")
    md.append("## Tool Inventory (host)")
    md.append("```json")
    md.append(j(tool_inv))
    md.append("```")

    # High-level summary
    md.append("")
    md.append("## Summary")
    md.append("")
    md.append(f"- Image: `{self.image}`")
    md.append(f"- Root: `{self.root_dev}`" + (f" (btrfs subvol `{self.root_btrfs_subvol}`)" if self.root_btrfs_subvol else ""))
    md.append(f"- Dry-run: `{self.dry_run}`")
    md.append(f"- fstab changes: `{changes.get('fstab', 0)}`")
    md.append(f"- crypttab changes: `{crypttab_count}`")
    md.append(f"- network files updated: `{net.get('count', 0)}`")
    md.append(f"- grub root updated: `{changes.get('grub_root', 0)}`")
    md.append(f"- stale device.map removed: `{changes.get('grub_device_map_removed', 0)}`")
    md.append(f"- vmware tools removed: `{bool(vmware_rm.get('removed', False))}`")
    md.append(f"- cloud-init injected: `{bool(cloud.get('injected', False))}`")
    md.append("")

    # Validation section (richer)
    if validation_payload is not None:
        md.append("## Validation")
        if validation_stats:
            md.append("### Validation Stats")
            md.append("```json")
            md.append(j(validation_stats))
            md.append("```")
        md.append("### Validation Results")
        md.append("```json")
        md.append(j(validation_payload))
        md.append("```")
        if failed:
            md.append("")
            md.append("### Failed Checks")
            md.append("")
            md.append("- Critical failed: " + (", ".join(critical_failed) if critical_failed else "`none`"))
            md.append("- Non-critical failed: " + (", ".join([x for x in failed if x not in critical_failed]) if len(failed) > len(critical_failed) else "`none`"))

    # Changes section
    md.append("")
    md.append("## Changes")
    md.append("```json")
    md.append(j(changes))
    md.append("```")

    # Nicer fstab change table
    if fstab_changes:
        md.append("")
        md.append("### /etc/fstab Rewrites")
        md.append("")
        md.append("| Line | Mount | Old | New | Reason |")
        md.append("|---:|---|---|---|---|")
        for ch in fstab_changes:
            # supports both dicts and Change objects serialized via vars()
            if isinstance(ch, dict):
                line_no = ch.get("line_no") or ch.get("line") or "?"
                mp = ch.get("mountpoint", "")
                old = ch.get("old", "")
                new = ch.get("new", "")
                reason = ch.get("reason", "")
            else:
                line_no = getattr(ch, "line_no", "?")
                mp = getattr(ch, "mountpoint", "")
                old = getattr(ch, "old", "")
                new = getattr(ch, "new", "")
                reason = getattr(ch, "reason", "")
            md.append(f"| {line_no} | `{mp}` | `{old}` | `{new}` | `{reason}` |")

        audit = analysis.get("fstab_audit", {}) or {}
        if audit:
            md.append("")
            md.append("#### fstab Audit")
            md.append("```json")
            md.append(j(audit))
            md.append("```")

    # crypttab summary
    md.append("")
    md.append("### /etc/crypttab")
    md.append(f"- Changes: `{crypttab_count}`")

    # network summary
    md.append("")
    md.append("### Network Config")
    md.append(f"- Updated files: `{len(net_files)}`")
    if net_files:
        md.append("")
        for fp in net_files[:50]:
            md.append(f"  - `{fp}`")
        if len(net_files) > 50:
            md.append(f"  - … and `{len(net_files) - 50}` more")

    # Analysis section (expanded)
    md.append("")
    md.append("## Analysis")
    md.append("")
    md.append("### Disk Usage")
    md.append("```json")
    md.append(j(disk))
    md.append("```")

    md.append("")
    md.append("### mdraid")
    md.append("```json")
    md.append(j(mdraid))
    md.append("```")

    md.append("")
    md.append("### Windows")
    md.append("```json")
    md.append(j(win))
    md.append("```")

    md.append("")
    md.append("### Virtio Injection")
    md.append("```json")
    md.append(j(virtio))
    md.append("```")

    md.append("")
    md.append("### Initramfs/GRUB Regeneration")
    md.append("```json")
    md.append(j(regen))
    md.append("```")

    # Cloud-init details
    md.append("")
    md.append("### Cloud-init")
    md.append("```json")
    md.append(j(cloud))
    md.append("```")

    # VMware tools details
    md.append("")
    md.append("### VMware Tools Removal")
    md.append("```json")
    md.append(j(vmware_rm))
    md.append("```")

    # Error section (if any)
    if error_payload is not None:
        md.append("")
        md.append("## Error")
        md.append("```json")
        md.append(j(error_payload))
        md.append("```")

    # Recovery checkpoints
    if checkpoints_summary:
        md.append("")
        md.append("## Recovery Checkpoints")
        md.append("```json")
        md.append(j(checkpoints_summary))
        md.append("```")

    # Suggested next actions (tiny, but super useful in CI logs)
    md.append("")
    md.append("## Next Actions (hints)")
    hints: List[str] = []

    if critical_failed:
        hints.append(f"- Fix CRITICAL validation failures: `{', '.join(critical_failed)}`")

    if disk.get("analysis") == "success":
        if disk.get("recommend_cleanup"):
            hints.append("- Guest disk is very full; consider cleaning logs/cache or expanding partition+fs.")
        elif disk.get("recommend_resize"):
            hints.append("- Guest disk is getting tight; consider expanding disk or cleaning space.")

    if self.update_grub and changes.get("grub_root", 0) == 0 and self.root_dev:
        hints.append("- GRUB root= may not have been updated (no match found). Verify kernel cmdline in grub.cfg.")

    if self.regen_initramfs and isinstance(regen, dict) and not regen.get("dry_run", False):
        hints.append("- If the guest still fails to boot, run initramfs+grub regen inside the VM once after first boot (or re-run with --regen-initramfs).")

    if vmware_rm.get("removed"):
        hints.append("- If networking is weird after VMware tools removal, verify NIC naming rules (udev/systemd) and regenerate initramfs if needed.")

    if cloud.get("injected"):
        hints.append("- Verify cloud-init datasource + config syntax on first boot (check /var/log/cloud-init*.log).")

    if not hints:
        hints.append("- No obvious follow-ups detected. If it still doesn’t boot, collect console logs + grub.cfg + fstab + initramfs tool output.")

    md.extend(hints)
    md.append("")

    p.write_text("\n".join(md) + "\n", encoding="utf-8")
    self.logger.info(f"Report written: {p}")
