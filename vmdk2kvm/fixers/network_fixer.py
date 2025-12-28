# vmdk2kvm/fixers/network_fixer.py
"""
Comprehensive network configuration fixer for VMware to KVM migration.

Handles multiple network configuration formats:
- RedHat/CentOS: /etc/sysconfig/network-scripts/ifcfg-*
- Debian/Ubuntu: /etc/network/interfaces, /etc/netplan/*
- SUSE/openSUSE: /etc/sysconfig/network/ifcfg-*, /etc/wicked/*
- Systemd-networkd: /etc/systemd/network/*.network
- NetworkManager: /etc/NetworkManager/system-connections/*

Removes VMware-specific configurations and ensures KVM compatibility.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import guestfs  # type: ignore
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn, TimeRemainingColumn

from ..config.config_loader import YAML_AVAILABLE, yaml
from ..core.utils import U, guest_ls_glob


class NetworkConfigType(Enum):
    """Types of network configuration files."""

    IFCONFIG_RH = "ifcfg-rh"  # RedHat ifcfg files
    NETPLAN = "netplan"  # Ubuntu netplan YAML
    INTERFACES = "interfaces"  # Debian interfaces
    SYSTEMD_NETWORK = "systemd-network"  # systemd-networkd
    SYSTEMD_NETDEV = "systemd-netdev"  # systemd netdev
    NETWORK_MANAGER = "network-manager"  # NetworkManager
    WICKED = "wicked"  # SUSE wicked
    WICKED_IFCFG = "wicked-ifcfg"  # SUSE ifcfg files
    UNKNOWN = "unknown"


class FixLevel(Enum):
    """Level of fix aggressiveness."""

    CONSERVATIVE = "conservative"  # Minimal changes, only VMware specifics
    MODERATE = "moderate"  # Fix VMware + MAC pinning
    AGGRESSIVE = "aggressive"  # Full normalization for KVM


@dataclass
class NetworkConfig:
    """Represents a network configuration file."""

    path: str
    content: str
    type: NetworkConfigType
    original_hash: str = ""
    modified: bool = False
    backup_path: str = ""
    error: Optional[str] = None
    fixes_applied: List[str] = field(default_factory=list)


@dataclass
class FixResult:
    """Result of fixing a network configuration."""

    config: NetworkConfig
    new_content: str
    applied_fixes: List[str]
    validation_errors: List[str] = field(default_factory=list)


class NetworkFixer:
    """Main network fixing class."""

    # VMware-specific patterns to remove
    VMWARE_DRIVERS = {
        "vmxnet3": r"\bvmxnet3\b",
        "e1000": r"\be1000\b",
        "e1000e": r"\be1000e\b",
        "vmxnet": r"\bvmxnet\b",
        "vlance": r"\bvlance\b",
        "pvscsi": r"\bpvscsi\b",
        "vmw_pvscsi": r"\bvmw_pvscsi\b",
    }

    # MAC address pinning patterns
    MAC_PINNING_PATTERNS = [
        # ifcfg format
        (r"(?im)^\s*HWADDR\s*=.*$", "ifcfg-hwaddr"),
        (r"(?im)^\s*MACADDR\s*=.*$", "ifcfg-macaddr"),
        (r"(?im)^\s*MACADDRESS\s*=.*$", "ifcfg-macaddress"),
        (r"(?im)^\s*CLONED_MAC\s*=.*$", "ifcfg-cloned-mac"),
        (r"(?im)^\s*ATTR\{address\}\s*=.*$", "udev-attr-address"),
        # netplan format
        (r"(?im)^\s*macaddress\s*:.*$", "netplan-macaddress"),
        (r"(?im)^\s*cloned-mac-address\s*:.*$", "netplan-cloned-mac"),
        # interfaces format
        (r"(?im)^\s*hwaddress\s+ether\s+.*$", "interfaces-hwaddress"),
        # systemd-networkd format
        (r"(?im)^\s*MACAddress\s*=.*$", "systemd-macaddress"),
        (r"(?im)^\s*Match\s+MACAddress\s*=.*$", "systemd-match-mac"),
        # NetworkManager format
        (r"(?im)^\s*mac-address\s*=.*$", "nm-mac-address"),
        (r"(?im)^\s*mac-address-blacklist\s*=.*$", "nm-mac-blacklist"),
    ]

    # Interface name patterns that need fixing
    INTERFACE_NAME_PATTERNS = [
        # VMware predictable naming (commonly seen in VMware guests)
        (r"(?i)^ens(192|224|256|193|225)$", "vmware-ens-pattern"),
        # Old VMware names
        (r"(?i)^vmnic\d+$", "vmware-vmnic"),
        # Some vendor-ish patterns
        (r"(?i)^p\d+p\d+s\d+$", "pci-pattern"),
    ]

    # Configuration file patterns by OS/distro
    CONFIG_PATTERNS = {
        # RedHat/CentOS/Fedora
        NetworkConfigType.IFCONFIG_RH: [
            "/etc/sysconfig/network-scripts/ifcfg-*",
            "/etc/sysconfig/network/ifcfg-*",
        ],
        # Ubuntu/Debian with netplan
        NetworkConfigType.NETPLAN: [
            "/etc/netplan/*.yaml",
            "/etc/netplan/*.yml",
        ],
        # Debian/Ubuntu classic
        NetworkConfigType.INTERFACES: [
            "/etc/network/interfaces",
            "/etc/network/interfaces.d/*",
        ],
        # Systemd-networkd (all distros)
        NetworkConfigType.SYSTEMD_NETWORK: [
            "/etc/systemd/network/*.network",
        ],
        NetworkConfigType.SYSTEMD_NETDEV: [
            "/etc/systemd/network/*.netdev",
        ],
        # NetworkManager (all distros)
        NetworkConfigType.NETWORK_MANAGER: [
            "/etc/NetworkManager/system-connections/*.nmconnection",
            "/etc/NetworkManager/system-connections/*",
        ],
        # SUSE/openSUSE wicked
        NetworkConfigType.WICKED: [
            "/etc/wicked/ifconfig/*.xml",
            "/etc/wicked/ifconfig/*",
        ],
        NetworkConfigType.WICKED_IFCFG: [
            "/etc/sysconfig/network/ifcfg-*",
        ],
    }

    def __init__(self, logger: logging.Logger, fix_level: FixLevel = FixLevel.MODERATE):
        self.logger = logger
        self.fix_level = fix_level
        self.backup_suffix = f".vmdk2kvm_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # ---------------------------
    # Detection / IO
    # ---------------------------

    def detect_config_type(self, path: str) -> NetworkConfigType:
        """Detect the type of network configuration file."""
        if "/etc/sysconfig/network-scripts/ifcfg-" in path:
            return NetworkConfigType.IFCONFIG_RH
        if "/etc/netplan/" in path and (path.endswith(".yaml") or path.endswith(".yml")):
            return NetworkConfigType.NETPLAN
        if "/etc/network/interfaces" in path:
            return NetworkConfigType.INTERFACES
        if "/etc/systemd/network/" in path:
            if path.endswith(".network"):
                return NetworkConfigType.SYSTEMD_NETWORK
            if path.endswith(".netdev"):
                return NetworkConfigType.SYSTEMD_NETDEV
        if "/etc/NetworkManager/system-connections/" in path:
            return NetworkConfigType.NETWORK_MANAGER
        if "/etc/wicked/" in path:
            return NetworkConfigType.WICKED
        if "/etc/sysconfig/network/ifcfg-" in path:
            return NetworkConfigType.WICKED_IFCFG
        return NetworkConfigType.UNKNOWN

    def create_backup(self, g: guestfs.GuestFS, path: str, content: str) -> str:
        """Create a backup of the original file."""
        backup_path = f"{path}{self.backup_suffix}"
        try:
            # Best-effort: write backup alongside original file
            g.write(backup_path, content.encode("utf-8"))
            self.logger.debug(f"Created backup: {backup_path}")
            return backup_path
        except Exception as e:
            self.logger.warning(f"Failed to create backup for {path}: {e}")
            return ""

    def calculate_hash(self, content: str) -> str:
        """
        Stable content hash.

        (Your old hex(abs(hash(content))) is per-process randomized by Python,
        so the same content can get a different hash between runs.)
        """
        h = hashlib.sha256(content.encode("utf-8", errors="replace")).hexdigest()
        return h[:12]

    def read_config_file(self, g: guestfs.GuestFS, path: str) -> Optional[NetworkConfig]:
        """Read and parse a network configuration file."""
        try:
            if not g.is_file(path):
                return None
            content_bytes = g.read_file(path)
            content = U.to_text(content_bytes)
            config_type = self.detect_config_type(path)
            content_hash = self.calculate_hash(content)
            return NetworkConfig(path=path, content=content, type=config_type, original_hash=content_hash)
        except Exception as e:
            self.logger.error(f"Failed to read config file {path}: {e}")
            return None

    def find_network_configs(self, g: guestfs.GuestFS) -> List[NetworkConfig]:
        """Find all network configuration files."""
        configs: List[NetworkConfig] = []
        seen: Set[str] = set()

        for config_type, patterns in self.CONFIG_PATTERNS.items():
            for pattern in patterns:
                try:
                    files = guest_ls_glob(g, pattern)
                    for file_path in files:
                        if file_path in seen:
                            continue
                        seen.add(file_path)
                        config = self.read_config_file(g, file_path)
                        if config:
                            configs.append(config)
                except Exception as e:
                    self.logger.debug(f"Pattern {pattern} failed: {e}")

        # Also look for any ifcfg-* files in common locations (deduped)
        additional_locations = [
            "/etc/sysconfig/network/ifcfg-*",
            "/etc/ifcfg-*",
        ]
        for location in additional_locations:
            try:
                files = guest_ls_glob(g, location)
                for file_path in files:
                    if file_path in seen:
                        continue
                    seen.add(file_path)
                    config = self.read_config_file(g, file_path)
                    if config:
                        configs.append(config)
            except Exception:
                pass

        return configs

    # ---------------------------
    # Helpers: interface rename
    # ---------------------------

    def needs_interface_rename(self, interface_name: str) -> bool:
        """
        Check if an interface name needs to be renamed.

        Enhancement: check *problematic VMware patterns first*.
        Your previous logic short-circuited on ^ens\\d+$ and therefore NEVER renamed ens192/ens224/etc,
        even though you explicitly list those as VMware-pattern names.
        """
        name = (interface_name or "").strip()

        # If it matches a known-problem pattern, rename it (even if it looks "predictable")
        for pattern, _tag in self.INTERFACE_NAME_PATTERNS:
            if re.match(pattern, name, re.IGNORECASE):
                return True

        # Keep standard names
        standard_patterns = [
            r"^eth\d+$",
            r"^en[opsx]\w+$",  # Predictable network interface names
            r"^ens\d+$",
            r"^eno\d+$",
            r"^enp\d+s\d+$",
        ]
        for pattern in standard_patterns:
            if re.match(pattern, name, re.IGNORECASE):
                return False

        return False

    def get_safe_interface_name(self, current_name: str) -> str:
        """Get a safe interface name for KVM."""
        match = re.search(r"\d+", current_name or "")
        if match:
            return f"eth{match.group()}"
        return "eth0"

    # ---------------------------
    # Fixers
    # ---------------------------

    def fix_ifcfg_rh(self, config: NetworkConfig) -> FixResult:
        """Fix RedHat-style ifcfg files (also works decently for SUSE ifcfg)."""
        content = config.content
        fixes_applied: List[str] = []

        lines = content.split("\n")
        new_lines: List[str] = []

        for line in lines:
            # Remove VMware driver references in DEVICE/TYPE lines
            for driver_name, pattern in self.VMWARE_DRIVERS.items():
                if re.search(pattern, line, re.IGNORECASE):
                    if re.match(r"^\s*(DEVICE|TYPE)\s*=", line, re.IGNORECASE):
                        line = f"# {line}  # VMware driver removed by vmdk2kvm"
                        fixes_applied.append(f"removed-vmware-driver-{driver_name}")
                    break

            # Handle MAC address pinning based on fix level
            if self.fix_level in (FixLevel.MODERATE, FixLevel.AGGRESSIVE):
                for pattern, pattern_name in self.MAC_PINNING_PATTERNS:
                    if re.match(pattern, line):
                        line = f"# {line}  # MAC pinning removed by vmdk2kvm"
                        fixes_applied.append(f"removed-mac-pinning-{pattern_name}")
                        break

            # Fix interface names in aggressive mode (NAME=)
            if self.fix_level == FixLevel.AGGRESSIVE:
                if re.match(r"^\s*NAME\s*=\s*", line, re.IGNORECASE):
                    m = re.match(r"^\s*NAME\s*=\s*(.+?)\s*$", line, re.IGNORECASE)
                    if m:
                        current_name = m.group(1).strip().strip('"\'')
                        if self.needs_interface_rename(current_name):
                            new_name = self.get_safe_interface_name(current_name)
                            line = f"# {line}  # Renamed by vmdk2kvm\nNAME={new_name}"
                            fixes_applied.append("renamed-interface")

            # Ensure BOOTPROTO is sane: if it's neither static nor dhcp-ish, normalize to dhcp
            if re.match(r"^\s*BOOTPROTO\s*=", line, re.IGNORECASE):
                v = line.split("=", 1)[-1].strip().strip('"\'').lower()
                if v not in ("dhcp", "static", "none", "bootp"):
                    line = "BOOTPROTO=dhcp"
                    fixes_applied.append("enabled-dhcp")
                elif v == "none" and self.fix_level == FixLevel.AGGRESSIVE:
                    # In aggressive mode, prefer DHCP unless explicitly static later
                    line = "BOOTPROTO=dhcp"
                    fixes_applied.append("normalized-bootproto-none-to-dhcp")

            # Remove VMware-specific parameters (comment them out)
            vmware_params = ["VMWARE_", "VMXNET_", "SCSIDEVICE", "SUBCHANNELS"]
            for param in vmware_params:
                if param in line.upper():
                    line = f"# {line}  # VMware-specific parameter removed"
                    fixes_applied.append(f"removed-vmware-param-{param.lower()}")
                    break

            new_lines.append(line)

        new_content = "\n".join(new_lines)
        return FixResult(config=config, new_content=new_content, applied_fixes=fixes_applied)

    def fix_netplan(self, config: NetworkConfig) -> FixResult:
        """Fix Ubuntu netplan YAML configuration."""
        if not YAML_AVAILABLE:
            return FixResult(
                config=config,
                new_content=config.content,
                applied_fixes=[],
                validation_errors=["YAML support not available"],
            )

        try:
            data = yaml.safe_load(config.content) or {}
            fixes_applied: List[str] = []

            if isinstance(data, dict) and "network" in data and isinstance(data["network"], dict):
                network = data["network"]

                # ethernets
                eths = network.get("ethernets")
                if isinstance(eths, dict):
                    for _iface_name, iface_config in eths.items():
                        if not isinstance(iface_config, dict):
                            continue

                        # Remove MAC matching
                        if self.fix_level in (FixLevel.MODERATE, FixLevel.AGGRESSIVE):
                            match_cfg = iface_config.get("match")
                            if isinstance(match_cfg, dict) and "macaddress" in match_cfg:
                                del match_cfg["macaddress"]
                                fixes_applied.append("removed-mac-match")
                                if not match_cfg:
                                    del iface_config["match"]
                                    fixes_applied.append("removed-empty-match")

                        # Remove explicit macaddress pinning keys
                        if self.fix_level in (FixLevel.MODERATE, FixLevel.AGGRESSIVE):
                            for k in ("macaddress", "cloned-mac-address"):
                                if k in iface_config:
                                    del iface_config[k]
                                    fixes_applied.append(f"removed-{k}")

                        # Remove VMware driver hints
                        if "driver" in iface_config:
                            driver = str(iface_config.get("driver") or "")
                            for vmware_driver in self.VMWARE_DRIVERS:
                                if vmware_driver in driver.lower():
                                    del iface_config["driver"]
                                    fixes_applied.append(f"removed-vmware-driver-{vmware_driver}")
                                    break

                        # Ensure DHCP if no addresses configured
                        if "addresses" not in iface_config and "dhcp4" not in iface_config:
                            iface_config["dhcp4"] = True
                            fixes_applied.append("enabled-dhcp4")

                        # Fix interface names
                        if self.fix_level == FixLevel.AGGRESSIVE:
                            if "set-name" in iface_config:
                                current_name = str(iface_config["set-name"])
                                if self.needs_interface_rename(current_name):
                                    iface_config["set-name"] = self.get_safe_interface_name(current_name)
                                    fixes_applied.append("fixed-interface-name")

                # bonds/vlans/bridges
                for section in ("bonds", "vlans", "bridges"):
                    sec = network.get(section)
                    if isinstance(sec, dict):
                        for _n, iface_config in sec.items():
                            if not isinstance(iface_config, dict):
                                continue
                            if "macaddress" in iface_config:
                                del iface_config["macaddress"]
                                fixes_applied.append(f"removed-{section}-mac")

            # Keep key order stable-ish, avoid flow style
            new_content = yaml.safe_dump(data, sort_keys=False, default_flow_style=False)
            return FixResult(config=config, new_content=new_content, applied_fixes=fixes_applied)
        except Exception as e:
            return FixResult(
                config=config,
                new_content=config.content,
                applied_fixes=[],
                validation_errors=[f"YAML parse error: {e}"],
            )

    def _interfaces_block_has_address(self, block_lines: List[str]) -> bool:
        for ln in block_lines:
            if re.match(r"^\s*address\s+\S+", ln):
                return True
        return False

    def fix_interfaces(self, config: NetworkConfig) -> FixResult:
        """Fix Debian/Ubuntu interfaces file."""
        content = config.content
        fixes_applied: List[str] = []

        lines = content.split("\n")
        new_lines: List[str] = []

        current_iface: Optional[str] = None
        iface_block_lines: List[str] = []
        in_iface_block = False

        def flush_block() -> None:
            nonlocal iface_block_lines, current_iface, in_iface_block
            if not in_iface_block or not current_iface:
                iface_block_lines = []
                current_iface = None
                in_iface_block = False
                return

            # If iface is declared static but has no "address", normalize to dhcp (only in moderate+)
            if self.fix_level in (FixLevel.MODERATE, FixLevel.AGGRESSIVE):
                has_address = self._interfaces_block_has_address(iface_block_lines)
                for idx, ln in enumerate(iface_block_lines):
                    if re.match(r"^\s*iface\s+\S+\s+inet\s+static\b", ln) and not has_address:
                        iface_block_lines[idx] = re.sub(r"\bstatic\b", "dhcp", ln)
                        fixes_applied.append(f"iface-{current_iface}-static-without-address->dhcp")
                        break

            new_lines.extend(iface_block_lines)
            iface_block_lines = []
            current_iface = None
            in_iface_block = False

        for line in lines:
            # Start of iface stanza
            if line.strip().startswith("iface "):
                flush_block()
                parts = line.split()
                if len(parts) >= 4:
                    current_iface = parts[1]
                    in_iface_block = True
                else:
                    current_iface = None
                    in_iface_block = False
                iface_block_lines = [line]
                continue

            # End of block if a new top-level stanza begins
            if line.strip() and not line.startswith((" ", "\t")) and in_iface_block:
                flush_block()

            if in_iface_block:
                # Remove VMware driver hints
                for driver_name, pattern in self.VMWARE_DRIVERS.items():
                    if re.search(pattern, line, re.IGNORECASE):
                        line = f"# {line}  # VMware driver removed"
                        fixes_applied.append(f"removed-vmware-driver-{driver_name}")
                        break

                # Handle MAC address pinning (hwaddress)
                if self.fix_level in (FixLevel.MODERATE, FixLevel.AGGRESSIVE):
                    for pattern, pattern_name in self.MAC_PINNING_PATTERNS:
                        if pattern_name == "interfaces-hwaddress" and re.match(pattern, line):
                            line = f"# {line}  # MAC address removed by vmdk2kvm"
                            fixes_applied.append("removed-hwaddress")
                            break

                iface_block_lines.append(line)
            else:
                # Outside iface block: just remove VMware driver hints
                for driver_name, pattern in self.VMWARE_DRIVERS.items():
                    if re.search(pattern, line, re.IGNORECASE):
                        line = f"# {line}  # VMware driver removed"
                        fixes_applied.append(f"removed-vmware-driver-{driver_name}")
                        break
                new_lines.append(line)

        flush_block()

        new_content = "\n".join(new_lines)
        return FixResult(config=config, new_content=new_content, applied_fixes=fixes_applied)

    def fix_systemd_network(self, config: NetworkConfig) -> FixResult:
        """Fix systemd-networkd configuration (.network/.netdev)."""
        content = config.content
        fixes_applied: List[str] = []

        lines = content.split("\n")
        new_lines: List[str] = []
        in_match_section = False
        saw_dhcp = False
        saw_network_section = False
        in_network_section = False

        for line in lines:
            stripped = line.strip()

            # Track sections
            if stripped == "[Match]":
                in_match_section = True
                in_network_section = False
            elif stripped == "[Network]":
                in_match_section = False
                in_network_section = True
                saw_network_section = True
            elif stripped.startswith("[") and stripped.endswith("]"):
                in_match_section = False
                in_network_section = False

            # Remove MACAddress match in [Match]
            if in_match_section and self.fix_level in (FixLevel.MODERATE, FixLevel.AGGRESSIVE):
                if re.match(r"^\s*MACAddress\s*=", line, re.IGNORECASE):
                    line = f"# {line}  # MAC matching removed by vmdk2kvm"
                    fixes_applied.append("removed-mac-match")

            # Remove VMware driver hints
            for driver_name, pattern in self.VMWARE_DRIVERS.items():
                if re.search(pattern, line, re.IGNORECASE):
                    line = f"# {line}  # VMware driver removed"
                    fixes_applied.append(f"removed-vmware-driver-{driver_name}")
                    break

            # Normalize DHCP
            if in_network_section and re.match(r"^\s*DHCP\s*=", line, re.IGNORECASE):
                saw_dhcp = True
                if not re.search(r"(?i)=\s*(yes|true|ipv4|ipv6|both)\b", line):
                    line = "DHCP=yes"
                    fixes_applied.append("enabled-dhcp")

            new_lines.append(line)

        # If no DHCP directive exists and we're being proactive, add it under [Network]
        if self.fix_level == FixLevel.AGGRESSIVE and saw_network_section and not saw_dhcp:
            out: List[str] = []
            inserted = False
            for ln in new_lines:
                out.append(ln)
                if ln.strip() == "[Network]" and not inserted:
                    out.append("DHCP=yes")
                    fixes_applied.append("added-dhcp")
                    inserted = True
            new_lines = out

        new_content = "\n".join(new_lines)
        return FixResult(config=config, new_content=new_content, applied_fixes=fixes_applied)

    def fix_network_manager(self, config: NetworkConfig) -> FixResult:
        """Fix NetworkManager connection profiles (ini-like)."""
        content = config.content
        fixes_applied: List[str] = []

        lines = content.split("\n")
        new_lines: List[str] = []

        for line in lines:
            # Remove MAC address settings
            if self.fix_level in (FixLevel.MODERATE, FixLevel.AGGRESSIVE):
                if re.match(r"^\s*(mac-address|cloned-mac-address)\s*=", line, re.IGNORECASE):
                    line = f"# {line}  # MAC address removed by vmdk2kvm"
                    fixes_applied.append("removed-nm-mac")

            # Remove VMware-specific settings
            if re.search(r"(?i)vmware|vmxnet|e1000", line):
                line = f"# {line}  # VMware-specific setting removed"
                fixes_applied.append("removed-vmware-setting")

            # Fix interface names in aggressive mode
            if self.fix_level == FixLevel.AGGRESSIVE:
                if re.match(r"^\s*interface-name\s*=", line, re.IGNORECASE):
                    m = re.match(r"^\s*interface-name\s*=\s*(.+?)\s*$", line, re.IGNORECASE)
                    if m:
                        current_name = m.group(1).strip()
                        if self.needs_interface_rename(current_name):
                            new_name = self.get_safe_interface_name(current_name)
                            line = f"interface-name={new_name}"
                            fixes_applied.append("renamed-nm-interface")

            new_lines.append(line)

        new_content = "\n".join(new_lines)
        return FixResult(config=config, new_content=new_content, applied_fixes=fixes_applied)

    # ---------------------------
    # Validation / apply
    # ---------------------------

    def validate_fix(self, original: str, fixed: str, config_type: NetworkConfigType) -> List[str]:
        """Validate that the fix didn't break the configuration."""
        errors: List[str] = []

        if not fixed.strip():
            errors.append("Empty configuration after fix")

        # Type-specific validation
        if config_type == NetworkConfigType.NETPLAN and YAML_AVAILABLE:
            try:
                yaml.safe_load(fixed)
            except Exception as e:
                errors.append(f"Invalid YAML: {e}")

        essential_keywords = {
            NetworkConfigType.IFCONFIG_RH: ["DEVICE", "ONBOOT"],
            NetworkConfigType.INTERFACES: ["iface"],
            NetworkConfigType.SYSTEMD_NETWORK: ["[Match]", "[Network]"],
            NetworkConfigType.NETWORK_MANAGER: ["[connection]"],
        }
        if config_type in essential_keywords:
            for keyword in essential_keywords[config_type]:
                if keyword in original and keyword not in fixed:
                    errors.append(f"Missing essential keyword: {keyword}")

        return errors

    def apply_fix(self, g: guestfs.GuestFS, config: NetworkConfig, result: FixResult) -> bool:
        """Apply the fix to the guest filesystem."""
        if result.new_content == config.content and not result.applied_fixes:
            return False  # No changes needed

        validation_errors = self.validate_fix(config.content, result.new_content, config.type)
        if validation_errors:
            self.logger.warning(f"Validation errors for {config.path}: {validation_errors}")
            result.validation_errors.extend(validation_errors)
            return False

        backup_path = self.create_backup(g, config.path, config.content)

        try:
            g.write(config.path, result.new_content.encode("utf-8"))
            self.logger.info(f"Updated {config.path} with fixes: {result.applied_fixes}")
            config.modified = True
            config.backup_path = backup_path
            config.fixes_applied.extend(result.applied_fixes)
            return True
        except Exception as e:
            self.logger.error(f"Failed to write {config.path}: {e}")

            # Best-effort restore
            if backup_path and g.is_file(backup_path):
                try:
                    backup_content = g.read_file(backup_path)
                    g.write(config.path, backup_content)
                    self.logger.info(f"Restored {config.path} from backup")
                except Exception as restore_error:
                    self.logger.error(f"Failed to restore backup: {restore_error}")

            return False

    # ---------------------------
    # Orchestration / report
    # ---------------------------

    def fix_network_config(
        self, g: guestfs.GuestFS, progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> Dict[str, Any]:
        """
        Main entry point for fixing network configurations.

        Args:
            g: GuestFS instance
            progress_callback: Optional callback for progress updates

        Returns:
            Dictionary with fix results
        """
        self.logger.info(f"Starting network configuration fixes (level: {self.fix_level.value})")

        configs = self.find_network_configs(g)
        self.logger.info(f"Found {len(configs)} network configuration files")

        stats: Dict[str, Any] = {
            "total_files": len(configs),
            "files_modified": 0,
            "files_skipped": 0,
            "files_failed": 0,
            "total_fixes_applied": 0,
            "by_type": {},
            "details": [],
            "backups_created": 0,
        }

        fixer_map = {
            NetworkConfigType.IFCONFIG_RH: self.fix_ifcfg_rh,
            NetworkConfigType.NETPLAN: self.fix_netplan,
            NetworkConfigType.INTERFACES: self.fix_interfaces,
            NetworkConfigType.SYSTEMD_NETWORK: self.fix_systemd_network,
            NetworkConfigType.SYSTEMD_NETDEV: self.fix_systemd_network,  # same style
            NetworkConfigType.NETWORK_MANAGER: self.fix_network_manager,
            NetworkConfigType.WICKED: self.fix_ifcfg_rh,  # best-effort fallback
            NetworkConfigType.WICKED_IFCFG: self.fix_ifcfg_rh,
        }

        for i, config in enumerate(configs):
            if progress_callback:
                progress_callback(i, len(configs), f"Processing {config.path}")

            self.logger.debug(f"Processing {config.path} ({config.type.value})")

            fixer = fixer_map.get(config.type)
            if not fixer:
                self.logger.warning(f"No fixer for {config.type.value}, skipping {config.path}")
                stats["files_skipped"] += 1
                continue

            try:
                result = fixer(config)

                # Apply only if there's something to do, or if validation errors exist (record them)
                success = False
                if result.applied_fixes:
                    success = self.apply_fix(g, config, result)
                elif result.validation_errors:
                    # keep track, but don't write
                    self.logger.warning(f"Validation errors for {config.path}: {result.validation_errors}")

                config_type_str = config.type.value
                stats["by_type"].setdefault(config_type_str, {"total": 0, "modified": 0, "fixes": 0})
                stats["by_type"][config_type_str]["total"] += 1

                if result.applied_fixes:
                    if success:
                        stats["files_modified"] += 1
                        stats["by_type"][config_type_str]["modified"] += 1
                        stats["total_fixes_applied"] += len(result.applied_fixes)
                        stats["by_type"][config_type_str]["fixes"] += len(result.applied_fixes)
                        if config.backup_path:
                            stats["backups_created"] += 1
                    else:
                        stats["files_failed"] += 1

                detail = {
                    "path": config.path,
                    "type": config.type.value,
                    "modified": config.modified,
                    "fixes_applied": result.applied_fixes,
                    "validation_errors": result.validation_errors,
                    "backup": config.backup_path,
                    "original_hash": config.original_hash,
                    "new_hash": self.calculate_hash(result.new_content) if config.modified else config.original_hash,
                }
                stats["details"].append(detail)

            except Exception as e:
                self.logger.error(f"Error fixing {config.path}: {e}")
                stats["files_failed"] += 1
                stats["details"].append(
                    {
                        "path": config.path,
                        "type": config.type.value,
                        "modified": False,
                        "error": str(e),
                    }
                )

        summary = {
            "fix_level": self.fix_level.value,
            "stats": stats,
            "recommendations": self.generate_recommendations(stats),
        }

        self.logger.info(
            f"Network fix complete: {stats['files_modified']} files modified, {stats['total_fixes_applied']} fixes applied"
        )
        return summary

    def generate_recommendations(self, stats: Dict[str, Any]) -> List[str]:
        """Generate post-fix recommendations."""
        recommendations: List[str] = []

        if stats["files_modified"] > 0:
            recommendations.append(
                f"Modified {stats['files_modified']} network configuration files. "
                "Review changes and test network connectivity after boot."
            )
            if stats["total_fixes_applied"] > 0:
                recommendations.append(
                    f"Applied {stats['total_fixes_applied']} fixes including: "
                    "MAC address pinning removal, VMware driver cleanup, and DHCP configuration."
                )
            if stats["backups_created"] > 0:
                recommendations.append(
                    f"Created {stats['backups_created']} backup files with suffix '{self.backup_suffix}'. "
                    "These can be restored if needed."
                )

        if stats["files_failed"] > 0:
            recommendations.append(
                f"Failed to process {stats['files_failed']} files. Manual network configuration may be required."
            )

        if "ifcfg-rh" in stats["by_type"]:
            recommendations.append(
                "RedHat/CentOS system detected. Run 'systemctl restart network' (or reboot) to apply changes."
            )

        if "netplan" in stats["by_type"]:
            recommendations.append("Netplan detected. After boot, run 'netplan apply' to activate configuration.")

        if stats["total_fixes_applied"] == 0 and stats["files_modified"] == 0:
            recommendations.append("No network configuration changes were needed. The existing config looks KVM-safe.")

        return recommendations


# -------------------------------------------------------------------
# Legacy function for backward compatibility
# -------------------------------------------------------------------
def fix_network_config(self, g: guestfs.GuestFS) -> Dict[str, Any]:
    """
    Legacy wrapper for the new NetworkFixer class.

    This maintains compatibility with the original function signature.
    """
    fix_level_str = getattr(self, "network_fix_level", "moderate")
    try:
        fix_level = FixLevel(fix_level_str)
    except Exception:
        fix_level = FixLevel.MODERATE

    fixer = NetworkFixer(
        logger=getattr(self, "logger", logging.getLogger(__name__)),
        fix_level=fix_level,
    )

    progress: Optional[Progress] = None
    task = None
    if hasattr(self, "show_progress") and getattr(self, "show_progress", False):
        progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            transient=True,
        )
        progress.start()
        task = progress.add_task("Fixing network configurations...", total=100)

    def update_progress(current: int, total: int, message: str) -> None:
        if progress and task is not None and total > 0:
            pct = int((current / total) * 100)
            progress.update(task, completed=pct, description=message)

    try:
        result = fixer.fix_network_config(g, update_progress)

        if hasattr(self, "report"):
            self.report.setdefault("network", {})
            self.report["network"] = result

        updated_files = [d["path"] for d in result["stats"]["details"] if d.get("modified", False)]
        return {
            "updated_files": updated_files,
            "count": len(updated_files),
            "analysis": result,
        }
    finally:
        if progress:
            progress.stop()
