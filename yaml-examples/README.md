vmdk2kvm YAML Examples Pack
==========================

These examples are designed for the vmdk2kvm CLI + two-phase config parsing:
  Phase 0: read only --config / logging
  Phase 1: merge config files and apply as argparse defaults
  Phase 2: parse full args (required args can come from YAML)

How to run
----------
Pick one example and run it like:

  sudo ./vmdk2kvm.py --config examples/<file>.yaml <command>

Or merge defaults + overrides:

  sudo ./vmdk2kvm.py --config examples/00-common/common.yaml --config examples/10-local/local-linux-basic.yaml local


