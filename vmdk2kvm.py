#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
from vmdk2kvm.cli.argument_parser import parse_args_with_config
from vmdk2kvm.orchestrator.magic_orchestrator import Magic
from vmdk2kvm.core.exceptions import Fatal

def main() -> None:
    # Two-phase parse so required args can come from --config files (matches monolith behavior).
    args, _conf, logger = parse_args_with_config()

    try:
        rc = Magic(logger, args).run()
    except Fatal as e:
        logger.error(str(e))
        rc = e.code
    sys.exit(rc)

if __name__ == "__main__":
    main()
