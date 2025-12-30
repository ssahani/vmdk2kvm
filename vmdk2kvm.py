#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys

from vmdk2kvm.cli.argument_parser import parse_args_with_config
from vmdk2kvm.orchestrator.orchestrator import Orchestrator as PipelineOrchestrator
from vmdk2kvm.core.exceptions import Fatal


def main() -> None:
    logger = None

    # Phase 1: parse (Fatal can happen here)
    try:
        args, _conf, logger = parse_args_with_config()
    except Fatal as e:
        # IMPORTANT: don't double-print.
        # Config loader / parse layer usually already logged via U.die(logger, ...).
        # Only print if we truly never got a logger.
        if logger is None:
            print(f"💥 ERROR    {e}", file=sys.stderr)
        raise SystemExit(getattr(e, "code", 1))
    except KeyboardInterrupt:
        if logger is None:
            print("Interrupted by user (Ctrl+C).", file=sys.stderr)
        else:
            logger.warning("Interrupted by user (Ctrl+C).")
        raise SystemExit(130)

    # Phase 2: run pipeline
    try:
        rc = PipelineOrchestrator(logger, args).run()
    except Fatal as e:
        # Orchestrator layer may raise Fatal without having logged it.
        # Here we DO log once.
        logger.error(str(e))
        rc = getattr(e, "code", 1)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Ctrl+C).")
        rc = 130

    raise SystemExit(rc)


if __name__ == "__main__":
    main()
