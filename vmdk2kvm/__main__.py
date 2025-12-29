from __future__ import annotations
import sys
from .cli.argument_parser import parse_args_with_config
from .orchestrator.orchestrator import Orchestrator
from .core.exceptions import Fatal
def main() -> None:
    args, _conf, logger = parse_args_with_config()
    try:
        Orchestrator(logger, args).run()
        rc = 0
    except Fatal as e:
        logger.error(str(e))
        rc = e.code
    sys.exit(int(rc))
if __name__ == "__main__":
    main()