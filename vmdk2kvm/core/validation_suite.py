from __future__ import annotations
import logging
from typing import Any, Callable, Dict, List

from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
class ValidationSuite:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.checks: List[Dict[str, Any]] = []
    def add_check(self, name: str, check_func: Callable, critical: bool = False):
        self.checks.append({
            "name": name,
            "func": check_func,
            "critical": critical
        })
    def run_all(self, context: Dict[str, Any]) -> Dict[str, Any]:
        results = {}
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
            task = progress.add_task("Running validations", total=len(self.checks))
            for check in self.checks:
                try:
                    result = check["func"](context)
                    results[check["name"]] = {
                        "passed": True,
                        "result": result,
                        "critical": check["critical"]
                    }
                    self.logger.debug(f"Validation check passed: {check['name']}")
                except Exception as e:
                    results[check["name"]] = {
                        "passed": False,
                        "error": str(e),
                        "critical": check["critical"]
                    }
                    msg = f"Validation check failed: {check['name']} - {e}"
                    if check["critical"]:
                        self.logger.error(msg)
                    else:
                        self.logger.warning(msg)
                progress.update(task, advance=1)
        return results
