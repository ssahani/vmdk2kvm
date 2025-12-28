from __future__ import annotations
import json
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from .utils import U
class Checkpoint:
    stage: str
    timestamp: str
    data: Dict[str, Any]
    completed: bool = False

class RecoveryManager:
    def __init__(self, logger: logging.Logger, workdir: Path):
        self.logger = logger
        self.workdir = workdir
        self.checkpoints: List[Checkpoint] = []
        U.ensure_dir(workdir)
    def save_checkpoint(self, stage: str, data: Dict[str, Any]):
        cp = Checkpoint(
            stage=stage,
            timestamp=U.now_ts(),
            data=data,
            completed=False
        )
        self.checkpoints.append(cp)
        cp_file = self.workdir / f"checkpoint_{stage}_{cp.timestamp}.json"
        cp_file.write_text(json.dumps(asdict(cp), indent=2))
        self.logger.debug(f"Checkpoint saved: {stage}")
    def mark_checkpoint_complete(self, stage: str):
        for cp in self.checkpoints:
            if cp.stage == stage and not cp.completed:
                cp.completed = True
                cp_file = self.workdir / f"checkpoint_{stage}_{cp.timestamp}.json"
                if cp_file.exists():
                    cp_data = json.loads(cp_file.read_text())
                    cp_data["completed"] = True
                    cp_file.write_text(json.dumps(cp_data, indent=2))
                self.logger.debug(f"Checkpoint completed: {stage}")
                break
    def recover_from_checkpoint(self, stage: str) -> Optional[Dict[str, Any]]:
        checkpoint_files = sorted(self.workdir.glob("checkpoint_*.json"))
        if not checkpoint_files:
            return None
        latest_completed = None
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
            task = progress.add_task("Recovering checkpoint", total=len(checkpoint_files))
            for cp_file in reversed(checkpoint_files):
                try:
                    cp_data = json.loads(cp_file.read_text())
                    if cp_data.get("completed") and cp_data.get("stage") != stage:
                        latest_completed = cp_data["data"]
                        self.logger.info(f"Recovering from checkpoint: {cp_data['stage']}")
                        break
                except Exception:
                    continue
                progress.update(task, advance=1)
        return latest_completed
    def cleanup_old_checkpoints(self, keep_last: int = 5):
        checkpoint_files = sorted(self.workdir.glob("checkpoint_*.json"))
        if len(checkpoint_files) <= keep_last:
            return
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), TimeElapsedColumn(), TimeRemainingColumn()) as progress:
            task = progress.add_task("Cleaning checkpoints", total=len(checkpoint_files) - keep_last)
            for cp_file in checkpoint_files[:-keep_last]:
                try:
                    cp_file.unlink()
                    self.logger.debug(f"Cleaned up old checkpoint: {cp_file}")
                except Exception:
                    pass
                progress.update(task, advance=1)
