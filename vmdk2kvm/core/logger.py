from __future__ import annotations
import datetime as _dt
import logging
from typing import List, Optional

from pathlib import Path
# Optional: colors
try:
    from termcolor import colored as _colored  # type: ignore
except Exception:  # pragma: no cover
    _colored = None

_LEVEL_EMOJI = {
    "DEBUG": "🔍",
    "INFO": "✅",
    "WARNING": "⚠️",
    "ERROR": "💥",
    "CRITICAL": "🧨",
}
_LEVEL_COLOR = {
    "DEBUG": "blue",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "red",
}
def c(text: str, color: Optional[str] = None, attrs: Optional[List[str]] = None) -> str:
    """Colorize text if termcolor is available."""
    if _colored is None or not color:
        return text
    try:
        return _colored(text, color=color, attrs=attrs or [])
    except Exception:
        return text
class EmojiFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = _dt.datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        emoji = _LEVEL_EMOJI.get(record.levelname, "•")
        lvl = record.levelname
        msg = record.getMessage()
        if _colored is not None:
            lvl = c(lvl, _LEVEL_COLOR.get(record.levelname))
            if record.levelno >= logging.WARNING:
                msg = c(msg, _LEVEL_COLOR.get(record.levelname), attrs=["bold"])
        return f"{ts} {emoji} {lvl:<8} {msg}"
class Log:
    @staticmethod
    def setup(verbose: int, log_file: Optional[str]) -> logging.Logger:
        logger = logging.getLogger("vmdk2kvm")
        logger.handlers.clear()
        logger.propagate = False
        level = logging.DEBUG if verbose >= 2 else logging.INFO
        logger.setLevel(level)
        fmt = EmojiFormatter()
        sh = logging.StreamHandler()
        sh.setLevel(level)
        sh.setFormatter(fmt)
        logger.addHandler(sh)
        if log_file:
            fp = Path(log_file).expanduser().resolve()
            fp.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(fp, encoding="utf-8")
            fh.setLevel(level)
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        logger.debug("Logger initialized")
        return logger
