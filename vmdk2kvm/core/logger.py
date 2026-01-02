from __future__ import annotations

import datetime as _dt
import logging
import os
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

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


def _is_tty() -> bool:
    try:
        return sys.stderr.isatty()
    except Exception:
        return False


def _supports_unicode() -> bool:
    """
    Best-effort check: if the stream encoding can't handle emoji, degrade gracefully.
    """
    try:
        enc = getattr(sys.stderr, "encoding", None) or "utf-8"
        "✅".encode(enc)
        return True
    except Exception:
        return False


def c(
    text: str,
    color: Optional[str] = None,
    attrs: Optional[List[str]] = None,
    *,
    enable: bool = True,
) -> str:
    """Colorize text if termcolor is available and enabled."""
    if not enable or _colored is None or not color:
        return text
    try:
        return _colored(text, color=color, attrs=attrs or [])
    except Exception:
        return text


@dataclass(frozen=True)
class LogStyle:
    color: bool = True
    show_ms: bool = False
    show_src: bool = False     # module:line
    show_pid: bool = False
    show_thread: bool = False
    show_logger: bool = False  # logger name
    utc: bool = False
    indent_exceptions: bool = True
    exception_indent: int = 2
    align_level: int = 8       # width for level alignment (INFO/WARN/etc)
    unicode: bool = True       # emoji/unicode decorations


class EmojiFormatter(logging.Formatter):
    def __init__(self, style: LogStyle):
        super().__init__()
        self._style = style

    def _now(self, created: float) -> str:
        dt = (
            _dt.datetime.fromtimestamp(created, tz=_dt.timezone.utc)
            if self._style.utc
            else _dt.datetime.fromtimestamp(created)
        )
        return dt.strftime("%H:%M:%S.%f")[:-3] if self._style.show_ms else dt.strftime("%H:%M:%S")

    def _emoji(self, levelname: str) -> str:
        if not self._style.unicode:
            return "·"
        return _LEVEL_EMOJI.get(levelname, "•")

    def _prefix_bits(self, record: logging.LogRecord) -> str:
        bits: List[str] = []
        if self._style.show_pid:
            bits.append(f"pid={os.getpid()}")
        if self._style.show_thread:
            # record.threadName is always present; keep short-ish.
            bits.append(record.threadName)
        if self._style.show_logger:
            bits.append(record.name)
        if self._style.show_src:
            bits.append(f"{record.module}:{record.lineno}")
        return (" [" + " ".join(bits) + "]") if bits else ""

    def _format_exception_block(self, record: logging.LogRecord, color_ok: bool) -> str:
        exc_text = self.formatException(record.exc_info) if record.exc_info else ""
        if not exc_text:
            return ""

        if not self._style.indent_exceptions:
            return "\n" + exc_text

        indent = " " * max(0, int(self._style.exception_indent))
        # Indent every line; optionally tint it red for TTY.
        lines = exc_text.splitlines()
        block = "\n".join(indent + ln for ln in lines)
        if color_ok:
            block = c(block, "red", enable=True)
        return "\n" + block

    def format(self, record: logging.LogRecord) -> str:
        ts = self._now(record.created)
        emoji = self._emoji(record.levelname)

        lvl = record.levelname
        msg = record.getMessage()

        color_ok = bool(self._style.color and _is_tty() and _colored is not None)

        lvl = c(lvl, _LEVEL_COLOR.get(record.levelname), enable=color_ok)
        if record.levelno >= logging.WARNING:
            msg = c(msg, _LEVEL_COLOR.get(record.levelname), attrs=["bold"], enable=color_ok)

        bits = self._prefix_bits(record)

        line = f"{ts} {emoji} {lvl:<{self._style.align_level}}{bits} {msg}"
        line += self._format_exception_block(record, color_ok)
        return line


class Log:
    @staticmethod
    def _level_from_flags(verbose: int, quiet: int) -> int:
        """
        Typical CLI mapping:
          quiet=0: default INFO
          -q: WARNING
          -qq: ERROR
          -v: INFO
          -vv: DEBUG
        Quiet wins over verbose if both are set.
        """
        if quiet >= 2:
            return logging.ERROR
        if quiet == 1:
            return logging.WARNING
        if verbose >= 2:
            return logging.DEBUG
        return logging.INFO

    @staticmethod
    def banner(logger: logging.Logger, title: str, *, char: str = "─") -> None:
        """
        Pretty section separator for long workflows.
        """
        width = 72
        t = f" {title.strip()} "
        line = (char * max(8, (width - len(t)) // 2)) + t + (char * max(8, (width - len(t)) // 2))
        logger.info(line[:width])

    @staticmethod
    def step(logger: logging.Logger, msg: str) -> None:
        """
        A “doing X…” line that reads nicely in CLI output.
        """
        logger.info("➡️  %s", msg)

    @staticmethod
    def ok(logger: logging.Logger, msg: str) -> None:
        logger.info("✅ %s", msg)

    @staticmethod
    def warn(logger: logging.Logger, msg: str) -> None:
        logger.warning("⚠️  %s", msg)

    @staticmethod
    def fail(logger: logging.Logger, msg: str) -> None:
        logger.error("💥 %s", msg)

    @staticmethod
    def setup(
        verbose: int = 0,
        log_file: Optional[str] = None,
        *,
        quiet: int = 0,
        color: Optional[bool] = None,
        show_ms: bool = False,
        utc: bool = False,
        show_pid: bool = False,
        show_thread: bool = False,
        show_logger: bool = False,
        indent_exceptions: bool = True,
        logger_name: str = "vmdk2kvm",
    ) -> logging.Logger:
        """
        Create/refresh logger:
          - Avoids handler duplication on repeated setup()
          - Stream handler always goes to stderr (CLI-friendly)
          - Optional file handler (no ANSI color; richer context)
        """
        logger = logging.getLogger(logger_name)
        logger.propagate = False

        level = Log._level_from_flags(verbose, quiet)
        logger.setLevel(level)

        # Decide coloring: default on when termcolor exists, but only if TTY.
        if color is None:
            color = True

        unicode_ok = _supports_unicode()

        style = LogStyle(
            color=bool(color),
            show_ms=bool(show_ms or verbose >= 3),
            show_src=bool(verbose >= 3),
            show_pid=bool(show_pid or verbose >= 3),
            show_thread=bool(show_thread or verbose >= 4),
            show_logger=bool(show_logger or verbose >= 4),
            utc=bool(utc),
            indent_exceptions=bool(indent_exceptions),
            unicode=bool(unicode_ok),
        )

        # Clear existing handlers safely (prevents dupes)
        for h in list(logger.handlers):
            logger.removeHandler(h)
            try:
                h.close()
            except Exception:
                pass

        sh = logging.StreamHandler(stream=sys.stderr)
        sh.setLevel(level)
        sh.setFormatter(EmojiFormatter(style))
        logger.addHandler(sh)

        if log_file:
            fp = Path(log_file).expanduser().resolve()
            fp.parent.mkdir(parents=True, exist_ok=True)

            # File format: disable color; include ms + source + pid for forensics.
            file_style = LogStyle(
                color=False,
                show_ms=True,
                show_src=True,
                show_pid=True,
                show_thread=True,
                show_logger=True,
                utc=style.utc,
                indent_exceptions=True,
                unicode=style.unicode,  # keep emoji if encoding supports it
            )
            fh = logging.FileHandler(fp, encoding="utf-8")
            fh.setLevel(level)
            fh.setFormatter(EmojiFormatter(file_style))
            logger.addHandler(fh)

        logger.debug("Logger initialized (level=%s, pid=%s)", logging.getLevelName(level), os.getpid())
        return logger
