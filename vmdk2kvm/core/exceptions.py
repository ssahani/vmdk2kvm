# vmdk2kvm/core/exceptions.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


def _safe_int(x: Any, default: int = 1) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _one_line(s: str, limit: int = 600) -> str:
    s = (s or "").strip().replace("\r", " ").replace("\n", " ")
    s = " ".join(s.split())
    return s if len(s) <= limit else (s[: limit - 3] + "...")


@dataclass(eq=False)
class Vmdk2KvmError(Exception):
    """
    Base project error with:
      - stable fields for reporting/JSON
      - readable __str__ (what users see)
      - safe code handling (never crashes on int())
    """
    code: int = 1
    msg: str = "error"
    cause: Optional[BaseException] = None
    context: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        self.code = _safe_int(self.code, default=1)
        self.msg = _one_line(self.msg)
        super().__init__(self.msg)

    def with_context(self, **ctx: Any) -> "Vmdk2KvmError":
        if self.context is None:
            self.context = {}
        self.context.update(ctx)
        return self

    def user_message(self, *, include_context: bool = False, include_cause: bool = False) -> str:
        """
        Human-friendly message for CLI output/logs.
        """
        base = self.msg or self.__class__.__name__

        parts = [base]

        if include_context and self.context:
            # Keep it compact and stable
            kv = ", ".join(f"{k}={self.context[k]!r}" for k in sorted(self.context.keys()))
            parts.append(f"[{kv}]")

        if include_cause and self.cause is not None:
            parts.append(f"(cause: {type(self.cause).__name__}: {_one_line(str(self.cause))})")

        return " ".join(parts)

    def __str__(self) -> str:
        # Default string should be clean and user-facing
        return self.user_message(include_context=False, include_cause=False)

    def to_dict(self, *, include_cause: bool = False) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "type": self.__class__.__name__,
            "code": self.code,
            "message": self.msg,
            "context": self.context or {},
        }
        if include_cause and self.cause is not None:
            d["cause"] = {"type": type(self.cause).__name__, "message": _one_line(str(self.cause))}
        return d


class Fatal(Vmdk2KvmError):
    """
    User-facing fatal error (exit code should be honored by top-level main()).
    """
    pass


class VMwareError(Vmdk2KvmError):
    """
    vSphere/vCenter operation failed.
    Use for pyvmomi / SDK / ESXi errors.
    """
    pass


def wrap_fatal(code: int, msg: str, exc: Optional[BaseException] = None, **context: Any) -> Fatal:
    return Fatal(code=code, msg=msg, cause=exc, context=context or None)


def wrap_vmware(msg: str, exc: Optional[BaseException] = None, code: int = 50, **context: Any) -> VMwareError:
    return VMwareError(code=code, msg=msg, cause=exc, context=context or None)


def format_exception_for_cli(e: BaseException, *, verbose: int = 0) -> str:
    """
    One-liner output for CLI.

    verbose=0: just message
    verbose=1: message + compact context (if any)
    verbose>=2: message + context + cause
    """
    if isinstance(e, Vmdk2KvmError):
        return e.user_message(
            include_context=(verbose >= 1),
            include_cause=(verbose >= 2),
        )

    # Non-project exceptions: keep them short unless verbose
    if verbose >= 2:
        return f"{type(e).__name__}: {_one_line(str(e))}"
    return _one_line(str(e)) or type(e).__name__
