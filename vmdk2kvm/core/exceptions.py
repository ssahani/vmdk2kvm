from __future__ import annotations

class Fatal(Exception):
    def __init__(self, code: int, msg: str):
        super().__init__(msg)
        self.code = int(code)

class VMwareError(Exception):
    """vSphere/vCenter operation failed."""
    pass
