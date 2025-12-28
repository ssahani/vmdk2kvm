from __future__ import annotations

class BaseFixer:
    def run(self) -> int:
        raise NotImplementedError
