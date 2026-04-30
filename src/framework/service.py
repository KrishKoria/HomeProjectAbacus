from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final


_HEALTHY_PREFIX: Final[str] = "OK"
_UNHEALTHY_PREFIX: Final[str] = "FAIL"


@dataclass(frozen=True, slots=True)
class HealthCheckResult:
    service_name: str
    healthy: bool
    message: str
    details: dict[str, str] = field(default_factory=dict)

    def summary_line(self) -> str:
        prefix = _HEALTHY_PREFIX if self.healthy else _UNHEALTHY_PREFIX
        return f"{prefix}: {self.service_name} - {self.message}"


__all__ = ["HealthCheckResult"]
