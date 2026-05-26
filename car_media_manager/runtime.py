from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from datetime import timezone


@dataclass(frozen=True, slots=True)
class RuntimeMessage:
    level: str
    code: str
    message: str
    created_at: datetime

    def as_dict(self) -> dict[str, str]:
        return {
            "level": self.level,
            "code": self.code,
            "message": self.message,
            "created_at": self.created_at.isoformat(),
        }


_last_message: RuntimeMessage | None = None


def set_message(*, level: str, code: str, message: str) -> None:
    global _last_message
    _last_message = RuntimeMessage(
        level=level,
        code=code,
        message=message,
        created_at=datetime.now(tz=timezone.utc),
    )


def clear_message(*, code: str | None = None) -> None:
    global _last_message
    if code is None or (_last_message is not None and _last_message.code == code):
        _last_message = None


def get_message() -> dict[str, str] | None:
    if _last_message is None:
        return None
    return _last_message.as_dict()
