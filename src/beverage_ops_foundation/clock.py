"""提供可替换的 UTC 时钟。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    """定义服务所需的最小时钟接口。"""

    def now(self) -> datetime:
        """返回带时区的当前时间。"""


class SystemClock:
    """使用系统 UTC 时间。"""

    def now(self) -> datetime:
        """返回当前 UTC 时间。"""

        return datetime.now(timezone.utc)


class FixedClock:
    """为测试与离线验收提供固定时间。"""

    def __init__(self, value: datetime) -> None:
        if value.tzinfo is None:
            raise ValueError("固定时间必须包含时区")
        self._value = value.astimezone(timezone.utc)

    def now(self) -> datetime:
        """返回固定的 UTC 时间。"""

        return self._value


class MutableClock:
    """提供可被测试与验收脚本推进的时间。"""

    def __init__(self, value: datetime) -> None:
        if value.tzinfo is None:
            raise ValueError("初始时间必须包含时区")
        self._value = value.astimezone(timezone.utc)

    def now(self) -> datetime:
        """返回当前设置的 UTC 时间。"""

        return self._value

    def advance(self, seconds: float = 0, **kwargs: float) -> None:
        """按秒数或 timedelta 关键字推进时间。"""

        if kwargs:
            from datetime import timedelta

            self._value += timedelta(**kwargs)
        if seconds:
            from datetime import timedelta

            self._value += timedelta(seconds=seconds)
