"""后台维护：进程启动恢复与周期性超时回收。"""

from __future__ import annotations

import threading
from typing import Callable

from .fulfillment import FulfillmentService


class ReconcileLoop:
    """以固定间隔执行超时回收的守护线程。

    全部待履约状态都持久化在 SQLite 中，因此进程重启后先同步执行一次
    reconcile()，即可恢复在途订单并释放重启期间到期的占位。
    """

    def __init__(self, service: FulfillmentService, interval_seconds: float = 5.0,
                 sink: Callable[[dict], None] | None = None) -> None:
        self.service = service
        self.interval_seconds = interval_seconds
        self.sink = sink
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def startup_recovery(self) -> dict:
        """进程启动时同步执行一次回收。"""

        return self.service.reconcile()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="reconcile-loop", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                result = self.service.reconcile()
                if self.sink is not None:
                    self.sink(result)
            except Exception:  # noqa: BLE001 - 守护循环不能因单次失败退出
                pass

    def stop(self, timeout: float | None = None) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
