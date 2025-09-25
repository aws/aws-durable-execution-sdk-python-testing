"""Checkpoint processors can notify the Execution of notable event state changes. Observer pattern."""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.lambda_service import ErrorObject


class ExecutionObserver(ABC):
    """Observer for execution lifecycle events."""

    @abstractmethod
    def on_completed(self, execution_arn: str, result: str | None = None) -> None:
        """Called when execution completes successfully."""

    @abstractmethod
    def on_failed(self, execution_arn: str, error: ErrorObject) -> None:
        """Called when execution fails."""

    @abstractmethod
    def on_wait_timer_scheduled(
        self, execution_arn: str, operation_id: str, delay: float
    ) -> None:
        """Called when wait timer scheduled."""

    @abstractmethod
    def on_step_retry_scheduled(
        self, execution_arn: str, operation_id: str, delay: float
    ) -> None:
        """Called when step retry scheduled."""


class ExecutionNotifier:
    """Notifies observers about execution events. Thread-safe."""

    def __init__(self) -> None:
        self._observers: list[ExecutionObserver] = []
        self._lock = threading.RLock()

    def add_observer(self, observer: ExecutionObserver) -> None:
        """Add an observer to be notified of execution events."""
        with self._lock:
            self._observers.append(observer)

    def _notify_observers(self, method: Callable, *args, **kwargs) -> None:
        """Notify all observers by calling the specified method."""
        with self._lock:
            observers = self._observers.copy()
        for observer in observers:
            getattr(observer, method.__name__)(*args, **kwargs)

    # region event emitters
    def notify_completed(self, execution_arn: str, result: str | None = None) -> None:
        """Notify observers about execution completion."""
        self._notify_observers(
            ExecutionObserver.on_completed, execution_arn=execution_arn, result=result
        )

    def notify_failed(self, execution_arn: str, error: ErrorObject) -> None:
        """Notify observers about execution failure."""
        self._notify_observers(
            ExecutionObserver.on_failed, execution_arn=execution_arn, error=error
        )

    def notify_wait_timer_scheduled(
        self, execution_arn: str, operation_id: str, delay: float
    ) -> None:
        """Notify observers about wait timer scheduling."""
        self._notify_observers(
            ExecutionObserver.on_wait_timer_scheduled,
            execution_arn=execution_arn,
            operation_id=operation_id,
            delay=delay,
        )

    def notify_step_retry_scheduled(
        self, execution_arn: str, operation_id: str, delay: float
    ) -> None:
        """Notify observers about step retry scheduling."""
        self._notify_observers(
            ExecutionObserver.on_step_retry_scheduled,
            execution_arn=execution_arn,
            operation_id=operation_id,
            delay=delay,
        )

    # endregion event emitters
