"""Execution stores for persisting durable function executions."""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Protocol

from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.execution import Execution


class StoreType(Enum):
    """Supported execution store types."""

    MEMORY = "memory"
    FILESYSTEM = "filesystem"


class ExecutionStore(Protocol):
    """Protocol for execution storage implementations."""

    # ignore cover because coverage doesn't understand elipses
    def save(self, execution: Execution) -> None: ...  # pragma: no cover
    def load(self, execution_arn: str) -> Execution: ...  # pragma: no cover
    def update(self, execution: Execution) -> None: ...  # pragma: no cover
    def list_all(self) -> list[Execution]: ...  # pragma: no cover


__all__ = [
    "StoreType",
    "ExecutionStore",
    "InMemoryExecutionStore",
    "FileSystemExecutionStore",
]
