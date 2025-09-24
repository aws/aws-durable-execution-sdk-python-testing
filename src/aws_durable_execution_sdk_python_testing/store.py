"""Datestore for the execution data."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.execution import Execution


class ExecutionStore(Protocol):
    # ignore cover because coverage doesn't understand elipses
    def save(self, execution: Execution) -> None: ...  # pragma: no cover
    def load(self, execution_arn: str) -> Execution: ...  # pragma: no cover
    def update(self, execution: Execution) -> None: ...  # pragma: no cover


class InMemoryExecutionStore(ExecutionStore):
    # Dict-based storage for testing
    def __init__(self) -> None:
        self._store: dict[str, Execution] = {}

    def save(self, execution: Execution) -> None:
        self._store[execution.durable_execution_arn] = execution

    def load(self, execution_arn: str) -> Execution:
        return self._store[execution_arn]

    def update(self, execution: Execution) -> None:
        self._store[execution.durable_execution_arn] = execution


# class SQLiteExecutionStore(ExecutionStore):
#     # SQLite persistence for web server
#     def __init__(self) -> None:
#         pass

#     def save(self, execution: Execution) -> None:
#         pass

#     def load(self, execution_arn: str) -> Execution:
#         return Execution.new()

#     def update(self, execution: Execution) -> None:
#         pass
