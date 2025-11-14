"""File system-based execution store implementation."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from aws_durable_execution_sdk_python_testing.exceptions import (
    ResourceNotFoundException,
)
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.stores.base import (
    BaseExecutionStore,
)


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.timestamp()
        return super().default(obj)


def datetime_object_hook(obj):
    """JSON object hook to convert unix timestamps back to datetime objects."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, int | float) and key.endswith(
                ("_timestamp", "_time", "Timestamp", "Time")
            ):
                try:  # noqa: SIM105
                    obj[key] = datetime.fromtimestamp(value, tz=UTC)
                except (ValueError, OSError):
                    # Leave as number if not a valid timestamp
                    pass
    return obj


class FileSystemExecutionStore(BaseExecutionStore):
    """File system-based execution store for persistence."""

    def __init__(self, storage_dir: Path) -> None:
        self._storage_dir = storage_dir

    @classmethod
    def create(cls, storage_dir: str | Path | None = None) -> FileSystemExecutionStore:
        """Create a FileSystemExecutionStore with directory creation.

        Args:
            storage_dir: Directory path for storage. Defaults to '.durable_executions'

        Returns:
            FileSystemExecutionStore instance with created directory
        """
        path = Path(storage_dir) if storage_dir else Path(".durable_executions")
        path.mkdir(exist_ok=True)
        return cls(storage_dir=path)

    def _get_file_path(self, execution_arn: str) -> Path:
        """Get file path for execution ARN."""
        # Use ARN as filename with .json extension, replacing unsafe characters
        safe_filename = execution_arn.replace(":", "_").replace("/", "_")
        return self._storage_dir / f"{safe_filename}.json"

    def save(self, execution: Execution) -> None:
        """Save execution to file system."""
        file_path = self._get_file_path(execution.durable_execution_arn)
        data = execution.to_dict()

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, cls=DateTimeEncoder)

    def load(self, execution_arn: str) -> Execution:
        """Load execution from file system."""
        file_path = self._get_file_path(execution_arn)
        if not file_path.exists():
            msg = f"Execution {execution_arn} not found"
            raise ResourceNotFoundException(msg)

        with open(file_path, encoding="utf-8") as f:
            data = json.load(f, object_hook=datetime_object_hook)

        return Execution.from_dict(data)

    def update(self, execution: Execution) -> None:
        """Update execution in file system (same as save)."""
        self.save(execution)

    def list_all(self) -> list[Execution]:
        """List all executions from file system."""
        executions = []
        for file_path in self._storage_dir.glob("*.json"):
            try:
                with open(file_path, encoding="utf-8") as f:
                    data = json.load(f, object_hook=datetime_object_hook)
                executions.append(Execution.from_dict(data))
            except (json.JSONDecodeError, KeyError, OSError) as e:
                logging.warning("Skipping corrupted file %s: %s", file_path, e)
                continue
        return executions
