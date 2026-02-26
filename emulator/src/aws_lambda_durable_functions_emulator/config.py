import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

from aws_durable_execution_sdk_python_testing.stores.base import StoreType
from aws_durable_execution_sdk_python_testing.web.server import WebServiceConfig

# Constants
MAX_PORT = 65535


def get_host() -> str:
    """Get the server host from environment variable or default."""
    return os.environ.get("HOST", "0.0.0.0")


def get_port() -> int:
    """Get the server port from environment variable or default."""
    return int(os.environ.get("PORT", "5000"))


def get_log_level() -> int:
    """Get the logging level from environment variable or default."""
    log_level_str = os.environ.get("LOG", "INFO").upper()
    return getattr(logging, log_level_str, logging.INFO)


def get_lambda_endpoint() -> str:
    """Get the Lambda endpoint from environment variable."""
    return os.environ.get("LAMBDA_ENDPOINT", "http://localhost:3001")


def get_storage_dir() -> str | None:
    """Get the storage directory from environment variable."""
    return os.environ.get("STORAGE_DIR")


def get_execution_store_type() -> str:
    """Get the execution store type from environment variable."""
    return os.environ.get("EXECUTION_STORE_TYPE", StoreType.SQLITE.value).lower()


@dataclass
class EmulatorConfig:
    """Configuration for the AWS Lambda Durable Functions Emulator."""

    host: str = field(default_factory=get_host)
    port: int = field(default_factory=get_port)
    log_level: int = field(default_factory=get_log_level)
    lambda_endpoint: str = field(default_factory=get_lambda_endpoint)
    storage_dir: str | None = field(default_factory=get_storage_dir)
    execution_store_type: str = field(default_factory=get_execution_store_type)

    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate_config()

    def to_web_service_config(self):
        """Convert to testing library web service config."""
        return WebServiceConfig(
            host=self.host, port=self.port, log_level=self.log_level
        )

    def _validate_config(self):
        """Validate all configuration parameters."""

        # Validate Lambda endpoint URL
        def _raise_invalid_endpoint(
            endpoint: str, cause: Exception | None = None
        ) -> None:
            msg = f"Invalid Lambda endpoint URL: {endpoint}"
            raise ValueError(msg) from cause

        try:
            parsed = urlparse(self.lambda_endpoint)
            if not parsed.scheme or not parsed.netloc:
                _raise_invalid_endpoint(self.lambda_endpoint)
        except (ValueError, TypeError) as e:
            _raise_invalid_endpoint(self.lambda_endpoint, e)

        # Validate storage directory if specified
        if self.storage_dir:

            def _raise_storage_error(
                storage_dir: str, cause: Exception | None = None
            ) -> None:
                msg = f"Storage directory is not writable: {storage_dir}"
                raise ValueError(msg) from cause

            def _raise_access_error(
                storage_dir: str, cause: Exception | None = None
            ) -> None:
                msg = f"Cannot access storage directory: {storage_dir}"
                raise ValueError(msg) from cause

            try:
                storage_path = Path(self.storage_dir)
                if storage_path.exists():
                    if not storage_path.is_dir():
                        msg = f"Storage path is not a directory: {self.storage_dir}"
                        raise ValueError(msg)
                    # Test write permissions
                    test_file = storage_path / ".write_test"
                    try:
                        test_file.write_text("test")
                        test_file.unlink()
                    except (OSError, PermissionError) as e:
                        _raise_storage_error(self.storage_dir, e)
                else:
                    # Try to create the directory
                    storage_path.mkdir(parents=True, exist_ok=True)
            except (OSError, PermissionError) as e:
                _raise_access_error(self.storage_dir, e)

        # Validate log level
        valid_log_levels = [
            logging.DEBUG,
            logging.INFO,
            logging.WARNING,
            logging.ERROR,
            logging.CRITICAL,
        ]
        if self.log_level not in valid_log_levels:
            msg = f"Invalid log level: {self.log_level}. Must be one of {valid_log_levels}"
            raise ValueError(msg)

        # Validate port range
        if not (1 <= self.port <= MAX_PORT):
            msg = f"Invalid port: {self.port}. Must be between 1 and {MAX_PORT}"
            raise ValueError(msg)

        # Validate execution store type
        valid_store_types = [StoreType.FILESYSTEM.value, StoreType.SQLITE.value]
        if self.execution_store_type not in valid_store_types:
            msg = f"Invalid execution store type: {self.execution_store_type}. Must be one of {valid_store_types}"
            raise ValueError(msg)
