"""Factory for creating testing library components with emulator configuration."""

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

import aws_durable_execution_sdk_python
import botocore.loaders
from aws_durable_execution_sdk_python_testing.checkpoint.processor import CheckpointProcessor
from aws_durable_execution_sdk_python_testing.executor import Executor
from aws_durable_execution_sdk_python_testing.invoker import LambdaInvoker
from aws_durable_execution_sdk_python_testing.scheduler import Scheduler
from aws_durable_execution_sdk_python_testing.stores.base import (
    StoreType,
)
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.sqlite import (
    SQLiteExecutionStore,
)

if TYPE_CHECKING:
    from aws_lambda_durable_functions_emulator.config import EmulatorConfig

logger = logging.getLogger(__name__)


class TestingLibraryComponentFactory:
    """Factory for creating testing library components."""

    @staticmethod
    def create_store(config: "EmulatorConfig"):
        """Create execution store based on emulator configuration."""
        store_type = config.execution_store_type

        if store_type == StoreType.SQLITE.value:
            logger.info("Creating SQLite execution store")
            if config.storage_dir:
                db_path = Path(config.storage_dir) / "durable-executions.db"
            else:
                db_path = Path("durable-executions.db")
            return SQLiteExecutionStore.create_and_initialize(db_path)

        logger.info("Creating file-system execution store")
        return FileSystemExecutionStore.create(config.storage_dir or ".")

    @staticmethod
    def create_scheduler():
        """Create scheduler for timer and event management."""
        logger.info("Creating scheduler")
        scheduler = Scheduler()
        logger.info("Starting scheduler")
        scheduler.start()
        return scheduler

    @staticmethod
    def create_invoker(config: "EmulatorConfig"):
        """Create Lambda invoker with emulator configuration."""
        logger.info("Creating Lambda invoker with endpoint: %s", config.lambda_endpoint)

        # Load lambdainternal service model
        package_path = os.path.dirname(aws_durable_execution_sdk_python.__file__)
        data_path = f"{package_path}/botocore/data"
        os.environ["AWS_DATA_PATH"] = data_path

        loader = botocore.loaders.Loader()
        loader.search_paths.append(data_path)

        return LambdaInvoker.create(config.lambda_endpoint, "us-east-1")

    @staticmethod
    def create_checkpoint_processor(store, scheduler):
        logger.info("Creating checkpoint processor")
        checkpoint_processor = CheckpointProcessor(store, scheduler)
        logger.info("Created checkpoint processor")
        return checkpoint_processor

    @staticmethod
    def create_executor(config: "EmulatorConfig"):
        """Create complete executor with all components."""
        logger.info("Creating executor with all components")

        store = TestingLibraryComponentFactory.create_store(config)
        scheduler = TestingLibraryComponentFactory.create_scheduler()
        invoker = TestingLibraryComponentFactory.create_invoker(config)
        checkpoint_processor = TestingLibraryComponentFactory.create_checkpoint_processor(store, scheduler)

        executor = Executor(store, scheduler, invoker, checkpoint_processor)
        checkpoint_processor.add_execution_observer(executor)

        return executor
