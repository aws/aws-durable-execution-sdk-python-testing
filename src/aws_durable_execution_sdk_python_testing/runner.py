from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    ParamSpec,
    Protocol,
    Self,
    TypeVar,
    cast,
)

import aws_durable_execution_sdk_python
import boto3  # type: ignore
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_handler,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from aws_durable_execution_sdk_python.lambda_service import Operation as SvcOperation

from aws_durable_execution_sdk_python_testing.checkpoint.processor import (
    CheckpointProcessor,
)
from aws_durable_execution_sdk_python_testing.client import InMemoryServiceClient
from aws_durable_execution_sdk_python_testing.exceptions import (
    DurableFunctionsLocalRunnerError,
    DurableFunctionsTestError,
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.executor import Executor
from aws_durable_execution_sdk_python_testing.invoker import (
    InProcessInvoker,
    LambdaInvoker,
)
from aws_durable_execution_sdk_python_testing.model import (
    StartDurableExecutionInput,
    StartDurableExecutionOutput,
)
from aws_durable_execution_sdk_python_testing.scheduler import Scheduler
from aws_durable_execution_sdk_python_testing.stores.base import (
    ExecutionStore,
    StoreType,
)
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)
from aws_durable_execution_sdk_python_testing.web.server import WebServer


if TYPE_CHECKING:
    import datetime
    from collections.abc import Callable, MutableMapping

    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import InvocationStatus

    from aws_durable_execution_sdk_python_testing.execution import Execution
    from aws_durable_execution_sdk_python_testing.web.server import WebServiceConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WebRunnerConfig:
    """Configuration for the WebRunner using composition pattern.

    This configuration class encapsulates all settings needed to run the web server
    for durable functions testing, including HTTP server configuration and Lambda
    service configuration.
    """

    # HTTP server configuration (existing WebServiceConfig)
    web_service: WebServiceConfig

    # Lambda service configuration (web runner specific)
    lambda_endpoint: str = "http://127.0.0.1:3001"
    local_runner_endpoint: str = "http://0.0.0.0:5000"
    local_runner_region: str = "us-west-2"
    local_runner_mode: str = "local"

    # Store configuration
    store_type: StoreType = StoreType.MEMORY
    store_path: str | None = None  # Path for filesystem store


@dataclass(frozen=True)
class Operation:
    operation_id: str
    operation_type: OperationType
    status: OperationStatus
    parent_id: str | None = field(default=None, kw_only=True)
    name: str | None = field(default=None, kw_only=True)
    sub_type: OperationSubType | None = field(default=None, kw_only=True)
    start_timestamp: datetime.datetime | None = field(default=None, kw_only=True)
    end_timestamp: datetime.datetime | None = field(default=None, kw_only=True)


T = TypeVar("T", bound=Operation)
P = ParamSpec("P")


class OperationFactory(Protocol):
    @staticmethod
    def from_svc_operation(
        operation: SvcOperation, all_operations: list[SvcOperation] | None = None
    ) -> Operation: ...


@dataclass(frozen=True)
class ExecutionOperation(Operation):
    input_payload: str | None = None

    @staticmethod
    def from_svc_operation(
        operation: SvcOperation,
        all_operations: list[SvcOperation] | None = None,  # noqa: ARG004
    ) -> ExecutionOperation:
        if operation.operation_type != OperationType.EXECUTION:
            msg: str = f"Expected EXECUTION operation, got {operation.operation_type}"
            raise InvalidParameterValueException(msg)
        return ExecutionOperation(
            operation_id=operation.operation_id,
            operation_type=operation.operation_type,
            status=operation.status,
            parent_id=operation.parent_id,
            name=operation.name,
            sub_type=operation.sub_type,
            start_timestamp=operation.start_timestamp,
            end_timestamp=operation.end_timestamp,
            input_payload=(
                operation.execution_details.input_payload
                if operation.execution_details
                else None
            ),
        )


@dataclass(frozen=True)
class ContextOperation(Operation):
    child_operations: list[Operation]
    result: Any = None
    error: ErrorObject | None = None

    @staticmethod
    def from_svc_operation(
        operation: SvcOperation, all_operations: list[SvcOperation] | None = None
    ) -> ContextOperation:
        if operation.operation_type != OperationType.CONTEXT:
            msg: str = f"Expected CONTEXT operation, got {operation.operation_type}"
            raise InvalidParameterValueException(msg)

        child_operations = []
        if all_operations:
            child_operations = [
                create_operation(op, all_operations)
                for op in all_operations
                if op.parent_id == operation.operation_id
            ]

        return ContextOperation(
            operation_id=operation.operation_id,
            operation_type=operation.operation_type,
            status=operation.status,
            parent_id=operation.parent_id,
            name=operation.name,
            sub_type=operation.sub_type,
            start_timestamp=operation.start_timestamp,
            end_timestamp=operation.end_timestamp,
            child_operations=child_operations,
            result=(
                json.loads(operation.context_details.result)
                if operation.context_details and operation.context_details.result
                else None
            ),
            error=operation.context_details.error
            if operation.context_details
            else None,
        )

    def get_operation_by_name(self, name: str) -> Operation:
        for operation in self.child_operations:
            if operation.name == name:
                return operation
        msg: str = f"Child Operation with name '{name}' not found"
        raise DurableFunctionsTestError(msg)

    def get_step(self, name: str) -> StepOperation:
        return cast(StepOperation, self.get_operation_by_name(name))

    def get_wait(self, name: str) -> WaitOperation:
        return cast(WaitOperation, self.get_operation_by_name(name))

    def get_context(self, name: str) -> ContextOperation:
        return cast(ContextOperation, self.get_operation_by_name(name))

    def get_callback(self, name: str) -> CallbackOperation:
        return cast(CallbackOperation, self.get_operation_by_name(name))

    def get_invoke(self, name: str) -> InvokeOperation:
        return cast(InvokeOperation, self.get_operation_by_name(name))

    def get_execution(self, name: str) -> ExecutionOperation:
        return cast(ExecutionOperation, self.get_operation_by_name(name))


@dataclass(frozen=True)
class StepOperation(ContextOperation):
    attempt: int = 0
    next_attempt_timestamp: datetime.datetime | None = None
    result: Any = None
    error: ErrorObject | None = None

    @staticmethod
    def from_svc_operation(
        operation: SvcOperation, all_operations: list[SvcOperation] | None = None
    ) -> StepOperation:
        if operation.operation_type != OperationType.STEP:
            msg: str = f"Expected STEP operation, got {operation.operation_type}"
            raise InvalidParameterValueException(msg)

        child_operations = []
        if all_operations:
            child_operations = [
                create_operation(op, all_operations)
                for op in all_operations
                if op.parent_id == operation.operation_id
            ]

        return StepOperation(
            operation_id=operation.operation_id,
            operation_type=operation.operation_type,
            status=operation.status,
            parent_id=operation.parent_id,
            name=operation.name,
            sub_type=operation.sub_type,
            start_timestamp=operation.start_timestamp,
            end_timestamp=operation.end_timestamp,
            child_operations=child_operations,
            attempt=operation.step_details.attempt if operation.step_details else 0,
            next_attempt_timestamp=(
                operation.step_details.next_attempt_timestamp
                if operation.step_details
                else None
            ),
            result=(
                json.loads(operation.step_details.result)
                if operation.step_details and operation.step_details.result
                else None
            ),
            error=operation.step_details.error if operation.step_details else None,
        )


@dataclass(frozen=True)
class WaitOperation(Operation):
    scheduled_timestamp: datetime.datetime | None = None

    @staticmethod
    def from_svc_operation(
        operation: SvcOperation,
        all_operations: list[SvcOperation] | None = None,  # noqa: ARG004
    ) -> WaitOperation:
        if operation.operation_type != OperationType.WAIT:
            msg: str = f"Expected WAIT operation, got {operation.operation_type}"
            raise InvalidParameterValueException(msg)
        return WaitOperation(
            operation_id=operation.operation_id,
            operation_type=operation.operation_type,
            status=operation.status,
            parent_id=operation.parent_id,
            name=operation.name,
            sub_type=operation.sub_type,
            start_timestamp=operation.start_timestamp,
            end_timestamp=operation.end_timestamp,
            scheduled_timestamp=(
                operation.wait_details.scheduled_timestamp
                if operation.wait_details
                else None
            ),
        )


@dataclass(frozen=True)
class CallbackOperation(ContextOperation):
    callback_id: str | None = None
    result: Any = None
    error: ErrorObject | None = None

    @staticmethod
    def from_svc_operation(
        operation: SvcOperation, all_operations: list[SvcOperation] | None = None
    ) -> CallbackOperation:
        if operation.operation_type != OperationType.CALLBACK:
            msg: str = f"Expected CALLBACK operation, got {operation.operation_type}"
            raise InvalidParameterValueException(msg)

        child_operations = []
        if all_operations:
            child_operations = [
                create_operation(op, all_operations)
                for op in all_operations
                if op.parent_id == operation.operation_id
            ]

        return CallbackOperation(
            operation_id=operation.operation_id,
            operation_type=operation.operation_type,
            status=operation.status,
            parent_id=operation.parent_id,
            name=operation.name,
            sub_type=operation.sub_type,
            start_timestamp=operation.start_timestamp,
            end_timestamp=operation.end_timestamp,
            child_operations=child_operations,
            callback_id=(
                operation.callback_details.callback_id
                if operation.callback_details
                else None
            ),
            result=(
                json.loads(operation.callback_details.result)
                if operation.callback_details and operation.callback_details.result
                else None
            ),
            error=operation.callback_details.error
            if operation.callback_details
            else None,
        )


@dataclass(frozen=True)
class InvokeOperation(Operation):
    result: Any = None
    error: ErrorObject | None = None

    @staticmethod
    def from_svc_operation(
        operation: SvcOperation,
        all_operations: list[SvcOperation] | None = None,  # noqa: ARG004
    ) -> InvokeOperation:
        if operation.operation_type != OperationType.CHAINED_INVOKE:
            msg: str = f"Expected INVOKE operation, got {operation.operation_type}"
            raise InvalidParameterValueException(msg)
        return InvokeOperation(
            operation_id=operation.operation_id,
            operation_type=operation.operation_type,
            status=operation.status,
            parent_id=operation.parent_id,
            name=operation.name,
            sub_type=operation.sub_type,
            start_timestamp=operation.start_timestamp,
            end_timestamp=operation.end_timestamp,
            result=(
                json.loads(operation.chained_invoke_details.result)
                if operation.chained_invoke_details
                and operation.chained_invoke_details.result
                else None
            ),
            error=operation.chained_invoke_details.error
            if operation.chained_invoke_details
            else None,
        )


OPERATION_FACTORIES: MutableMapping[OperationType, type[OperationFactory]] = {
    OperationType.EXECUTION: ExecutionOperation,
    OperationType.CONTEXT: ContextOperation,
    OperationType.STEP: StepOperation,
    OperationType.WAIT: WaitOperation,
    OperationType.CHAINED_INVOKE: InvokeOperation,
    OperationType.CALLBACK: CallbackOperation,
}


def create_operation(
    svc_operation: SvcOperation, all_operations: list[SvcOperation] | None = None
) -> Operation:
    operation_class: type[OperationFactory] | None = OPERATION_FACTORIES.get(
        svc_operation.operation_type
    )
    if not operation_class:
        msg: str = f"Unknown operation type: {svc_operation.operation_type}"
        raise DurableFunctionsTestError(msg)
    return operation_class.from_svc_operation(svc_operation, all_operations)


@dataclass(frozen=True)
class DurableFunctionTestResult:
    status: InvocationStatus
    operations: list[Operation]
    result: Any = None
    error: ErrorObject | None = None

    @classmethod
    def create(cls, execution: Execution) -> DurableFunctionTestResult:
        operations = []
        for operation in execution.operations:
            if operation.operation_type is OperationType.EXECUTION:
                # don't want the EXECUTION operations in the list test code asserts against
                continue

            if operation.parent_id is None:
                operations.append(create_operation(operation, execution.operations))

        if execution.result is None:
            msg: str = "Execution result must exist to create test result."
            raise DurableFunctionsTestError(msg)

        deserialized_result = (
            json.loads(execution.result.result) if execution.result.result else None
        )

        return cls(
            status=execution.result.status,
            operations=operations,
            result=deserialized_result,
            error=execution.result.error,
        )

    def get_operation_by_name(self, name: str) -> Operation:
        for operation in self.operations:
            if operation.name == name:
                return operation
        msg: str = f"Operation with name '{name}' not found"
        raise DurableFunctionsTestError(msg)

    def get_step(self, name: str) -> StepOperation:
        return cast(StepOperation, self.get_operation_by_name(name))

    def get_wait(self, name: str) -> WaitOperation:
        return cast(WaitOperation, self.get_operation_by_name(name))

    def get_context(self, name: str) -> ContextOperation:
        return cast(ContextOperation, self.get_operation_by_name(name))

    def get_callback(self, name: str) -> CallbackOperation:
        return cast(CallbackOperation, self.get_operation_by_name(name))

    def get_invoke(self, name: str) -> InvokeOperation:
        return cast(InvokeOperation, self.get_operation_by_name(name))

    def get_execution(self, name: str) -> ExecutionOperation:
        return cast(ExecutionOperation, self.get_operation_by_name(name))


class DurableFunctionTestRunner:
    def __init__(self, handler: Callable):
        self._scheduler: Scheduler = Scheduler()
        self._scheduler.start()
        self._store = InMemoryExecutionStore()
        self._checkpoint_processor = CheckpointProcessor(
            store=self._store, scheduler=self._scheduler
        )
        self._service_client = InMemoryServiceClient(self._checkpoint_processor)
        self._invoker = InProcessInvoker(handler, self._service_client)
        self._executor = Executor(
            store=self._store, scheduler=self._scheduler, invoker=self._invoker
        )

        # Wire up observer pattern - CheckpointProcessor uses this to notify executor of state changes
        self._checkpoint_processor.add_execution_observer(self._executor)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._scheduler.stop()

    def run(
        self,
        input: str | None = None,  # noqa: A002
        timeout: int = 900,
        function_name: str = "test-function",
        execution_name: str = "execution-name",
        account_id: str = "123456789012",
    ) -> DurableFunctionTestResult:
        start_input = StartDurableExecutionInput(
            account_id=account_id,
            function_name=function_name,
            function_qualifier="$LATEST",
            execution_name=execution_name,
            execution_timeout_seconds=timeout,
            execution_retention_period_days=7,
            invocation_id="inv-12345678-1234-1234-1234-123456789012",
            trace_fields={"trace_id": "abc123", "span_id": "def456"},
            tenant_id="tenant-001",
            input=input,
        )

        output: StartDurableExecutionOutput = self._executor.start_execution(
            start_input
        )

        if output.execution_arn is None:
            msg_arn: str = "Execution ARN must exist to run test."
            raise DurableFunctionsTestError(msg_arn)

        # Block until completion
        completed = self._executor.wait_until_complete(output.execution_arn, timeout)

        if not completed:
            msg_timeout: str = "Execution did not complete within timeout"

            raise TimeoutError(msg_timeout)

        execution: Execution = self._store.load(output.execution_arn)
        return DurableFunctionTestResult.create(execution=execution)


class DurableChildContextTestRunner(DurableFunctionTestRunner):
    """Test a durable block, annotated with @durable_with_child_context, in isolation."""

    def __init__(
        self,
        context_function: Callable[Concatenate[DurableContext, P], Any],
        *args,
        **kwargs,
    ):
        # wrap the durable context around a durable handler as a convenience to run directly
        @durable_handler
        def handler(event: Any, context: DurableContext):  # noqa: ARG001
            return context_function(*args, **kwargs)(context)

        super().__init__(handler)


class WebRunner:
    """Web server runner for durable functions testing with HTTP API endpoints."""

    def __init__(self, config: WebRunnerConfig) -> None:
        """Initialize WebRunner with configuration.

        Args:
            config: WebRunnerConfig containing server and Lambda service settings
        """
        self._config = config
        self._server: WebServer | None = None
        self._scheduler: Scheduler | None = None
        self._store: ExecutionStore | None = None
        self._invoker: LambdaInvoker | None = None
        self._executor: Executor | None = None

    def __enter__(self) -> Self:
        """Context manager entry point.

        Returns:
            WebRunner: Self for use in with statement
        """
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit point with cleanup.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        self.stop()

    def start(self) -> None:
        """Start the server and initialize all dependencies.

        Creates and configures all required components including scheduler,
        store, invoker, executor, and web server. It does not however start
        serving web requests, for that you need serve_forever.

        Raises:
            DurableFunctionsLocalRunnerError: If server is already started
        """
        if self._server is not None:
            msg = "Server is already running"
            raise DurableFunctionsLocalRunnerError(msg)

        # Create dependencies and server
        if self._config.store_type == StoreType.FILESYSTEM:
            store_path = self._config.store_path or ".durable_executions"
            self._store = FileSystemExecutionStore.create(store_path)
        else:
            self._store = InMemoryExecutionStore()
        self._scheduler = Scheduler()
        self._invoker = LambdaInvoker(self._create_boto3_client())

        # Create executor with all dependencies
        self._executor = Executor(
            store=self._store, scheduler=self._scheduler, invoker=self._invoker
        )

        # Start the scheduler
        self._scheduler.start()

        # Create web server with configuration and executor
        self._server = WebServer(
            config=self._config.web_service, executor=self._executor
        )

    def serve_forever(self) -> None:
        """Start serving HTTP requests indefinitely.

        Delegates to the underlying WebServer.serve_forever() method.
        This method blocks until the server is stopped.

        Raises:
            DurableFunctionsLocalRunnerError: If server has not been started
        """
        if self._server is None:
            msg = "Server not started"
            raise DurableFunctionsLocalRunnerError(msg)

        # This blocks until KeyboardInterrupt - let caller handle the exception
        self._server.serve_forever()

    def stop(self) -> None:
        """Stop the web server and cleanup resources.

        Gracefully shuts down the server, scheduler, and cleans up
        all allocated resources. Safe to call multiple times.
        Handles cleanup exceptions gracefully to ensure all resources
        are cleaned up even if some fail.
        """
        if self._server is not None:
            try:
                self._server.server_close()
            except Exception:
                # Log the exception but continue cleanup
                logger.exception("error closing web server")

            self._server = None

        if self._scheduler is not None:
            try:
                self._scheduler.stop()
            except Exception:
                logger.exception("error stopping scheduler")
            self._scheduler = None

        self._store = None
        self._invoker = None
        self._executor = None

    def _create_boto3_client(self) -> Any:
        """Create boto3 client for lambdainternal-local service.

        Configures AWS data path and creates a boto3 client with the
        local runner endpoint and region from configuration.

        Returns:
            Configured boto3 client for lambdainternal-local service

        Raises:
            Exception: If client creation fails - exceptions propagate naturally
                      for CLI to handle as general Exception
        """
        # Set up AWS data path for boto models
        package_path = os.path.dirname(aws_durable_execution_sdk_python.__file__)
        data_path = f"{package_path}/botocore/data"
        os.environ["AWS_DATA_PATH"] = data_path

        # Create client with Lambda endpoint configuration
        return boto3.client(
            "lambdainternal-local",
            endpoint_url=self._config.lambda_endpoint,
            region_name=self._config.local_runner_region,
        )
