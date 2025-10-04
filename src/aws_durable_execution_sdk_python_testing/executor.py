"""Execution life-cycle logic."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject, OperationUpdate

from aws_durable_execution_sdk_python_testing.exceptions import (
    ExecutionAlreadyStartedException,
    IllegalStateException,
    InvalidParameterValueException,
    ResourceNotFoundException,
)
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.model import (
    CheckpointDurableExecutionResponse,
    GetDurableExecutionHistoryResponse,
    GetDurableExecutionResponse,
    GetDurableExecutionStateResponse,
    ListDurableExecutionsByFunctionResponse,
    ListDurableExecutionsResponse,
    SendDurableExecutionCallbackFailureResponse,
    SendDurableExecutionCallbackHeartbeatResponse,
    SendDurableExecutionCallbackSuccessResponse,
    StartDurableExecutionInput,
    StartDurableExecutionOutput,
    StopDurableExecutionResponse,
)
from aws_durable_execution_sdk_python_testing.model import (
    Event as HistoryEvent,
)
from aws_durable_execution_sdk_python_testing.model import (
    Execution as ExecutionSummary,
)
from aws_durable_execution_sdk_python_testing.observer import ExecutionObserver


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from aws_durable_execution_sdk_python_testing.invoker import Invoker
    from aws_durable_execution_sdk_python_testing.scheduler import Event, Scheduler
    from aws_durable_execution_sdk_python_testing.store import ExecutionStore

logger = logging.getLogger(__name__)


class Executor(ExecutionObserver):
    MAX_CONSECUTIVE_FAILED_ATTEMPTS = 5
    RETRY_BACKOFF_SECONDS = 5

    def __init__(self, store: ExecutionStore, scheduler: Scheduler, invoker: Invoker):
        self._store = store
        self._scheduler = scheduler
        self._invoker = invoker
        self._completion_events: dict[str, Event] = {}

    def start_execution(
        self,
        input: StartDurableExecutionInput,  # noqa: A002
    ) -> StartDurableExecutionOutput:
        execution = Execution.new(input=input)
        execution.start()
        self._store.save(execution)

        completion_event = self._scheduler.create_event()
        self._completion_events[execution.durable_execution_arn] = completion_event

        # Schedule initial invocation to run immediately
        self._invoke_execution(execution.durable_execution_arn)

        return StartDurableExecutionOutput(
            execution_arn=execution.durable_execution_arn
        )

    def get_execution(self, execution_arn: str) -> Execution:
        """Get execution by ARN.

        Args:
            execution_arn: The execution ARN to retrieve

        Returns:
            Execution: The execution object

        Raises:
            ResourceNotFoundException: If execution does not exist
        """
        try:
            return self._store.load(execution_arn)
        except KeyError as e:
            msg: str = f"Execution {execution_arn} not found"
            raise ResourceNotFoundException(msg) from e

    def get_execution_details(self, execution_arn: str) -> GetDurableExecutionResponse:
        """Get detailed execution information for web API response.

        Args:
            execution_arn: The execution ARN to retrieve

        Returns:
            GetDurableExecutionResponse: Detailed execution information

        Raises:
            ResourceNotFoundException: If execution does not exist
        """
        execution = self.get_execution(execution_arn)

        # Extract execution details from the first operation (EXECUTION type)
        execution_op = execution.get_operation_execution_started()

        # Determine status based on execution state
        if execution.is_complete:
            if (
                execution.result
                and execution.result.status == InvocationStatus.SUCCEEDED
            ):
                status = "SUCCEEDED"
            else:
                status = "FAILED"
        else:
            status = "RUNNING"

        # Extract result and error from execution result
        result = None
        error = None
        if execution.result:
            if execution.result.status == InvocationStatus.SUCCEEDED:
                result = execution.result.result
            elif execution.result.status == InvocationStatus.FAILED:
                error = execution.result.error

        return GetDurableExecutionResponse(
            durable_execution_arn=execution.durable_execution_arn,
            durable_execution_name=execution.start_input.execution_name,
            function_arn=f"arn:aws:lambda:us-east-1:123456789012:function:{execution.start_input.function_name}",
            status=status,
            start_date=execution_op.start_timestamp.isoformat()
            if execution_op.start_timestamp
            else datetime.now(UTC).isoformat(),
            input_payload=execution_op.execution_details.input_payload
            if execution_op.execution_details
            else None,
            result=result,
            error=error,
            stop_date=execution_op.end_timestamp.isoformat()
            if execution_op.end_timestamp
            else None,
            version="1.0",
        )

    def list_executions(
        self,
        function_name: str | None = None,
        function_version: str | None = None,  # noqa: ARG002
        execution_name: str | None = None,
        status_filter: str | None = None,
        time_after: str | None = None,  # noqa: ARG002
        time_before: str | None = None,  # noqa: ARG002
        marker: str | None = None,
        max_items: int | None = None,
        reverse_order: bool = False,  # noqa: FBT001, FBT002
    ) -> ListDurableExecutionsResponse:
        """List executions with filtering and pagination.

        Args:
            function_name: Filter by function name
            function_version: Filter by function version
            execution_name: Filter by execution name
            status_filter: Filter by status (RUNNING, SUCCEEDED, FAILED)
            time_after: Filter executions started after this time
            time_before: Filter executions started before this time
            marker: Pagination marker
            max_items: Maximum items to return (default 50)
            reverse_order: Return results in reverse chronological order

        Returns:
            ListDurableExecutionsResponse: List of executions with pagination
        """
        # Get all executions from store
        all_executions = self._store.list_all()

        # Apply filters
        filtered_executions = []
        for execution in all_executions:
            # Filter by function name
            if function_name and execution.start_input.function_name != function_name:
                continue

            # Filter by execution name
            if (
                execution_name
                and execution.start_input.execution_name != execution_name
            ):
                continue

            # Determine execution status
            execution_status = "RUNNING"
            if execution.is_complete:
                if (
                    execution.result
                    and execution.result.status == InvocationStatus.SUCCEEDED
                ):
                    execution_status = "SUCCEEDED"
                else:
                    execution_status = "FAILED"

            # Filter by status
            if status_filter and execution_status != status_filter:
                continue

            # Convert to ExecutionSummary
            execution_op = execution.get_operation_execution_started()
            execution_summary = ExecutionSummary(
                durable_execution_arn=execution.durable_execution_arn,
                durable_execution_name=execution.start_input.execution_name,
                function_arn=f"arn:aws:lambda:us-east-1:123456789012:function:{execution.start_input.function_name}",
                status=execution_status,
                start_date=execution_op.start_timestamp.isoformat()
                if execution_op.start_timestamp
                else datetime.now(UTC).isoformat(),
                stop_date=execution_op.end_timestamp.isoformat()
                if execution_op.end_timestamp
                else None,
            )
            filtered_executions.append(execution_summary)

        # Sort by start date
        filtered_executions.sort(key=lambda e: e.start_date, reverse=reverse_order)

        # Apply pagination
        if max_items is None:
            max_items = 50

        start_index = 0
        if marker:
            try:
                start_index = int(marker)
            except ValueError:
                start_index = 0

        end_index = start_index + max_items
        paginated_executions = filtered_executions[start_index:end_index]

        next_marker = None
        if end_index < len(filtered_executions):
            next_marker = str(end_index)

        return ListDurableExecutionsResponse(
            durable_executions=paginated_executions, next_marker=next_marker
        )

    def list_executions_by_function(
        self,
        function_name: str,
        qualifier: str | None = None,  # noqa: ARG002
        execution_name: str | None = None,
        status_filter: str | None = None,
        time_after: str | None = None,
        time_before: str | None = None,
        marker: str | None = None,
        max_items: int | None = None,
        reverse_order: bool = False,  # noqa: FBT001, FBT002
    ) -> ListDurableExecutionsByFunctionResponse:
        """List executions for a specific function.

        Args:
            function_name: The function name to filter by
            qualifier: Function qualifier/version
            execution_name: Filter by execution name
            status_filter: Filter by status (RUNNING, SUCCEEDED, FAILED)
            time_after: Filter executions started after this time
            time_before: Filter executions started before this time
            marker: Pagination marker
            max_items: Maximum items to return (default 50)
            reverse_order: Return results in reverse chronological order

        Returns:
            ListDurableExecutionsByFunctionResponse: List of executions for the function
        """
        # Use the general list_executions method with function_name filter
        list_response = self.list_executions(
            function_name=function_name,
            execution_name=execution_name,
            status_filter=status_filter,
            time_after=time_after,
            time_before=time_before,
            marker=marker,
            max_items=max_items,
            reverse_order=reverse_order,
        )

        return ListDurableExecutionsByFunctionResponse(
            durable_executions=list_response.durable_executions,
            next_marker=list_response.next_marker,
        )

    def stop_execution(
        self, execution_arn: str, error: ErrorObject | None = None
    ) -> StopDurableExecutionResponse:
        """Stop a running execution.

        Args:
            execution_arn: The execution ARN to stop
            error: Optional error to use when stopping the execution

        Returns:
            StopDurableExecutionResponse: Response containing stop date

        Raises:
            ResourceNotFoundException: If execution does not exist
            ExecutionAlreadyStartedException: If execution is already completed
        """
        execution = self.get_execution(execution_arn)

        if execution.is_complete:
            # Context-aware mapping: execution already completed maps to ExecutionAlreadyStartedException
            msg: str = f"Execution {execution_arn} is already completed"
            raise ExecutionAlreadyStartedException(msg, execution_arn)

        # Use provided error or create a default one
        stop_error = error or ErrorObject.from_message(
            "Execution stopped by user request"
        )

        # Stop the execution
        self.fail_execution(execution_arn, stop_error)

        return StopDurableExecutionResponse(stop_date=datetime.now(UTC).isoformat())

    def get_execution_state(
        self,
        execution_arn: str,
        checkpoint_token: str | None = None,
        marker: str | None = None,
        max_items: int | None = None,
    ) -> GetDurableExecutionStateResponse:
        """Get execution state with operations.

        Args:
            execution_arn: The execution ARN
            checkpoint_token: Checkpoint token for state consistency
            marker: Pagination marker
            max_items: Maximum items to return

        Returns:
            GetDurableExecutionStateResponse: Execution state with operations

        Raises:
            ResourceNotFoundException: If execution does not exist
            InvalidParameterValueException: If checkpoint token is invalid
        """
        execution = self.get_execution(execution_arn)

        # TODO: Validate checkpoint token if provided
        if checkpoint_token and checkpoint_token not in execution.used_tokens:
            msg: str = f"Invalid checkpoint token: {checkpoint_token}"
            raise InvalidParameterValueException(msg)

        # Get operations (excluding the initial EXECUTION operation for state)
        operations = execution.get_assertable_operations()

        # Apply pagination
        if max_items is None:
            max_items = 100

        # Simple pagination - in real implementation would need proper marker handling
        start_index = 0
        if marker:
            try:
                start_index = int(marker)
            except ValueError:
                start_index = 0

        end_index = start_index + max_items
        paginated_operations = operations[start_index:end_index]

        next_marker = None
        if end_index < len(operations):
            next_marker = str(end_index)

        return GetDurableExecutionStateResponse(
            operations=paginated_operations, next_marker=next_marker
        )

    def get_execution_history(
        self,
        execution_arn: str,
        include_execution_data: bool = False,  # noqa: FBT001, FBT002, ARG002
        reverse_order: bool = False,  # noqa: FBT001, FBT002, ARG002
        marker: str | None = None,
        max_items: int | None = None,
    ) -> GetDurableExecutionHistoryResponse:
        """Get execution history with events.

        TODO: incomplete

        Args:
            execution_arn: The execution ARN
            include_execution_data: Whether to include execution data in events
            reverse_order: Return events in reverse chronological order
            marker: Pagination marker
            max_items: Maximum items to return

        Returns:
            GetDurableExecutionHistoryResponse: Execution history with events

        Raises:
            ResourceNotFoundException: If execution does not exist
        """
        execution = self.get_execution(execution_arn)  # noqa: F841

        # Convert operations to events
        # This is a simplified implementation - real implementation would need
        # to generate proper event history from operations
        events: list[HistoryEvent] = []

        # Apply pagination
        if max_items is None:
            max_items = 100

        start_index = 0
        if marker:
            try:
                start_index = int(marker)
            except ValueError:
                start_index = 0

        end_index = start_index + max_items
        paginated_events = events[start_index:end_index]

        next_marker = None
        if end_index < len(events):
            next_marker = str(end_index)

        return GetDurableExecutionHistoryResponse(
            events=paginated_events, next_marker=next_marker
        )

    def checkpoint_execution(
        self,
        execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate] | None = None,  # noqa: ARG002
        client_token: str | None = None,  # noqa: ARG002
    ) -> CheckpointDurableExecutionResponse:
        """Process checkpoint for an execution.

        Args:
            execution_arn: The execution ARN
            checkpoint_token: Current checkpoint token
            updates: List of operation updates to process
            client_token: Client token for idempotency

        Returns:
            CheckpointDurableExecutionResponse: Updated checkpoint token and state

        Raises:
            ResourceNotFoundException: If execution does not exist
            InvalidParameterValueException: If checkpoint token is invalid
        """
        execution = self.get_execution(execution_arn)

        # Validate checkpoint token
        if checkpoint_token not in execution.used_tokens:
            msg: str = f"Invalid checkpoint token: {checkpoint_token}"
            raise InvalidParameterValueException(msg)

        # TODO: Process operation updates using the checkpoint processor
        # This would integrate with the existing checkpoint processing pipeline

        # Generate new checkpoint token
        new_checkpoint_token = execution.get_new_checkpoint_token()

        # Get current execution state - for now return None (simplified implementation)
        # In a full implementation, this would return CheckpointUpdatedExecutionState with operations
        new_execution_state = None

        return CheckpointDurableExecutionResponse(
            checkpoint_token=new_checkpoint_token,
            new_execution_state=new_execution_state,
        )

    def send_callback_success(
        self,
        callback_id: str,
        result: bytes | None = None,  # noqa: ARG002
    ) -> SendDurableExecutionCallbackSuccessResponse:
        """Send callback success response.

        Args:
            callback_id: The callback ID to respond to
            result: Optional result data for the callback

        Returns:
            SendDurableExecutionCallbackSuccessResponse: Empty response

        Raises:
            InvalidParameterValueException: If callback_id is invalid
            ResourceNotFoundException: If callback does not exist
        """
        if not callback_id:
            msg: str = "callback_id is required"
            raise InvalidParameterValueException(msg)

        # TODO: Implement actual callback success logic
        # This would involve finding the callback operation and completing it
        logger.info("Callback success sent for callback_id: %s", callback_id)

        return SendDurableExecutionCallbackSuccessResponse()

    def send_callback_failure(
        self,
        callback_id: str,
        error: ErrorObject | None = None,  # noqa: ARG002
    ) -> SendDurableExecutionCallbackFailureResponse:
        """Send callback failure response.

        Args:
            callback_id: The callback ID to respond to
            error: Optional error object for the callback failure

        Returns:
            SendDurableExecutionCallbackFailureResponse: Empty response

        Raises:
            InvalidParameterValueException: If callback_id is invalid
            ResourceNotFoundException: If callback does not exist
        """
        if not callback_id:
            msg: str = "callback_id is required"
            raise InvalidParameterValueException(msg)

        # TODO: Implement actual callback failure logic
        # This would involve finding the callback operation and failing it
        logger.info("Callback failure sent for callback_id: %s", callback_id)

        return SendDurableExecutionCallbackFailureResponse()

    def send_callback_heartbeat(
        self, callback_id: str
    ) -> SendDurableExecutionCallbackHeartbeatResponse:
        """Send callback heartbeat to keep callback alive.

        Args:
            callback_id: The callback ID to send heartbeat for

        Returns:
            SendDurableExecutionCallbackHeartbeatResponse: Empty response

        Raises:
            InvalidParameterValueException: If callback_id is invalid
            ResourceNotFoundException: If callback does not exist
        """
        if not callback_id:
            msg: str = "callback_id is required"
            raise InvalidParameterValueException(msg)

        # TODO: Implement actual callback heartbeat logic
        # This would involve updating the callback timeout
        logger.info("Callback heartbeat sent for callback_id: %s", callback_id)

        return SendDurableExecutionCallbackHeartbeatResponse()

    def _validate_invocation_response_and_store(
        self,
        execution_arn: str,
        response: DurableExecutionInvocationOutput,
        execution: Execution,
    ):
        """Validate response status and save it to the store if fine.

        Raises:
            InvalidParameterValueException: If the response status is invalid.
            IllegalStateException: If the response status is valid but the execution is already completed.
        """
        if execution.is_complete:
            msg_already_complete: str = "Execution already completed, ignoring result"

            raise IllegalStateException(msg_already_complete)

        if response.status is None:
            msg_status_required: str = "Response status is required"

            raise InvalidParameterValueException(msg_status_required)

        match response.status:
            case InvocationStatus.FAILED:
                if response.result is not None:
                    msg_failed_result: str = (
                        "Cannot provide a Result for FAILED status."
                    )
                    raise InvalidParameterValueException(msg_failed_result)
                logger.info("[%s] Execution failed", execution_arn)
                self._complete_workflow(
                    execution_arn, result=None, error=response.error
                )
                self._store.save(execution)

            case InvocationStatus.SUCCEEDED:
                if response.error is not None:
                    msg_success_error: str = (
                        "Cannot provide an Error for SUCCEEDED status."
                    )
                    raise InvalidParameterValueException(msg_success_error)
                logger.info("[%s] Execution succeeded", execution_arn)
                self._complete_workflow(
                    execution_arn, result=response.result, error=None
                )
                self._store.save(execution)

            case InvocationStatus.PENDING:
                if not execution.has_pending_operations(execution):
                    msg_pending_ops: str = (
                        "Cannot return PENDING status with no pending operations."
                    )
                    raise InvalidParameterValueException(msg_pending_ops)
                logger.info("[%s] Execution pending async work", execution_arn)

            case _:
                msg_unexpected_status: str = (
                    f"Unexpected invocation status: {response.status}"
                )
                raise IllegalStateException(msg_unexpected_status)

    def _invoke_handler(self, execution_arn: str) -> Callable[[], Awaitable[None]]:
        """Create a parameterless callable that captures execution arn for the scheduler."""

        async def invoke() -> None:
            execution: Execution = self._store.load(execution_arn)

            # Early exit if execution is already completed - like Java's COMPLETED check
            if execution.is_complete:
                logger.info(
                    "[%s] Execution already completed, ignoring result", execution_arn
                )
                return

            try:
                invocation_input: DurableExecutionInvocationInput = (
                    self._invoker.create_invocation_input(execution=execution)
                )

                response: DurableExecutionInvocationOutput = self._invoker.invoke(
                    execution.start_input.function_name, invocation_input
                )

                # Reload execution after invocation in case it was completed via checkpoint
                execution = self._store.load(execution_arn)
                if execution.is_complete:
                    logger.info(
                        "[%s] Execution completed during invocation, ignoring result",
                        execution_arn,
                    )
                    return

                # Process successful received response - validate status and handle accordingly
                try:
                    self._validate_invocation_response_and_store(
                        execution_arn, response, execution
                    )
                except (InvalidParameterValueException, IllegalStateException) as e:
                    logger.warning(
                        "[%s] Lambda output validation failure: %s", execution_arn, e
                    )
                    error_obj = ErrorObject.from_exception(e)
                    self._retry_invocation(execution, error_obj)

            except ResourceNotFoundException:
                logger.warning(
                    "[%s] Function No longer exists: %s",
                    execution_arn,
                    execution.start_input.function_name,
                )
                error_obj = ErrorObject.from_message(
                    message=f"Function not found: {execution.start_input.function_name}"
                )
                self._fail_workflow(execution_arn, error_obj)

            except Exception as e:  # noqa: BLE001
                # Handle invocation errors (network, function not found, etc.)
                logger.warning("[%s] Invocation failed: %s", execution_arn, e)
                error_obj = ErrorObject.from_exception(e)
                self._retry_invocation(execution, error_obj)

        return invoke

    def _invoke_execution(self, execution_arn: str, delay: float = 0) -> None:
        """Invoke execution after delay in seconds."""
        completion_event = self._completion_events.get(execution_arn)
        self._scheduler.call_later(
            self._invoke_handler(execution_arn),
            delay=delay,
            completion_event=completion_event,
        )

    def _complete_workflow(
        self, execution_arn: str, result: str | None, error: ErrorObject | None
    ):
        """Complete workflow - handles both success and failure with terminal state validation."""
        execution = self._store.load(execution_arn)

        if execution.is_complete:
            msg: str = "Cannot make multiple close workflow decisions."

            raise IllegalStateException(msg)

        if error is not None:
            self.fail_execution(execution_arn, error)
        else:
            self.complete_execution(execution_arn, result)

    def _fail_workflow(self, execution_arn: str, error: ErrorObject):
        """Fail workflow with terminal state validation."""
        execution = self._store.load(execution_arn)

        if execution.is_complete:
            msg: str = "Cannot make multiple close workflow decisions."

            raise IllegalStateException(msg)

        self.fail_execution(execution_arn, error)

    def _retry_invocation(self, execution: Execution, error: ErrorObject):
        """Handle retry logic or fail execution if retries exhausted."""
        if (
            execution.consecutive_failed_invocation_attempts
            > self.MAX_CONSECUTIVE_FAILED_ATTEMPTS
        ):
            # Exhausted retries - fail the execution
            self._fail_workflow(
                execution_arn=execution.durable_execution_arn, error=error
            )
        else:
            # Schedule retry with backoff
            execution.consecutive_failed_invocation_attempts += 1
            self._store.save(execution)
            self._invoke_execution(
                execution_arn=execution.durable_execution_arn,
                delay=self.RETRY_BACKOFF_SECONDS,
            )

    def _complete_events(self, execution_arn: str):
        # complete doesn't actually checkpoint explicitly
        if event := self._completion_events.get(execution_arn):
            event.set()

    def wait_until_complete(
        self, execution_arn: str, timeout: float | None = None
    ) -> bool:
        """Block until execution completion. Don't do this unless you actually want to block.

        Args
            timeout (int|float|None): Wait for event to set until this timeout.

        Returns:
            True when set. False if the event timed out without being set.
        """
        if event := self._completion_events.get(execution_arn):
            return event.wait(timeout)

        # this really shouldn't happen - implies execution timed out?
        msg: str = "execution does not exist."

        raise ResourceNotFoundException(msg)

    def complete_execution(self, execution_arn: str, result: str | None = None) -> None:
        """Complete execution successfully."""
        logger.debug("[%s] Completing execution with result: %s", execution_arn, result)
        execution: Execution = self._store.load(execution_arn=execution_arn)
        execution.complete_success(result=result)
        self._store.update(execution)
        if execution.result is None:
            msg: str = "Execution result is required"

            raise IllegalStateException(msg)
        self._complete_events(execution_arn=execution_arn)

    def fail_execution(self, execution_arn: str, error: ErrorObject) -> None:
        """Fail execution with error."""
        logger.exception("[%s] Completing execution with error.", execution_arn)
        execution: Execution = self._store.load(execution_arn=execution_arn)
        execution.complete_fail(error=error)
        self._store.update(execution)
        # set by complete_fail
        if execution.result is None:
            msg: str = "Execution result is required"

            raise IllegalStateException(msg)
        self._complete_events(execution_arn=execution_arn)

    def _on_wait_succeeded(self, execution_arn: str, operation_id: str) -> None:
        """Private method - called when a wait operation completes successfully."""
        execution = self._store.load(execution_arn)

        if execution.is_complete:
            logger.info(
                "[%s] Execution already completed, ignoring wait succeeded event",
                execution_arn,
            )
            return

        try:
            execution.complete_wait(operation_id=operation_id)
            self._store.update(execution)
            logger.debug(
                "[%s] Wait succeeded for operation %s", execution_arn, operation_id
            )
        except Exception:
            logger.exception("[%s] Error processing wait succeeded.", execution_arn)

    def _on_retry_ready(self, execution_arn: str, operation_id: str) -> None:
        """Private method - called when a retry delay has elapsed and retry is ready."""
        execution = self._store.load(execution_arn)

        if execution.is_complete:
            logger.info(
                "[%s] Execution already completed, ignoring retry", execution_arn
            )
            return

        try:
            execution.complete_retry(operation_id=operation_id)
            self._store.update(execution)
            logger.debug(
                "[%s] Retry ready for operation %s", execution_arn, operation_id
            )
        except Exception:
            logger.exception("[%s] Error processing retry ready.", execution_arn)

    # region ExecutionObserver
    def on_completed(self, execution_arn: str, result: str | None = None) -> None:
        """Complete execution successfully. Observer method triggered by notifier."""
        self.complete_execution(execution_arn, result)

    def on_failed(self, execution_arn: str, error: ErrorObject) -> None:
        """Fail execution. Observer method triggered by notifier."""
        self.fail_execution(execution_arn, error)

    def on_wait_timer_scheduled(
        self, execution_arn: str, operation_id: str, delay: float
    ) -> None:
        """Schedule a wait operation. Observer method triggered by notifier."""
        logger.debug("[%s] scheduling wait with delay: %d", execution_arn, delay)

        def wait_handler() -> None:
            self._on_wait_succeeded(execution_arn, operation_id)
            self._invoke_execution(execution_arn, delay=0)

        completion_event = self._completion_events.get(execution_arn)
        self._scheduler.call_later(
            wait_handler, delay=delay, completion_event=completion_event
        )

    def on_step_retry_scheduled(
        self, execution_arn: str, operation_id: str, delay: float
    ) -> None:
        """Schedule a retry a step. Observer method triggered by notifier."""
        logger.debug(
            "[%s] scheduling retry for %s with delay: %d",
            execution_arn,
            operation_id,
            delay,
        )

        def retry_handler() -> None:
            self._on_retry_ready(execution_arn, operation_id)
            self._invoke_execution(execution_arn, delay=0)

        completion_event = self._completion_events.get(execution_arn)
        self._scheduler.call_later(
            retry_handler, delay=delay, completion_event=completion_event
        )

    # endregion ExecutionObserver
