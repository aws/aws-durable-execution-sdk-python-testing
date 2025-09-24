"""Execution life-cycle logic."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject

from aws_durable_execution_sdk_python_testing.exceptions import (
    IllegalStateError,
    InvalidParameterError,
    ResourceNotFoundError,
)
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.model import (
    StartDurableExecutionInput,
    StartDurableExecutionOutput,
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
        """Get execution by ARN."""
        return self._store.load(execution_arn)

    def _validate_invocation_response_and_store(
        self,
        execution_arn: str,
        response: DurableExecutionInvocationOutput,
        execution: Execution,
    ):
        """Validate response status and save it to the store if fine.

        Raises:
            InvalidParameterError: If the response status is invalid.
            IllegalStateError: If the response status is valid but the execution is already completed.
        """
        if execution.is_complete:
            msg_already_complete: str = "Execution already completed, ignoring result"

            raise IllegalStateError(msg_already_complete)

        if response.status is None:
            msg_status_required: str = "Response status is required"

            raise InvalidParameterError(msg_status_required)

        match response.status:
            case InvocationStatus.FAILED:
                if response.result is not None:
                    msg_failed_result: str = (
                        "Cannot provide a Result for FAILED status."
                    )
                    raise InvalidParameterError(msg_failed_result)
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
                    raise InvalidParameterError(msg_success_error)
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
                    raise InvalidParameterError(msg_pending_ops)
                logger.info("[%s] Execution pending async work", execution_arn)

            case _:
                msg_unexpected_status: str = (
                    f"Unexpected invocation status: {response.status}"
                )
                raise IllegalStateError(msg_unexpected_status)

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
                except (InvalidParameterError, IllegalStateError) as e:
                    logger.warning(
                        "[%s] Lambda output validation failure: %s", execution_arn, e
                    )
                    error_obj = ErrorObject.from_exception(e)
                    self._retry_invocation(execution, error_obj)

            except ResourceNotFoundError:
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

            raise IllegalStateError(msg)

        if error is not None:
            self.fail_execution(execution_arn, error)
        else:
            self.complete_execution(execution_arn, result)

    def _fail_workflow(self, execution_arn: str, error: ErrorObject):
        """Fail workflow with terminal state validation."""
        execution = self._store.load(execution_arn)

        if execution.is_complete:
            msg: str = "Cannot make multiple close workflow decisions."

            raise IllegalStateError(msg)

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

        raise ValueError(msg)

    def complete_execution(self, execution_arn: str, result: str | None = None) -> None:
        """Complete execution successfully."""
        logger.debug("[%s] Completing execution with result: %s", execution_arn, result)
        execution: Execution = self._store.load(execution_arn=execution_arn)
        execution.complete_success(result=result)
        self._store.update(execution)
        if execution.result is None:
            msg: str = "Execution result is required"

            raise IllegalStateError(msg)
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

            raise IllegalStateError(msg)
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
