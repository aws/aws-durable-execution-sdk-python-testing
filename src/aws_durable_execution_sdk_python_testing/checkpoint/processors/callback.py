"""Callback operation processor for handling CALLBACK operation updates."""

from __future__ import annotations

from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.base import (
    OperationProcessor,
)

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier


class CallbackProcessor(OperationProcessor):
    """Processes CALLBACK operation updates with activity scheduling."""

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        notifier: ExecutionNotifier,  # noqa: ARG002
        execution_arn: str,  # noqa: ARG002
    ) -> Operation:
        """Process CALLBACK operation update with scheduler integration for activities."""
        match update.action:
            case OperationAction.START:
                # TODO: create CallbackToken (see token module). Add Observer/Notifier for on_callback_created possibly,
                # but token might well have enough so don't need to maintain token list on execution itself
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.STARTED,
                )
            case _:
                msg: str = "Invalid action for CALLBACK operation."

                raise ValueError(msg)
