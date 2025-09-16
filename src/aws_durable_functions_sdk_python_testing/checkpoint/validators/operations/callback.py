"""Callback operation validator."""

from __future__ import annotations

from aws_durable_functions_sdk_python.lambda_service import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationUpdate,
)

from aws_durable_functions_sdk_python_testing.exceptions import InvalidParameterError

VALID_ACTIONS_FOR_CALLBACK = frozenset(
    [
        OperationAction.START,
        OperationAction.CANCEL,
    ]
)


class CallbackOperationValidator:
    """Validates CALLBACK operation transitions."""

    _ALLOWED_STATUS_TO_CANCEL = frozenset(
        [
            OperationStatus.STARTED,
        ]
    )

    @staticmethod
    def validate(current_state: Operation | None, update: OperationUpdate) -> None:
        """Validate CALLBACK operation update."""
        match update.action:
            case OperationAction.START:
                if current_state is not None:
                    msg_callback_exists: str = (
                        "Cannot start a CALLBACK that already exist."
                    )
                    raise InvalidParameterError(msg_callback_exists)
            case OperationAction.CANCEL:
                if (
                    current_state is None
                    or current_state.status
                    not in CallbackOperationValidator._ALLOWED_STATUS_TO_CANCEL
                ):
                    msg_callback_cancel: str = "Cannot cancel a CALLBACK that does not exist or has already completed."
                    raise InvalidParameterError(msg_callback_cancel)
            case _:
                msg_callback_invalid: str = "Invalid CALLBACK action."
                raise InvalidParameterError(msg_callback_invalid)
