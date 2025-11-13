from typing import Any

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import WaitForCallbackConfig, Duration
from .external_system import ExternalSystem  # noqa: TID252

external_system = ExternalSystem()  # Singleton instance


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    name = "Callback Heartbeat"
    config = WaitForCallbackConfig(timeout=Duration(30), retry_strategy=None)

    def submitter(callback_id: str) -> None:
        """Submitter function that sends heartbeat then succeeds."""
        external_system.send_heartbeat(callback_id)
        external_system.send_success(callback_id, b"")
        external_system.start()

    context.wait_for_callback(submitter, name=name, config=config)
    return "OK"
