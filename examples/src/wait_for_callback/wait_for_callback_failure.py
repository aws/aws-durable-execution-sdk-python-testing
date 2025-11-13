from typing import Any

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import WaitForCallbackConfig, Duration
from .external_system import ExternalSystem  # noqa: TID252

external_system = ExternalSystem()  # Singleton instance


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    name = "Callback Failure"
    config = WaitForCallbackConfig(timeout=Duration(10), retry_strategy=None)

    def submitter(callback_id: str) -> None:
        """Submitter function that triggers failure."""
        try:
            raise Exception("Callback failed")
        except Exception as e:
            external_system.send_failure(callback_id, e)
            external_system.start()

    try:
        context.wait_for_callback(submitter, name=name, config=config)
        return "OK"
    except Exception as e:
        result = str(e)
        return result
