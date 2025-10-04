from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any, Protocol

import boto3  # type: ignore
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationInputWithClient,
    DurableExecutionInvocationOutput,
    InitialExecutionState,
)
from aws_durable_execution_sdk_python.lambda_context import LambdaContext

from aws_durable_execution_sdk_python_testing.exceptions import (
    DurableFunctionsTestError,
)


if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python_testing.client import InMemoryServiceClient
    from aws_durable_execution_sdk_python_testing.execution import Execution


def create_test_lambda_context() -> LambdaContext:
    # Create client context as a dictionary, not as objects
    # LambdaContext.__init__ expects dictionaries and will create the objects internally
    client_context_dict = {
        "custom": {"test_key": "test_value"},
        "env": {"platform": "test", "make": "test", "model": "test"},
        "client": {
            "installation_id": "test-installation-123",
            "app_title": "TestApp",
            "app_version_name": "1.0.0",
            "app_version_code": "100",
            "app_package_name": "com.test.app",
        },
    }

    cognito_identity_dict = {
        "cognitoIdentityId": "test-cognito-identity-123",
        "cognitoIdentityPoolId": "us-west-2:test-pool-456",
    }

    return LambdaContext(
        invoke_id="test-invoke-12345",
        client_context=client_context_dict,
        cognito_identity=cognito_identity_dict,
        epoch_deadline_time_in_ms=int(
            (time.time() + 900) * 1000
        ),  # 15 minutes from now
        invoked_function_arn="arn:aws:lambda:us-west-2:123456789012:function:test-function",
        tenant_id="test-tenant-789",
    )


class Invoker(Protocol):
    def create_invocation_input(
        self, execution: Execution
    ) -> DurableExecutionInvocationInput: ...  # pragma: no cover

    def invoke(
        self,
        function_name: str,
        input: DurableExecutionInvocationInput,
    ) -> DurableExecutionInvocationOutput: ...  # pragma: no cover


class InProcessInvoker(Invoker):
    def __init__(self, handler: Callable, service_client: InMemoryServiceClient):
        self.handler = handler
        self.service_client = service_client

    def create_invocation_input(
        self, execution: Execution
    ) -> DurableExecutionInvocationInput:
        return DurableExecutionInvocationInputWithClient(
            durable_execution_arn=execution.durable_execution_arn,
            # TODO: this needs better logic - use existing if not used yet, vs create new
            checkpoint_token=execution.get_new_checkpoint_token(),
            initial_execution_state=InitialExecutionState(
                operations=execution.operations,
                next_marker="",
            ),
            is_local_runner=False,
            service_client=self.service_client,
        )

    def invoke(
        self,
        function_name: str,  # noqa: ARG002
        input: DurableExecutionInvocationInput,
    ) -> DurableExecutionInvocationOutput:
        # TODO: reasses if function_name will be used in future
        input_with_client = DurableExecutionInvocationInputWithClient.from_durable_execution_invocation_input(
            input, self.service_client
        )
        context = create_test_lambda_context()
        response_dict = self.handler(input_with_client, context)
        return DurableExecutionInvocationOutput.from_dict(response_dict)


class LambdaInvoker(Invoker):
    def __init__(self, lambda_client: Any) -> None:
        self.lambda_client = lambda_client

    @staticmethod
    def create(endpoint_url: str, region_name: str) -> LambdaInvoker:
        """Create with the boto lambda client."""
        return LambdaInvoker(
            boto3.client(
                "lambdainternal", 
                endpoint_url=endpoint_url, 
                region_name=region_name,
                aws_access_key_id="test",
                aws_secret_access_key="test"
            )
        )

    def create_invocation_input(
        self, execution: Execution
    ) -> DurableExecutionInvocationInput:
        return DurableExecutionInvocationInput(
            durable_execution_arn=execution.durable_execution_arn,
            checkpoint_token=execution.get_new_checkpoint_token(),
            initial_execution_state=InitialExecutionState(
                operations=execution.operations,
                next_marker="",
            ),
            is_local_runner=False,
        )

    def invoke(
        self,
        function_name: str,
        input: DurableExecutionInvocationInput,
    ) -> DurableExecutionInvocationOutput:
        # TODO: temporary method name pre-build - switch to `invoke` for final
        # TODO: wrap ResourceNotFoundException from lambda in ResourceNotFoundException from this lib
        response = self.lambda_client.invoke20150331(
            FunctionName=function_name,
            InvocationType="RequestResponse",  # Synchronous invocation
            Payload=json.dumps(input.to_dict(), default=str),
        )

        # very simplified placeholder lol
        if response["StatusCode"] == 200:  # noqa: PLR2004
            json_response = json.loads(response["Payload"].read().decode("utf-8"))
            return DurableExecutionInvocationOutput.from_dict(json_response)

        msg: str = f"Lambda invocation failed with status code: {response['StatusCode']}, {response['Payload']=}"
        raise DurableFunctionsTestError(msg)
