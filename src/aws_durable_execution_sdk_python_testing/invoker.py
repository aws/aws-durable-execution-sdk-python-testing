from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Protocol

import boto3  # type: ignore
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationInputWithClient,
    DurableExecutionInvocationOutput,
    InitialExecutionState,
    InvocationStatus,
)

from aws_durable_execution_sdk_python_testing.exceptions import (
    DurableFunctionsTestError,
)
from aws_durable_execution_sdk_python_testing.model import LambdaContext

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
        aws_request_id="test-invoke-12345",
        client_context=client_context_dict,
        identity=cognito_identity_dict,
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

    def update_endpoint(
        self, endpoint_url: str, region_name: str
    ) -> None: ...  # pragma: no cover


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

    def update_endpoint(self, endpoint_url: str, region_name: str) -> None:
        """No-op for in-process invoker."""


class LambdaInvoker(Invoker):
    def __init__(self, lambda_client: Any) -> None:
        self.lambda_client = lambda_client

    @staticmethod
    def create(endpoint_url: str, region_name: str) -> LambdaInvoker:
        """Create with the boto lambda client."""
        return LambdaInvoker(
            boto3.client(
                "lambdainternal", endpoint_url=endpoint_url, region_name=region_name
            )
        )

    def update_endpoint(self, endpoint_url: str, region_name: str) -> None:
        """Update the Lambda client endpoint."""
        self.lambda_client = boto3.client(
            "lambdainternal", endpoint_url=endpoint_url, region_name=region_name
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
        """Invoke AWS Lambda function and return durable execution result.

        Args:
            function_name: Name of the Lambda function to invoke
            input: Durable execution invocation input

        Returns:
            DurableExecutionInvocationOutput: Result of the function execution

        Raises:
            ResourceNotFoundException: If function does not exist
            InvalidParameterValueException: If parameters are invalid
            DurableFunctionsTestError: For other invocation failures
        """
        from aws_durable_execution_sdk_python_testing.exceptions import (
            ResourceNotFoundException,
            InvalidParameterValueException,
        )

        # Parameter validation
        if not function_name or not function_name.strip():
            msg = "Function name is required"
            raise InvalidParameterValueException(msg)

        try:
            # Invoke AWS Lambda function using standard invoke method
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",  # Synchronous invocation
                Payload=json.dumps(input.to_dict(), default=str),
            )

            # Check HTTP status code
            status_code = response.get("StatusCode")
            if status_code not in (200, 202, 204):
                msg = f"Lambda invocation failed with status code: {status_code}"
                raise DurableFunctionsTestError(msg)

            # Check for function errors
            if "FunctionError" in response:
                error_payload = response["Payload"].read().decode("utf-8")
                msg = f"Lambda invocation failed with status {status_code}: {error_payload}"
                raise DurableFunctionsTestError(msg)

            # Parse response payload
            response_payload = response["Payload"].read().decode("utf-8")
            response_dict = json.loads(response_payload)

            # Convert to DurableExecutionInvocationOutput
            return DurableExecutionInvocationOutput.from_dict(response_dict)

        except self.lambda_client.exceptions.ResourceNotFoundException as e:
            msg = f"Function not found: {function_name}"
            raise ResourceNotFoundException(msg) from e
        except self.lambda_client.exceptions.InvalidParameterValueException as e:
            msg = f"Invalid parameter: {e}"
            raise InvalidParameterValueException(msg) from e
        except (
            self.lambda_client.exceptions.TooManyRequestsException,
            self.lambda_client.exceptions.ServiceException,
            self.lambda_client.exceptions.ResourceConflictException,
            self.lambda_client.exceptions.InvalidRequestContentException,
            self.lambda_client.exceptions.RequestTooLargeException,
            self.lambda_client.exceptions.UnsupportedMediaTypeException,
            self.lambda_client.exceptions.InvalidRuntimeException,
            self.lambda_client.exceptions.InvalidZipFileException,
            self.lambda_client.exceptions.ResourceNotReadyException,
            self.lambda_client.exceptions.SnapStartTimeoutException,
            self.lambda_client.exceptions.SnapStartNotReadyException,
            self.lambda_client.exceptions.SnapStartException,
            self.lambda_client.exceptions.RecursiveInvocationException,
        ) as e:
            msg = f"Lambda invocation failed: {e}"
            raise DurableFunctionsTestError(msg) from e
        except (
            self.lambda_client.exceptions.InvalidSecurityGroupIDException,
            self.lambda_client.exceptions.EC2ThrottledException,
            self.lambda_client.exceptions.EFSMountConnectivityException,
            self.lambda_client.exceptions.SubnetIPAddressLimitReachedException,
            self.lambda_client.exceptions.EC2UnexpectedException,
            self.lambda_client.exceptions.InvalidSubnetIDException,
            self.lambda_client.exceptions.EC2AccessDeniedException,
            self.lambda_client.exceptions.EFSIOException,
            self.lambda_client.exceptions.ENILimitReachedException,
            self.lambda_client.exceptions.EFSMountTimeoutException,
            self.lambda_client.exceptions.EFSMountFailureException,
        ) as e:
            msg = f"Lambda infrastructure error: {e}"
            raise DurableFunctionsTestError(msg) from e
        except (
            self.lambda_client.exceptions.KMSAccessDeniedException,
            self.lambda_client.exceptions.KMSDisabledException,
            self.lambda_client.exceptions.KMSNotFoundException,
            self.lambda_client.exceptions.KMSInvalidStateException,
        ) as e:
            msg = f"Lambda KMS error: {e}"
            raise DurableFunctionsTestError(msg) from e
        except Exception as e:
            # Handle any remaining exceptions, including custom ones like DurableExecutionAlreadyStartedException
            if "DurableExecutionAlreadyStartedException" in str(type(e)):
                msg = f"Durable execution already started: {e}"
                raise DurableFunctionsTestError(msg) from e
            msg = f"Unexpected error during Lambda invocation: {e}"
            raise DurableFunctionsTestError(msg) from e
