"""HTTP endpoint handlers for AWS Lambda Durable Functions operations."""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, cast

from aws_durable_execution_sdk_python_testing.exceptions import (
    AwsApiException,
    ExecutionAlreadyStartedException,
    ExecutionConflictException,
    IllegalStateException,
    InvalidParameterValueException,
    ServiceException,
)
from aws_durable_execution_sdk_python_testing.model import (
    CheckpointDurableExecutionRequest,
    CheckpointDurableExecutionResponse,
    GetDurableExecutionHistoryResponse,
    GetDurableExecutionStateResponse,
    ListDurableExecutionsByFunctionRequest,
    ListDurableExecutionsByFunctionResponse,
    ListDurableExecutionsRequest,
    ListDurableExecutionsResponse,
    SendDurableExecutionCallbackFailureRequest,
    SendDurableExecutionCallbackFailureResponse,
    SendDurableExecutionCallbackHeartbeatRequest,
    SendDurableExecutionCallbackHeartbeatResponse,
    SendDurableExecutionCallbackSuccessResponse,
    StartDurableExecutionInput,
    StartDurableExecutionOutput,
    StopDurableExecutionRequest,
    StopDurableExecutionResponse,
)
from aws_durable_execution_sdk_python_testing.web.models import (
    HTTPRequest,
    HTTPResponse,
)
from aws_durable_execution_sdk_python_testing.web.routes import (
    CallbackFailureRoute,
    CallbackHeartbeatRoute,
    CallbackSuccessRoute,
    CheckpointDurableExecutionRoute,
    GetDurableExecutionHistoryRoute,
    GetDurableExecutionRoute,
    GetDurableExecutionStateRoute,
    ListDurableExecutionsByFunctionRoute,
    StopDurableExecutionRoute,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.executor import Executor
    from aws_durable_execution_sdk_python_testing.web.routes import Route

logger = logging.getLogger(__name__)


class EndpointHandler(ABC):
    """Abstract base class for HTTP endpoint handlers."""

    def __init__(self, executor: Executor) -> None:
        """Initialize the handler with an executor.

        Args:
            executor: The executor instance for handling operations
        """
        self.executor = executor

    @abstractmethod
    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:
        """Handle an HTTP request and return an HTTP response.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """

    def _parse_json_body(
        self, request: HTTPRequest, required: bool = True
    ) -> dict[str, Any]:
        """Parse JSON body from HTTP request with validation.

        Args:
            request: The HTTP request containing the JSON body
            required: Whether the request body is required

        Returns:
            dict: The parsed JSON data, or empty dict if not required and missing

        Raises:
            InvalidParameterValueException: If the request body is required but empty or invalid JSON
        """
        if not request.body:
            if required:
                msg = "Request body is required"
                raise InvalidParameterValueException(msg)
            return {}

        # Handle both dict and bytes body types
        if isinstance(request.body, dict):
            return request.body

        try:
            return json.loads(request.body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            msg = f"Invalid JSON in request body: {e}"
            raise InvalidParameterValueException(msg) from e

    def _json_response(
        self,
        status_code: int,
        data: dict[str, Any],
        additional_headers: dict[str, str] | None = None,
    ) -> HTTPResponse:
        """Create a JSON HTTP response.

        Args:
            status_code: HTTP status code
            data: Data to serialize as JSON
            additional_headers: Optional additional headers to include

        Returns:
            HTTPResponse: The HTTP response with JSON body
        """
        return HTTPResponse.create_json(status_code, data, additional_headers)

    def _success_response(
        self, data: dict[str, Any], additional_headers: dict[str, str] | None = None
    ) -> HTTPResponse:
        """Create a successful JSON response (200 OK).

        Args:
            data: Data to serialize as JSON
            additional_headers: Optional additional headers to include

        Returns:
            HTTPResponse: The HTTP response with JSON body
        """
        return self._json_response(200, data, additional_headers)

    def _created_response(
        self, data: dict[str, Any], additional_headers: dict[str, str] | None = None
    ) -> HTTPResponse:
        """Create a created JSON response (201 Created).

        Args:
            data: Data to serialize as JSON
            additional_headers: Optional additional headers to include

        Returns:
            HTTPResponse: The HTTP response with JSON body
        """
        return self._json_response(201, data, additional_headers)

    def _no_content_response(
        self, additional_headers: dict[str, str] | None = None
    ) -> HTTPResponse:
        """Create a no content response (204 No Content).

        Args:
            additional_headers: Optional additional headers to include

        Returns:
            HTTPResponse: The HTTP response with empty body
        """
        return HTTPResponse.create_empty(204, additional_headers)

    # Removed deprecated _error_response method - use AWS exceptions directly

    def _parse_query_param(self, request: HTTPRequest, param_name: str) -> str | None:
        """Parse a single query parameter from the request.

        Args:
            request: The HTTP request
            param_name: Name of the query parameter

        Returns:
            str | None: The parameter value or None if not present
        """
        param_values = request.query_params.get(param_name)
        return param_values[0] if param_values else None

    def _parse_query_param_list(
        self, request: HTTPRequest, param_name: str
    ) -> list[str]:
        """Parse a query parameter that can have multiple values.

        Args:
            request: The HTTP request
            param_name: Name of the query parameter

        Returns:
            list[str]: List of parameter values (empty if not present)
        """
        return request.query_params.get(param_name, [])

    def _validate_required_fields(
        self, data: dict[str, Any], required_fields: list[str]
    ) -> None:
        """Validate that required fields are present in the data.

        Args:
            data: The data dictionary to validate
            required_fields: List of required field names

        Raises:
            ValueError: If any required field is missing
        """
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            msg = f"Missing required fields: {', '.join(missing_fields)}"
            raise InvalidParameterValueException(msg)

    def _handle_aws_exception(self, exception: AwsApiException) -> HTTPResponse:
        """Handle AWS API exceptions directly.

        Args:
            exception: The AWS API exception

        Returns:
            HTTPResponse: AWS-compliant error response
        """
        # Log server errors
        if exception.http_status_code >= 500:  # noqa: PLR2004
            logger.exception("Server error: %s", exception)
        return HTTPResponse.create_error_from_exception(exception)

    def _handle_framework_exception(self, exception: Exception) -> HTTPResponse:
        """Handle framework exceptions by mapping to AWS exceptions.

        Args:
            exception: The framework exception

        Returns:
            HTTPResponse: AWS-compliant error response
        """
        if isinstance(exception, (ValueError | KeyError)):
            return HTTPResponse.create_error_from_exception(
                InvalidParameterValueException(str(exception))
            )
        logger.exception("Unexpected error: %s", exception)
        return HTTPResponse.create_error_from_exception(
            ServiceException(str(exception))
        )


class StartExecutionHandler(EndpointHandler):
    """Handler for POST /start-durable-execution."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:  # noqa: ARG002
        """Handle start execution request.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        try:
            body_data: dict[str, Any] = self._parse_json_body(request)

            start_input: StartDurableExecutionInput = (
                StartDurableExecutionInput.from_dict(body_data)
            )

            start_output: StartDurableExecutionOutput = self.executor.start_execution(
                start_input
            )

            response_data: dict[str, Any] = start_output.to_dict()

            # Return HTTP 201 Created response
            return self._created_response(response_data)

        except IllegalStateException as e:
            # For StartExecution operations, map to ExecutionAlreadyStartedException
            aws_exception = ExecutionAlreadyStartedException(
                str(e),
                "arn:aws:lambda:us-east-1:123456789012:function:test",
            )
            return HTTPResponse.create_error_from_exception(aws_exception)
        except AwsApiException as e:
            return self._handle_aws_exception(e)
        except Exception as e:  # noqa: BLE001
            return self._handle_framework_exception(e)


class GetDurableExecutionHandler(EndpointHandler):
    """Handler for GET /2025-12-01/durable-executions/{arn}."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:  # noqa: ARG002
        """Handle get durable execution request.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        try:
            route = cast(GetDurableExecutionRoute, parsed_route)

            execution_response = self.executor.get_execution_details(route.arn)

            response_data: dict[str, Any] = execution_response.to_dict()

            # HTTP 200 OK response
            return self._success_response(response_data)

        except AwsApiException as e:
            return self._handle_aws_exception(e)
        except Exception as e:  # noqa: BLE001
            return self._handle_framework_exception(e)


class CheckpointDurableExecutionHandler(EndpointHandler):
    """Handler for POST /2025-12-01/durable-executions/{arn}/checkpoint."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:
        """Handle checkpoint durable execution request.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        try:
            body_data: dict[str, Any] = self._parse_json_body(request)

            checkpoint_route = cast(CheckpointDurableExecutionRoute, parsed_route)
            execution_arn: str = checkpoint_route.arn

            checkpoint_request: CheckpointDurableExecutionRequest = (
                CheckpointDurableExecutionRequest.from_dict(body_data, execution_arn)
            )

            checkpoint_response: CheckpointDurableExecutionResponse = (
                self.executor.checkpoint_execution(
                    execution_arn,
                    checkpoint_request.checkpoint_token,
                    checkpoint_request.updates,
                    checkpoint_request.client_token,
                )
            )

            response_data: dict[str, Any] = checkpoint_response.to_dict()

            return self._success_response(response_data)

        except AwsApiException as e:
            return self._handle_aws_exception(e)
        except Exception as e:  # noqa: BLE001
            return self._handle_framework_exception(e)


class StopDurableExecutionHandler(EndpointHandler):
    """Handler for POST /2025-12-01/durable-executions/{arn}/stop."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:
        """Handle stop durable execution request.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        try:
            body_data: dict[str, Any] = self._parse_json_body(request)
            stop_request: StopDurableExecutionRequest = (
                StopDurableExecutionRequest.from_dict(body_data)
            )

            stop_route = cast(StopDurableExecutionRoute, parsed_route)
            execution_arn: str = stop_route.arn

            stop_response: StopDurableExecutionResponse = self.executor.stop_execution(
                execution_arn, stop_request.error
            )

            response_data: dict[str, Any] = stop_response.to_dict()

            return self._success_response(response_data)

        except AwsApiException as e:
            return self._handle_aws_exception(e)
        except Exception as e:  # noqa: BLE001
            return self._handle_framework_exception(e)


class GetDurableExecutionStateHandler(EndpointHandler):
    """Handler for GET /2025-12-01/durable-executions/{arn}/state."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:  # noqa: ARG002
        """Handle get durable execution state request.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        try:
            state_route = cast(GetDurableExecutionStateRoute, parsed_route)
            execution_arn: str = state_route.arn

            state_response: GetDurableExecutionStateResponse = (
                self.executor.get_execution_state(execution_arn)
            )

            response_data: dict[str, Any] = state_response.to_dict()

            return self._success_response(response_data)

        except AwsApiException as e:
            return self._handle_aws_exception(e)
        except Exception as e:  # noqa: BLE001
            return self._handle_framework_exception(e)


class GetDurableExecutionHistoryHandler(EndpointHandler):
    """Handler for GET /2025-12-01/durable-executions/{arn}/history."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:
        """Handle get durable execution history request.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        try:
            history_route = cast(GetDurableExecutionHistoryRoute, parsed_route)
            execution_arn: str = history_route.arn

            max_results: str | None = self._parse_query_param(request, "maxResults")
            next_token: str | None = self._parse_query_param(request, "nextToken")

            history_response: GetDurableExecutionHistoryResponse = (
                self.executor.get_execution_history(
                    execution_arn,
                    include_execution_data=False,
                    reverse_order=False,
                    marker=next_token,
                    max_items=int(max_results) if max_results else None,
                )
            )

            response_data: dict[str, Any] = history_response.to_dict()

            return self._success_response(response_data)

        except AwsApiException as e:
            return self._handle_aws_exception(e)
        except Exception as e:  # noqa: BLE001
            return self._handle_framework_exception(e)


class ListDurableExecutionsHandler(EndpointHandler):
    """Handler for GET /2025-12-01/durable-executions."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:  # noqa: ARG002
        """Handle list durable executions request.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        try:
            query_params: dict[str, Any] = {}

            # TODO: encapsulate this better. Also, it is a GET, to confirm AWS SDK does
            # pass args in querystring rather than body (per spec body should be ignored)
            if function_name := self._parse_query_param(request, "FunctionName"):
                query_params["FunctionName"] = function_name
            if function_version := self._parse_query_param(request, "FunctionVersion"):
                query_params["FunctionVersion"] = function_version
            if durable_execution_name := self._parse_query_param(
                request, "DurableExecutionName"
            ):
                query_params["DurableExecutionName"] = durable_execution_name
            if status_filter := self._parse_query_param(request, "StatusFilter"):
                query_params["StatusFilter"] = [
                    status_filter
                ]  # Convert to list for model
            if time_after := self._parse_query_param(request, "TimeAfter"):
                query_params["TimeAfter"] = time_after
            if time_before := self._parse_query_param(request, "TimeBefore"):
                query_params["TimeBefore"] = time_before
            if marker := self._parse_query_param(request, "Marker"):
                query_params["Marker"] = marker

            # Parse integer parameters
            if max_items_str := self._parse_query_param(request, "MaxItems"):
                try:
                    query_params["MaxItems"] = int(max_items_str)
                except ValueError as e:
                    error_msg: str = f"Invalid MaxItems value: {max_items_str}"
                    raise InvalidParameterValueException(error_msg) from e

            # Parse boolean parameters
            if reverse_order_str := self._parse_query_param(request, "ReverseOrder"):
                query_params["ReverseOrder"] = reverse_order_str.lower() in (
                    "true",
                    "1",
                    "yes",
                )

            # Create request object from query parameters
            list_request: ListDurableExecutionsRequest = (
                ListDurableExecutionsRequest.from_dict(query_params)
            )

            # Call executor method with correct attribute mapping
            list_response: ListDurableExecutionsResponse = self.executor.list_executions(
                function_name=list_request.function_name,
                function_version=list_request.function_version,
                execution_name=list_request.durable_execution_name,  # Map to executor parameter
                status_filter=list_request.status_filter[0]
                if list_request.status_filter
                else None,  # Executor expects single string
                time_after=list_request.time_after,
                time_before=list_request.time_before,
                marker=list_request.marker,
                max_items=list_request.max_items
                if list_request.max_items > 0
                else None,
                reverse_order=list_request.reverse_order or False,
            )

            # Serialize response
            response_data: dict[str, Any] = list_response.to_dict()

            return self._success_response(response_data)

        except AwsApiException as e:
            return self._handle_aws_exception(e)
        except Exception as e:  # noqa: BLE001
            return self._handle_framework_exception(e)


class ListDurableExecutionsByFunctionHandler(EndpointHandler):
    """Handler for GET /2025-12-01/functions/{function_name}/durable-executions."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:
        """Handle list durable executions by function request.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        try:
            function_route = cast(ListDurableExecutionsByFunctionRoute, parsed_route)
            function_name: str = function_route.function_name

            # Parse query parameters and map to dataclass field names
            query_params: dict[str, Any] = {"FunctionName": function_name}

            if qualifier := self._parse_query_param(request, "functionVersion"):
                query_params["Qualifier"] = qualifier
            if execution_name := self._parse_query_param(request, "executionName"):
                query_params["DurableExecutionName"] = execution_name
            if status_filter := self._parse_query_param(request, "statusFilter"):
                query_params["StatusFilter"] = [status_filter]  # Convert to list
            if time_after := self._parse_query_param(request, "timeAfter"):
                query_params["StartedAfter"] = time_after
            if time_before := self._parse_query_param(request, "timeBefore"):
                query_params["StartedBefore"] = time_before
            if marker := self._parse_query_param(request, "marker"):
                query_params["Marker"] = marker
            if max_items_str := self._parse_query_param(request, "maxItems"):
                try:
                    query_params["MaxItems"] = int(max_items_str)
                except ValueError as ve:
                    error_msg: str = f"Invalid MaxItems value: {max_items_str}"
                    raise InvalidParameterValueException(error_msg) from ve
            if reverse_order_str := self._parse_query_param(request, "reverseOrder"):
                query_params["ReverseOrder"] = reverse_order_str.lower() in (
                    "true",
                    "1",
                    "yes",
                )

            list_request: ListDurableExecutionsByFunctionRequest = (
                ListDurableExecutionsByFunctionRequest.from_dict(query_params)
            )

            list_response: ListDurableExecutionsByFunctionResponse = (
                self.executor.list_executions_by_function(
                    function_name=list_request.function_name,
                    qualifier=list_request.qualifier,
                    execution_name=list_request.durable_execution_name,
                    status_filter=list_request.status_filter[0]
                    if list_request.status_filter
                    else None,
                    time_after=list_request.started_after,
                    time_before=list_request.started_before,
                    marker=list_request.marker,
                    max_items=list_request.max_items
                    if list_request.max_items > 0
                    else None,
                    reverse_order=list_request.reverse_order or False,
                )
            )

            response_data: dict[str, Any] = list_response.to_dict()

            return self._success_response(response_data)

        except AwsApiException as e:
            return self._handle_aws_exception(e)
        except Exception as e:  # noqa: BLE001
            return self._handle_framework_exception(e)


class SendDurableExecutionCallbackSuccessHandler(EndpointHandler):
    """Handler for POST /2025-12-01/durable-execution-callbacks/{callback_id}/succeed."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:
        """Handle send durable execution callback success request.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        try:
            callback_route = cast(CallbackSuccessRoute, parsed_route)
            callback_id: str = callback_route.callback_id

            # For binary payload operations, body is raw bytes
            result_bytes = request.body if isinstance(request.body, bytes) else b""

            callback_response: SendDurableExecutionCallbackSuccessResponse = (  # noqa: F841
                self.executor.send_callback_success(
                    callback_id=callback_id, result=result_bytes
                )
            )

            logger.debug(
                "Callback %s succeeded with result: %s",
                callback_id,
                result_bytes.decode("utf-8", errors="replace"),
            )

            # Callback success response is empty
            return self._success_response({})

        except IllegalStateException as e:
            # For callback operations, map to ExecutionConflictException
            aws_exception = ExecutionConflictException(str(e))
            return HTTPResponse.create_error_from_exception(aws_exception)
        except AwsApiException as e:
            return self._handle_aws_exception(e)
        except Exception as e:  # noqa: BLE001
            return self._handle_framework_exception(e)


class SendDurableExecutionCallbackFailureHandler(EndpointHandler):
    """Handler for POST /2025-12-01/durable-execution-callbacks/{callback_id}/fail."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:
        """Handle send durable execution callback failure request.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        try:
            callback_route = cast(CallbackFailureRoute, parsed_route)
            callback_id: str = callback_route.callback_id

            body_data: dict[str, Any] = self._parse_json_body(request, required=False)
            callback_request: SendDurableExecutionCallbackFailureRequest = (
                SendDurableExecutionCallbackFailureRequest.from_dict(
                    body_data, callback_id
                )
            )

            callback_response: SendDurableExecutionCallbackFailureResponse = (  # noqa: F841
                self.executor.send_callback_failure(
                    callback_id=callback_id, error=callback_request.error
                )
            )

            logger.debug(
                "Callback %s failed with error: %s", callback_id, callback_request.error
            )

            # Callback failure response is empty
            return self._success_response({})

        except IllegalStateException as e:
            # For callback operations, map to ExecutionConflictException
            aws_exception = ExecutionConflictException(str(e))
            return HTTPResponse.create_error_from_exception(aws_exception)
        except AwsApiException as e:
            return self._handle_aws_exception(e)
        except Exception as e:  # noqa: BLE001
            return self._handle_framework_exception(e)


class SendDurableExecutionCallbackHeartbeatHandler(EndpointHandler):
    """Handler for POST /2025-12-01/durable-execution-callbacks/{callback_id}/heartbeat."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:
        """Handle send durable execution callback heartbeat request.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        try:
            # Heartbeat requests don't have a body, only callback_id from URL
            callback_route = cast(CallbackHeartbeatRoute, parsed_route)
            callback_id: str = callback_route.callback_id

            callback_response: SendDurableExecutionCallbackHeartbeatResponse = (  # noqa: F841
                self.executor.send_callback_heartbeat(callback_id=callback_id)
            )

            # Callback heartbeat response is empty
            return self._success_response({})

        except IllegalStateException as e:
            # For callback operations, map to ExecutionConflictException
            aws_exception = ExecutionConflictException(str(e))
            return HTTPResponse.create_error_from_exception(aws_exception)
        except AwsApiException as e:
            return self._handle_aws_exception(e)
        except Exception as e:  # noqa: BLE001
            return self._handle_framework_exception(e)


# TODO: should this be /ping instead?
class HealthHandler(EndpointHandler):
    """Handler for GET /health."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:  # noqa: ARG002
        """Handle health check request.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        return self._success_response({"status": "healthy"})


class MetricsHandler(EndpointHandler):
    """Handler for GET /metrics."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:  # noqa: ARG002
        """Handle metrics request.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        # TODO: Implement metrics collection logic
        return self._success_response({"metrics": {}})


class UpdateLambdaEndpointHandler(EndpointHandler):
    """Handler for PUT /lambda-endpoint."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:  # noqa: ARG002
        """Handle update Lambda endpoint request.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        try:
            body = self._parse_json_body(request)
            endpoint_url = body.get("EndpointUrl")
            region_name = body.get("RegionName", "us-east-1")

            if not endpoint_url:
                return self._handle_aws_exception(
                    InvalidParameterValueException("EndpointUrl is required")
                )

            # Update the invoker's Lambda endpoint
            invoker = self.executor._invoker  # noqa: SLF001
            logger.info("Updating lambda endpoint to %s", endpoint_url)
            invoker.update_endpoint(endpoint_url, region_name)
            return self._success_response(
                {"message": "Lambda endpoint updated successfully"}
            )

        except Exception as e:  # noqa: BLE001
            return self._handle_framework_exception(e)
