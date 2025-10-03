"""HTTP request/response data models and utilities for the web runner."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Protocol

from aws_durable_execution_sdk_python_testing.exceptions import (
    AwsApiException,
    InvalidParameterValueException,
)

# Removed deprecated imports from web.errors
from aws_durable_execution_sdk_python_testing.web.routes import Route
from aws_durable_execution_sdk_python_testing.web.serialization import (
    AwsRestJsonDeserializer,
    AwsRestJsonSerializer,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HTTPRequest:
    """HTTP request data model with dict body for handler logic."""

    method: str
    path: Route
    headers: dict[str, str]
    query_params: dict[str, list[str]]
    body: dict[str, Any]

    @classmethod
    def from_bytes(
        cls,
        body_bytes: bytes,
        operation_name: str | None = None,
        method: str = "POST",
        path: Route | None = None,
        headers: dict[str, str] | None = None,
        query_params: dict[str, list[str]] | None = None,
    ) -> HTTPRequest:
        """Create HTTPRequest from raw bytes, deserializing to dict body.

        Args:
            body_bytes: Raw bytes to deserialize
            operation_name: Optional AWS operation name for boto deserialization
            method: HTTP method (default: POST)
            path: Route object (required for actual usage)
            headers: HTTP headers (default: empty dict)
            query_params: Query parameters (default: empty dict)

        Returns:
            HTTPRequest: Request with deserialized dict body

        Raises:
            InvalidParameterValueException: If deserialization fails with both AWS and JSON methods
        """
        if headers is None:
            headers = {}
        if query_params is None:
            query_params = {}

        # Try AWS deserialization first if operation_name provided
        if operation_name:
            try:
                deserializer = AwsRestJsonDeserializer.create(operation_name)
                body_dict = deserializer.from_bytes(body_bytes)
                logger.debug(
                    "Successfully deserialized request using AWS deserializer for %s",
                    operation_name,
                )
            except InvalidParameterValueException as e:
                logger.warning(
                    "AWS deserialization failed for %s, falling back to JSON: %s",
                    operation_name,
                    e,
                )
                # Fall back to standard JSON
                try:
                    body_dict = json.loads(body_bytes.decode("utf-8"))
                    logger.debug(
                        "Successfully deserialized request using JSON fallback"
                    )
                except (json.JSONDecodeError, UnicodeDecodeError) as json_error:
                    msg = f"Both AWS and JSON deserialization failed: AWS error: {e}, JSON error: {json_error}"
                    raise InvalidParameterValueException(msg) from json_error
        else:
            # Use standard JSON deserialization
            try:
                body_dict = json.loads(body_bytes.decode("utf-8"))
                logger.debug("Successfully deserialized request using standard JSON")
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                msg = f"JSON deserialization failed: {e}"
                raise InvalidParameterValueException(msg) from e

        # Handle case where path is None for testing
        if path is None:
            path = Route.from_string("")

        return cls(
            method=method,
            path=path,
            headers=headers,
            query_params=query_params,
            body=body_dict,
        )


@dataclass(frozen=True)
class HTTPResponse:
    """HTTP response data model with dict body and serialization capabilities."""

    status_code: int
    headers: dict[str, str]
    body: dict[str, Any]

    def body_to_bytes(self, operation_name: str | None = None) -> bytes:
        """Convert response dict body to bytes for HTTP transmission.

        Args:
            operation_name: Optional AWS operation name for boto serialization

        Returns:
            bytes: Serialized response body

        Raises:
            InvalidParameterValueException: If serialization fails with both AWS and JSON methods
        """
        # Try AWS serialization first if operation_name provided
        if operation_name:
            try:
                serializer = AwsRestJsonSerializer.create(operation_name)
                result = serializer.to_bytes(self.body)
                logger.debug(
                    "Successfully serialized response using AWS serializer for %s",
                    operation_name,
                )
                return result  # noqa: TRY300
            except InvalidParameterValueException as e:
                logger.warning(
                    "AWS serialization failed for %s, falling back to JSON: %s",
                    operation_name,
                    e,
                )
                # Fall back to standard JSON
                try:
                    result = json.dumps(self.body, separators=(",", ":")).encode(
                        "utf-8"
                    )
                    logger.debug("Successfully serialized response using JSON fallback")
                    return result  # noqa: TRY300
                except (TypeError, ValueError) as json_error:
                    msg = f"Both AWS and JSON serialization failed: AWS error: {e}, JSON error: {json_error}"
                    raise InvalidParameterValueException(msg) from json_error
        else:
            # Use standard JSON serialization
            try:
                result = json.dumps(self.body, separators=(",", ":")).encode("utf-8")
                logger.debug("Successfully serialized response using standard JSON")
                return result  # noqa: TRY300
            except (TypeError, ValueError) as e:
                msg = f"JSON serialization failed: {e}"
                raise InvalidParameterValueException(msg) from e

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> HTTPResponse:
        """Create HTTPResponse from dict data.

        Args:
            data: Response data as dictionary
            status_code: HTTP status code (default: 200)
            headers: HTTP headers (default: empty dict)

        Returns:
            HTTPResponse: Response with dict body
        """
        if headers is None:
            headers = {}

        return cls(status_code=status_code, headers=headers, body=data)

    @staticmethod
    def create_json(
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
            HTTPResponse: The HTTP response with dict body
        """
        headers = {"Content-Type": "application/json"}
        if additional_headers:
            headers.update(additional_headers)

        return HTTPResponse(status_code=status_code, headers=headers, body=data)

    # Removed deprecated create_error method - use create_error_from_exception instead

    @staticmethod
    def create_error_from_exception(aws_exception: AwsApiException) -> HTTPResponse:
        """Create AWS-compliant error response from AwsApiException.

        Args:
            aws_exception: The AWS API exception to convert to HTTP response

        Returns:
            HTTPResponse: The HTTP error response with AWS-compliant format
        """
        if not isinstance(aws_exception, AwsApiException):
            msg = f"Expected AwsApiException, got {type(aws_exception)}"
            raise TypeError(msg)

        # Use exception's http_status_code and to_dict() method
        # This removes the wrapper "error" object to match AWS format
        error_data = aws_exception.to_dict()
        return HTTPResponse.create_json(aws_exception.http_status_code, error_data)

    @staticmethod
    def create_empty(
        status_code: int, additional_headers: dict[str, str] | None = None
    ) -> HTTPResponse:
        """Create an empty HTTP response.

        Args:
            status_code: HTTP status code
            additional_headers: Optional additional headers to include

        Returns:
            HTTPResponse: The HTTP response with empty dict body
        """
        headers = {}
        if additional_headers:
            headers.update(additional_headers)

        return HTTPResponse(status_code=status_code, headers=headers, body={})


class OperationHandler(Protocol):
    """Protocol for handling HTTP operations with strongly-typed paths."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:
        """Handle an HTTP request and return an HTTP response.

        Args:
            parsed_route: The strongly-typed route object
            request: The HTTP request data

        Returns:
            HTTPResponse: The HTTP response to send to the client
        """
        ...  # pragma: no cover


def parse_json_body(request: HTTPRequest) -> dict[str, Any]:
    """Parse JSON body from HTTP request.

    Args:
        request: The HTTP request containing the dict body

    Returns:
        dict: The parsed JSON data (now just returns the body directly)

    Raises:
        ValueError: If the request body is empty
    """
    if not request.body:
        msg = "Request body is required"
        raise InvalidParameterValueException(msg)

    return request.body
