"""Unit tests for model.py."""

import pytest

from aws_durable_functions_sdk_python_testing.model import (
    StartDurableExecutionInput,
    StartDurableExecutionOutput,
)


def test_start_durable_execution_input_minimal():
    """Test StartDurableExecutionInput with only required fields."""
    data = {
        "AccountId": "123456789012",
        "FunctionName": "test-function",
        "FunctionQualifier": "$LATEST",
        "ExecutionName": "test-execution",
        "ExecutionTimeoutSeconds": 900,
        "ExecutionRetentionPeriodDays": 7,
    }

    input_obj = StartDurableExecutionInput.from_dict(data)

    assert input_obj.account_id == "123456789012"
    assert input_obj.function_name == "test-function"
    assert input_obj.function_qualifier == "$LATEST"
    assert input_obj.execution_name == "test-execution"
    assert input_obj.execution_timeout_seconds == 900
    assert input_obj.execution_retention_period_days == 7
    assert input_obj.invocation_id is None
    assert input_obj.trace_fields is None
    assert input_obj.tenant_id is None
    assert input_obj.input is None

    assert input_obj.to_dict() == data


def test_start_durable_execution_input_maximal():
    """Test StartDurableExecutionInput with all fields."""
    data = {
        "AccountId": "123456789012",
        "FunctionName": "test-function",
        "FunctionQualifier": "$LATEST",
        "ExecutionName": "test-execution",
        "ExecutionTimeoutSeconds": 900,
        "ExecutionRetentionPeriodDays": 7,
        "InvocationId": "invocation-123",
        "TraceFields": {"key": "value"},
        "TenantId": "tenant-456",
        "Input": '{"test": "data"}',
    }

    input_obj = StartDurableExecutionInput.from_dict(data)

    assert input_obj.account_id == "123456789012"
    assert input_obj.function_name == "test-function"
    assert input_obj.function_qualifier == "$LATEST"
    assert input_obj.execution_name == "test-execution"
    assert input_obj.execution_timeout_seconds == 900
    assert input_obj.execution_retention_period_days == 7
    assert input_obj.invocation_id == "invocation-123"
    assert input_obj.trace_fields == {"key": "value"}
    assert input_obj.tenant_id == "tenant-456"
    assert input_obj.input == '{"test": "data"}'

    assert input_obj.to_dict() == data


def test_start_durable_execution_output_minimal():
    """Test StartDurableExecutionOutput with no fields."""
    data = {}

    output_obj = StartDurableExecutionOutput.from_dict(data)

    assert output_obj.execution_arn is None
    assert output_obj.to_dict() == {}


def test_start_durable_execution_output_maximal():
    """Test StartDurableExecutionOutput with all fields."""
    data = {"ExecutionArn": "arn:aws:lambda:us-west-2:123456789012:execution:test"}

    output_obj = StartDurableExecutionOutput.from_dict(data)

    assert (
        output_obj.execution_arn
        == "arn:aws:lambda:us-west-2:123456789012:execution:test"
    )
    assert output_obj.to_dict() == data


def test_start_durable_execution_input_dataclass_properties():
    """Test that StartDurableExecutionInput is frozen."""
    input_obj = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=900,
        execution_retention_period_days=7,
    )

    with pytest.raises(AttributeError):
        input_obj.account_id = "different-account"


def test_start_durable_execution_output_dataclass_properties():
    """Test that StartDurableExecutionOutput is frozen."""
    output_obj = StartDurableExecutionOutput(execution_arn="test-arn")

    with pytest.raises(AttributeError):
        output_obj.execution_arn = "different-arn"
