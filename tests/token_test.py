"""Unit tests for token models."""

import base64
import json
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.token import (
    CallbackToken,
    CheckpointToken,
)


def test_checkpoint_token_init():
    """Test CheckpointToken initialization."""
    token = CheckpointToken("arn:aws:states:us-east-1:123456789012:execution:test", 42)

    assert token.execution_arn == "arn:aws:states:us-east-1:123456789012:execution:test"
    assert token.token_sequence == 42


def test_checkpoint_token_to_str():
    """Test CheckpointToken serialization to string."""
    token = CheckpointToken("arn:aws:states:us-east-1:123456789012:execution:test", 42)

    result = token.to_str()

    # Decode and verify the structure
    decoded = base64.b64decode(result).decode()
    data = json.loads(decoded)
    assert data["arn"] == "arn:aws:states:us-east-1:123456789012:execution:test"
    assert data["seq"] == 42


def test_checkpoint_token_from_str():
    """Test CheckpointToken deserialization from string."""
    data = {"arn": "arn:aws:states:us-east-1:123456789012:execution:test", "seq": 42}
    json_str = json.dumps(data, separators=(",", ":"))
    token_str = base64.b64encode(json_str.encode()).decode()

    token = CheckpointToken.from_str(token_str)

    assert token.execution_arn == "arn:aws:states:us-east-1:123456789012:execution:test"
    assert token.token_sequence == 42


def test_checkpoint_token_round_trip():
    """Test CheckpointToken serialization and deserialization round trip."""
    original = CheckpointToken(
        "arn:aws:states:us-east-1:123456789012:execution:test", 123
    )

    token_str = original.to_str()
    restored = CheckpointToken.from_str(token_str)

    assert restored == original


def test_checkpoint_token_frozen_dataclass():
    """Test that CheckpointToken is immutable."""
    token = CheckpointToken("arn:aws:states:us-east-1:123456789012:execution:test", 42)

    with pytest.raises(AttributeError):
        token.execution_arn = "new-arn"

    with pytest.raises(AttributeError):
        token.token_sequence = 999


def test_callback_token_init():
    """Test CallbackToken initialization."""
    token = CallbackToken(
        "arn:aws:states:us-east-1:123456789012:execution:test", "op-123"
    )

    assert token.execution_arn == "arn:aws:states:us-east-1:123456789012:execution:test"
    assert token.operation_id == "op-123"


def test_callback_token_to_str():
    """Test CallbackToken serialization to string."""
    token = CallbackToken(
        "arn:aws:states:us-east-1:123456789012:execution:test", "op-123"
    )

    result = token.to_str()

    # Decode and verify the structure
    decoded = base64.b64decode(result).decode()
    data = json.loads(decoded)
    assert data["arn"] == "arn:aws:states:us-east-1:123456789012:execution:test"
    assert data["op"] == "op-123"


def test_callback_token_from_str():
    """Test CallbackToken deserialization from string."""
    data = {
        "arn": "arn:aws:states:us-east-1:123456789012:execution:test",
        "op": "op-123",
    }
    json_str = json.dumps(data, separators=(",", ":"))
    token_str = base64.b64encode(json_str.encode()).decode()

    token = CallbackToken.from_str(token_str)

    assert token.execution_arn == "arn:aws:states:us-east-1:123456789012:execution:test"
    assert token.operation_id == "op-123"


def test_callback_token_round_trip():
    """Test CallbackToken serialization and deserialization round trip."""
    original = CallbackToken(
        "arn:aws:states:us-east-1:123456789012:execution:test", "callback-op"
    )

    token_str = original.to_str()
    restored = CallbackToken.from_str(token_str)

    assert restored == original


def test_callback_token_frozen_dataclass():
    """Test that CallbackToken is immutable."""
    token = CallbackToken(
        "arn:aws:states:us-east-1:123456789012:execution:test", "op-123"
    )

    with pytest.raises(AttributeError):
        token.execution_arn = "new-arn"

    with pytest.raises(AttributeError):
        token.operation_id = "new-op"


def test_checkpoint_token_validate_for_execution_success():
    """Test successful token validation."""
    token = CheckpointToken("test-arn", 5)
    execution = Execution("test-arn", Mock(), [])
    execution._token_sequence = 10  # noqa: SLF001
    execution.generated_tokens = {token.to_str()}

    execution.validate_checkpoint_token(token.to_str())


def test_checkpoint_token_validate_for_execution_arn_mismatch():
    """Test token validation fails when ARN doesn't match."""
    token = CheckpointToken("test-arn", 5)
    execution = Execution("different-arn", "test-name", "test-input")
    execution._token_sequence = 10  # noqa: SLF001

    with pytest.raises(
        InvalidParameterValueException, match="does not match execution ARN"
    ):
        execution.validate_checkpoint_token(token.to_str())


def test_checkpoint_token_validate_for_execution_completed():
    """Test token validation fails when execution is complete."""
    token = CheckpointToken("test-arn", 5)
    start_input = Mock()
    execution = Execution("test-arn", start_input, [])
    execution.generated_tokens = {token.to_str()}  # Add token to used_tokens
    execution.is_complete = True

    with pytest.raises(InvalidParameterValueException, match="Invalid or expired"):
        execution.validate_checkpoint_token(token.to_str())


def test_checkpoint_token_validate_for_execution_future_sequence():
    """Test token validation fails when token sequence is from future."""
    token = CheckpointToken("test-arn", 15)
    execution = Execution("test-arn", "test-name", "test-input")
    execution._token_sequence = 10  # noqa: SLF001

    with pytest.raises(InvalidParameterValueException, match="Invalid or expired"):
        execution.validate_checkpoint_token(token.to_str())


def test_checkpoint_token_validate_for_execution_equal_sequence():
    """Test token validation succeeds when sequences are equal."""
    token = CheckpointToken("test-arn", 10)
    execution = Execution("test-arn", "test-name", "test-input")
    execution._token_sequence = 10  # noqa: SLF001
    execution.generated_tokens = {token.to_str()}

    execution.validate_checkpoint_token(token.to_str())


def test_checkpoint_token_validate_for_execution_not_in_used_tokens():
    """Test token validation fails when token not in used_tokens."""
    token = CheckpointToken("test-arn", 5)
    execution = Execution("test-arn", "test-name", "test-input")
    execution._token_sequence = 10  # noqa: SLF001
    execution.generated_tokens = {"other-token"}

    with pytest.raises(
        InvalidParameterValueException, match="Invalid checkpoint token"
    ):
        execution.validate_checkpoint_token(token.to_str())


def test_checkpoint_token_validate_for_execution_in_used_tokens():
    """Test token validation succeeds when token is in used_tokens."""
    token = CheckpointToken("test-arn", 5)
    execution = Execution("test-arn", "test-name", "test-input")
    execution._token_sequence = 10  # noqa: SLF001
    # Mock the token string that would be generated
    token_str = token.to_str()
    execution.generated_tokens = {token_str}

    execution.validate_checkpoint_token(token_str)
