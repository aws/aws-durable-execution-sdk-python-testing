"""Unit tests for CheckpointProcessor."""

from unittest.mock import Mock, patch

import pytest
from aws_durable_functions_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    OperationAction,
    OperationType,
    OperationUpdate,
    StateOutput,
)

from aws_durable_functions_sdk_python_testing.checkpoint.processor import (
    CheckpointProcessor,
)
from aws_durable_functions_sdk_python_testing.exceptions import InvalidParameterError
from aws_durable_functions_sdk_python_testing.execution import Execution
from aws_durable_functions_sdk_python_testing.scheduler import Scheduler
from aws_durable_functions_sdk_python_testing.store import ExecutionStore
from aws_durable_functions_sdk_python_testing.token import CheckpointToken


def test_init():
    """Test CheckpointProcessor initialization."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)

    processor = CheckpointProcessor(store, scheduler)

    assert processor._store == store  # noqa: SLF001
    assert processor._scheduler == scheduler  # noqa: SLF001
    assert processor._notifier is not None  # noqa: SLF001
    assert processor._transformer is not None  # noqa: SLF001


def test_add_execution_observer():
    """Test adding execution observer."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)
    observer = Mock()

    processor.add_execution_observer(observer)

    # Verify observer was added to notifier
    assert observer in processor._notifier._observers  # noqa: SLF001


@patch(
    "aws_durable_functions_sdk_python_testing.checkpoint.processor.CheckpointValidator"
)
def test_process_checkpoint_success(mock_validator):
    """Test successful checkpoint processing."""
    # Setup mocks
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    # Mock execution
    execution = Mock(spec=Execution)
    execution.is_complete = False
    execution.token_sequence = 1
    execution.operations = []
    execution.updates = []
    execution.get_new_checkpoint_token.return_value = "new-token"
    execution.get_navigable_operations.return_value = []

    store.load.return_value = execution

    # Mock transformer
    with patch.object(processor._transformer, "process_updates") as mock_process:  # noqa: SLF001
        mock_process.return_value = ([], [])

        # Test data
        checkpoint_token = "test-token"  # noqa: S105
        updates = [
            OperationUpdate(
                operation_id="test-id",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
            )
        ]

        # Mock token parsing
        with patch.object(CheckpointToken, "from_str") as mock_from_str:
            mock_token = Mock()
            mock_token.execution_arn = "arn:test"
            mock_token.token_sequence = 1
            mock_from_str.return_value = mock_token

            result = processor.process_checkpoint(
                checkpoint_token, updates, "client-token"
            )

    # Verify calls
    store.load.assert_called_once_with("arn:test")
    mock_validator.validate_input.assert_called_once_with(updates, execution)
    mock_process.assert_called_once()
    store.update.assert_called_once_with(execution)

    # Verify result
    assert isinstance(result, CheckpointOutput)
    assert result.checkpoint_token == "new-token"  # noqa: S105
    assert isinstance(result.new_execution_state, CheckpointUpdatedExecutionState)


@patch(
    "aws_durable_functions_sdk_python_testing.checkpoint.processor.CheckpointValidator"
)
def test_process_checkpoint_invalid_token_complete_execution(mock_validator):
    """Test checkpoint processing with complete execution."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    # Mock execution as complete
    execution = Mock(spec=Execution)
    execution.is_complete = True
    execution.token_sequence = 1

    store.load.return_value = execution

    checkpoint_token = "test-token"  # noqa: S105
    updates = []

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_token.token_sequence = 1
        mock_from_str.return_value = mock_token

        with pytest.raises(InvalidParameterError, match="Invalid checkpoint token"):
            processor.process_checkpoint(checkpoint_token, updates, "client-token")


@patch(
    "aws_durable_functions_sdk_python_testing.checkpoint.processor.CheckpointValidator"
)
def test_process_checkpoint_invalid_token_sequence(mock_validator):
    """Test checkpoint processing with invalid token sequence."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    # Mock execution with different token sequence
    execution = Mock(spec=Execution)
    execution.is_complete = False
    execution.token_sequence = 2

    store.load.return_value = execution

    checkpoint_token = "test-token"  # noqa: S105
    updates = []

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_token.token_sequence = 1  # Different from execution
        mock_from_str.return_value = mock_token

        with pytest.raises(InvalidParameterError, match="Invalid checkpoint token"):
            processor.process_checkpoint(checkpoint_token, updates, "client-token")


@patch(
    "aws_durable_functions_sdk_python_testing.checkpoint.processor.CheckpointValidator"
)
def test_process_checkpoint_updates_execution_state(mock_validator):
    """Test that checkpoint processing updates execution state correctly."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    # Mock execution
    execution = Mock(spec=Execution)
    execution.is_complete = False
    execution.token_sequence = 1
    execution.operations = []
    execution.updates = []
    execution.get_new_checkpoint_token.return_value = "new-token"
    execution.get_navigable_operations.return_value = []

    store.load.return_value = execution

    # Mock transformer to return updated operations and updates
    updated_operations = [Mock()]
    all_updates = [Mock()]

    with patch.object(processor._transformer, "process_updates") as mock_process:  # noqa: SLF001
        mock_process.return_value = (updated_operations, all_updates)

        checkpoint_token = "test-token"  # noqa: S105
        updates = [
            OperationUpdate(
                operation_id="test-id",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
            )
        ]

        with patch.object(CheckpointToken, "from_str") as mock_from_str:
            mock_token = Mock()
            mock_token.execution_arn = "arn:test"
            mock_token.token_sequence = 1
            mock_from_str.return_value = mock_token

            processor.process_checkpoint(checkpoint_token, updates, "client-token")

    # Verify execution state was updated
    assert execution.operations == updated_operations
    # Check that updates were extended (execution.updates is a real list)
    assert len(execution.updates) == len(all_updates)


def test_get_execution_state():
    """Test getting execution state."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    # Mock execution
    execution = Mock(spec=Execution)
    navigable_ops = [Mock()]
    execution.get_navigable_operations.return_value = navigable_ops

    store.load.return_value = execution

    checkpoint_token = "test-token"  # noqa: S105

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_from_str.return_value = mock_token

        result = processor.get_execution_state(checkpoint_token, "next-marker", 500)

    # Verify calls
    store.load.assert_called_once_with("arn:test")
    execution.get_navigable_operations.assert_called_once()

    # Verify result
    assert isinstance(result, StateOutput)
    assert result.operations == navigable_ops
    assert result.next_marker is None


def test_get_execution_state_default_max_items():
    """Test getting execution state with default max_items."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    execution = Mock(spec=Execution)
    execution.get_navigable_operations.return_value = []
    store.load.return_value = execution

    checkpoint_token = "test-token"  # noqa: S105

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_from_str.return_value = mock_token

        result = processor.get_execution_state(checkpoint_token, "next-marker")

    assert isinstance(result, StateOutput)
