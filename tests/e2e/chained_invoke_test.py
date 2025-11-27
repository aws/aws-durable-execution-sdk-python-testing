"""Integration tests for chained invoke functionality.

These tests verify the end-to-end flow of chained invokes, including:
- Handler registration and invocation
- Result propagation
- Error handling
"""

import json
from typing import Any
from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ChainedInvokeOptions,
    ErrorObject,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processor import (
    CheckpointProcessor,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
    ResourceNotFoundException,
)
from aws_durable_execution_sdk_python_testing.executor import Executor
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestRunner,
    InvokeOperation,
)
from aws_durable_execution_sdk_python_testing.scheduler import Scheduler
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)


class TestChainedInvokeIntegration:
    """Integration tests for end-to-end chained invoke functionality."""

    def test_handler_registration_and_retrieval(self):
        """
        Test that handlers can be registered and retrieved from the test runner.

        _Requirements: 1.1, 1.4, 2.1_
        """

        def dummy_handler(event, context):
            return {"status": "ok"}

        def child_handler(event, context):
            return {"result": "child_result"}

        with DurableFunctionTestRunner(handler=dummy_handler) as runner:
            # Register handler
            runner.register_handler("child-function", child_handler)

            # Verify handler can be retrieved (returns wrapped marshalled handler)
            retrieved = runner.get_handler("child-function")
            assert retrieved is not None

            # Verify non-existent handler returns None
            assert runner.get_handler("non-existent") is None

    def test_multiple_handler_registration(self):
        """
        Test that multiple handlers can be registered independently.

        _Requirements: 1.4_
        """

        def dummy_handler(event, context):
            return {"status": "ok"}

        handlers = {
            "handler-a": lambda event, context: {"result": "a"},
            "handler-b": lambda event, context: {"result": "b"},
            "handler-c": lambda event, context: {"result": "c"},
        }

        with DurableFunctionTestRunner(handler=dummy_handler) as runner:
            # Register all handlers
            for name, handler in handlers.items():
                runner.register_handler(name, handler)

            # Verify all handlers are retrievable (returns wrapped marshalled handlers)
            for name in handlers:
                assert runner.get_handler(name) is not None

    def test_handler_registration_validation(self):
        """
        Test that handler registration validates inputs.

        _Requirements: 1.2, 1.3_
        """

        def dummy_handler(event, context):
            return {"status": "ok"}

        with DurableFunctionTestRunner(handler=dummy_handler) as runner:
            # Empty function name should raise
            with pytest.raises(
                InvalidParameterValueException, match="function_name is required"
            ):
                runner.register_handler("", lambda p: p)

            # None function name should raise
            with pytest.raises(
                InvalidParameterValueException, match="function_name is required"
            ):
                runner.register_handler(None, lambda p: p)

            # None handler should raise
            with pytest.raises(
                InvalidParameterValueException, match="handler is required"
            ):
                runner.register_handler("test-fn", None)


class TestChainedInvokeExecution:
    """Integration tests for chained invoke execution flow."""

    @pytest.fixture
    def test_components(self):
        """Create test components for chained invoke testing."""
        store = InMemoryExecutionStore()
        scheduler = Scheduler()
        scheduler.start()
        invoker = Mock()
        checkpoint_processor = CheckpointProcessor(store=store, scheduler=scheduler)

        handler_registry = {}

        executor = Executor(
            store=store,
            scheduler=scheduler,
            invoker=invoker,
            checkpoint_processor=checkpoint_processor,
            handler_registry=handler_registry,
        )

        # Wire up observer pattern
        checkpoint_processor.add_execution_observer(executor)

        yield {
            "store": store,
            "scheduler": scheduler,
            "invoker": invoker,
            "checkpoint_processor": checkpoint_processor,
            "executor": executor,
            "handler_registry": handler_registry,
        }

        scheduler.stop()

    def test_chained_invoke_handler_invocation(self, test_components):
        """
        Test that registered handlers are invoked with correct payload.

        _Requirements: 2.1, 2.3_
        """
        received_payloads = []

        def child_handler(payload: str | None) -> str | None:
            received_payloads.append(payload)
            return '{"result": "success"}'

        # Register handler directly in executor's registry (same dict reference)
        test_components["executor"]._handler_registry["child-fn"] = child_handler

        # Simulate chained invoke start via checkpoint processor
        execution_arn = "test-arn"
        operation_id = "op-123"
        input_payload = '{"input": "data"}'

        # Create a mock execution for the store
        mock_execution = Mock()
        mock_execution.durable_execution_arn = execution_arn
        mock_execution.is_complete = False
        mock_execution.get_new_checkpoint_token.return_value = "token-123"
        test_components["store"]._store[execution_arn] = mock_execution

        # Create completion event
        completion_event = test_components["scheduler"].create_event()
        test_components["executor"]._completion_events[execution_arn] = completion_event

        # Trigger chained invoke via observer
        test_components["executor"].on_chained_invoke_started(
            execution_arn=execution_arn,
            operation_id=operation_id,
            function_name="child-fn",
            payload=input_payload,
        )

        # Wait for handler to be invoked (scheduler runs async)
        import time

        time.sleep(0.1)

        # Verify handler was called with correct payload
        assert len(received_payloads) == 1
        assert received_payloads[0] == input_payload

    def test_chained_invoke_handler_not_found(self, test_components):
        """
        Test that ResourceNotFoundException is raised for unregistered handlers.

        _Requirements: 2.2_
        """
        execution_arn = "test-arn"
        operation_id = "op-123"

        # Don't register any handler

        # Trigger chained invoke should raise
        with pytest.raises(ResourceNotFoundException, match="No handler registered"):
            test_components["executor"].on_chained_invoke_started(
                execution_arn=execution_arn,
                operation_id=operation_id,
                function_name="non-existent-fn",
                payload='{"test": true}',
            )

    def test_chained_invoke_success_result_capture(self, test_components):
        """
        Test that successful handler results are captured via checkpoint.

        _Requirements: 2.3, 9.3_
        """
        result_payload = '{"computed": "value", "count": 42}'

        def child_handler(payload: str | None) -> str | None:
            return result_payload

        # Register handler directly in executor's registry
        test_components["executor"]._handler_registry["child-fn"] = child_handler

        # Track checkpoint calls
        checkpoint_calls = []
        original_process = test_components["checkpoint_processor"].process_checkpoint

        def mock_process_checkpoint(**kwargs):
            checkpoint_calls.append(kwargs)
            # Don't actually process to avoid side effects

        test_components[
            "checkpoint_processor"
        ].process_checkpoint = mock_process_checkpoint

        # Create mock execution
        execution_arn = "test-arn"
        mock_execution = Mock()
        mock_execution.durable_execution_arn = execution_arn
        mock_execution.is_complete = False
        mock_execution.get_new_checkpoint_token.return_value = "token-123"
        test_components["store"]._store[execution_arn] = mock_execution

        # Create completion event
        completion_event = test_components["scheduler"].create_event()
        test_components["executor"]._completion_events[execution_arn] = completion_event

        # Trigger chained invoke
        test_components["executor"].on_chained_invoke_started(
            execution_arn=execution_arn,
            operation_id="op-123",
            function_name="child-fn",
            payload='{"input": "data"}',
        )

        # Wait for handler to complete
        import time

        time.sleep(0.1)

        # Verify checkpoint was called with SUCCEED action
        assert len(checkpoint_calls) == 1
        updates = checkpoint_calls[0]["updates"]
        assert len(updates) == 1
        assert updates[0].action == OperationAction.SUCCEED
        assert updates[0].payload == result_payload

    def test_chained_invoke_failure_error_capture(self, test_components):
        """
        Test that handler exceptions are captured as errors via checkpoint.

        _Requirements: 2.4, 9.4_
        """
        error_message = "Handler failed with error"

        def failing_handler(payload: str | None) -> str | None:
            raise ValueError(error_message)

        # Register handler directly in executor's registry
        test_components["executor"]._handler_registry["failing-fn"] = failing_handler

        # Track checkpoint calls
        checkpoint_calls = []

        def mock_process_checkpoint(**kwargs):
            checkpoint_calls.append(kwargs)

        test_components[
            "checkpoint_processor"
        ].process_checkpoint = mock_process_checkpoint

        # Create mock execution
        execution_arn = "test-arn"
        mock_execution = Mock()
        mock_execution.durable_execution_arn = execution_arn
        mock_execution.is_complete = False
        mock_execution.get_new_checkpoint_token.return_value = "token-123"
        test_components["store"]._store[execution_arn] = mock_execution

        # Create completion event
        completion_event = test_components["scheduler"].create_event()
        test_components["executor"]._completion_events[execution_arn] = completion_event

        # Trigger chained invoke
        test_components["executor"].on_chained_invoke_started(
            execution_arn=execution_arn,
            operation_id="op-123",
            function_name="failing-fn",
            payload='{"input": "data"}',
        )

        # Wait for handler to complete
        import time

        time.sleep(0.1)

        # Verify checkpoint was called with FAIL action
        assert len(checkpoint_calls) == 1
        updates = checkpoint_calls[0]["updates"]
        assert len(updates) == 1
        assert updates[0].action == OperationAction.FAIL
        assert updates[0].error is not None
        assert error_message in updates[0].error.message


class TestNonDurableFunctionExecution:
    """
    Integration tests for non-durable child function execution.

    **Property 11: Non-Durable Function Synchronous Execution**

    *For any* non-durable child function, the invocation should complete synchronously
    and the result should be serialized and stored in the invoke operation.

    **Validates: Requirements 8.1, 8.2**
    """

    def test_non_durable_function_synchronous_execution(self):
        """
        Test that non-durable child functions execute synchronously.

        _Requirements: 8.1, 8.2_
        """
        execution_order = []

        def child_handler(event, context):
            execution_order.append("child_executed")
            value = event.get("value", 0) if event else 0
            return {"processed": value * 2}

        def dummy_handler(event, context):
            return {"status": "ok"}

        with DurableFunctionTestRunner(handler=dummy_handler) as runner:
            runner.register_handler("sync-child", child_handler)

            # Get the marshalled handler and invoke it directly (simulating synchronous execution)
            handler = runner.get_handler("sync-child")
            assert handler is not None

            # Execute synchronously (marshalled handler takes JSON string payload)
            result = handler('{"value": 21}')
            execution_order.append("after_child")

            # Verify synchronous execution
            assert execution_order == ["child_executed", "after_child"]
            assert result == '{"processed": 42}'

    def test_non_durable_function_result_serialization(self):
        """
        Test that non-durable function results are properly serialized.

        _Requirements: 8.2_
        """

        def child_handler(event, context):
            # Handler returns dict (Lambda-style), marshalled handler serializes it
            return {
                "string": "value",
                "number": 123,
                "boolean": True,
                "array": [1, 2, 3],
                "nested": {"key": "value"},
            }

        def dummy_handler(event, context):
            return {"status": "ok"}

        with DurableFunctionTestRunner(handler=dummy_handler) as runner:
            runner.register_handler("serializing-child", child_handler)

            handler = runner.get_handler("serializing-child")
            result = handler(None)

            # Verify result is valid JSON (marshalled handler serializes the dict)
            parsed = json.loads(result)
            assert parsed["string"] == "value"
            assert parsed["number"] == 123
            assert parsed["boolean"] is True
            assert parsed["array"] == [1, 2, 3]
            assert parsed["nested"]["key"] == "value"

    def test_non_durable_function_exception_capture(self):
        """
        Test that non-durable function exceptions are captured as ErrorObject.

        _Requirements: 8.3_
        """

        def failing_child(event, context):
            raise RuntimeError("Child function failed")

        def dummy_handler(event, context):
            return {"status": "ok"}

        with DurableFunctionTestRunner(handler=dummy_handler) as runner:
            runner.register_handler("failing-child", failing_child)

            handler = runner.get_handler("failing-child")

            # Verify exception is raised (marshalled handler propagates exceptions)
            with pytest.raises(RuntimeError, match="Child function failed"):
                handler('{"input": "data"}')
