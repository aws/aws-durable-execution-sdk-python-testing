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
        Test that unregistered handlers raise ResourceNotFoundException immediately.

        This is a test configuration error - the developer forgot to register a handler.
        We want to fail fast with a clear error message.

        _Requirements: 2.2_
        """
        execution_arn = "test-arn"
        operation_id = "op-123"

        # Don't register any handler

        # Create mock execution
        mock_execution = Mock()
        mock_execution.durable_execution_arn = execution_arn
        mock_execution.is_complete = False
        mock_execution.get_new_checkpoint_token.return_value = "token-123"
        test_components["store"]._store[execution_arn] = mock_execution

        # Create completion event
        completion_event = test_components["scheduler"].create_event()
        test_components["executor"]._completion_events[execution_arn] = completion_event

        # Trigger chained invoke - should raise ResourceNotFoundException immediately
        with pytest.raises(
            ResourceNotFoundException,
            match="No handler registered for function: non-existent-fn",
        ):
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


def test_chained_invoke_parent_invokes_child_and_receives_result() -> None:
    """
    Test that a parent function can invoke a child and receive its result.

    This is the basic happy path for chained invokes.
    _Requirements: 2.1, 2.3, 2.5_
    """
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )

    def child_handler(event, ctx):
        # Child doubles the value
        value = event.get("value", 0) if event else 0
        return {"doubled": value * 2}

    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        # Invoke child function and get result
        child_result = context.invoke(
            function_name="child-fn",
            payload={"value": 10},
            name="invoke-child",
        )
        return {"parent_received": child_result}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("child-fn", child_handler)
        result = runner.run(input=json.dumps({"test": "input"}), timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)
    assert parsed_result["parent_received"]["doubled"] == 20


def test_chained_invoke_parent_invokes_multiple_children_sequentially() -> None:
    """
    Test that a parent can invoke multiple children in sequence.

    _Requirements: 2.1, 2.3, 2.5_
    """
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )

    def adder_handler(event, ctx):
        return {"sum": event["a"] + event["b"]}

    def multiplier_handler(event, ctx):
        return {"product": event["value"] * event["factor"]}

    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        # Invoke first child
        result1 = context.invoke(
            function_name="adder",
            payload={"a": 5, "b": 3},
            name="add-step",
        )

        # Invoke second child with result from first
        result2 = context.invoke(
            function_name="multiplier",
            payload={"value": result1["sum"], "factor": 2},
            name="multiply-step",
        )

        return {"final": result2["product"]}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("adder", adder_handler)
        runner.register_handler("multiplier", multiplier_handler)
        result = runner.run(timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)
    # (5 + 3) * 2 = 16
    assert parsed_result["final"] == 16


def test_chained_invoke_with_steps() -> None:
    """
    Test that chained invokes work alongside regular steps.

    _Requirements: 2.1, 2.3, 2.5_
    """
    from aws_durable_execution_sdk_python.context import (
        DurableContext,
        durable_step,
    )
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )
    from aws_durable_execution_sdk_python.types import StepContext

    @durable_step
    def local_step(step_ctx: StepContext, value: int) -> int:
        return value + 100

    def remote_handler(event, ctx):
        return {"processed": event["input"] * 2}

    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        # Do a local step first
        step_result = context.step(local_step(5), name="local-step")

        # Then invoke a child function
        invoke_result = context.invoke(
            function_name="remote-fn",
            payload={"input": step_result},
            name="remote-invoke",
        )

        return {"step": step_result, "invoke": invoke_result}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("remote-fn", remote_handler)
        result = runner.run(timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)
    assert parsed_result["step"] == 105  # 5 + 100
    assert parsed_result["invoke"]["processed"] == 210  # 105 * 2


def test_chained_invoke_child_failure_propagates() -> None:
    """
    Test that child function failures are properly propagated to parent.

    _Requirements: 2.4, 8.3_
    """
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )

    def failing_handler(event, ctx):
        raise ValueError("Child function intentionally failed")

    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        # This invoke should fail
        result = context.invoke(
            function_name="failing-fn",
            payload={"trigger": "error"},
            name="failing-invoke",
        )
        return {"should_not_reach": result}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("failing-fn", failing_handler)
        result = runner.run(timeout=10)

    assert result.status is InvocationStatus.FAILED
    assert result.error is not None
    assert "Child function intentionally failed" in result.error.message


def test_chained_invoke_unregistered_handler_fails() -> None:
    """
    Test that invoking an unregistered handler fails the execution.

    This is a test configuration error - the developer forgot to register a handler.
    The ResourceNotFoundException is raised during checkpoint processing, which
    causes the execution to fail with an error.

    _Requirements: 2.2_
    """
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )

    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        # Try to invoke a handler that doesn't exist
        result = context.invoke(
            function_name="non-existent-fn",
            payload={"test": True},
            name="missing-invoke",
        )
        return {"result": result}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        # Don't register any handler - execution should fail
        result = runner.run(timeout=10)

    # The execution fails because the handler is not registered
    assert result.status is InvocationStatus.FAILED
    assert result.error is not None


def test_chained_invoke_with_none_payload() -> None:
    """
    Test that chained invoke works with None payload.

    _Requirements: 2.1, 2.3_
    """
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )

    def no_input_handler(event, ctx):
        # Handler that doesn't need input
        return {"message": "no input needed", "received": event}

    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        result = context.invoke(
            function_name="no-input-fn",
            payload=None,
            name="no-input-invoke",
        )
        return {"result": result}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("no-input-fn", no_input_handler)
        result = runner.run(timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)
    assert parsed_result["result"]["message"] == "no input needed"
    assert parsed_result["result"]["received"] is None


def test_chained_invoke_result_in_operations() -> None:
    """
    Test that chained invoke operations appear in the result operations list.

    _Requirements: 3.1, 3.2_
    """
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )

    def tracked_handler(event, ctx):
        return {"tracked": True, "data": event.get("data")}

    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        result = context.invoke(
            function_name="tracked-fn",
            payload={"data": "test"},
            name="tracked-invoke",
        )
        return {"result": result}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("tracked-fn", tracked_handler)
        result = runner.run(timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    # Find the invoke operation in the results
    invoke_op = result.get_invoke("tracked-invoke")
    assert invoke_op is not None
    assert invoke_op.status is OperationStatus.SUCCEEDED
    assert invoke_op.result is not None
    parsed_invoke_result = json.loads(invoke_op.result)
    assert parsed_invoke_result["tracked"] is True


def test_chained_invoke_within_map() -> None:
    """
    Test that chained invokes work correctly within a map operation.

    Each item in the map should be able to invoke a child function.
    _Requirements: 2.1, 2.3, 2.5_
    """
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )
    from aws_durable_execution_sdk_python.types import BatchResult

    def processor_handler(event, ctx):
        """Child handler that processes each item."""
        value = event.get("value", 0) if event else 0
        return {"processed": value * 10}

    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        items = [1, 2, 3, 4, 5]

        def process_item(ctx: DurableContext, item: int, idx: int, all_items) -> dict:
            # Each map iteration invokes a child function
            result = ctx.invoke(
                function_name="processor",
                payload={"value": item},
                name=f"process-{item}",
            )
            return {"item": item, "result": result}

        results: BatchResult = context.map(items, process_item, name="map-with-invokes")
        return {"results": results.get_results()}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("processor", processor_handler)
        result = runner.run(timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)
    results = parsed_result["results"]

    # Verify all items were processed
    assert len(results) == 5
    for i, r in enumerate(results):
        expected_item = i + 1
        assert r["item"] == expected_item
        assert r["result"]["processed"] == expected_item * 10


def test_chained_invoke_within_parallel() -> None:
    """
    Test that chained invokes work correctly within parallel operations.

    Each parallel branch should be able to invoke a child function.
    _Requirements: 2.1, 2.3, 2.5_
    """
    from aws_durable_execution_sdk_python.context import (
        DurableContext,
        durable_with_child_context,
    )
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )
    from aws_durable_execution_sdk_python.types import BatchResult

    def adder_handler(event, ctx):
        return {"sum": event["a"] + event["b"]}

    def multiplier_handler(event, ctx):
        return {"product": event["x"] * event["y"]}

    def divider_handler(event, ctx):
        return {"quotient": event["num"] / event["denom"]}

    @durable_with_child_context
    def branch_add(ctx: DurableContext, a: int, b: int) -> dict:
        result = ctx.invoke(
            function_name="adder",
            payload={"a": a, "b": b},
            name="add-invoke",
        )
        return {"operation": "add", "result": result}

    @durable_with_child_context
    def branch_multiply(ctx: DurableContext, x: int, y: int) -> dict:
        result = ctx.invoke(
            function_name="multiplier",
            payload={"x": x, "y": y},
            name="multiply-invoke",
        )
        return {"operation": "multiply", "result": result}

    @durable_with_child_context
    def branch_divide(ctx: DurableContext, num: int, denom: int) -> dict:
        result = ctx.invoke(
            function_name="divider",
            payload={"num": num, "denom": denom},
            name="divide-invoke",
        )
        return {"operation": "divide", "result": result}

    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        branches = [
            branch_add(10, 5),
            branch_multiply(6, 7),
            branch_divide(100, 4),
        ]

        results: BatchResult = context.parallel(branches, name="parallel-invokes")
        return {"results": results.get_results()}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("adder", adder_handler)
        runner.register_handler("multiplier", multiplier_handler)
        runner.register_handler("divider", divider_handler)
        result = runner.run(timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)
    results = parsed_result["results"]

    # Verify all branches completed
    assert len(results) == 3

    # Find each result by operation
    add_result = next(r for r in results if r["operation"] == "add")
    multiply_result = next(r for r in results if r["operation"] == "multiply")
    divide_result = next(r for r in results if r["operation"] == "divide")

    assert add_result["result"]["sum"] == 15  # 10 + 5
    assert multiply_result["result"]["product"] == 42  # 6 * 7
    assert divide_result["result"]["quotient"] == 25.0  # 100 / 4


def test_chained_invoke_failure_within_map() -> None:
    """
    Test that a chained invoke failure within map is properly handled.

    Map operations complete all branches and track failures individually.
    get_results() returns only successful results.
    _Requirements: 2.4, 8.3_
    """
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )
    from aws_durable_execution_sdk_python.lambda_service import OperationStatus
    from aws_durable_execution_sdk_python.types import BatchResult

    def failing_handler(event, ctx):
        value = event.get("value", 0) if event else 0
        if value == 3:
            raise ValueError(f"Cannot process value {value}")
        return {"processed": value}

    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        items = [1, 2, 3, 4, 5]

        def process_item(ctx: DurableContext, item: int, idx: int, all_items) -> dict:
            result = ctx.invoke(
                function_name="failing-processor",
                payload={"value": item},
                name=f"process-{item}",
            )
            return {"item": item, "result": result}

        results: BatchResult = context.map(items, process_item, name="map-with-failure")
        # get_results() returns only successful results
        return {"results": results.get_results()}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("failing-processor", failing_handler)
        result = runner.run(timeout=30)

    # Map completes successfully even with failed branches
    # get_results() filters out failed items
    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)
    # Only 4 results (items 1, 2, 4, 5 succeeded; item 3 failed)
    assert len(parsed_result["results"]) == 4

    # Verify the failed invoke operation is tracked
    map_op = result.operations[0]
    assert map_op.name == "map-with-failure"

    # Find the failed map iteration (item 3 is at index 2)
    failed_iteration = None
    for child in map_op.child_operations:
        if child.status is OperationStatus.FAILED:
            failed_iteration = child
            break

    assert failed_iteration is not None
    assert failed_iteration.error is not None
    assert "Cannot process value 3" in failed_iteration.error.message


def test_chained_invoke_failure_within_parallel() -> None:
    """
    Test that a chained invoke failure within parallel is properly handled.

    Parallel operations complete all branches and track failures individually.
    get_results() returns only successful results.
    _Requirements: 2.4, 8.3_
    """
    from aws_durable_execution_sdk_python.context import (
        DurableContext,
        durable_with_child_context,
    )
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )
    from aws_durable_execution_sdk_python.lambda_service import OperationStatus
    from aws_durable_execution_sdk_python.types import BatchResult

    def success_handler(event, ctx):
        return {"status": "ok"}

    def failing_handler(event, ctx):
        raise RuntimeError("Parallel branch failed intentionally")

    @durable_with_child_context
    def branch_success(ctx: DurableContext) -> dict:
        result = ctx.invoke(
            function_name="success-fn",
            payload={},
            name="success-invoke",
        )
        return result

    @durable_with_child_context
    def branch_failure(ctx: DurableContext) -> dict:
        result = ctx.invoke(
            function_name="failing-fn",
            payload={},
            name="failing-invoke",
        )
        return result

    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        branches = [
            branch_success(),
            branch_failure(),
        ]

        results: BatchResult = context.parallel(branches, name="parallel-with-failure")
        # get_results() returns only successful results
        return {"results": results.get_results()}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("success-fn", success_handler)
        runner.register_handler("failing-fn", failing_handler)
        result = runner.run(timeout=30)

    # Parallel completes successfully even with failed branches
    # get_results() filters out failed items
    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)
    # Only 1 result (branch_success succeeded; branch_failure failed)
    assert len(parsed_result["results"]) == 1
    assert parsed_result["results"][0]["status"] == "ok"

    # Verify the failed parallel branch is tracked
    parallel_op = result.operations[0]
    assert parallel_op.name == "parallel-with-failure"

    # Find the failed branch
    failed_branch = None
    for child in parallel_op.child_operations:
        if child.status is OperationStatus.FAILED:
            failed_branch = child
            break

    assert failed_branch is not None
    assert failed_branch.error is not None
    assert "Parallel branch failed intentionally" in failed_branch.error.message


def test_nested_map_with_chained_invokes() -> None:
    """
    Test chained invokes in a more complex scenario with nested operations.

    _Requirements: 2.1, 2.3, 2.5_
    """
    from aws_durable_execution_sdk_python.context import (
        DurableContext,
        durable_step,
    )
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )
    from aws_durable_execution_sdk_python.types import BatchResult, StepContext

    @durable_step
    def local_transform(step_ctx: StepContext, value: int) -> int:
        return value + 1

    def remote_double(event, ctx):
        return {"doubled": event["value"] * 2}

    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        items = [1, 2, 3]

        def process_item(ctx: DurableContext, item: int, idx: int, all_items) -> dict:
            # First do a local step
            transformed = ctx.step(local_transform(item), name=f"transform-{item}")

            # Then invoke a remote function
            remote_result = ctx.invoke(
                function_name="doubler",
                payload={"value": transformed},
                name=f"double-{item}",
            )

            return {
                "original": item,
                "transformed": transformed,
                "doubled": remote_result["doubled"],
            }

        results: BatchResult = context.map(items, process_item, name="complex-map")
        return {"results": results.get_results()}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("doubler", remote_double)
        result = runner.run(timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)
    results = parsed_result["results"]

    assert len(results) == 3
    # Item 1: transformed = 2, doubled = 4
    assert results[0]["original"] == 1
    assert results[0]["transformed"] == 2
    assert results[0]["doubled"] == 4
    # Item 2: transformed = 3, doubled = 6
    assert results[1]["original"] == 2
    assert results[1]["transformed"] == 3
    assert results[1]["doubled"] == 6
    # Item 3: transformed = 4, doubled = 8
    assert results[2]["original"] == 3
    assert results[2]["transformed"] == 4
    assert results[2]["doubled"] == 8


def test_double_chained_invoke() -> None:
    """
    Test that a parent can invoke a child, which invokes a grandchild.

    This tests nested/double chained invokes where:
    - Parent invokes Child (durable)
    - Child invokes Grandchild (non-durable)
    - Results propagate back up the chain

    _Requirements: 2.1, 2.3, 2.5_
    """
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )

    # Grandchild - the innermost function (non-durable, just a regular handler)
    def grandchild_handler(event, ctx):
        value = event.get("value", 0) if event else 0
        return {"grandchild_result": value * 3, "level": "grandchild"}

    # Child - a durable function that invokes the grandchild
    @durable_execution
    def child_function(event: Any, context: DurableContext) -> dict:
        value = event.get("value", 0) if event else 0
        # Child invokes grandchild
        grandchild_result = context.invoke(
            function_name="grandchild-fn",
            payload={"value": value * 2},
            name="invoke-grandchild",
        )
        return {
            "child_result": value * 2,
            "grandchild": grandchild_result,
            "level": "child",
        }

    # Parent - the outermost durable function
    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        # Parent invokes child
        child_result = context.invoke(
            function_name="child-fn",
            payload={"value": 5},
            name="invoke-child",
        )
        return {"parent_received": child_result, "level": "parent"}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        # Register child as durable (it's a @durable_execution function)
        runner.register_handler("child-fn", child_function, durable=True)
        # Register grandchild as non-durable (simple handler)
        runner.register_handler("grandchild-fn", grandchild_handler)
        result = runner.run(timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)

    # Verify the chain: parent -> child -> grandchild
    assert parsed_result["level"] == "parent"
    assert parsed_result["parent_received"]["level"] == "child"
    assert parsed_result["parent_received"]["child_result"] == 10  # 5 * 2
    assert parsed_result["parent_received"]["grandchild"]["level"] == "grandchild"
    assert (
        parsed_result["parent_received"]["grandchild"]["grandchild_result"] == 30
    )  # 5 * 2 * 3


def test_double_chained_invoke_with_failure_at_grandchild() -> None:
    """
    Test that failures in grandchild propagate back through the chain.

    _Requirements: 2.4, 8.3_
    """
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )

    # Grandchild that fails
    def failing_grandchild_handler(event, ctx):
        raise ValueError("Grandchild failed!")

    # Child that invokes the failing grandchild
    @durable_execution
    def child_function(event: Any, context: DurableContext) -> dict:
        grandchild_result = context.invoke(
            function_name="failing-grandchild",
            payload={},
            name="invoke-grandchild",
        )
        return {"grandchild": grandchild_result}

    # Parent
    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        child_result = context.invoke(
            function_name="child-fn",
            payload={},
            name="invoke-child",
        )
        return {"child": child_result}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("child-fn", child_function, durable=True)
        runner.register_handler("failing-grandchild", failing_grandchild_handler)
        result = runner.run(timeout=30)

    # The failure should propagate all the way up
    assert result.status is InvocationStatus.FAILED
    assert result.error is not None
    assert "Grandchild failed!" in result.error.message


def test_triple_chained_invoke() -> None:
    """
    Test three levels of chained invokes: parent -> child -> grandchild -> great-grandchild.

    _Requirements: 2.1, 2.3, 2.5_
    """
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )

    # Great-grandchild (non-durable)
    def great_grandchild_handler(event, ctx):
        value = event.get("value", 0) if event else 0
        return {"result": value + 1000, "level": 4}

    # Grandchild (durable)
    @durable_execution
    def grandchild_function(event: Any, context: DurableContext) -> dict:
        value = event.get("value", 0) if event else 0
        gg_result = context.invoke(
            function_name="great-grandchild",
            payload={"value": value + 100},
            name="invoke-gg",
        )
        return {"result": value + 100, "level": 3, "next": gg_result}

    # Child (durable)
    @durable_execution
    def child_function(event: Any, context: DurableContext) -> dict:
        value = event.get("value", 0) if event else 0
        gc_result = context.invoke(
            function_name="grandchild",
            payload={"value": value + 10},
            name="invoke-gc",
        )
        return {"result": value + 10, "level": 2, "next": gc_result}

    # Parent (durable)
    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        child_result = context.invoke(
            function_name="child",
            payload={"value": 1},
            name="invoke-child",
        )
        return {"result": 1, "level": 1, "next": child_result}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        # Register durable handlers with durable=True
        runner.register_handler("child", child_function, durable=True)
        runner.register_handler("grandchild", grandchild_function, durable=True)
        # Register non-durable handler
        runner.register_handler("great-grandchild", great_grandchild_handler)
        result = runner.run(timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)

    # Verify the full chain
    assert parsed_result["level"] == 1
    assert parsed_result["result"] == 1
    assert parsed_result["next"]["level"] == 2
    assert parsed_result["next"]["result"] == 11  # 1 + 10
    assert parsed_result["next"]["next"]["level"] == 3
    assert parsed_result["next"]["next"]["result"] == 111  # 1 + 10 + 100
    assert parsed_result["next"]["next"]["next"]["level"] == 4
    assert parsed_result["next"]["next"]["next"]["result"] == 1111  # 1 + 10 + 100 + 1000


def test_two_durable_children_sequentially() -> None:
    """
    Test that a parent can invoke two durable children in sequence.

    Parent invokes Durable Child A, then invokes Durable Child B.
    Both children are @durable_execution functions.

    _Requirements: 2.1, 2.3, 2.5_
    """
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )

    # Durable Child A - doubles the value
    @durable_execution
    def child_a_function(event: Any, context: DurableContext) -> dict:
        value = event.get("value", 0) if event else 0
        return {"result": value * 2, "source": "child_a"}

    # Durable Child B - adds 100 to the value
    @durable_execution
    def child_b_function(event: Any, context: DurableContext) -> dict:
        value = event.get("value", 0) if event else 0
        return {"result": value + 100, "source": "child_b"}

    # Parent - invokes both children in sequence
    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        # First invoke child A
        result_a = context.invoke(
            function_name="child-a",
            payload={"value": 5},
            name="invoke-child-a",
        )

        # Then invoke child B with result from A
        result_b = context.invoke(
            function_name="child-b",
            payload={"value": result_a["result"]},
            name="invoke-child-b",
        )

        return {
            "child_a_result": result_a,
            "child_b_result": result_b,
            "final": result_b["result"],
        }

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("child-a", child_a_function, durable=True)
        runner.register_handler("child-b", child_b_function, durable=True)
        result = runner.run(timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)

    # Child A: 5 * 2 = 10
    assert parsed_result["child_a_result"]["result"] == 10
    assert parsed_result["child_a_result"]["source"] == "child_a"

    # Child B: 10 + 100 = 110
    assert parsed_result["child_b_result"]["result"] == 110
    assert parsed_result["child_b_result"]["source"] == "child_b"

    assert parsed_result["final"] == 110


def test_durable_grandchild_with_wait() -> None:
    """
    Test that a durable grandchild can use context.wait() which causes PENDING state.

    This tests the replay mechanism where:
    - Parent invokes Child (durable)
    - Child invokes Grandchild (durable)
    - Grandchild uses context.wait() which suspends execution
    - After wait completes, grandchild resumes and returns result
    - Results propagate back up the chain

    _Requirements: 2.1, 2.3, 2.5_
    """
    from aws_durable_execution_sdk_python.config import Duration
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )

    # Grandchild - uses wait() which causes PENDING state
    @durable_execution
    def grandchild_function(event: Any, context: DurableContext) -> dict:
        value = event.get("value", 0) if event else 0

        # Wait for 10 seconds - this will cause the execution to go PENDING
        context.wait(Duration.from_seconds(10), name="grandchild-wait")

        return {"grandchild_result": value * 3, "waited": True}

    # Child - invokes the grandchild
    @durable_execution
    def child_function(event: Any, context: DurableContext) -> dict:
        value = event.get("value", 0) if event else 0

        grandchild_result = context.invoke(
            function_name="grandchild",
            payload={"value": value * 2},
            name="invoke-grandchild",
        )

        return {
            "child_result": value * 2,
            "grandchild": grandchild_result,
        }

    # Parent
    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        child_result = context.invoke(
            function_name="child",
            payload={"value": 5},
            name="invoke-child",
        )
        return {"parent_received": child_result}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("child", child_function, durable=True)
        runner.register_handler("grandchild", grandchild_function, durable=True)
        result = runner.run(timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)

    # Verify the chain completed correctly
    # Parent -> Child (5 * 2 = 10) -> Grandchild (10 * 3 = 30)
    assert parsed_result["parent_received"]["child_result"] == 10
    assert parsed_result["parent_received"]["grandchild"]["grandchild_result"] == 30
    assert parsed_result["parent_received"]["grandchild"]["waited"] is True


def test_durable_child_with_multiple_waits() -> None:
    """
    Test that a durable child can have multiple wait operations.

    This tests multiple PENDING states in a single child execution.

    _Requirements: 2.1, 2.3, 2.5_
    """
    from aws_durable_execution_sdk_python.config import Duration
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )

    # Child with multiple waits
    @durable_execution
    def child_with_waits(event: Any, context: DurableContext) -> dict:
        value = event.get("value", 0) if event else 0
        steps = []

        # First wait
        context.wait(Duration.from_seconds(10), name="wait-1")
        steps.append("after-wait-1")

        # Second wait
        context.wait(Duration.from_seconds(10), name="wait-2")
        steps.append("after-wait-2")

        return {"result": value * 2, "steps": steps}

    # Parent
    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        child_result = context.invoke(
            function_name="child-with-waits",
            payload={"value": 10},
            name="invoke-child",
        )
        return {"child": child_result}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("child-with-waits", child_with_waits, durable=True)
        result = runner.run(timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)

    assert parsed_result["child"]["result"] == 20
    assert parsed_result["child"]["steps"] == ["after-wait-1", "after-wait-2"]


def test_durable_child_with_steps_and_waits() -> None:
    """
    Test that a durable child can combine steps, waits, and nested invokes.

    This is a complex scenario testing the full replay mechanism.

    _Requirements: 2.1, 2.3, 2.5_
    """
    from aws_durable_execution_sdk_python.config import Duration
    from aws_durable_execution_sdk_python.context import DurableContext, durable_step
    from aws_durable_execution_sdk_python.execution import (
        InvocationStatus,
        durable_execution,
    )
    from aws_durable_execution_sdk_python.types import StepContext

    @durable_step
    def compute_step(step_ctx: StepContext, value: int) -> int:
        return value + 100

    # Simple handler for nested invoke
    def simple_handler(event, ctx):
        return {"doubled": event.get("value", 0) * 2}

    # Child with steps, waits, and nested invoke
    @durable_execution
    def complex_child(event: Any, context: DurableContext) -> dict:
        value = event.get("value", 0) if event else 0
        operations = []

        # Step 1: compute
        step_result = context.step(compute_step(value), name="compute")
        operations.append(f"step:{step_result}")

        # Wait
        context.wait(Duration.from_seconds(10), name="wait-after-step")
        operations.append("waited")

        # Nested invoke
        invoke_result = context.invoke(
            function_name="simple-fn",
            payload={"value": step_result},
            name="nested-invoke",
        )
        operations.append(f"invoke:{invoke_result['doubled']}")

        return {"final": invoke_result["doubled"], "operations": operations}

    # Parent
    @durable_execution
    def parent_function(event: Any, context: DurableContext) -> dict:
        child_result = context.invoke(
            function_name="complex-child",
            payload={"value": 5},
            name="invoke-child",
        )
        return {"child": child_result}

    with DurableFunctionTestRunner(handler=parent_function) as runner:
        runner.register_handler("complex-child", complex_child, durable=True)
        runner.register_handler("simple-fn", simple_handler)
        result = runner.run(timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    parsed_result = json.loads(result.result)

    # 5 + 100 = 105, then 105 * 2 = 210
    assert parsed_result["child"]["final"] == 210
    assert parsed_result["child"]["operations"] == ["step:105", "waited", "invoke:210"]
