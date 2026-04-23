#!/usr/bin/env python3
"""CLI tool for generating event assertion files from durable function executions.

This tool runs durable functions locally and captures their execution events
to generate JSON files that can be used for event-based test assertions.

Usage:
    python examples/cli_event_generator.py \
        --function-module examples.src.hello_world \
        --function-name handler \
        --input '{"test": "data"}' \
        --output examples/events/hello_world_events.json
"""

import argparse
import importlib
import json
import logging
import sys
from pathlib import Path
from typing import Any

# Add src directories to Python path
examples_dir = Path(__file__).parent
src_dir = examples_dir / "src"
main_src_dir = examples_dir.parent / "src"

for path in [str(src_dir), str(main_src_dir)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner


logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the CLI tool."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(levelname)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def import_function(module_name: str, function_name: str) -> Any:
    """Import a function from a module dynamically.

    Args:
        module_name: Python module path (e.g., 'examples.src.hello_world')
        function_name: Function name within the module (e.g., 'handler')

    Returns:
        The imported function

    Raises:
        ImportError: If module or function cannot be imported
    """
    try:
        module = importlib.import_module(module_name)
        return getattr(module, function_name)
    except ImportError as e:
        raise ImportError(f"Failed to import module '{module_name}': {e}") from e
    except AttributeError as e:
        raise ImportError(
            f"Function '{function_name}' not found in module '{module_name}': {e}"
        ) from e


def serialize_event(event: Any) -> dict:
    """Serialize an Event object to a JSON-serializable dictionary.

    Args:
        event: Event object to serialize

    Returns:
        Dictionary representation of the event
    """
    # Convert the event to a dictionary, handling datetime objects
    event_dict = {}

    for field_name, field_value in event.__dict__.items():
        if field_value is None:
            continue

        if hasattr(field_value, "isoformat"):  # datetime objects
            event_dict[field_name] = field_value.isoformat()
        elif hasattr(field_value, "__dict__"):  # nested objects
            event_dict[field_name] = serialize_nested_object(field_value)
        else:
            event_dict[field_name] = field_value

    return event_dict


def serialize_nested_object(obj: Any) -> dict:
    """Serialize nested objects recursively."""
    if obj is None:
        return None

    result = {}
    for field_name, field_value in obj.__dict__.items():
        if field_value is None:
            continue

        if hasattr(field_value, "isoformat"):  # datetime objects
            result[field_name] = field_value.isoformat()
        elif hasattr(field_value, "__dict__"):  # nested objects
            result[field_name] = serialize_nested_object(field_value)
        else:
            result[field_name] = field_value

    return result


def generate_events_file(
    function_module: str,
    function_name: str,
    input_data: str | None,
    output_path: Path,
    timeout: int = 60,
) -> None:
    """Generate events file by running the durable function locally.

    Args:
        function_module: Python module containing the function
        function_name: Name of the durable function
        input_data: JSON string input for the function
        output_path: Path where to save the events JSON file
        timeout: Execution timeout in seconds
    """
    logger.info(f"Importing function {function_name} from {function_module}")
    handler = import_function(function_module, function_name)

    logger.info("Running durable function locally...")
    with DurableFunctionTestRunner(handler=handler) as runner:
        result = runner.run(input=input_data, timeout=timeout)

    logger.info(f"Execution completed with status: {result.status}")
    logger.info(f"Captured {len(result.events)} events")

    # Serialize events to JSON-compatible format
    events_data = {"events": [serialize_event(event) for event in result.events]}

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write events to JSON file
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(events_data, f, indent=2, ensure_ascii=False)

    logger.info(f"Events saved to: {output_path}")


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate event assertion files from durable function executions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate events for hello_world example
  python examples/cli_event_generator.py \\
    --function-module hello_world \\
    --function-name handler \\
    --input '"test input"' \\
    --output examples/events/hello_world_events.json

  # Generate events for a function with complex input
  python examples/cli_event_generator.py \\
    --function-module step.step_with_retry \\
    --function-name handler \\
    --input '{"retries": 3, "data": "test"}' \\
    --output examples/events/step_with_retry_events.json
        """,
    )

    parser.add_argument(
        "--function-module",
        required=True,
        help="Python module containing the durable function (e.g., 'hello_world' or 'step.step_with_retry')",
    )

    parser.add_argument(
        "--function-name",
        required=True,
        help="Name of the durable function within the module (e.g., 'handler')",
    )

    parser.add_argument(
        "--input", help="JSON string input for the function (default: None)"
    )

    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output path for the events JSON file",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Execution timeout in seconds (default: 60)",
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    try:
        generate_events_file(
            function_module=args.function_module,
            function_name=args.function_name,
            input_data=args.input,
            output_path=args.output,
            timeout=args.timeout,
        )
        logger.info("Event generation completed successfully!")

    except Exception as e:
        logger.error(f"Event generation failed: {e}")
        if args.verbose:
            logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
