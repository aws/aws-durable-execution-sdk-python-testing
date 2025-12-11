"""Advanced event assertion helper for examples.

This module provides sophisticated event assertion capabilities with three categories:
1. STRICT_EQUAL: Key and value must match exactly
2. KEY_EQUAL: Key must exist but value can vary
3. IGNORE: Field is completely ignored

The helper handles nested objects and provides detailed assertion control.
"""

import json
import logging
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Set

logger = logging.getLogger(__name__)


class FieldCategory(Enum):
    """Field assertion categories."""

    STRICT_EQUAL = "strict_equal"  # Key and value must match exactly
    KEY_EQUAL = "key_equal"  # Key must exist but value can vary
    IGNORE = "ignore"  # Field is completely ignored


class EventAssertionError(Exception):
    """Exception raised when event assertions fail."""

    pass


# Field categorization for Event assertions using dot notation for nested fields
FIELD_CATEGORIES = {
    # STRICT_EQUAL: Key and value must match exactly
    FieldCategory.STRICT_EQUAL: {
        "event_type",  # Must match exactly
        "sub_type",  # Must match exactly (Step, Wait, etc.)
        "name",  # Must match exactly
        # Nested field examples - use dot notation
        "execution_succeeded_details.result",  # Execution result should match
        "wait_started_details.duration",  # Wait duration should match exactly
        "wait_succeeded_details.duration",  # Wait duration should match exactly
    },
    # KEY_EQUAL: Key must exist but value can vary
    FieldCategory.KEY_EQUAL: {
        "event_timestamp",  # Must exist but timestamp will vary
        "event_id",  # Must exist but ID will vary
        "operation_id",  # Must exist but UUID will vary
        "parent_id",  # Must exist but UUID will vary
    },
    # IGNORE: Completely ignore these fields
    FieldCategory.IGNORE: set(),
}

# Event type specific overrides using same format as FIELD_CATEGORIES
# Fields are optional - only specify what you want to override
EVENT_TYPE_OVERRIDE = {
    "ExecutionStarted": {
        FieldCategory.IGNORE: {"name"},  # Execution names can vary based on test setup
    },
    "ExecutionSucceeded": {
        FieldCategory.IGNORE: {"name"},  # Execution names can vary based on test setup
    },
}


def get_nested_value(obj: Any, path: str) -> Any:
    """Get a nested value from an object using dot notation.

    Args:
        obj: Object to get value from
        path: Dot-separated path (e.g., 'step_succeeded_details.result')

    Returns:
        The nested value or None if path doesn't exist
    """
    if obj is None:
        return None

    current = obj
    for part in path.split("."):
        if hasattr(current, "__dict__"):
            current = getattr(current, part, None)
        elif isinstance(current, dict):
            current = current.get(part)
        else:
            return None

        if current is None:
            return None

    return current


def get_field_category(field_path: str, event_type: str = "") -> FieldCategory:
    """Get the category for a field path, considering event type overrides.

    Override only affects specific keys mentioned in the override dict.
    All other keys follow the general FIELD_CATEGORIES rules.

    Args:
        field_path: Field path (can be nested with dots)
        event_type: Event type for override checking

    Returns:
        FieldCategory enum value
    """
    # Check event type specific overrides first - only for keys explicitly mentioned
    if event_type and event_type in EVENT_TYPE_OVERRIDE:
        override_categories = EVENT_TYPE_OVERRIDE[event_type]

        # Check if this specific field_path is mentioned in any override category
        if field_path in override_categories.get(FieldCategory.STRICT_EQUAL, set()):
            return FieldCategory.STRICT_EQUAL
        elif field_path in override_categories.get(FieldCategory.KEY_EQUAL, set()):
            return FieldCategory.KEY_EQUAL
        elif field_path in override_categories.get(FieldCategory.IGNORE, set()):
            return FieldCategory.IGNORE
        # If field_path is not in any override category, fall through to general rules

    # Apply general FIELD_CATEGORIES rules for all other fields
    if field_path in FIELD_CATEGORIES[FieldCategory.STRICT_EQUAL]:
        return FieldCategory.STRICT_EQUAL
    elif field_path in FIELD_CATEGORIES[FieldCategory.KEY_EQUAL]:
        return FieldCategory.KEY_EQUAL
    elif field_path in FIELD_CATEGORIES[FieldCategory.IGNORE]:
        return FieldCategory.IGNORE
    else:
        # Default behavior for unspecified fields
        return FieldCategory.IGNORE


def assert_field_by_category(
    field_path: str,
    expected_value: Any,
    actual_value: Any,
    event_type: str = "",
    context: str = "",
) -> None:
    """Assert a field value based on its category.

    Args:
        field_path: Path to the field (can be nested with dots)
        expected_value: Expected value from JSON
        actual_value: Actual value from event object (can be dict, object, or primitive)
        event_type: Event type for override checking
        context: Context string for error messages
    """
    category = get_field_category(field_path, event_type)

    if category is FieldCategory.STRICT_EQUAL:
        # Convert actual_value to comparable format if it's an object
        if expected_value != actual_value:
            raise EventAssertionError(
                f"{context}Field '{field_path}' strict equality failed: "
                f"expected {expected_value}, got {actual_value}"
            )
    elif category is FieldCategory.KEY_EQUAL:
        # Just check that both have the field (not None)
        if expected_value is not None and actual_value is None:
            raise EventAssertionError(
                f"{context}Field '{field_path}' missing in actual event"
            )
        if expected_value is None and actual_value is not None:
            raise EventAssertionError(
                f"{context}Field '{field_path}' unexpected in actual event"
            )
    # If category is FieldCategory.IGNORE, do nothing


def assert_nested_fields(
    expected_obj: dict,
    actual_obj: Any,
    parent_path: str = "",
    event_type: str = "",
    context: str = "",
) -> None:
    """Recursively assert nested fields using dot notation paths.

    Args:
        expected_obj: Expected object/dict from JSON
        actual_obj: Actual object from event
        parent_path: Current path prefix for nested fields
        event_type: Event type for override checking
        context: Context string for error messages
    """
    if not isinstance(expected_obj, dict):
        return

    for key, expected_value in expected_obj.items():
        current_path = f"{parent_path}.{key}" if parent_path else key

        # Get actual value using dot notation
        actual_value = get_nested_value(actual_obj, current_path)

        if isinstance(expected_value, dict) and expected_value:
            # This is a nested object, recurse into it
            assert_nested_fields(
                expected_value, actual_obj, current_path, event_type, context
            )
        else:
            # This is a leaf value, assert it based on category
            assert_field_by_category(
                current_path, expected_value, actual_value, event_type, context
            )


def assert_events(path_to_json: str, events: list[Any]) -> None:
    """Advanced event assertion with categorized field checking.

    This function provides sophisticated event assertion with three categories:
    - STRICT_EQUAL: Key and value must match exactly
    - KEY_EQUAL: Key must exist but value can vary
    - IGNORE: Field is completely ignored

    Args:
        path_to_json: Path to JSON file containing expected events
        events: List of actual Event objects from execution

    Raises:
        EventAssertionError: If events don't match expectations
        FileNotFoundError: If JSON file doesn't exist

    Example:
        assert_events('events/hello_world_events.json', result.events)
    """
    events_file_path = Path(path_to_json)

    logger.info(f"Asserting events from: {path_to_json}")

    # Load expected data
    if not events_file_path.exists():
        raise FileNotFoundError(f"Events file not found: {events_file_path}")

    with open(events_file_path, "r", encoding="utf-8") as f:
        expected_data = json.load(f)

    expected_events = expected_data.get("events", [])

    # 1. Assert total event count
    if len(events) != len(expected_events):
        raise EventAssertionError(
            f"Event count mismatch: expected {len(expected_events)}, got {len(events)}"
        )

    # 2. Assert each event with categorized field checking using dot notation
    for i, (actual_event, expected_event) in enumerate(zip(events, expected_events)):
        context = f"Event {i}: "

        # Get event type for override checking
        event_type = expected_event.get("event_type", "")

        # Use recursive nested field assertion with dot notation
        for field_name, expected_value in expected_event.items():
            # Get actual value using dot notation (handles both simple and nested fields)
            actual_value = get_nested_value(actual_event, field_name)

            # Check if this field path should be treated as a whole object assertion
            field_category = get_field_category(field_name, event_type)

            if (
                isinstance(expected_value, dict)
                and expected_value
                and field_category is not FieldCategory.STRICT_EQUAL
            ):
                # This is a nested object and NOT marked for strict_equal, recurse into it
                assert_nested_fields(
                    expected_value, actual_event, field_name, event_type, context
                )
            else:
                # This is either:
                # 1. A leaf value (string, int, etc.)
                # 2. A nested object marked for strict_equal (assert the whole dict)
                # 3. An empty dict
                assert_field_by_category(
                    field_name, expected_value, actual_value, event_type, context
                )

    logger.info(
        f"✅ All {len(events)} events match expected patterns from {path_to_json}"
    )
