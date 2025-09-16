"""Exceptions for the Durable Executions Testing Library.

Avoid any non-stdlib references in this module, it is at the bottom of the dependency chain.
"""

from __future__ import annotations


# region Local Runner
class DurableFunctionsLocalRunnerError(Exception):
    """Base class for Durable Executions exceptions"""


class InvalidParameterError(DurableFunctionsLocalRunnerError):
    pass


class IllegalStateError(DurableFunctionsLocalRunnerError):
    pass


class ResourceNotFoundError(DurableFunctionsLocalRunnerError):
    pass


# endregion Local Runner


# region Testing
class DurableFunctionsTestError(Exception):
    """Base class for testing errors."""


# endregion Testing
