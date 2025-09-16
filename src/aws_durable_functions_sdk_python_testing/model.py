"""Model classes."""

from dataclasses import dataclass


@dataclass(frozen=True)
class StartDurableExecutionInput:
    account_id: str
    function_name: str
    function_qualifier: str
    execution_name: str
    execution_timeout_seconds: int
    execution_retention_period_days: int
    invocation_id: str | None = None
    trace_fields: dict | None = None
    tenant_id: str | None = None
    input: str | None = None

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            account_id=data["AccountId"],
            function_name=data["FunctionName"],
            function_qualifier=data["FunctionQualifier"],
            execution_name=data["ExecutionName"],
            execution_timeout_seconds=data["ExecutionTimeoutSeconds"],
            execution_retention_period_days=data["ExecutionRetentionPeriodDays"],
            invocation_id=data.get("InvocationId"),
            trace_fields=data.get("TraceFields"),
            tenant_id=data.get("TenantId"),
            input=data.get("Input"),
        )

    def to_dict(self) -> dict:
        result = {
            "AccountId": self.account_id,
            "FunctionName": self.function_name,
            "FunctionQualifier": self.function_qualifier,
            "ExecutionName": self.execution_name,
            "ExecutionTimeoutSeconds": self.execution_timeout_seconds,
            "ExecutionRetentionPeriodDays": self.execution_retention_period_days,
        }
        if self.invocation_id is not None:
            result["InvocationId"] = self.invocation_id
        if self.trace_fields is not None:
            result["TraceFields"] = self.trace_fields
        if self.tenant_id is not None:
            result["TenantId"] = self.tenant_id
        if self.input is not None:
            result["Input"] = self.input
        return result


@dataclass(frozen=True)
class StartDurableExecutionOutput:
    execution_arn: str | None = None

    @classmethod
    def from_dict(cls, data: dict):
        return cls(execution_arn=data.get("ExecutionArn"))

    def to_dict(self) -> dict:
        result = {}
        if self.execution_arn is not None:
            result["ExecutionArn"] = self.execution_arn
        return result
