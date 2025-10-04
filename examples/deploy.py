#!/usr/bin/env python3

import json
import os
import sys
import zipfile
from pathlib import Path


try:
    import boto3
    from aws_durable_execution_sdk_python.lambda_service import LambdaClient
except ImportError:
    print("Error: boto3 and aws_durable_execution_sdk_python are required.")
    sys.exit(1)


def load_catalog():
    """Load examples catalog."""
    catalog_path = Path(__file__).parent / "examples-catalog.json"
    with open(catalog_path) as f:
        return json.load(f)


def create_deployment_package(example_name: str) -> Path:
    """Create deployment package for example."""
    print(f"Creating deployment package for {example_name}...")

    # Use the build directory that already has SDK + examples
    build_dir = Path(__file__).parent / "build"
    if not build_dir.exists():
        msg = "Build directory not found. Run 'hatch run examples:build' first."
        raise ValueError(msg)

    # Create zip from build directory
    zip_path = Path(__file__).parent / f"{example_name}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for file_path in build_dir.rglob("*"):
            if file_path.is_file():
                zf.write(file_path, file_path.relative_to(build_dir))

    print(f"Package created: {zip_path}")
    return zip_path


def deploy_function(example_config: dict, function_name: str):
    """Deploy function to AWS Lambda."""
    handler_file = example_config["handler"].replace(".handler", "")
    zip_path = create_deployment_package(handler_file)

    # AWS configuration
    region = os.getenv("AWS_REGION", "us-west-2")
    lambda_endpoint = os.getenv("LAMBDA_ENDPOINT")
    account_id = os.getenv("AWS_ACCOUNT_ID")
    invoke_account_id = os.getenv("INVOKE_ACCOUNT_ID")
    kms_key_arn = os.getenv("KMS_KEY_ARN")

    print("Debug - Environment variables:")
    print(f"  AWS_REGION: {region}")
    print(f"  LAMBDA_ENDPOINT: {lambda_endpoint}")
    print(f"  AWS_ACCOUNT_ID: {account_id}")
    print(f"  INVOKE_ACCOUNT_ID: {invoke_account_id}")

    if not all([account_id, lambda_endpoint, invoke_account_id]):
        msg = "Missing required environment variables"
        raise ValueError(msg)

    # Initialize Lambda client with custom models
    LambdaClient.load_preview_botocore_models()

    # Use regular lambda client for now
    lambda_client = boto3.client(
        "lambda", endpoint_url=lambda_endpoint, region_name=region
    )

    role_arn = f"arn:aws:iam::{account_id}:role/DurableFunctionsIntegrationTestRole"

    # Function configuration
    function_config = {
        "FunctionName": function_name,
        "Runtime": "python3.13",
        "Role": role_arn,
        "Handler": example_config["handler"],
        "Description": example_config["description"],
        "Timeout": 60,
        "MemorySize": 128,
        "Environment": {"Variables": {"DEX_ENDPOINT": lambda_endpoint}},
        "DurableConfig": example_config["durableConfig"],
    }

    if kms_key_arn:
        function_config["KMSKeyArn"] = kms_key_arn

    # Read zip file
    with open(zip_path, "rb") as f:
        zip_content = f.read()

    try:
        # Try to get existing function
        lambda_client.get_function(FunctionName=function_name)
        print(f"Updating existing function: {function_name}")

        # Update code
        lambda_client.update_function_code(
            FunctionName=function_name, ZipFile=zip_content
        )

        # Update configuration
        lambda_client.update_function_configuration(**function_config)

    except lambda_client.exceptions.ResourceNotFoundException:
        print(f"Creating new function: {function_name}")

        # Create function
        lambda_client.create_function(**function_config, Code={"ZipFile": zip_content})

    # Add invoke permission
    try:
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId="dex-invoke-permission",
            Action="lambda:InvokeFunction",
            Principal=invoke_account_id,
        )
        print("Added invoke permission")
    except lambda_client.exceptions.ResourceConflictException:
        print("Invoke permission already exists")

    print(f"Successfully deployed: {function_name}")


def main():
    """Main deployment function."""
    if len(sys.argv) < 2:
        print("Usage: python deploy.py <example-name> [function-name]")
        sys.exit(1)

    example_name = sys.argv[1]
    function_name = sys.argv[2] if len(sys.argv) > 2 else f"{example_name}-Python"

    catalog = load_catalog()

    # Find example
    example_config = None
    for example in catalog["examples"]:
        if example["handler"].startswith(example_name):
            example_config = example
            break

    if not example_config:
        print(f"Example '{example_name}' not found in catalog")
        sys.exit(1)

    deploy_function(example_config, function_name)


if __name__ == "__main__":
    main()
