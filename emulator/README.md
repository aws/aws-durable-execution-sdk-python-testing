# AWS Durable Execution Emulator

A local emulator for AWS Lambda durable functions that enables local development and testing of durable function applications. Powered by the AWS Durable Execution Testing SDK for Python.

## Overview

The AWS Lambda Durable Execution Emulator provides a local development environment for building and testing durable function applications before deploying to AWS Lambda. It uses the AWS Durable Execution Testing SDK for Python as its execution engine, providing robust durable execution capabilities with full AWS API compatibility.

## Features

- **Local Development**: Run durable functions locally without AWS infrastructure
- **API Compatibility**: Compatible with AWS Lambda Durable Functions APIs
- **Health Check Endpoint**: Built-in health monitoring
- **Logging**: Configurable logging levels for debugging
- **Testing Support**: Built-in test framework support

## Installation

### From source

```bash
git clone https://github.com/aws/aws-lambda-durable-functions-emulator.git
cd aws-lambda-durable-functions-emulator
hatch run pip install -e .
```

## Usage

### Starting the Emulator

```bash
# Using the installed command
durable-functions-emulator

# Or using hatch for development
hatch run dev

# With custom host and port
durable-functions-emulator --host 0.0.0.0 --port 8080
```

### Environment Variables

- `HOST`: Server host (default: 0.0.0.0)
- `PORT`: Server port (default: 5000)
- `LOG`: Logging level (default: INFO)
- `STORAGE_DIR`: Directory for persistent storage
- `EXECUTION_STORE_TYPE`: Type of execution store (default: sqlite)
  - `filesystem`: File-based storage
  - `sqlite`: SQLite database storage (default)
- `LAMBDA_ENDPOINT`: Lambda endpoint URL for testing
- `LOCAL_RUNNER_ENDPOINT`: Local runner endpoint URL
- `LOCAL_RUNNER_REGION`: AWS region for local runner
- `LOCAL_RUNNER_MODE`: Runner mode (default: local)

### Health Check

The emulator provides a health check endpoint:

```bash
curl http://localhost:5000/ping
```

## Development

### Prerequisites

- Python 3.13+
- [Hatch](https://hatch.pypa.io/) for project management

### Setup

```bash
git clone https://github.com/aws/aws-lambda-durable-functions-emulator.git
cd aws-lambda-durable-functions-emulator
hatch run pip install -e .
```

### Running Tests

```bash
# Run all tests
hatch run test

# Run with coverage
hatch run test:cov

# Type checking
hatch run types:check
```

### Building

```bash
hatch build
```

## API Reference

### Health Check

- **GET** `/ping` - Returns emulator status

### Durable Execution APIs

- **POST** `/2025-12-01/durable-execution-state/<token>/checkpoint` - Checkpoint execution state
- **GET** `/2025-12-01/durable-execution-state/<token>/getState` - Get execution state
- **GET** `/2025-12-01/durable-executions/<arn>` - Get execution details
- **GET** `/2025-12-01/durable-executions/<arn>/history` - Get execution history

### Callback APIs

- **POST** `/2025-12-01/durable-execution-callbacks/<id>/succeed` - Send success callback
- **POST** `/2025-12-01/durable-execution-callbacks/<id>/fail` - Send failure callback
- **POST** `/2025-12-01/durable-execution-callbacks/<id>/heartbeat` - Send heartbeat callback

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Security

See [CONTRIBUTING.md](CONTRIBUTING.md#security-issue-notifications) for more information.
