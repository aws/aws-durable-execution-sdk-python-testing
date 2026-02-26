"""Main server for the AWS Lambda Durable Functions Emulator using testing library"""

import argparse
import logging
import sys
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python_testing.web.server import WebServer

from aws_lambda_durable_functions_emulator.config import (
    EmulatorConfig,
    get_host,
    get_lambda_endpoint,
    get_log_level,
    get_port,
    get_storage_dir,
)
from aws_lambda_durable_functions_emulator.factory import TestingLibraryComponentFactory

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.executor import Executor

# Configure logging
log_level = get_log_level()
logging.basicConfig(
    level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("durable_functions_emulator")
logger.setLevel(log_level)

# Suppress third-party debug logging
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger.info("Logging level set to: %s", logging.getLevelName(log_level))


class EmulatorServer:
    """Emulator server that wraps the testing library's WebServer."""

    def __init__(self, config: EmulatorConfig) -> None:
        self.config = config

        logger.info("Configuration validation passed")

        # Create testing library components using emulator config
        self.executor = self._create_executor(config)

        # Convert emulator config to testing library config
        web_config = self._create_web_config(config)

        # Use testing library's WebServer directly
        self.web_server: WebServer = WebServer(web_config, self.executor)

        logger.info("EmulatorServer initialized on %s:%s", config.host, config.port)

    def _create_executor(self, config: EmulatorConfig) -> "Executor":
        """Create executor with emulator configuration using factory."""
        return TestingLibraryComponentFactory.create_executor(config)

    def _create_web_config(self, config: EmulatorConfig):
        """Convert emulator config to testing library config."""
        return config.to_web_service_config()

    def start(self):
        """Start the emulator server."""
        try:
            logger.info("Starting emulator server...")
            with self.web_server:
                logger.info(
                    "Server listening on %s:%s", self.config.host, self.config.port
                )
                self.web_server.serve_forever()
        except KeyboardInterrupt:
            logger.info("Server shutdown requested by user")
        except Exception:
            logger.exception("Server error")
            raise
        finally:
            logger.info("Server shutdown complete")


def main():
    """Main entry point for the emulator server."""
    parser = argparse.ArgumentParser(
        description="AWS Lambda Durable Functions Emulator (powered by testing library)"
    )
    parser.add_argument(
        "--host",
        type=str,
        help="Host to bind to (default: from HOST env var or 0.0.0.0)",
    )
    parser.add_argument(
        "--port", type=int, help="Port to bind to (default: from PORT env var or 5000)"
    )
    args = parser.parse_args()

    try:
        # Create emulator configuration
        config = EmulatorConfig(
            host=args.host or get_host(), port=args.port or get_port()
        )

        # Create and start emulator server
        logger.info(
            "Starting AWS Lambda Durable Functions Emulator on %s:%s",
            config.host,
            config.port,
        )
        server = EmulatorServer(config)
        server.start()

    except ValueError:
        logger.exception("Configuration error")
        logger.info("Please check your configuration and try again.")
        logger.info("Environment variables:")
        logger.info("  HOST=%s", config.host if "config" in locals() else get_host())
        logger.info("  PORT=%s", config.port if "config" in locals() else get_port())
        logger.info("  LAMBDA_ENDPOINT=%s", get_lambda_endpoint())
        logger.info("  STORAGE_DIR=%s", get_storage_dir())
        sys.exit(1)

    except ImportError:
        logger.exception("Missing dependency")
        logger.info(
            "Please install the aws-durable-execution-sdk-python-testing package:"
        )
        logger.info("  pip install aws-durable-execution-sdk-python-testing")
        sys.exit(1)

    except OSError:
        logger.exception("Network error")
        logger.info("Failed to bind to %s:%s", config.host, config.port)
        logger.info("Please check that the port is not already in use and try again.")
        sys.exit(1)

    except Exception:
        logger.exception("Unexpected error during startup")
        logger.exception("Full error details:")
        sys.exit(1)


if __name__ == "__main__":
    main()
