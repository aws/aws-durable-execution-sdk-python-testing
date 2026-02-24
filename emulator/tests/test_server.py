"""Tests for the emulator server"""

import pytest

from aws_lambda_durable_functions_emulator.config import EmulatorConfig
from aws_lambda_durable_functions_emulator.server import EmulatorServer


def test_emulator_config_creation():
    """Test that EmulatorConfig can be created with defaults"""
    config = EmulatorConfig()
    assert config.host == "0.0.0.0"
    assert config.port == 5000
    assert config.lambda_endpoint == "http://localhost:3001"
    assert config.storage_dir is None


def test_emulator_config_validation():
    """Test that EmulatorConfig validates parameters"""
    # Test invalid port
    with pytest.raises(ValueError, match="Invalid port"):
        EmulatorConfig(port=0)

    # Test invalid Lambda endpoint
    with pytest.raises(ValueError, match="Invalid Lambda endpoint URL"):
        EmulatorConfig(lambda_endpoint="not-a-url")


def test_emulator_server_creation():
    """Test that EmulatorServer can be created"""
    config = EmulatorConfig()
    server = EmulatorServer(config)
    assert server.config == config
    assert server.executor is not None
    assert server.web_server is not None
