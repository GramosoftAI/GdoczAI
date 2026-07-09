"""
Test Suite: Configuration Loading
Covers: config/config.yaml and ConfigManager
"""
import pytest
from pathlib import Path

def test_config_loads():
    from src.api.models.api_server_models import ConfigManager
    config = ConfigManager('config/config.yaml')
    assert config.get('postgres.host')
    assert config.get('postgres.database')
    assert config.get('processing.supported_extensions')
    assert isinstance(config.get('processing.max_concurrent_files'), int) or config.get('processing.max_concurrent_files') is None
