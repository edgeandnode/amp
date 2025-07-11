# tests/conftest.py
"""
Shared pytest configuration and fixtures for the data loader test suite.
Updated to work with UV and pyproject.toml setup.
"""

import logging
import os
from datetime import date, datetime
from unittest.mock import Mock

import pyarrow as pa
import pytest

logging.basicConfig(level=logging.INFO)


@pytest.fixture(scope='session')
def test_config():
    """Test configuration that can be overridden by environment variables"""
    return {
        'postgresql': {
            'host': os.getenv('TEST_POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('TEST_POSTGRES_PORT', 5432)),
            'database': os.getenv('TEST_POSTGRES_DB', 'test_db'),
            'user': os.getenv('TEST_POSTGRES_USER', 'test_user'),
            'password': os.getenv('TEST_POSTGRES_PASSWORD', 'test_pass'),
        },
        'redis': {
            'host': os.getenv('TEST_REDIS_HOST', 'localhost'),
            'port': int(os.getenv('TEST_REDIS_PORT', 6379)),
            'db': int(os.getenv('TEST_REDIS_DB', 0)),
        },
    }


@pytest.fixture
def mock_flight_client():
    """Mock Flight SQL client for unit tests"""
    mock_client = Mock()
    mock_client.conn = Mock()
    return mock_client


@pytest.fixture
def small_test_table():
    """Small Arrow table for quick tests"""
    data = {'id': range(100), 'name': [f'user_{i}' for i in range(100)], 'value': [i * 0.1 for i in range(100)], 'active': [i % 2 == 0 for i in range(100)], 'created_at': [datetime.now() for _ in range(100)]}
    return pa.Table.from_pydict(data)


@pytest.fixture
def medium_test_table():
    """Medium Arrow table for integration tests"""
    data = {'id': range(10000), 'name': [f'user_{i}' for i in range(10000)], 'category': ['A', 'B', 'C'] * 3334, 'score': [i * 0.01 for i in range(10000)], 'active': [i % 2 == 0 for i in range(10000)], 'created_date': [date.today() for _ in range(10000)], 'updated_at': [datetime.now() for _ in range(10000)]}
    return pa.Table.from_pydict(data)


def pytest_configure(config):
    """Configure custom pytest markers"""
    config.addinivalue_line('markers', 'unit: Unit tests (fast, no external dependencies)')
    config.addinivalue_line('markers', 'integration: Integration tests (require databases)')
    config.addinivalue_line('markers', 'performance: Performance and benchmark tests')
    config.addinivalue_line('markers', 'postgresql: Tests requiring PostgreSQL')
    config.addinivalue_line('markers', 'redis: Tests requiring Redis')
    config.addinivalue_line('markers', 'slow: Slow tests (> 30 seconds)')


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add skip conditions"""
    skip_integration = pytest.mark.skip(reason='Integration tests disabled (set TEST_INTEGRATION=1 to enable)')
    skip_performance = pytest.mark.skip(reason='Performance tests disabled (set TEST_PERFORMANCE=1 to enable)')

    for item in items:
        if 'integration' in item.keywords and not os.getenv('TEST_INTEGRATION'):
            item.add_marker(skip_integration)
        if 'performance' in item.keywords and not os.getenv('TEST_PERFORMANCE'):
            item.add_marker(skip_performance)
