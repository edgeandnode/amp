# tests/conftest.py
"""
Shared pytest configuration and fixtures for the data loader test suite.
"""

import tempfile
import shutil
from pathlib import Path
import logging
import os

import pyarrow as pa
import pandas as pd
from datetime import datetime, date, timedelta
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
        'delta_lake': {
            'base_path': os.getenv('TEST_DELTA_BASE_PATH', None),  # Will use temp if None
        },
    }


@pytest.fixture(scope='session')
def delta_test_env():
    """Create Delta Lake test environment for the session"""
    # Create temporary directory for Delta Lake tests
    temp_dir = tempfile.mkdtemp(prefix='delta_test_')

    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def delta_basic_config(delta_test_env):
    """Basic Delta Lake configuration for testing"""
    return {'table_path': str(Path(delta_test_env) / 'basic_table'), 'partition_by': ['year', 'month'], 'optimize_after_write': True, 'vacuum_after_write': False, 'schema_evolution': True, 'merge_schema': True, 'storage_options': {}}


@pytest.fixture
def delta_partitioned_config(delta_test_env):
    """Partitioned Delta Lake configuration for testing"""
    return {'table_path': str(Path(delta_test_env) / 'partitioned_table'), 'partition_by': ['year', 'month', 'day'], 'optimize_after_write': True, 'vacuum_after_write': True, 'schema_evolution': True, 'merge_schema': True, 'storage_options': {}}


@pytest.fixture
def delta_temp_config(delta_test_env):
    """Temporary Delta Lake configuration with unique path"""
    unique_path = str(Path(delta_test_env) / f'temp_table_{datetime.now().strftime("%Y%m%d_%H%M%S_%f")}')
    return {'table_path': unique_path, 'partition_by': ['year', 'month'], 'optimize_after_write': False, 'vacuum_after_write': False, 'schema_evolution': True, 'merge_schema': True, 'storage_options': {}}


@pytest.fixture
def small_test_table():
    """Small Arrow table for quick tests"""
    data = {'id': range(100), 'name': [f'user_{i}' for i in range(100)], 'value': [i * 0.1 for i in range(100)], 'active': [i % 2 == 0 for i in range(100)], 'created_at': [datetime.now() - timedelta(hours=i) for i in range(100)]}
    return pa.Table.from_pydict(data)


@pytest.fixture
def medium_test_table():
    """Medium Arrow table for integration tests"""
    data = {'id': range(10000), 'name': [f'user_{i}' for i in range(10000)], 'category': ['A', 'B', 'C'] * 3334, 'score': [i * 0.01 for i in range(10000)], 'active': [i % 2 == 0 for i in range(10000)], 'created_date': [date.today() - timedelta(days=i % 365) for i in range(10000)], 'updated_at': [datetime.now() - timedelta(hours=i) for i in range(10000)]}
    return pa.Table.from_pydict(data)


@pytest.fixture
def comprehensive_test_data():
    """Comprehensive test data for Delta Lake testing"""
    base_date = datetime(2024, 1, 1)

    data = {'id': list(range(1000)), 'user_id': [f'user_{i % 100}' for i in range(1000)], 'transaction_amount': [round((i * 12.34) % 1000, 2) for i in range(1000)], 'category': [['electronics', 'clothing', 'books', 'food', 'travel'][i % 5] for i in range(1000)], 'timestamp': [(base_date + timedelta(days=i // 50, hours=i % 24)).isoformat() for i in range(1000)], 'year': [2024 if i < 800 else 2023 for i in range(1000)], 'month': [(i // 80) % 12 + 1 for i in range(1000)], 'day': [(i // 30) % 28 + 1 for i in range(1000)], 'is_weekend': [i % 7 in [0, 6] for i in range(1000)], 'metadata': [f'{{"session_id": "session_{i}", "device": "{["mobile", "desktop", "tablet"][i % 3]}"}}' for i in range(1000)], 'score': [i * 0.123 for i in range(1000)], 'active': [i % 2 == 0 for i in range(1000)]}

    df = pd.DataFrame(data)
    return pa.Table.from_pandas(df)


@pytest.fixture
def small_test_data():
    """Small test data for quick tests"""
    data = {'id': [1, 2, 3, 4, 5], 'name': ['a', 'b', 'c', 'd', 'e'], 'value': [10.1, 20.2, 30.3, 40.4, 50.5], 'year': [2024, 2024, 2024, 2024, 2024], 'month': [1, 1, 1, 1, 1], 'day': [1, 2, 3, 4, 5], 'active': [True, False, True, False, True]}

    df = pd.DataFrame(data)
    return pa.Table.from_pandas(df)


def pytest_configure(config):
    """Configure custom pytest markers"""
    config.addinivalue_line('markers', 'unit: Unit tests (fast, no external dependencies)')
    config.addinivalue_line('markers', 'integration: Integration tests (require databases)')
    config.addinivalue_line('markers', 'performance: Performance and benchmark tests')
    config.addinivalue_line('markers', 'postgresql: Tests requiring PostgreSQL')
    config.addinivalue_line('markers', 'redis: Tests requiring Redis')
    config.addinivalue_line('markers', 'delta_lake: Tests requiring Delta Lake')
    config.addinivalue_line('markers', 'slow: Slow tests (> 30 seconds)')


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add skip conditions"""
    # Skip integration tests if not explicitly enabled
    skip_integration = pytest.mark.skip(reason='Integration tests disabled (set TEST_INTEGRATION=1 to enable)')
    skip_performance = pytest.mark.skip(reason='Performance tests disabled (set TEST_PERFORMANCE=1 to enable)')
    skip_delta_lake = pytest.mark.skip(reason='Delta Lake tests disabled or package not available')

    # Check if Delta Lake is available
    try:
        import deltalake

        delta_available = True
    except ImportError:
        delta_available = False

    for item in items:
        if 'integration' in item.keywords and not os.getenv('TEST_INTEGRATION'):
            item.add_marker(skip_integration)
        if 'performance' in item.keywords and not os.getenv('TEST_PERFORMANCE'):
            item.add_marker(skip_performance)
        if 'delta_lake' in item.keywords and not delta_available:
            item.add_marker(skip_delta_lake)


# Utility fixtures for mocking
@pytest.fixture
def mock_flight_client():
    """Mock Flight SQL client for unit tests"""
    from unittest.mock import Mock

    mock_client = Mock()
    mock_client.conn = Mock()
    return mock_client


@pytest.fixture
def mock_connection_manager():
    """Mock connection manager for testing"""
    from unittest.mock import Mock

    mock_manager = Mock()
    mock_manager.connections = {}

    def mock_get_connection_info(name):
        if name == 'test_connection':
            return {'loader': 'postgresql', 'config': {'host': 'localhost', 'database': 'test_db', 'user': 'test_user', 'password': 'test_pass'}}
        raise ValueError(f"Connection '{name}' not found")

    mock_manager.get_connection_info = mock_get_connection_info
    return mock_manager


# Environment setup helpers
@pytest.fixture(autouse=True)
def setup_test_environment():
    """Setup test environment before each test"""
    # Set environment variables for testing
    os.environ['TESTING'] = '1'

    yield

    # Cleanup after test
    if 'TESTING' in os.environ:
        del os.environ['TESTING']
