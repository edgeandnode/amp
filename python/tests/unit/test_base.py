# tests/unit/test_base.py
"""
Unit tests for base classes and utilities.
Updated for nozzle project structure.
"""

import pytest

try:
    from src.nozzle.loaders.base import LoadResult, LoadConfig, LoadMode
except ImportError:
    # Skip tests if modules not available
    pytest.skip('nozzle modules not available', allow_module_level=True)

from tests.fixtures.mock_clients import MockDataLoader
from tests.fixtures.test_data import create_test_arrow_table


@pytest.mark.unit
class TestLoadResult:
    """Test LoadResult dataclass"""

    def test_success_result_string_representation(self):
        """Test string representation of successful result"""
        result = LoadResult(rows_loaded=1000, duration=2.5, table_name='test_table', loader_type='postgresql', success=True)

        result_str = str(result)
        assert '✅' in result_str
        assert '1000 rows' in result_str
        assert '2.50s' in result_str
        assert 'test_table' in result_str

    def test_failure_result_string_representation(self):
        """Test string representation of failed result"""
        result = LoadResult(rows_loaded=0, duration=1.0, table_name='test_table', loader_type='postgresql', success=False, error='Connection failed')

        result_str = str(result)
        assert '❌' in result_str
        assert 'Connection failed' in result_str
        assert 'test_table' in result_str


@pytest.mark.unit
class TestLoadConfig:
    """Test LoadConfig dataclass"""

    def test_default_values(self):
        """Test default configuration values"""
        config = LoadConfig()

        assert config.batch_size == 10000
        assert config.mode == LoadMode.APPEND
        assert config.create_table == True
        assert config.schema_evolution == False
        assert config.max_retries == 3
        assert config.retry_delay == 1.0

    def test_custom_values(self):
        """Test custom configuration values"""
        config = LoadConfig(batch_size=5000, mode=LoadMode.OVERWRITE, create_table=False, max_retries=5)

        assert config.batch_size == 5000
        assert config.mode == LoadMode.OVERWRITE
        assert config.create_table == False
        assert config.max_retries == 5


@pytest.mark.unit
class TestMockDataLoader:
    """Test MockDataLoader functionality"""

    def test_successful_batch_loading(self, small_test_table):
        """Test successful batch loading"""
        loader = MockDataLoader({'test': 'config'})
        batch = small_test_table.to_batches()[0]

        with loader:
            result = loader.load_batch(batch, 'test_table')

        assert result.success
        assert result.rows_loaded == batch.num_rows
        assert result.table_name == 'test_table'
        assert result.loader_type == 'mock'
        assert len(loader.load_calls) == 1

    def test_successful_table_loading(self, small_test_table):
        """Test successful table loading"""
        loader = MockDataLoader({'test': 'config'})

        with loader:
            result = loader.load_table(small_test_table, 'test_table')

        assert result.success
        assert result.rows_loaded == small_test_table.num_rows
        assert len(loader.load_calls) == 1

    def test_failure_simulation(self, small_test_table):
        """Test failure simulation"""
        loader = MockDataLoader({'test': 'config'})
        loader.should_fail = True
        loader.fail_message = 'Simulated failure'

        with loader:
            result = loader.load_table(small_test_table, 'test_table')

        assert not result.success
        assert result.error == 'Simulated failure'
        assert result.rows_loaded == 0
