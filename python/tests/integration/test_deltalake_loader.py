# tests/integration/test_deltalake_loader.py
"""
Integration tests for Delta Lake loader implementation.
These tests require actual Delta Lake functionality and local filesystem access.
"""

import pytest
import json
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import pyarrow as pa
import pandas as pd

try:
    from src.nozzle.loaders.implementations.deltalake_loader import DeltaLakeLoader, DELTALAKE_AVAILABLE
    from src.nozzle.loaders.base import LoadResult, LoadMode

    # Skip all tests if deltalake is not available
    if not DELTALAKE_AVAILABLE:
        pytest.skip('Delta Lake not available', allow_module_level=True)

except ImportError:
    pytest.skip('nozzle modules not available', allow_module_level=True)


@pytest.fixture(scope='session')
def delta_test_env():
    """Setup Delta Lake test environment for the session"""
    temp_dir = tempfile.mkdtemp(prefix='delta_test_')
    yield temp_dir
    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def delta_basic_config(delta_test_env):
    """Get basic Delta Lake configuration"""
    return {'table_path': str(Path(delta_test_env) / 'basic_table'), 'partition_by': ['year', 'month'], 'optimize_after_write': True, 'vacuum_after_write': False, 'schema_evolution': True, 'merge_schema': True, 'storage_options': {}}


@pytest.fixture
def delta_partitioned_config(delta_test_env):
    """Get partitioned Delta Lake configuration"""
    return {'table_path': str(Path(delta_test_env) / 'partitioned_table'), 'partition_by': ['year', 'month', 'day'], 'optimize_after_write': True, 'vacuum_after_write': True, 'schema_evolution': True, 'merge_schema': True, 'storage_options': {}}


@pytest.fixture
def delta_temp_config(delta_test_env):
    """Get temporary Delta Lake configuration with unique path"""
    temp_path = str(Path(delta_test_env) / f'temp_table_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
    return {'table_path': temp_path, 'partition_by': ['year', 'month'], 'optimize_after_write': False, 'vacuum_after_write': False, 'schema_evolution': True, 'merge_schema': True, 'storage_options': {}}


@pytest.fixture
def comprehensive_test_data():
    """Create comprehensive test data for Delta Lake testing"""
    base_date = datetime(2024, 1, 1)

    data = {'id': list(range(1000)), 'user_id': [f'user_{i % 100}' for i in range(1000)], 'transaction_amount': [round((i * 12.34) % 1000, 2) for i in range(1000)], 'category': [['electronics', 'clothing', 'books', 'food', 'travel'][i % 5] for i in range(1000)], 'timestamp': [(base_date + timedelta(days=i // 50, hours=i % 24)).isoformat() for i in range(1000)], 'year': [2024 if i < 800 else 2023 for i in range(1000)], 'month': [(i // 80) % 12 + 1 for i in range(1000)], 'day': [(i // 30) % 28 + 1 for i in range(1000)], 'is_weekend': [i % 7 in [0, 6] for i in range(1000)], 'metadata': [json.dumps({'session_id': f'session_{i}', 'device': ['mobile', 'desktop', 'tablet'][i % 3], 'location': ['US', 'UK', 'DE', 'FR', 'JP'][i % 5]}) for i in range(1000)], 'score': [i * 0.123 for i in range(1000)], 'active': [i % 2 == 0 for i in range(1000)]}

    df = pd.DataFrame(data)
    return pa.Table.from_pandas(df)


@pytest.fixture
def small_test_data():
    """Create small test data for quick tests"""
    data = {'id': [1, 2, 3, 4, 5], 'name': ['a', 'b', 'c', 'd', 'e'], 'value': [10.1, 20.2, 30.3, 40.4, 50.5], 'year': [2024, 2024, 2024, 2024, 2024], 'month': [1, 1, 1, 1, 1], 'day': [1, 2, 3, 4, 5], 'active': [True, False, True, False, True]}

    df = pd.DataFrame(data)
    return pa.Table.from_pandas(df)


@pytest.mark.integration
class TestDeltaLakeLoaderIntegration:
    """Integration tests for Delta Lake loader"""

    def test_loader_initialization(self, delta_basic_config):
        """Test loader initialization and connection"""
        loader = DeltaLakeLoader(delta_basic_config)

        # Test configuration
        assert loader.storage_config.table_path == delta_basic_config['table_path']
        assert loader.storage_config.partition_by == ['year', 'month']
        assert loader.storage_config.optimize_after_write == True
        assert loader.storage_backend == 'Local'

        # Test connection
        loader.connect()
        assert loader._is_connected == True

        # Test disconnection
        loader.disconnect()
        assert loader._is_connected == False

    def test_basic_table_operations(self, delta_basic_config, comprehensive_test_data):
        """Test basic table creation and data loading"""
        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            # Test initial table creation
            result = loader.load_table(comprehensive_test_data, 'test_transactions', mode=LoadMode.OVERWRITE)

            assert result.success == True
            assert result.rows_loaded == 1000
            assert result.metadata['write_mode'] == 'overwrite'
            assert result.metadata['storage_backend'] == 'Local'
            assert result.metadata['partition_columns'] == ['year', 'month']

            # Verify table exists
            assert loader._table_exists == True
            assert loader._delta_table is not None

            # Test table statistics
            stats = loader.get_table_stats()
            assert 'version' in stats
            assert stats['storage_backend'] == 'Local'
            assert stats['partition_columns'] == ['year', 'month']

    def test_append_mode(self, delta_basic_config, comprehensive_test_data):
        """Test append mode functionality"""
        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            # Initial load
            result = loader.load_table(comprehensive_test_data, 'test_append', mode=LoadMode.OVERWRITE)
            assert result.success == True
            assert result.rows_loaded == 1000

            # Append additional data
            additional_data = comprehensive_test_data.slice(0, 100)  # First 100 rows
            result = loader.load_table(additional_data, 'test_append', mode=LoadMode.APPEND)

            assert result.success == True
            assert result.rows_loaded == 100
            assert result.metadata['write_mode'] == 'append'

            # Verify total data
            final_query = loader.query_table()
            assert final_query.num_rows == 1100  # 1000 + 100

    def test_batch_loading(self, delta_basic_config, comprehensive_test_data):
        """Test batch loading functionality"""
        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            # Test loading individual batches
            batches = comprehensive_test_data.to_batches(max_chunksize=200)

            for i, batch in enumerate(batches):
                mode = LoadMode.OVERWRITE if i == 0 else LoadMode.APPEND
                result = loader.load_batch(batch, 'test_batches', mode=mode)

                assert result.success == True
                assert result.rows_loaded == batch.num_rows
                assert result.metadata['operation'] == 'load_batch'
                assert result.metadata['batch_size'] == batch.num_rows

            # Verify all data was loaded
            final_query = loader.query_table()
            assert final_query.num_rows == 1000

    def test_partitioning(self, delta_partitioned_config, small_test_data):
        """Test table partitioning functionality"""
        loader = DeltaLakeLoader(delta_partitioned_config)

        with loader:
            # Load partitioned data
            result = loader.load_table(small_test_data, 'test_partitioned', mode=LoadMode.OVERWRITE)

            assert result.success == True
            assert result.metadata['partition_columns'] == ['year', 'month', 'day']

            # Verify partition structure exists
            table_path = Path(delta_partitioned_config['table_path'])
            assert table_path.exists()

    def test_schema_evolution(self, delta_basic_config, small_test_data):
        """Test schema evolution functionality"""
        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            # Load initial data
            result = loader.load_table(small_test_data, 'test_schema_evolution', mode=LoadMode.OVERWRITE)

            assert result.success == True
            initial_schema = loader.get_table_schema()
            initial_columns = set(initial_schema.names)

            # Create data with additional columns
            extended_data = small_test_data.to_pandas()
            extended_data['new_column'] = range(len(extended_data))
            extended_data['another_field'] = 'test_value'
            extended_table = pa.Table.from_pandas(extended_data)

            # Load extended data (should add new columns)
            result = loader.load_table(extended_table, 'test_schema_evolution', mode=LoadMode.APPEND)

            assert result.success == True

            # Verify schema has evolved
            evolved_schema = loader.get_table_schema()
            evolved_columns = set(evolved_schema.names)

            assert 'new_column' in evolved_columns
            assert 'another_field' in evolved_columns
            assert evolved_columns.issuperset(initial_columns)

    def test_optimization_operations(self, delta_basic_config, comprehensive_test_data):
        """Test table optimization operations"""
        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            # Load data multiple times to create multiple files
            for i in range(3):
                subset = comprehensive_test_data.slice(i * 300, 300)
                mode = LoadMode.OVERWRITE if i == 0 else LoadMode.APPEND

                result = loader.load_table(subset, 'test_optimization', mode=mode)
                assert result.success == True

            # Get initial stats
            initial_stats = loader.get_table_stats()
            initial_files = initial_stats.get('num_files', 0)

            # Manual optimization
            optimize_result = loader.optimize_table()

            assert optimize_result['success'] == True
            assert 'duration_seconds' in optimize_result
            assert 'metrics' in optimize_result

            # Verify data integrity after optimization
            final_data = loader.query_table()
            assert final_data.num_rows == 900  # 3 * 300

    def test_query_operations(self, delta_basic_config, comprehensive_test_data):
        """Test table querying operations"""
        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            # Load data
            result = loader.load_table(comprehensive_test_data, 'test_query', mode=LoadMode.OVERWRITE)
            assert result.success == True

            # Test basic query
            query_result = loader.query_table()
            assert query_result.num_rows == 1000

            # Test column selection
            query_result = loader.query_table(columns=['id', 'user_id', 'transaction_amount'])
            assert query_result.num_rows == 1000
            assert query_result.column_names == ['id', 'user_id', 'transaction_amount']

            # Test limit
            query_result = loader.query_table(limit=50)
            assert query_result.num_rows == 50

            # Test combined options
            query_result = loader.query_table(columns=['id', 'category'], limit=10)
            assert query_result.num_rows == 10
            assert query_result.column_names == ['id', 'category']

    def test_error_handling(self, delta_temp_config):
        """Test error handling scenarios"""
        loader = DeltaLakeLoader(delta_temp_config)

        with loader:
            # Test loading invalid data (missing partition columns)
            invalid_data = pa.table(
                {
                    'id': [1, 2, 3],
                    'name': ['a', 'b', 'c'],
                    # Missing 'year' and 'month' partition columns
                }
            )

            result = loader.load_table(invalid_data, 'test_errors', mode=LoadMode.OVERWRITE)

            # Should handle error gracefully
            assert result.success == False
            assert result.error is not None
            assert result.rows_loaded == 0

    def test_table_history(self, delta_basic_config, small_test_data):
        """Test table history functionality"""
        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            # Create multiple versions
            for i in range(3):
                subset = small_test_data.slice(i, 1)
                mode = LoadMode.OVERWRITE if i == 0 else LoadMode.APPEND

                result = loader.load_table(subset, 'test_history', mode=mode)
                assert result.success == True

            # Get history
            history = loader.get_table_history()
            assert len(history) >= 3

            # Verify history structure
            for entry in history:
                assert 'version' in entry
                assert 'operation' in entry
                assert 'timestamp' in entry

    def test_context_manager(self, delta_basic_config, small_test_data):
        """Test context manager functionality"""
        loader = DeltaLakeLoader(delta_basic_config)

        # Test context manager
        with loader:
            assert loader._is_connected == True

            result = loader.load_table(small_test_data, 'test_context', mode=LoadMode.OVERWRITE)
            assert result.success == True

        # Should be disconnected after context
        assert loader._is_connected == False

    def test_metadata_completeness(self, delta_basic_config, comprehensive_test_data):
        """Test metadata completeness in results"""
        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            result = loader.load_table(comprehensive_test_data, 'test_metadata', mode=LoadMode.OVERWRITE)

            assert result.success == True

            # Check required metadata fields
            metadata = result.metadata
            required_fields = ['write_mode', 'storage_backend', 'partition_columns', 'schema_fields', 'throughput_rows_per_sec', 'table_version']

            for field in required_fields:
                assert field in metadata, f'Missing metadata field: {field}'

            # Verify metadata values
            assert metadata['write_mode'] == 'overwrite'
            assert metadata['storage_backend'] == 'Local'
            assert metadata['partition_columns'] == ['year', 'month']
            assert metadata['schema_fields'] == len(comprehensive_test_data.schema)
            assert metadata['throughput_rows_per_sec'] > 0

    def test_file_size_calculation_modern_api(self, delta_basic_config, comprehensive_test_data):
        """Test file size calculation using modern get_add_file_sizes API"""
        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            result = loader.load_table(comprehensive_test_data, 'test_file_sizes', mode=LoadMode.OVERWRITE)
            assert result.success == True
            assert result.rows_loaded == 1000

            table_info = loader._get_table_info()
            
            # Verify size calculation worked
            assert 'size_bytes' in table_info
            assert table_info['size_bytes'] > 0, "File size should be greater than 0"
            assert table_info['num_files'] > 0, "Should have at least one file"
            
            # Verify metadata includes size information
            assert 'total_size_bytes' in result.metadata
            assert result.metadata['total_size_bytes'] > 0


@pytest.mark.integration
@pytest.mark.slow
class TestDeltaLakeLoaderAdvanced:
    """Advanced integration tests for Delta Lake loader"""

    def test_large_data_performance(self, delta_basic_config):
        """Test performance with larger datasets"""
        # Create larger test dataset
        large_data = {'id': list(range(50000)), 'value': [i * 0.123 for i in range(50000)], 'category': [f'category_{i % 10}' for i in range(50000)], 'year': [2024] * 50000, 'month': [(i // 4000) % 12 + 1 for i in range(50000)], 'timestamp': [datetime.now().isoformat() for _ in range(50000)]}

        df = pd.DataFrame(large_data)
        large_table = pa.Table.from_pandas(df)

        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            # Load large dataset
            result = loader.load_table(large_table, 'test_performance', mode=LoadMode.OVERWRITE)

            assert result.success == True
            assert result.rows_loaded == 50000

            # Verify performance metrics
            assert result.metadata['throughput_rows_per_sec'] > 100  # Should be reasonably fast
            assert result.duration < 120  # Should complete within reasonable time

    def test_concurrent_operations_safety(self, delta_basic_config, small_test_data):
        """Test that operations are handled safely (basic concurrency test)"""
        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            # Load initial data
            result = loader.load_table(small_test_data, 'test_concurrent', mode=LoadMode.OVERWRITE)
            assert result.success == True

            # Perform multiple operations in sequence (simulating concurrent-like scenario)
            operations = []

            # Append operations
            for i in range(3):
                subset = small_test_data.slice(i, 1)
                result = loader.load_table(subset, 'test_concurrent', mode=LoadMode.APPEND)
                operations.append(result)

            # Verify all operations succeeded
            for result in operations:
                assert result.success == True

            # Verify final data integrity
            final_data = loader.query_table()
            assert final_data.num_rows == 8  # 5 + 3 * 1
