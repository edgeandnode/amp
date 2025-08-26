# tests/integration/test_snowflake_loader.py
"""
Integration tests for Snowflake loader implementation.
These tests require a running Snowflake instance with proper credentials.
"""

import time
from datetime import datetime

import pyarrow as pa
import pytest

try:
    from src.nozzle.loaders.base import LoadMode
    from src.nozzle.loaders.implementations.snowflake_loader import SnowflakeLoader
except ImportError:
    pytest.skip('nozzle modules not available', allow_module_level=True)


@pytest.fixture
def test_table_name():
    """Generate unique table name for each test"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    return f'test_table_{timestamp}'


@pytest.fixture
def cleanup_tables(snowflake_config):
    """Cleanup test tables after tests"""
    tables_to_clean = []

    yield tables_to_clean

    loader = SnowflakeLoader(snowflake_config)
    try:
        loader.connect()
        for table in tables_to_clean:
            try:
                loader.cursor.execute(f'DROP TABLE IF EXISTS {table}')
                loader.connection.commit()
            except Exception:
                pass
    except Exception:
        pass
    finally:
        if loader._is_connected:
            loader.disconnect()


@pytest.mark.integration
@pytest.mark.snowflake
class TestSnowflakeLoaderIntegration:
    """Integration tests for Snowflake loader"""

    def test_loader_connection(self, snowflake_config):
        """Test basic connection to Snowflake"""
        loader = SnowflakeLoader(snowflake_config)

        loader.connect()
        assert loader._is_connected is True
        assert loader.connection is not None
        assert loader.cursor is not None

        loader.disconnect()
        assert loader._is_connected is False
        assert loader.connection is None
        assert loader.cursor is None

    def test_basic_table_loading_via_stage(self, snowflake_config, small_test_table, test_table_name, cleanup_tables):
        """Test basic table loading using stage"""
        cleanup_tables.append(test_table_name)

        config = {**snowflake_config, 'use_stage': True}
        loader = SnowflakeLoader(config)

        with loader:
            result = loader.load_table(small_test_table, test_table_name, create_table=True)

            assert result.success is True
            assert result.rows_loaded == small_test_table.num_rows
            assert result.table_name == test_table_name
            assert result.loader_type == 'snowflake'
            assert result.metadata['loading_method'] == 'stage'

            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == small_test_table.num_rows

    def test_basic_table_loading_via_insert(self, snowflake_config, small_test_table, test_table_name, cleanup_tables):
        """Test basic table loading using INSERT"""
        cleanup_tables.append(test_table_name)

        # Use insert loading
        config = {**snowflake_config, 'use_stage': False}
        loader = SnowflakeLoader(config)

        with loader:
            result = loader.load_table(small_test_table, test_table_name, create_table=True)

            assert result.success is True
            assert result.rows_loaded == small_test_table.num_rows
            assert result.metadata['loading_method'] == 'insert'

            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == small_test_table.num_rows

    def test_batch_loading(self, snowflake_config, medium_test_table, test_table_name, cleanup_tables):
        """Test loading data in batches"""
        cleanup_tables.append(test_table_name)
        
        config = {**snowflake_config, 'batch_size': 1000}
        loader = SnowflakeLoader(config)

        with loader:
            result = loader.load_table(medium_test_table, test_table_name, create_table=True)

            assert result.success is True
            assert result.rows_loaded == medium_test_table.num_rows
            assert result.metadata['batches_processed'] > 1

            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == medium_test_table.num_rows

    def test_overwrite_mode(self, snowflake_config, small_test_table, test_table_name, cleanup_tables):
        """Test OVERWRITE mode is not supported"""
        cleanup_tables.append(test_table_name)
        
        loader = SnowflakeLoader(snowflake_config)

        with loader:
            result1 = loader.load_table(small_test_table, test_table_name, create_table=True)
            assert result1.success is True

            # OVERWRITE mode should fail with error message
            result2 = loader.load_table(small_test_table, test_table_name, mode=LoadMode.OVERWRITE)
            assert result2.success is False
            assert "does not support OVERWRITE mode" in result2.error

    def test_append_mode(self, snowflake_config, small_test_table, test_table_name, cleanup_tables):
        """Test APPEND mode adds to existing data"""
        cleanup_tables.append(test_table_name)
        
        loader = SnowflakeLoader(snowflake_config)

        with loader:
            result1 = loader.load_table(small_test_table, test_table_name, create_table=True)
            assert result1.success is True

            result2 = loader.load_table(small_test_table, test_table_name, mode=LoadMode.APPEND)
            assert result2.success is True

            # Should have double the rows
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == small_test_table.num_rows * 2

    def test_comprehensive_data_types(self, snowflake_config, comprehensive_test_data, test_table_name, cleanup_tables):
        """Test various data types from comprehensive test data"""
        cleanup_tables.append(test_table_name)
        
        loader = SnowflakeLoader(snowflake_config)

        with loader:
            result = loader.load_table(comprehensive_test_data, test_table_name, create_table=True)
            assert result.success is True

            loader.cursor.execute(f"""
                SELECT 
                    "id",
                    "user_id",
                    "transaction_amount",
                    "category",
                    "timestamp",
                    "is_weekend",
                    "score",
                    "active"
                FROM {test_table_name}
                WHERE "id" = 0
            """)
            
            row = loader.cursor.fetchone()
            assert row['id'] == 0
            assert row['user_id'] == 'user_0'
            assert abs(row['transaction_amount'] - 0.0) < 0.001
            assert row['category'] == 'electronics'
            assert row['is_weekend'] is True  # id=0 is weekend
            assert abs(row['score'] - 0.0) < 0.001
            assert row['active'] is True

    def test_null_handling(self, snowflake_config, null_test_data, test_table_name, cleanup_tables):
        """Test proper handling of NULL values"""
        cleanup_tables.append(test_table_name)
        
        loader = SnowflakeLoader(snowflake_config)

        with loader:
            result = loader.load_table(null_test_data, test_table_name, create_table=True)
            assert result.success is True

            loader.cursor.execute(f"""
                SELECT COUNT(*) as null_count
                FROM {test_table_name}
                WHERE "text_field" IS NULL
            """)
            
            null_count = loader.cursor.fetchone()['NULL_COUNT']
            expected_nulls = sum(1 for val in null_test_data.column('text_field').to_pylist() if val is None)
            assert null_count == expected_nulls

            loader.cursor.execute(f"""
                SELECT 
                    COUNT(CASE WHEN "int_field" IS NULL THEN 1 END) as int_nulls,
                    COUNT(CASE WHEN "float_field" IS NULL THEN 1 END) as float_nulls,
                    COUNT(CASE WHEN "bool_field" IS NULL THEN 1 END) as bool_nulls
                FROM {test_table_name}
            """)
            
            null_counts = loader.cursor.fetchone()
            assert null_counts['INT_NULLS'] > 0
            assert null_counts['FLOAT_NULLS'] > 0
            assert null_counts['BOOL_NULLS'] > 0

    def test_table_info(self, snowflake_config, small_test_table, test_table_name, cleanup_tables):
        """Test getting table information"""
        cleanup_tables.append(test_table_name)
        
        loader = SnowflakeLoader(snowflake_config)

        with loader:
            result = loader.load_table(small_test_table, test_table_name, create_table=True)
            assert result.success is True

            info = loader.get_table_info(test_table_name)
            
            assert info is not None
            assert info['table_name'] == test_table_name.upper()
            assert info['schema'] == snowflake_config.get('schema', 'PUBLIC')
            assert len(info['columns']) == len(small_test_table.schema)

            # In Snowflake, quoted column names are case-sensitive but INFORMATION_SCHEMA may return them differently
            # Let's find the ID column by looking for either case variant
            id_col = None
            for col in info['columns']:
                if col['name'].upper() == 'ID' or col['name'] == 'id':
                    id_col = col
                    break
            
            assert id_col is not None, f"Could not find ID column in {[col['name'] for col in info['columns']]}"
            assert 'INT' in id_col['type'] or 'NUMBER' in id_col['type']

    @pytest.mark.slow
    def test_performance_batch_loading(self, snowflake_config, performance_test_data, test_table_name, cleanup_tables):
        """Test performance with larger dataset"""
        cleanup_tables.append(test_table_name)
        
        config = {**snowflake_config, 'batch_size': 5000, 'use_stage': True}
        loader = SnowflakeLoader(config)

        with loader:
            start_time = time.time()

            result = loader.load_table(performance_test_data, test_table_name, create_table=True)
            
            duration = time.time() - start_time
            
            assert result.success is True
            assert result.rows_loaded == performance_test_data.num_rows

            rows_per_second = result.rows_loaded / duration
            mb_per_second = (performance_test_data.nbytes / 1024 / 1024) / duration
            
            print("\nPerformance metrics:")
            print(f"  Total rows: {result.rows_loaded:,}")
            print(f"  Duration: {duration:.2f}s")
            print(f"  Throughput: {rows_per_second:,.0f} rows/sec")
            print(f"  Data rate: {mb_per_second:.2f} MB/sec")
            print(f"  Batches: {result.metadata.get('batches_processed', 'N/A')}")

    def test_error_handling_invalid_table(self, snowflake_config, small_test_table):
        """Test error handling for invalid table operations"""
        loader = SnowflakeLoader(snowflake_config)

        with loader:
            # Try to load without creating table
            result = loader.load_table(
                small_test_table, 
                'non_existent_table_xyz', 
                create_table=False
            )
            
            assert result.success is False
            assert result.error is not None

    @pytest.mark.slow
    def test_concurrent_batch_loading(self, snowflake_config, medium_test_table, test_table_name, cleanup_tables):
        """Test loading multiple batches concurrently"""
        cleanup_tables.append(test_table_name)
        
        config = {**snowflake_config, 'batch_size': 500}
        loader = SnowflakeLoader(config)

        with loader:
            # Create table first
            batch = medium_test_table.to_batches(max_chunksize=1)[0]
            loader.load_batch(batch, test_table_name, create_table=True)
            
            # Load multiple batches
            total_rows = 0
            for i, batch in enumerate(medium_test_table.to_batches(max_chunksize=500)):
                result = loader.load_batch(batch, test_table_name)
                assert result.success is True
                total_rows += result.rows_loaded
            
            assert total_rows == medium_test_table.num_rows
            
            # Verify all data loaded
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == medium_test_table.num_rows + 1  # +1 for initial batch

    def test_stage_and_compression_options(self, snowflake_config, medium_test_table, test_table_name, cleanup_tables):
        """Test different stage and compression options"""
        cleanup_tables.append(test_table_name)
        
        # Test with different compression
        config = {
            **snowflake_config, 
            'use_stage': True,
            'compression': 'zstd',
            'batch_size': 2000
        }
        loader = SnowflakeLoader(config)

        with loader:
            result = loader.load_table(medium_test_table, test_table_name, create_table=True)
            assert result.success is True
            assert result.rows_loaded == medium_test_table.num_rows

    def test_schema_with_special_characters(self, snowflake_config, test_table_name, cleanup_tables):
        """Test handling of column names with special characters"""
        cleanup_tables.append(test_table_name)

        data = {
            'user-id': [1, 2, 3],
            'first name': ['Alice', 'Bob', 'Charlie'],
            'total$amount': [100.0, 200.0, 300.0],
            '2024_data': ['a', 'b', 'c']
        }
        special_table = pa.Table.from_pydict(data)
        
        loader = SnowflakeLoader(snowflake_config)
        
        with loader:
            result = loader.load_table(special_table, test_table_name, create_table=True)
            assert result.success is True

            # Verify row count
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == 3

            # Verify we can query columns with special characters
            # Note: Snowflake typically converts column names to uppercase and may need quoting
            loader.cursor.execute(f'''
                SELECT 
                    "user-id",
                    "first name", 
                    "total$amount",
                    "2024_data"
                FROM {test_table_name}
                WHERE "user-id" = 1
            ''')
            
            row = loader.cursor.fetchone()
            
            assert row['user-id'] == 1
            assert row['first name'] == 'Alice'
            assert abs(row['total$amount'] - 100.0) < 0.001
            assert row['2024_data'] == 'a'