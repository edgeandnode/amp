# tests/unit/test_postgresql_loader.py
"""
Unit tests for PostgreSQL loader implementation.
Fixed to properly mock the context managers and method calls.
"""

from unittest.mock import Mock, patch

import psycopg2
import pyarrow as pa
import pytest

try:
    from src.nozzle.loaders.base import LoadMode, LoadResult
    from src.nozzle.loaders.implementations.postgresql_loader import PostgreSQLLoader
except ImportError:
    pytest.skip('nozzle modules not available', allow_module_level=True)


@pytest.mark.unit
class TestPostgreSQLLoader:
    """Unit tests for PostgreSQL loader"""

    @pytest.fixture
    def mock_config(self):
        """Mock PostgreSQL configuration"""
        return {'host': 'localhost', 'port': 5432, 'database': 'test_db', 'user': 'test_user', 'password': 'test_pass', 'max_connections': 5}

    @pytest.fixture
    def mock_pool_setup(self):
        """Mock connection pool with proper context manager support"""
        pool = Mock()
        conn = Mock()
        cursor = Mock()

        # Setup proper context manager for cursor
        cursor_context = Mock()
        cursor_context.__enter__ = Mock(return_value=cursor)
        cursor_context.__exit__ = Mock(return_value=None)

        conn.cursor.return_value = cursor_context
        conn.commit = Mock()

        pool.getconn.return_value = conn
        pool.putconn = Mock()

        return pool, conn, cursor

    @pytest.fixture
    def test_batch(self, small_test_table):
        """Create a test RecordBatch"""
        return small_test_table.to_batches()[0]

    def test_init(self, mock_config):
        """Test loader initialization"""
        loader = PostgreSQLLoader(mock_config)

        assert loader.config == mock_config
        assert loader.pool is None
        assert loader._default_batch_size == 10000
        assert not loader._is_connected

    @patch('src.nozzle.loaders.implementations.postgresql_loader.ThreadedConnectionPool')
    def test_connect_failure(self, mock_pool_class, mock_config):
        """Test connection failure"""
        mock_pool_class.side_effect = psycopg2.OperationalError('Connection failed')

        loader = PostgreSQLLoader(mock_config)

        with pytest.raises(psycopg2.OperationalError):
            loader.connect()

        assert not loader._is_connected
        assert loader.pool is None

    def test_disconnect(self, mock_config):
        """Test disconnection"""
        loader = PostgreSQLLoader(mock_config)
        mock_pool = Mock()
        loader.pool = mock_pool
        loader._is_connected = True

        loader.disconnect()

        mock_pool.closeall.assert_called_once()
        assert loader.pool is None
        assert not loader._is_connected

    def test_create_table_from_schema(self, mock_config):
        """Test table creation from Arrow schema"""
        loader = PostgreSQLLoader(mock_config)

        # Create test schema
        schema = pa.schema([pa.field('id', pa.int64()), pa.field('name', pa.string()), pa.field('score', pa.float64()), pa.field('active', pa.bool_()), pa.field('created_at', pa.timestamp('us')), pa.field('nullable_field', pa.string(), nullable=True)])

        mock_cursor = Mock()

        mock_cursor.fetchone.side_effect = [None]

        loader._create_table_from_schema(mock_cursor, schema, 'test_table')

        call_args_list = mock_cursor.execute.call_args_list

        args_call_1, kwargs_1 = call_args_list[0]
        args_call_2, kwargs_2 = call_args_list[1]

        assert len(call_args_list) >= 2

        assert 'SELECT 1 FROM information_schema.tables' in args_call_1[0]

        assert 'CREATE TABLE IF NOT EXISTS test_table' in args_call_2[0]
        assert '"id" BIGINT' in args_call_2[0]
        assert '"name" TEXT' in args_call_2[0]
        assert '"score" DOUBLE PRECISION' in args_call_2[0]
        assert '"active" BOOLEAN' in args_call_2[0]
        assert '"created_at" TIMESTAMP' in args_call_2[0]
        assert '"nullable_field" TEXT' in args_call_2[0] and 'NOT NULL' not in args_call_2[0].split('"nullable_field" TEXT')[1].split(',')[0]

    @patch('src.nozzle.loaders.implementations._postgres_helpers.prepare_csv_data')
    def test_copy_arrow_batch(self, mock_prepare_csv, mock_config, test_batch):
        """Test COPY operation for Arrow batch"""
        loader = PostgreSQLLoader(mock_config)
        mock_cursor = Mock()

        # Mock CSV data preparation
        mock_buffer = Mock()
        column_names = ['id', 'name', 'value', 'active', 'created_at']
        mock_prepare_csv.return_value = (mock_buffer, column_names)

        loader._copy_arrow_batch(mock_cursor, test_batch, 'test_table', LoadMode.APPEND)

        # Verify copy_from was called
        mock_cursor.copy_from.assert_called_once()
        call_args = mock_cursor.copy_from.call_args  # Get keyword arguments

        # Check that copy_from was called with correct parameters
        assert 'test_table' in str(call_args[0])
        assert call_args[1]['sep'] == '\t'
        assert call_args[1]['null'] == '\\N'
        assert call_args[1]['columns'] == column_names

    @patch('src.nozzle.loaders.implementations._postgres_helpers.prepare_csv_data')
    def test_copy_arrow_batch_overwrite_mode(self, mock_prepare_csv, mock_config, test_batch):
        """Test COPY operation with overwrite mode"""
        loader = PostgreSQLLoader(mock_config)
        mock_cursor = Mock()

        # Mock CSV data preparation
        mock_buffer = Mock()
        column_names = ['id', 'name', 'value', 'active', 'created_at']
        mock_prepare_csv.return_value = (mock_buffer, column_names)

        loader._copy_arrow_batch(mock_cursor, test_batch, 'test_table', LoadMode.OVERWRITE)

        # Verify TRUNCATE was called before COPY
        truncate_call = mock_cursor.execute.call_args[0][0]
        assert 'TRUNCATE TABLE test_table' in truncate_call
        mock_cursor.copy_from.assert_called_once()

    @patch('src.nozzle.loaders.implementations._postgres_helpers.prepare_csv_data')
    def test_load_batch_success(self, mock_prepare_csv, mock_config, test_batch, mock_pool_setup):
        """Test successful batch loading"""
        loader = PostgreSQLLoader(mock_config)
        mock_pool, mock_conn, mock_cursor = mock_pool_setup
        loader.pool = mock_pool

        # Mock CSV data preparation
        mock_buffer = Mock()
        column_names = ['id', 'name', 'value', 'active', 'created_at']
        mock_prepare_csv.return_value = (mock_buffer, column_names)

        result = loader.load_batch(test_batch, 'test_table', create_table=True)

        assert result.success
        assert result.rows_loaded == test_batch.num_rows
        assert result.table_name == 'test_table'
        assert result.loader_type == 'postgresql'
        assert result.duration > 0

    def test_load_batch_failure(self, mock_config, test_batch):
        """Test batch loading failure"""
        loader = PostgreSQLLoader(mock_config)

        # Mock pool that raises exception
        mock_pool = Mock()
        mock_pool.getconn.side_effect = psycopg2.OperationalError('Connection failed')
        loader.pool = mock_pool

        result = loader.load_batch(test_batch, 'test_table')

        assert not result.success
        assert result.rows_loaded == 0
        assert result.error == 'Connection failed'

    def test_pg_type_to_arrow_mapping(self, mock_config):
        """Test PostgreSQL to Arrow type mapping"""
        loader = PostgreSQLLoader(mock_config)

        # Test common type mappings
        assert loader._pg_type_to_arrow('INTEGER') == pa.int32()
        assert loader._pg_type_to_arrow('BIGINT') == pa.int64()
        assert loader._pg_type_to_arrow('TEXT') == pa.string()
        assert loader._pg_type_to_arrow('BOOLEAN') == pa.bool_()
        assert loader._pg_type_to_arrow('DOUBLE PRECISION') == pa.float64()
        assert loader._pg_type_to_arrow('TIMESTAMP') == pa.timestamp('us')
        assert loader._pg_type_to_arrow('TIMESTAMPTZ') == pa.timestamp('us', tz='UTC')

        # Test unknown type defaults to string
        assert loader._pg_type_to_arrow('UNKNOWN_TYPE') == pa.string()

    def test_get_table_schema(self, mock_config, mock_pool_setup):
        """Test getting table schema from PostgreSQL"""
        loader = PostgreSQLLoader(mock_config)
        mock_pool, mock_conn, mock_cursor = mock_pool_setup
        loader.pool = mock_pool

        # Mock query result
        mock_cursor.fetchall.return_value = [
            ('id', 'bigint', 'NO'),
            ('name', 'text', 'NO'),
            ('score', 'double precision', 'YES'),
        ]

        schema = loader.get_table_schema('test_table')

        assert schema is not None
        assert len(schema) == 3
        assert schema.field('id').type == pa.int64()
        assert schema.field('name').type == pa.string()
        assert schema.field('score').type == pa.float64()
        assert not schema.field('id').nullable
        assert not schema.field('name').nullable
        assert schema.field('score').nullable

    def test_context_manager(self, mock_config):
        """Test context manager functionality"""
        loader = PostgreSQLLoader(mock_config)

        # Mock connect and disconnect
        loader.connect = Mock()
        loader.disconnect = Mock()

        with loader:
            pass

        loader.connect.assert_called_once()
        loader.disconnect.assert_called_once()


@pytest.mark.unit
class TestPostgreSQLLoaderTypeMapping:
    """Test comprehensive type mapping"""

    @pytest.fixture
    def loader(self):
        config = {'host': 'localhost', 'database': 'test', 'user': 'test', 'password': 'test'}
        return PostgreSQLLoader(config)

    def test_arrow_to_pg_type_mapping(self, loader):
        """Test Arrow to PostgreSQL type mapping"""
        # Create schema with various Arrow types
        schema = pa.schema(
            [
                pa.field('int8_field', pa.int8()),
                pa.field('int16_field', pa.int16()),
                pa.field('int32_field', pa.int32()),
                pa.field('int64_field', pa.int64()),
                pa.field('uint8_field', pa.uint8()),
                pa.field('uint16_field', pa.uint16()),
                pa.field('uint32_field', pa.uint32()),
                pa.field('uint64_field', pa.uint64()),
                pa.field('float32_field', pa.float32()),
                pa.field('float64_field', pa.float64()),
                pa.field('string_field', pa.string()),
                pa.field('binary_field', pa.binary()),
                pa.field('bool_field', pa.bool_()),
                pa.field('date32_field', pa.date32()),
                pa.field('timestamp_field', pa.timestamp('us')),
                pa.field('timestamp_tz_field', pa.timestamp('us', tz='UTC')),
                pa.field('decimal_field', pa.decimal128(10, 2)),
            ]
        )

        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [None]

        loader._create_table_from_schema(mock_cursor, schema, 'type_test')

        call_args_list = mock_cursor.execute.call_args_list

        args_call_1, kwargs_1 = call_args_list[0]
        args_call_2, kwargs_2 = call_args_list[1]

        assert len(call_args_list) >= 2

        assert 'SELECT 1 FROM information_schema.tables' in args_call_1[0]

        # Get the CREATE TABLE statement
        create_sql = args_call_2[0]

        # Verify type mappings
        assert '"int8_field" SMALLINT' in create_sql
        assert '"int16_field" SMALLINT' in create_sql
        assert '"int32_field" INTEGER' in create_sql
        assert '"int64_field" BIGINT' in create_sql
        assert '"float32_field" REAL' in create_sql
        assert '"float64_field" DOUBLE PRECISION' in create_sql
        assert '"string_field" TEXT' in create_sql
        assert '"binary_field" BYTEA' in create_sql
        assert '"bool_field" BOOLEAN' in create_sql
        assert '"date32_field" DATE' in create_sql
        assert '"timestamp_field" TIMESTAMP' in create_sql
        assert '"timestamp_tz_field" TIMESTAMPTZ' in create_sql
        assert '"decimal_field" NUMERIC(10,2)' in create_sql


@pytest.mark.unit
class TestPostgreSQLLoaderEdgeCases:
    """Test edge cases and error conditions"""

    @pytest.fixture
    def loader(self):
        config = {'host': 'localhost', 'database': 'test', 'user': 'test', 'password': 'test'}
        return PostgreSQLLoader(config)

    def test_empty_batch_handling(self, loader):
        """Test handling of empty batches"""
        # Create empty batch
        empty_schema = pa.schema([pa.field('id', pa.int64())])
        empty_batch = empty_schema.empty_table()

        mock_cursor = Mock()

        # This should not raise an error
        loader._copy_arrow_batch(mock_cursor, empty_batch, 'test_table', LoadMode.APPEND)

    def test_null_value_handling(self, loader):
        """Test handling of null values in data"""
        # Create batch with null values
        data = [pa.array([1, 2, None, 4], type=pa.int64()), pa.array(['a', None, 'c', 'd'], type=pa.string())]
        schema = pa.schema([pa.field('id', pa.int64()), pa.field('name', pa.string())])
        batch = pa.RecordBatch.from_arrays(data, schema=schema)

        mock_cursor = Mock()

        # Mock helper function
        with patch('src.nozzle.loaders.implementations._postgres_helpers.prepare_csv_data') as mock_prepare:
            mock_buffer = Mock()
            mock_prepare.return_value = (mock_buffer, ['id', 'name'])

            loader._copy_arrow_batch(mock_cursor, batch, 'test_table', LoadMode.APPEND)

        # Verify copy_from was called
        mock_cursor.copy_from.assert_called_once()

    def test_special_characters_in_table_name(self, loader):
        """Test handling of special characters in table names"""
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [None]

        schema = pa.schema([pa.field('id', pa.int64())])

        # Test table name with special characters
        loader._create_table_from_schema(mock_cursor, schema, 'test-table_with.special$chars')

        call_args_list = mock_cursor.execute.call_args_list

        args_call_1, kwargs_1 = call_args_list[0]
        args_call_2, kwargs_2 = call_args_list[1]

        assert len(call_args_list) >= 2

        assert 'SELECT 1 FROM information_schema.tables' in args_call_1[0]

        # Get the CREATE TABLE statement
        create_sql = args_call_2[0]
        assert 'test-table_with.special$chars' in create_sql

    def test_copy_operation_failure(self, loader):
        """Test handling of COPY operation failure"""
        mock_cursor = Mock()
        mock_cursor.copy_from.side_effect = psycopg2.Error('COPY failed')

        # Create test batch
        data = [pa.array([1, 2, 3], type=pa.int64())]
        schema = pa.schema([pa.field('id', pa.int64())])
        batch = pa.RecordBatch.from_arrays(data, schema=schema)

        # Mock helper function
        with patch('src.nozzle.loaders.implementations._postgres_helpers.prepare_csv_data') as mock_prepare:
            mock_buffer = Mock()
            mock_prepare.return_value = (mock_buffer, ['id'])

            with pytest.raises(RuntimeError, match='COPY operation failed'):
                loader._copy_arrow_batch(mock_cursor, batch, 'test_table', LoadMode.APPEND)

    def test_table_does_not_exist_error(self, loader):
        """Test handling of table does not exist error"""
        mock_cursor = Mock()
        mock_cursor.copy_from.side_effect = psycopg2.Error('relation "test_table" does not exist')

        # Create test batch
        data = [pa.array([1, 2, 3], type=pa.int64())]
        schema = pa.schema([pa.field('id', pa.int64())])
        batch = pa.RecordBatch.from_arrays(data, schema=schema)

        # Mock helper function
        with patch('src.nozzle.loaders.implementations._postgres_helpers.prepare_csv_data') as mock_prepare:
            mock_buffer = Mock()
            mock_prepare.return_value = (mock_buffer, ['id'])

            with pytest.raises(RuntimeError, match='does not exist'):
                loader._copy_arrow_batch(mock_cursor, batch, 'test_table', LoadMode.APPEND)

    def test_permission_denied_error(self, loader):
        """Test handling of permission denied error"""
        mock_cursor = Mock()
        mock_cursor.copy_from.side_effect = psycopg2.Error('permission denied for relation test_table')

        # Create test batch
        data = [pa.array([1, 2, 3], type=pa.int64())]
        schema = pa.schema([pa.field('id', pa.int64())])
        batch = pa.RecordBatch.from_arrays(data, schema=schema)

        # Mock helper function
        with patch('src.nozzle.loaders.implementations._postgres_helpers.prepare_csv_data') as mock_prepare:
            mock_buffer = Mock()
            mock_prepare.return_value = (mock_buffer, ['id'])

            with pytest.raises(RuntimeError, match='Permission denied'):
                loader._copy_arrow_batch(mock_cursor, batch, 'test_table', LoadMode.APPEND)
