import time
from typing import Any, Dict, Optional, Set, Union

import pyarrow as pa
from psycopg2.pool import ThreadedConnectionPool

from ..base import DataLoader, LoadMode, LoadResult
from ._postgres_helpers import has_binary_columns, prepare_csv_data, prepare_insert_data


class PostgreSQLLoader(DataLoader):
    """PostgreSQL data loader with zero-copy operations and connection pooling."""

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self.pool: Optional[ThreadedConnectionPool] = None
        self._default_batch_size: int = config.get('batch_size', 10000)
        self._created_tables: Set[str] = set()  # Track tables we've already created

    def connect(self) -> None:
        """Establish connection pool to PostgreSQL"""
        try:
            # Create connection pool for efficient connection reuse
            self.pool = ThreadedConnectionPool(minconn=1, maxconn=self.config.get('max_connections', 10), host=self.config['host'], port=self.config.get('port', 5432), database=self.config['database'], user=self.config['user'], password=self.config['password'], **self.config.get('connection_params', {}))

            # Test connection
            with self.pool.getconn() as conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute('SELECT version();')
                        version = cur.fetchone()
                        self.logger.info(f'Connected to PostgreSQL: {version[0][:50]}...')

                        cur.execute('SELECT current_database()')
                        database = cur.fetchone()
                        self.logger.info(f'Connected to database: {database[0]}')  # Fixed: access first element

                        cur.execute("""SELECT
                        table_schema || '.' || table_name
                        FROM
                        information_schema.tables
                        WHERE
                        table_type = 'BASE TABLE'
                        AND
                        table_schema NOT IN ('pg_catalog', 'information_schema')
                        LIMIT 10""")
                        tables = cur.fetchall()
                        self.logger.info(f'Found {len(tables)} user tables')
                finally:
                    self.pool.putconn(conn)

            self._is_connected = True

        except Exception as e:
            self.logger.error(f'Failed to connect to PostgreSQL: {str(e)}')
            raise

    def disconnect(self) -> None:
        """Close PostgreSQL connection pool"""
        if self.pool:
            self.pool.closeall()
            self.pool = None
        self._is_connected = False
        self.logger.info('Disconnected from PostgreSQL')

    def load_batch(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> LoadResult:
        """Load a single Arrow RecordBatch to PostgreSQL using zero-copy operations"""
        start_time = time.time()

        try:
            conn = self.pool.getconn()
            try:
                with conn.cursor() as cur:
                    # Create table only once per table per loader instance
                    if kwargs.get('create_table', True) and table_name not in self._created_tables:
                        self._create_table_from_schema(cur, batch.schema, table_name)
                        self._created_tables.add(table_name)
                        conn.commit()

                    self._copy_arrow_batch(cur, batch, table_name, kwargs.get('mode', LoadMode.APPEND))

                    conn.commit()

            finally:
                self.pool.putconn(conn)

            duration = time.time() - start_time

            return LoadResult(rows_loaded=batch.num_rows, duration=duration, table_name=table_name, loader_type='postgresql', success=True, metadata={'batch_size': batch.num_rows, 'schema_fields': len(batch.schema)})

        except Exception as e:
            self.logger.error(f'Failed to load batch: {str(e)}')
            return LoadResult(rows_loaded=0, duration=time.time() - start_time, table_name=table_name, loader_type='postgresql', success=False, error=str(e))

    def load_table(self, table: pa.Table, table_name: str, **kwargs) -> LoadResult:
        """Load a complete Arrow Table to PostgreSQL using zero-copy operations"""
        start_time = time.time()

        try:
            conn = self.pool.getconn()
            try:
                with conn.cursor() as cur:
                    # Create table only once per table per loader instance
                    if kwargs.get('create_table', True) and table_name not in self._created_tables:
                        self._create_table_from_schema(cur, table.schema, table_name)
                        self._created_tables.add(table_name)
                        conn.commit()

                    self._copy_arrow_table(cur, table, table_name, kwargs.get('mode', LoadMode.APPEND))

                    conn.commit()

            finally:
                self.pool.putconn(conn)

            duration = time.time() - start_time

            return LoadResult(rows_loaded=table.num_rows, duration=duration, table_name=table_name, loader_type='postgresql', success=True, metadata={'total_rows': table.num_rows, 'schema_fields': len(table.schema), 'table_size_bytes': table.nbytes})

        except Exception as e:
            self.logger.error(f'Failed to load table: {str(e)}')
            return LoadResult(rows_loaded=0, duration=time.time() - start_time, table_name=table_name, loader_type='postgresql', success=False, error=str(e))

    def _copy_arrow_batch(self, cursor: Any, batch: pa.RecordBatch, table_name: str, mode: LoadMode) -> None:
        """Use PostgreSQL COPY for efficient data loading directly from Arrow RecordBatch"""
        self._copy_arrow_data(cursor, batch, table_name, mode)

    def _copy_arrow_table(self, cursor: Any, table: pa.Table, table_name: str, mode: LoadMode) -> None:
        """Use PostgreSQL COPY for efficient data loading directly from Arrow Table"""
        self._copy_arrow_data(cursor, table, table_name, mode)

    def _copy_arrow_data(self, cursor: Any, data: Union[pa.RecordBatch, pa.Table], table_name: str, mode: LoadMode) -> None:
        """Common method for copying Arrow data to PostgreSQL."""
        # Handle different load modes
        if mode == LoadMode.OVERWRITE:
            cursor.execute(f'TRUNCATE TABLE {table_name}')

        # Check if we have binary columns that need special handling
        if has_binary_columns(data.schema):
            # Use INSERT statements for binary data
            self._insert_arrow_data(cursor, data, table_name)
        else:
            # Use efficient CSV COPY for non-binary data
            self._csv_copy_arrow_data(cursor, data, table_name)

    def _csv_copy_arrow_data(self, cursor: Any, data: Union[pa.RecordBatch, pa.Table], table_name: str) -> None:
        """Use CSV COPY for non-binary data (most efficient)."""
        csv_buffer, column_names = prepare_csv_data(data)

        # Use PostgreSQL COPY command for maximum efficiency
        try:
            cursor.copy_from(csv_buffer, table_name, columns=column_names, sep='\t', null='\\N')
        except Exception as e:
            # Provide helpful error message for common issues
            if 'does not exist' in str(e):
                raise RuntimeError(f"Table '{table_name}' does not exist. Set create_table=True to auto-create. error: {e}")
            elif 'permission denied' in str(e).lower():
                raise RuntimeError(f"Permission denied writing to table '{table_name}'. Check user permissions.")
            else:
                raise RuntimeError(f'COPY operation failed: {str(e)}')

    def _insert_arrow_data(self, cursor: Any, data: Union[pa.RecordBatch, pa.Table], table_name: str) -> None:
        """Use INSERT statements for data with binary columns."""
        insert_sql_template, rows = prepare_insert_data(data)
        insert_sql = f'INSERT INTO {table_name} {insert_sql_template}'

        # Use executemany for efficiency
        try:
            cursor.executemany(insert_sql, rows)
        except Exception as e:
            raise RuntimeError(f'INSERT operation failed: {str(e)}')

    def _create_table_from_schema(self, cursor: Any, schema: pa.Schema, table_name: str) -> None:
        """Create PostgreSQL table from Arrow schema with comprehensive type mapping"""

        # Check if table already exists to avoid unnecessary work
        cursor.execute(
            """
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = %s AND table_schema = 'public'
        """,
            (table_name,),
        )

        if cursor.fetchone():
            self.logger.debug(f"Table '{table_name}' already exists, skipping creation")
            return

        # Comprehensive Arrow to PostgreSQL type mapping
        type_mapping = {
            # Integer types
            pa.int8(): 'SMALLINT',
            pa.int16(): 'SMALLINT',
            pa.int32(): 'INTEGER',
            pa.int64(): 'BIGINT',
            pa.uint8(): 'SMALLINT',
            pa.uint16(): 'INTEGER',
            pa.uint32(): 'BIGINT',
            pa.uint64(): 'BIGINT',
            # Floating point types
            pa.float32(): 'REAL',
            pa.float64(): 'DOUBLE PRECISION',
            pa.float16(): 'REAL',
            # String types - use TEXT for blockchain data which can be large
            pa.string(): 'TEXT',
            pa.large_string(): 'TEXT',
            pa.utf8(): 'TEXT',
            # Binary types - use BYTEA for efficient storage
            pa.binary(): 'BYTEA',
            pa.large_binary(): 'BYTEA',
            # Boolean type
            pa.bool_(): 'BOOLEAN',
            # Date and time types
            pa.date32(): 'DATE',
            pa.date64(): 'DATE',
            pa.time32('s'): 'TIME',
            pa.time32('ms'): 'TIME',
            pa.time64('us'): 'TIME',
            pa.time64('ns'): 'TIME',
        }

        # Build CREATE TABLE statement
        columns = []
        for field in schema:
            # Handle complex types
            if pa.types.is_timestamp(field.type):
                # Handle timezone-aware timestamps
                if field.type.tz is not None:
                    pg_type = 'TIMESTAMPTZ'
                else:
                    pg_type = 'TIMESTAMP'
            elif pa.types.is_date(field.type):
                pg_type = 'DATE'
            elif pa.types.is_time(field.type):
                pg_type = 'TIME'
            elif pa.types.is_decimal(field.type):
                # Extract precision and scale from decimal type
                decimal_type = field.type
                pg_type = f'NUMERIC({decimal_type.precision},{decimal_type.scale})'
            elif pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
                # Use TEXT for list types (JSON-like data)
                pg_type = 'TEXT'
            elif pa.types.is_struct(field.type):
                # Use TEXT for struct types (JSON-like data)
                pg_type = 'TEXT'
            elif pa.types.is_binary(field.type):
                # Binary data - use BYTEA for efficient storage
                pg_type = 'BYTEA'
            elif pa.types.is_large_binary(field.type):
                # Large binary data - use BYTEA for efficient storage
                pg_type = 'BYTEA'
            elif pa.types.is_fixed_size_binary(field.type):
                # Fixed size binary data - use BYTEA for efficient storage
                pg_type = 'BYTEA'
            else:
                # Use mapping or default to TEXT for unknown types
                pg_type = type_mapping.get(field.type, 'TEXT')

            # Handle nullability
            nullable = '' if field.nullable else ' NOT NULL'

            # Quote column name for safety (important for blockchain field names)
            columns.append(f'"{field.name}" {pg_type}{nullable}')

        # Create the table - Fixed: use proper identifier quoting
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        )
        """

        try:
            self.logger.info(f"Creating table '{table_name}' with {len(columns)} columns")
            cursor.execute(create_sql)
            self.logger.debug(f"Successfully created table '{table_name}'")
        except Exception as e:
            raise RuntimeError(f"Failed to create table '{table_name}': {str(e)}")

    def get_table_schema(self, table_name: str) -> Optional[pa.Schema]:
        """Get the schema of an existing PostgreSQL table"""
        try:
            conn = self.pool.getconn()
            try:
                with conn.cursor() as cur:
                    # Query PostgreSQL information schema
                    cur.execute(
                        """
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns 
                        WHERE table_name = %s
                        ORDER BY ordinal_position
                    """,
                        (table_name,),
                    )

                    columns = cur.fetchall()
                    if not columns:
                        return None

                    # Convert PostgreSQL types back to Arrow types
                    fields = []
                    for col_name, data_type, is_nullable in columns:
                        arrow_type = self._pg_type_to_arrow(data_type)
                        nullable = is_nullable.upper() == 'YES'
                        fields.append(pa.field(col_name, arrow_type, nullable))

                    return pa.schema(fields)

            finally:
                self.pool.putconn(conn)

        except Exception as e:
            self.logger.error(f"Failed to get schema for table '{table_name}': {str(e)}")
            return None

    def _pg_type_to_arrow(self, pg_type: str) -> pa.DataType:
        """Convert PostgreSQL type to Arrow type"""
        pg_type = pg_type.upper()

        # Type mapping from PostgreSQL to Arrow
        type_mapping = {
            'SMALLINT': pa.int16(),
            'INTEGER': pa.int32(),
            'BIGINT': pa.int64(),
            'REAL': pa.float32(),
            'DOUBLE PRECISION': pa.float64(),
            'TEXT': pa.string(),
            'VARCHAR': pa.string(),
            'CHAR': pa.string(),
            'BYTEA': pa.binary(),
            'BOOLEAN': pa.bool_(),
            'DATE': pa.date32(),
            'TIME': pa.time64('us'),
            'TIMESTAMP': pa.timestamp('us'),
            'TIMESTAMPTZ': pa.timestamp('us', tz='UTC'),
            'JSONB': pa.string(),
            'JSON': pa.string(),
        }

        # Handle NUMERIC types with precision/scale
        if pg_type.startswith('NUMERIC'):
            return pa.decimal128(18, 6)  # Default precision/scale

        return type_mapping.get(pg_type, pa.string())  # Default to string
