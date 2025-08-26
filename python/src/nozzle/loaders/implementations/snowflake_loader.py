import io
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import pyarrow as pa
import pyarrow.csv as pa_csv
import snowflake.connector
from snowflake.connector import DictCursor, SnowflakeConnection

from ..base import DataLoader, LoadMode, LoadResult


@dataclass
class SnowflakeConnectionConfig:
    """Configuration for Snowflake connection with required and optional parameters"""

    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str = 'PUBLIC'
    role: Optional[str] = None
    authenticator: Optional[str] = None
    private_key: Optional[Any] = None
    private_key_passphrase: Optional[str] = None
    token: Optional[str] = None
    okta_account_name: Optional[str] = None
    login_timeout: int = 60
    network_timeout: int = 300
    socket_timeout: int = 300
    ocsp_response_cache_filename: Optional[str] = None
    validate_default_parameters: bool = True
    paramstyle: str = 'qmark'
    timezone: Optional[str] = None
    connection_params: Dict[str, Any] = None

    def __post_init__(self):
        if self.connection_params is None:
            self.connection_params = {}


class SnowflakeLoader(DataLoader):
    """
    Snowflake data loader optimized for bulk loading operations.

    Features:
    - Zero-copy operations using COPY INTO
    - Efficient data staging through internal stages
    - Support for various authentication methods
    - Automatic schema creation
    - Comprehensive error handling
    - Support for all Snowflake data types
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self.connection: SnowflakeConnection = None
        self.cursor = None
        self._created_tables = set()  # Track tables we've already created

        # Connection configuration
        self.conn_config = SnowflakeConnectionConfig(
            account=config['account'],
            user=config['user'],
            password=config.get('password', ''),
            warehouse=config['warehouse'],
            database=config['database'],
            schema=config.get('schema', 'PUBLIC'),
            role=config.get('role'),
            authenticator=config.get('authenticator'),
            private_key=config.get('private_key'),
            private_key_passphrase=config.get('private_key_passphrase'),
            token=config.get('token'),
            okta_account_name=config.get('okta_account_name'),
            login_timeout=config.get('login_timeout', 60),
            network_timeout=config.get('network_timeout', 300),
            socket_timeout=config.get('socket_timeout', 300),
            ocsp_response_cache_filename=config.get('ocsp_response_cache_filename'),
            validate_default_parameters=config.get('validate_default_parameters', True),
            paramstyle=config.get('paramstyle', 'qmark'),
            timezone=config.get('timezone'),
            connection_params=config.get('connection_params', {}),
        )

        # Loading configuration
        self.use_stage = config.get('use_stage', True)
        self.stage_name = config.get('stage_name', 'NOZZLE_STAGE')
        self.batch_size = config.get('batch_size', 10000)

        self.compression = config.get('compression', 'gzip')

    def connect(self) -> None:
        """Establish connection to Snowflake"""
        try:
            # Build connection parameters
            conn_params = {
                'account': self.conn_config.account,
                'user': self.conn_config.user,
                'warehouse': self.conn_config.warehouse,
                'database': self.conn_config.database,
                'schema': self.conn_config.schema,
                'login_timeout': self.conn_config.login_timeout,
                'network_timeout': self.conn_config.network_timeout,
                'socket_timeout': self.conn_config.socket_timeout,
                'ocsp_response_cache_filename': self.conn_config.ocsp_response_cache_filename,
                'validate_default_parameters': self.conn_config.validate_default_parameters,
                'paramstyle': self.conn_config.paramstyle,
                **self.conn_config.connection_params,
            }

            # Add authentication parameters
            if self.conn_config.authenticator:
                conn_params['authenticator'] = self.conn_config.authenticator
                if self.conn_config.authenticator == 'oauth':
                    conn_params['token'] = self.conn_config.token
                elif self.conn_config.authenticator == 'externalbrowser':
                    pass  # No additional params needed
                elif self.conn_config.authenticator == 'okta' and self.conn_config.okta_account_name:
                    conn_params['authenticator'] = f'https://{self.conn_config.okta_account_name}.okta.com'
            elif self.conn_config.private_key:
                conn_params['private_key'] = self.conn_config.private_key
                if self.conn_config.private_key_passphrase:
                    conn_params['private_key_passphrase'] = self.conn_config.private_key_passphrase
            else:
                conn_params['password'] = self.conn_config.password

            # Optional parameters
            if self.conn_config.role:
                conn_params['role'] = self.conn_config.role
            if self.conn_config.timezone:
                conn_params['timezone'] = self.conn_config.timezone

            self.connection = snowflake.connector.connect(**conn_params)
            self.cursor = self.connection.cursor(DictCursor)

            self.cursor.execute('SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()')
            result = self.cursor.fetchone()

            self.logger.info(f'Connected to Snowflake {result["CURRENT_VERSION()"]}')
            self.logger.info(f'Warehouse: {result["CURRENT_WAREHOUSE()"]}')
            self.logger.info(f'Database: {result["CURRENT_DATABASE()"]}.{result["CURRENT_SCHEMA()"]}')

            if self.use_stage:
                self._create_stage()

            self._is_connected = True

        except Exception as e:
            self.logger.error(f'Failed to connect to Snowflake: {str(e)}')
            raise

    def disconnect(self) -> None:
        """Close Snowflake connection"""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
        self._is_connected = False
        self.logger.info('Disconnected from Snowflake')

    def load_batch(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> LoadResult:
        """Load a single Arrow RecordBatch to Snowflake"""
        start_time = time.time()

        try:
            rows_loaded = self._load_data(batch, table_name, **kwargs)
            duration = time.time() - start_time

            return LoadResult(
                rows_loaded=rows_loaded,
                duration=duration,
                table_name=table_name,
                loader_type='snowflake',
                success=True,
                metadata={
                    'batch_size': batch.num_rows,
                    'schema_fields': len(batch.schema),
                    'loading_method': 'stage' if self.use_stage else 'insert',
                    'warehouse': self.conn_config.warehouse,
                },
            )

        except Exception as e:
            self.logger.error(f'Failed to load batch: {str(e)}')
            self.connection.rollback()
            return LoadResult(
                rows_loaded=0,
                duration=time.time() - start_time,
                table_name=table_name,
                loader_type='snowflake',
                success=False,
                error=str(e),
            )

    def load_table(self, table: pa.Table, table_name: str, **kwargs) -> LoadResult:
        """Load a complete Arrow Table to Snowflake"""
        start_time = time.time()

        try:
            batch_size = kwargs.get('batch_size', self.batch_size)
            total_rows = 0
            batch_count = 0

            for batch in table.to_batches(max_chunksize=batch_size):
                rows_loaded = self._load_data(batch, table_name, **kwargs)
                total_rows += rows_loaded
                batch_count += 1
                self.logger.debug(f'Loaded batch {batch_count}: {rows_loaded} rows')

            duration = time.time() - start_time

            return LoadResult(
                rows_loaded=total_rows,
                duration=duration,
                table_name=table_name,
                loader_type='snowflake',
                success=True,
                metadata={
                    'total_rows': table.num_rows,
                    'schema_fields': len(table.schema),
                    'batches_processed': batch_count,
                    'avg_batch_size': round(total_rows / batch_count, 2) if batch_count > 0 else 0,
                    'loading_method': 'stage' if self.use_stage else 'insert',
                    'table_size_mb': round(table.nbytes / 1024 / 1024, 2),
                },
            )

        except Exception as e:
            self.logger.error(f'Failed to load table: {str(e)}')
            self.connection.rollback()
            return LoadResult(
                rows_loaded=0,
                duration=time.time() - start_time,
                table_name=table_name,
                loader_type='snowflake',
                success=False,
                error=str(e),
            )

    def _load_data(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        """Internal method to load data - used by both load_batch and load_table"""
        mode = kwargs.get('mode', LoadMode.APPEND)
        create_table = kwargs.get('create_table', True)

        # Snowflake loader only supports APPEND mode
        if mode == LoadMode.OVERWRITE:
            raise ValueError(
                "Snowflake loader does not support OVERWRITE mode. "
                "Please use APPEND mode or manually truncate/drop the table before loading."
            )

        if create_table and table_name.upper() not in self._created_tables:
            self._create_table_from_schema(batch.schema, table_name)
            self._created_tables.add(table_name.upper())

        if self.use_stage:
            rows_loaded = self._load_via_stage(batch, table_name)
        else:
            rows_loaded = self._load_via_insert(batch, table_name)

        self.connection.commit()
        return rows_loaded

    def _create_stage(self) -> None:
        """Create internal stage for data loading"""
        try:
            create_stage_sql = f"""
            CREATE STAGE IF NOT EXISTS {self.stage_name}
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_DELIMITER = '|'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                ESCAPE = '\\\\'
                ESCAPE_UNENCLOSED_FIELD = '\\\\'
                NULL_IF = ('\\\\N', 'NULL', 'null')
                EMPTY_FIELD_AS_NULL = TRUE
                COMPRESSION = {self.compression.upper()}
                ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
            )
            """
            self.cursor.execute(create_stage_sql)
            self.logger.info(f"Created or verified stage '{self.stage_name}'")
        except Exception as e:
            error_msg = f"Failed to create stage '{self.stage_name}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def _load_via_stage(self, batch: pa.RecordBatch, table_name: str) -> int:
        """Load data via Snowflake internal stage using COPY INTO"""

        csv_buffer = io.BytesIO()

        write_options = pa_csv.WriteOptions(include_header=False, delimiter='|', quoting_style='needed')

        pa_csv.write_csv(batch, csv_buffer, write_options=write_options)

        csv_content = csv_buffer.getvalue()
        csv_buffer.close()

        stage_path = f'@{self.stage_name}/temp_{table_name}_{int(time.time() * 1000)}.csv'

        self.cursor.execute(f"PUT 'file://-' {stage_path} OVERWRITE = TRUE", file_stream=io.BytesIO(csv_content))

        column_names = [f'"{field.name}"' for field in batch.schema]

        copy_sql = f"""
        COPY INTO {table_name} ({', '.join(column_names)})
        FROM {stage_path}
        ON_ERROR = 'ABORT_STATEMENT'
        PURGE = TRUE
        """

        result = self.cursor.execute(copy_sql).fetchone()
        rows_loaded = result['rows_loaded'] if result else batch.num_rows

        return rows_loaded

    def _load_via_insert(self, batch: pa.RecordBatch, table_name: str) -> int:
        """Load data via INSERT statements using Arrow's native iteration"""

        column_names = [field.name for field in batch.schema]
        quoted_column_names = [f'"{field.name}"' for field in batch.schema]

        placeholders = ', '.join(['?'] * len(quoted_column_names))
        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(quoted_column_names)})
        VALUES ({placeholders})
        """

        rows = []
        data_dict = batch.to_pydict()

        # Transpose to row-wise format
        for i in range(batch.num_rows):
            row = []
            for col_name in column_names:
                value = data_dict[col_name][i]

                # Convert Arrow nulls to None
                if value is None or (hasattr(value, 'is_valid') and not value.is_valid):
                    row.append(None)
                else:
                    row.append(value)
            rows.append(row)

        self.cursor.executemany(insert_sql, rows)

        return len(rows)

    def _create_table_from_schema(self, schema: pa.Schema, table_name: str) -> None:
        """Create Snowflake table from Arrow schema"""

        # Check if table already exists
        self.cursor.execute(
            """
            SELECT COUNT(*) as count
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """,
            (self.conn_config.schema, table_name.upper()),
        )

        result = self.cursor.fetchone()
        count = result['COUNT'] if result else 0
        if count > 0:
            self.logger.debug(f"Table '{table_name}' already exists, skipping creation")
            return

        # Arrow to Snowflake type mapping
        type_mapping = {
            # Integer types
            pa.int8(): 'TINYINT',
            pa.int16(): 'SMALLINT',
            pa.int32(): 'INTEGER',
            pa.int64(): 'BIGINT',
            pa.uint8(): 'SMALLINT',
            pa.uint16(): 'INTEGER',
            pa.uint32(): 'BIGINT',
            pa.uint64(): 'BIGINT',
            # Floating point types
            pa.float32(): 'FLOAT',
            pa.float64(): 'DOUBLE',
            pa.float16(): 'FLOAT',
            # String types
            pa.string(): 'VARCHAR',
            pa.large_string(): 'VARCHAR',
            pa.utf8(): 'VARCHAR',
            # Binary types
            pa.binary(): 'BINARY',
            pa.large_binary(): 'BINARY',
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
                if field.type.tz is not None:
                    snowflake_type = 'TIMESTAMP_TZ'
                else:
                    snowflake_type = 'TIMESTAMP_NTZ'
            elif pa.types.is_date(field.type):
                snowflake_type = 'DATE'
            elif pa.types.is_time(field.type):
                snowflake_type = 'TIME'
            elif pa.types.is_decimal(field.type):
                decimal_type = field.type
                snowflake_type = f'NUMBER({decimal_type.precision},{decimal_type.scale})'
            elif pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
                snowflake_type = 'VARIANT'
            elif pa.types.is_struct(field.type):
                snowflake_type = 'OBJECT'
            elif pa.types.is_map(field.type):
                snowflake_type = 'OBJECT'
            elif pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type):
                snowflake_type = 'BINARY'
            elif pa.types.is_fixed_size_binary(field.type):
                snowflake_type = f'BINARY({field.type.byte_width})'
            elif pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                # Use VARCHAR with no length limit for flexibility
                snowflake_type = 'VARCHAR'
            else:
                # Use mapping or default to VARCHAR
                snowflake_type = type_mapping.get(field.type, 'VARCHAR')

            # Handle nullability
            nullable = '' if field.nullable else ' NOT NULL'

            # Add column definition - quote column name for safety with special characters
            columns.append(f'"{field.name}" {snowflake_type}{nullable}')

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        )
        """

        try:
            self.logger.info(f"Creating table '{table_name}' with {len(columns)} columns")
            self.cursor.execute(create_sql)
            self.logger.debug(f"Successfully created table '{table_name}'")
        except Exception as e:
            raise RuntimeError(f"Failed to create table '{table_name}': {str(e)}") from e

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a Snowflake table"""
        try:
            # Get table metadata
            self.cursor.execute(
                """
                SELECT 
                    TABLE_NAME,
                    TABLE_SCHEMA,
                    TABLE_CATALOG,
                    TABLE_TYPE,
                    ROW_COUNT,
                    BYTES,
                    CLUSTERING_KEY,
                    COMMENT
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """,
                (self.conn_config.schema, table_name.upper()),
            )

            table_info = self.cursor.fetchone()
            if not table_info:
                return None

            # Get column information
            self.cursor.execute(
                """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """,
                (self.conn_config.schema, table_name.upper()),
            )

            columns = self.cursor.fetchall()

            return {
                'table_name': table_info['TABLE_NAME'],
                'schema': table_info['TABLE_SCHEMA'],
                'database': table_info['TABLE_CATALOG'],
                'type': table_info['TABLE_TYPE'],
                'row_count': table_info['ROW_COUNT'],
                'size_bytes': table_info['BYTES'],
                'size_mb': round(table_info['BYTES'] / 1024 / 1024, 2) if table_info['BYTES'] else 0,
                'clustering_key': table_info['CLUSTERING_KEY'],
                'comment': table_info['COMMENT'],
                'columns': [
                    {
                        'name': col['COLUMN_NAME'],
                        'type': col['DATA_TYPE'],
                        'nullable': col['IS_NULLABLE'] == 'YES',
                        'default': col['COLUMN_DEFAULT'],
                        'max_length': col['CHARACTER_MAXIMUM_LENGTH'],
                        'precision': col['NUMERIC_PRECISION'],
                        'scale': col['NUMERIC_SCALE'],
                    }
                    for col in columns
                ],
            }

        except Exception as e:
            self.logger.error(f"Failed to get table info for '{table_name}': {str(e)}")
            return None
