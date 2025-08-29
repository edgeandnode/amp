import io
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import pyarrow as pa
import pyarrow.csv as pa_csv
import snowflake.connector
from snowflake.connector import DictCursor, SnowflakeConnection

from ..base import DataLoader, LoadMode


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


class SnowflakeLoader(DataLoader[SnowflakeConnectionConfig]):
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
    
    # Declare loader capabilities
    SUPPORTED_MODES = {LoadMode.APPEND}  # Snowflake loader only supports APPEND
    REQUIRES_SCHEMA_MATCH = False
    SUPPORTS_TRANSACTIONS = True

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self.connection: SnowflakeConnection = None
        self.cursor = None
        self._created_tables = set()  # Track created tables

        # Loading configuration
        self.use_stage = config.get('use_stage', True)
        self.stage_name = config.get('stage_name', 'NOZZLE_STAGE')
        self.compression = config.get('compression', 'gzip')
    
    
    def _get_required_config_fields(self) -> list[str]:
        """Return required configuration fields"""
        return ['account', 'user', 'warehouse', 'database']

    def connect(self) -> None:
        """Establish connection to Snowflake"""
        try:
            # Build connection parameters
            conn_params = {
                'account': self.config.account,
                'user': self.config.user,
                'warehouse': self.config.warehouse,
                'database': self.config.database,
                'schema': self.config.schema,
                'login_timeout': self.config.login_timeout,
                'network_timeout': self.config.network_timeout,
                'socket_timeout': self.config.socket_timeout,
                'ocsp_response_cache_filename': self.config.ocsp_response_cache_filename,
                'validate_default_parameters': self.config.validate_default_parameters,
                'paramstyle': self.config.paramstyle,
                **self.config.connection_params,
            }

            # Add authentication parameters
            if self.config.authenticator:
                conn_params['authenticator'] = self.config.authenticator
                if self.config.authenticator == 'oauth':
                    conn_params['token'] = self.config.token
                elif self.config.authenticator == 'externalbrowser':
                    pass  # No additional params needed
                elif self.config.authenticator == 'okta' and self.config.okta_account_name:
                    conn_params['authenticator'] = f'https://{self.config.okta_account_name}.okta.com'
            elif self.config.private_key:
                conn_params['private_key'] = self.config.private_key
                if self.config.private_key_passphrase:
                    conn_params['private_key_passphrase'] = self.config.private_key_passphrase
            else:
                conn_params['password'] = self.config.password

            # Optional parameters
            if self.config.role:
                conn_params['role'] = self.config.role
            if self.config.timezone:
                conn_params['timezone'] = self.config.timezone

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
    
    def _clear_table(self, table_name: str) -> None:
        """Clear table for overwrite mode"""
        # Snowflake loader doesn't support overwrite mode
        raise ValueError("Snowflake loader does not support OVERWRITE mode")

    def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
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
            (self.config.schema, table_name.upper()),
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
    
    def _get_loader_batch_metadata(self, batch: pa.RecordBatch, duration: float, **kwargs) -> Dict[str, Any]:
        """Get Snowflake-specific metadata for batch operation"""
        return {
            'loading_method': 'stage' if self.use_stage else 'insert',
            'warehouse': self.config.warehouse,
            'database': self.config.database,
            'schema': self.config.schema
        }
    
    def _get_loader_table_metadata(self, table: pa.Table, duration: float, batch_count: int, **kwargs) -> Dict[str, Any]:
        """Get Snowflake-specific metadata for table operation"""
        return {
            'loading_method': 'stage' if self.use_stage else 'insert',
            'warehouse': self.config.warehouse,
            'database': self.config.database,
            'schema': self.config.schema
        }

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
                (self.config.schema, table_name.upper()),
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
                (self.config.schema, table_name.upper()),
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
