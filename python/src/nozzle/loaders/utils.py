"""
Common utilities for data loaders to reduce code duplication.
"""

from typing import Any, Dict, Set

import pyarrow as pa


class ArrowTypeConverter:
    """
    Centralized Arrow type conversion utilities used across multiple loaders.
    """
    
    @staticmethod
    def get_postgresql_type_mapping() -> Dict[pa.DataType, str]:
        """Get Arrow to PostgreSQL type mapping"""
        return {
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
    
    @staticmethod
    def get_snowflake_type_mapping() -> Dict[pa.DataType, str]:
        """Get Arrow to Snowflake type mapping"""
        return {
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
    
    @staticmethod
    def convert_arrow_to_iceberg_type(arrow_type: pa.DataType) -> Any:
        """
        Convert Arrow data type to equivalent Iceberg type.
        Extracted from IcebergLoader implementation.
        """
        # Import here to avoid circular dependencies
        try:
            from pyiceberg.types import (
                BinaryType,
                BooleanType,
                DateType,
                DecimalType,
                DoubleType,
                FloatType,
                IntegerType,
                ListType,
                LongType,
                NestedField,
                StringType,
                StructType,
                TimestampType,
            )
        except ImportError:
            raise ImportError("Iceberg type conversion requires 'pyiceberg' package")
        
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return StringType()
        elif pa.types.is_int32(arrow_type):
            return IntegerType()
        elif pa.types.is_int64(arrow_type):
            return LongType()
        elif pa.types.is_float32(arrow_type):
            return FloatType()
        elif pa.types.is_float64(arrow_type):
            return DoubleType()
        elif pa.types.is_boolean(arrow_type):
            return BooleanType()
        elif pa.types.is_decimal(arrow_type):
            return DecimalType(arrow_type.precision, arrow_type.scale)
        elif pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
            return DateType()
        elif pa.types.is_timestamp(arrow_type):
            return TimestampType()
        elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return BinaryType()
        elif pa.types.is_list(arrow_type):
            element_type = ArrowTypeConverter.convert_arrow_to_iceberg_type(arrow_type.value_type)
            return ListType(1, element_type, element_required=False)
        elif pa.types.is_struct(arrow_type):
            nested_fields = []
            for i, field in enumerate(arrow_type):
                field_type = ArrowTypeConverter.convert_arrow_to_iceberg_type(field.type)
                nested_fields.append(NestedField(i + 1, field.name, field_type, required=not field.nullable))
            return StructType(nested_fields)
        else:
            # Fallback to string for unsupported types
            return StringType()
    
    @staticmethod
    def convert_arrow_field_to_sql(field: pa.Field, target_system: str) -> str:
        """
        Convert an Arrow field to SQL column definition.
        
        Args:
            field: Arrow field to convert
            target_system: Target system ('postgresql', 'snowflake', etc.)
            
        Returns:
            SQL column definition string
        """
        type_mappings = {
            'postgresql': ArrowTypeConverter.get_postgresql_type_mapping(),
            'snowflake': ArrowTypeConverter.get_snowflake_type_mapping(),
        }
        
        if target_system not in type_mappings:
            raise ValueError(f"Unsupported target system: {target_system}")
        
        type_mapping = type_mappings[target_system]
        
        # Handle complex types
        if pa.types.is_timestamp(field.type):
            if target_system == 'postgresql':
                if field.type.tz is not None:
                    sql_type = 'TIMESTAMPTZ'
                else:
                    sql_type = 'TIMESTAMP'
            else:  # snowflake
                if field.type.tz is not None:
                    sql_type = 'TIMESTAMP_TZ'
                else:
                    sql_type = 'TIMESTAMP_NTZ'
        elif pa.types.is_date(field.type):
            sql_type = 'DATE'
        elif pa.types.is_time(field.type):
            sql_type = 'TIME'
        elif pa.types.is_decimal(field.type):
            decimal_type = field.type
            if target_system == 'postgresql':
                sql_type = f'NUMERIC({decimal_type.precision},{decimal_type.scale})'
            else:  # snowflake
                sql_type = f'NUMBER({decimal_type.precision},{decimal_type.scale})'
        elif pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
            if target_system == 'postgresql':
                sql_type = 'TEXT'  # JSON-like data
            else:  # snowflake
                sql_type = 'VARIANT'
        elif pa.types.is_struct(field.type):
            if target_system == 'postgresql':
                sql_type = 'TEXT'  # JSON-like data
            else:  # snowflake
                sql_type = 'OBJECT'
        elif pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type):
            if target_system == 'postgresql':
                sql_type = 'BYTEA'
            else:  # snowflake
                sql_type = 'BINARY'
        elif pa.types.is_fixed_size_binary(field.type):
            if target_system == 'postgresql':
                sql_type = 'BYTEA'
            else:  # snowflake
                sql_type = f'BINARY({field.type.byte_width})'
        else:
            # Use mapping or default
            if target_system == 'postgresql':
                sql_type = type_mapping.get(field.type, 'TEXT')
            else:  # snowflake
                sql_type = type_mapping.get(field.type, 'VARCHAR')
        
        # Handle nullability
        nullable = '' if field.nullable else ' NOT NULL'
        
        # Quote column name for safety
        return f'"{field.name}" {sql_type}{nullable}'


class LoaderConfigValidator:
    """
    Common configuration validation utilities.
    """
    
    @staticmethod
    def validate_required_fields(config: Dict[str, Any], required_fields: Set[str]) -> None:
        """
        Validate that all required fields are present in config.
        
        Args:
            config: Configuration dictionary
            required_fields: Set of required field names
            
        Raises:
            ValueError: If any required fields are missing
        """
        missing_fields = required_fields - set(config.keys())
        if missing_fields:
            raise ValueError(f"Missing required configuration fields: {missing_fields}")
    
    @staticmethod
    def validate_connection_config(config: Dict[str, Any], loader_type: str) -> None:
        """
        Validate connection-specific configuration.
        
        Args:
            config: Configuration dictionary
            loader_type: Type of loader (postgresql, redis, etc.)
        """
        connection_requirements = {
            'postgresql': {'host', 'database', 'user', 'password'},
            'redis': {'host'},  # port is optional with default
            'snowflake': {'account', 'user', 'warehouse', 'database'},
            'lmdb': {'db_path'},
            'deltalake': {'table_path'},
            'iceberg': {'catalog_config', 'namespace'},
        }
        
        if loader_type in connection_requirements:
            required_fields = connection_requirements[loader_type]
            LoaderConfigValidator.validate_required_fields(config, required_fields)


class TableNameUtils:
    """
    Utilities for table name handling and validation.
    """
    
    @staticmethod
    def sanitize_table_name(table_name: str, target_system: str) -> str:
        """
        Sanitize table name for target system.
        
        Args:
            table_name: Original table name
            target_system: Target system name
            
        Returns:
            Sanitized table name
        """
        # Basic sanitization - can be extended for specific systems
        sanitized = table_name.replace('-', '_').replace(' ', '_')
        
        # System-specific rules
        if target_system == 'postgresql':
            # PostgreSQL is case-sensitive when quoted, prefer lowercase
            sanitized = sanitized.lower()
        elif target_system == 'snowflake':
            # Snowflake prefers uppercase
            sanitized = sanitized.upper()
        
        return sanitized
    
    @staticmethod
    def quote_identifier(identifier: str, target_system: str) -> str:
        """
        Quote identifier for target system.
        
        Args:
            identifier: Identifier to quote
            target_system: Target system name
            
        Returns:
            Quoted identifier
        """
        if target_system in ['postgresql', 'snowflake']:
            return f'"{identifier}"'
        else:
            return identifier


class CommonPatterns:
    """
    Common patterns extracted from loader implementations.
    """
    
    @staticmethod
    def get_connection_info_logger_template() -> str:
        """Get template for logging connection info"""
        return "Connected to {system} {version} at {host}:{port} database: {database}"
    
    @staticmethod
    def get_standard_metadata_fields() -> Set[str]:
        """Get standard metadata fields that should be included in LoadResult"""
        return {
            'batch_size', 'schema_fields', 'throughput_rows_per_sec',
            'total_rows', 'batches_processed', 'table_size_mb'
        }
    
    @staticmethod
    def convert_bytes_for_redis(value: Any) -> bytes:
        """
        Convert value to bytes for Redis storage.
        Extracted from RedisLoader pattern.
        """
        if isinstance(value, bytes):
            return value
        elif isinstance(value, (int, float)):
            return str(value).encode('utf-8')
        elif isinstance(value, bool):
            return b'1' if value else b'0'
        elif isinstance(value, str):
            return value.encode('utf-8')
        else:
            return str(value).encode('utf-8')
    
    @staticmethod
    def has_binary_columns(schema: pa.Schema) -> bool:
        """
        Check if schema contains any binary column types.
        Extracted from PostgreSQL helpers.
        """
        return any(
            pa.types.is_binary(field.type) or 
            pa.types.is_large_binary(field.type) or 
            pa.types.is_fixed_size_binary(field.type) 
            for field in schema
        )