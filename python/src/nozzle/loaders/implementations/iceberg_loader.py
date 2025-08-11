# src/nozzle/loaders/implementations/iceberg_loader.py

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import pyarrow as pa
import pyarrow.compute as pc

try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import (
        NoSuchNamespaceError,
        NoSuchTableError,
        NoSuchIcebergTableError
    )
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.types import (
        NestedField,
        StringType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        BooleanType,
        DecimalType,
        DateType,
        TimestampType,
        BinaryType,
        ListType,
        StructType
    )
    from pyiceberg.partitioning import PartitionSpec
    ICEBERG_AVAILABLE = True
except ImportError:
    ICEBERG_AVAILABLE = False

# Import types for better IDE support
from .iceberg_types import IcebergTable, IcebergCatalog, UpsertResult

from ..base import DataLoader, LoadMode, LoadResult


@dataclass
class IcebergStorageConfig:
    """Configuration for Iceberg storage backend"""
    
    catalog_config: Dict[str, Any]
    namespace: str
    default_table_name: Optional[str] = None
    create_namespace: bool = True
    create_table: bool = True
    partition_spec: Optional['PartitionSpec'] = None  # Direct PartitionSpec object
    
    # Schema evolution settings
    schema_evolution: bool = True
    
    batch_size: int = 10000


class IcebergLoader(DataLoader):
    """
    Apache Iceberg loader with zero-copy Arrow integration.
    
    Supports all standard load modes:
    - APPEND: Add new data to the table
    - OVERWRITE: Replace all data in the table
    - UPSERT/MERGE: Update existing rows and insert new ones using PyIceberg's automatic matching
    
    Configuration Requirements:
    - catalog_config: PyIceberg catalog configuration
    - namespace: Iceberg namespace/database name
    - partition_spec: Optional PartitionSpec object for table partitioning
    - schema_evolution: Enable automatic schema evolution (default: True)
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        if not ICEBERG_AVAILABLE:
            raise ImportError(
                "Apache Iceberg support requires 'pyiceberg' package. "
                "Install with: pip install pyiceberg"
            )
        
        # Handle schema evolution configuration
        schema_evolution = config.get('schema_evolution', True)
        
        self.storage_config = IcebergStorageConfig(
            catalog_config=config['catalog_config'],
            namespace=config['namespace'],
            default_table_name=config.get('default_table_name'),
            create_namespace=config.get('create_namespace', True),
            create_table=config.get('create_table', True),
            partition_spec=config.get('partition_spec'),  # Accept PartitionSpec directly
            schema_evolution=schema_evolution,
            batch_size=config.get('batch_size', 10000)
        )
        
        self._catalog: Optional[IcebergCatalog] = None
        self._current_table: Optional[IcebergTable] = None
        self._namespace_exists: bool = False
        self.enable_statistics: bool = config.get('enable_statistics', True)
        
    def connect(self) -> None:
        """Initialize Iceberg catalog connection"""
        try:
            self._catalog = load_catalog(**self.storage_config.catalog_config)
            self._validate_catalog_connection()
            
            if self.storage_config.create_namespace:
                self._ensure_namespace(self.storage_config.namespace)
            else:
                self._check_namespace_exists(self.storage_config.namespace)
            
            self._is_connected = True
            self.logger.info(f"Iceberg loader connected successfully to namespace: {self.storage_config.namespace}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Iceberg catalog: {str(e)}")
            raise
    
    def disconnect(self) -> None:
        """Clean up Iceberg connection"""
        if self._current_table:
            self._current_table = None
        
        if self._catalog:
            self._catalog = None
        
        self._is_connected = False
        self.logger.info("Iceberg loader disconnected")
    
    def load_batch(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> LoadResult:
        """Load Arrow RecordBatch to Iceberg table"""
        start_time = time.time()
        
        try:
            table = pa.Table.from_batches([batch])
            result = self.load_table(table, table_name, **kwargs)
            
            result.metadata.update({
                'operation': 'load_batch',
                'batch_size': batch.num_rows,
                'schema_fields': len(batch.schema)
            })
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to load batch: {str(e)}")
            return LoadResult(
                rows_loaded=0,
                duration=time.time() - start_time,
                table_name=table_name,
                loader_type='iceberg',
                success=False,
                error=str(e)
            )
    
    def load_table(self, table: pa.Table, table_name: str, **kwargs) -> LoadResult:
        """Load Arrow Table to Iceberg using zero-copy operations"""
        start_time = time.time()
        
        try:
            table = self._fix_timestamps(table)
            mode = kwargs.get('mode', LoadMode.APPEND)
            iceberg_table = self._get_or_create_table(table_name, table.schema)
            
            if mode != LoadMode.OVERWRITE:
                self._validate_schema_compatibility(iceberg_table, table.schema)
            
            rows_written = self._perform_load_operation(iceberg_table, table, mode)
            duration = time.time() - start_time
            metadata = self._get_load_metadata(iceberg_table, table, duration)
            
            self.logger.info(
                f"Successfully loaded {rows_written} rows to {table_name} "
                f"in {duration:.2f}s (mode: {mode.value})"
            )
            
            return LoadResult(
                rows_loaded=rows_written,
                duration=duration,
                table_name=table_name,
                loader_type='iceberg',
                success=True,
                metadata=metadata
            )
            
        except Exception as e:
            self.logger.error(f"Failed to load table {table_name}: {str(e)}")
            return LoadResult(
                rows_loaded=0,
                duration=time.time() - start_time,
                table_name=table_name,
                loader_type='iceberg',
                success=False,
                error=str(e)
            )
    
    def _fix_timestamps(self, arrow_table: pa.Table) -> pa.Table:
        """Convert nanosecond timestamps to microseconds for Iceberg compatibility"""
        columns_to_fix = []
        for i, field in enumerate(arrow_table.schema):
            if pa.types.is_timestamp(field.type) and field.type.unit == 'ns':
                columns_to_fix.append((i, field.name))
        
        if not columns_to_fix:
            return arrow_table
        
        columns = []
        new_fields = []
        
        for i, field in enumerate(arrow_table.schema):
            if any(idx == i for idx, _ in columns_to_fix):
                timestamp_col = arrow_table.column(i)
                timestamp_us = pc.cast(timestamp_col, pa.timestamp('us', tz=field.type.tz))
                columns.append(timestamp_us)
                new_fields.append(pa.field(field.name, pa.timestamp('us', tz=field.type.tz)))
            else:
                columns.append(arrow_table.column(i))
                new_fields.append(field)
        
        new_schema = pa.schema(new_fields)
        return pa.table(columns, schema=new_schema)
    
    def _validate_catalog_connection(self) -> None:
        """Validate that catalog connection is working"""
        try:
            list(self._catalog.list_namespaces())
            self.logger.debug("Catalog connection validated successfully")
        except Exception as e:
            raise ConnectionError(f"Failed to validate catalog connection: {str(e)}")
    
    def _ensure_namespace(self, namespace: str) -> None:
        """Create namespace if it doesn't exist"""
        try:
            existing_namespaces = list(self._catalog.list_namespaces())
            if namespace not in [ns[0] if isinstance(ns, tuple) else ns for ns in existing_namespaces]:
                self._catalog.create_namespace(namespace)
                self.logger.info(f"Created namespace: {namespace}")
            else:
                self.logger.debug(f"Namespace already exists: {namespace}")
            
            self._namespace_exists = True
            
        except Exception as e:
            try:
                self._catalog.create_namespace(namespace)
                self._namespace_exists = True
                self.logger.info(f"Created namespace: {namespace}")
            except Exception:
                raise e
    
    def _check_namespace_exists(self, namespace: str) -> None:
        """Check that namespace exists (when create_namespace=False)"""
        try:
            existing_namespaces = list(self._catalog.list_namespaces())
            if namespace not in [ns[0] if isinstance(ns, tuple) else ns for ns in existing_namespaces]:
                raise NoSuchNamespaceError(f"Namespace '{namespace}' not found")
            
            self._namespace_exists = True
            self.logger.debug(f"Namespace exists: {namespace}")
            
        except Exception as e:
            raise NoSuchNamespaceError(f"Failed to verify namespace '{namespace}': {str(e)}")
    
    def _get_or_create_table(self, table_name: str, schema: pa.Schema) -> IcebergTable:
        """Get existing table or create new one"""
        table_identifier = f"{self.storage_config.namespace}.{table_name}"
        
        try:
            table = self._catalog.load_table(table_identifier)
            self.logger.debug(f"Loaded existing table: {table_identifier}")
            return table
            
        except (NoSuchTableError, NoSuchIcebergTableError):
            if not self.storage_config.create_table:
                raise NoSuchTableError(f"Table '{table_identifier}' not found and create_table=False")
            
            try:
                # Use partition_spec if provided
                if self.storage_config.partition_spec:
                    table = self._catalog.create_table(
                        identifier=table_identifier,
                        schema=schema,
                        partition_spec=self.storage_config.partition_spec
                    )
                else:
                    # Create table without partitioning
                    table = self._catalog.create_table(
                        identifier=table_identifier,
                        schema=schema
                    )
                self.logger.info(f"Created new table: {table_identifier}")
                return table
                
            except Exception as e:
                raise RuntimeError(f"Failed to create table '{table_identifier}': {str(e)}")
    
    
    def _validate_schema_compatibility(self, iceberg_table: IcebergTable, arrow_schema: pa.Schema) -> None:
        """Validate that Arrow schema is compatible with Iceberg table schema and perform schema evolution if enabled"""
        iceberg_schema = iceberg_table.schema()
        
        if not self.storage_config.schema_evolution:
            # Strict mode: schema must match exactly
            self._validate_schema_strict(iceberg_schema, arrow_schema)
        else:
            # Evolution mode: evolve schema to accommodate new fields
            self._evolve_schema_if_needed(iceberg_table, iceberg_schema, arrow_schema)
    
    def _validate_schema_strict(self, iceberg_schema: IcebergSchema, arrow_schema: pa.Schema) -> None:
        """Validate schema compatibility in strict mode (no evolution)"""
        iceberg_field_names = {field.name for field in iceberg_schema.fields}
        arrow_field_names = {field.name for field in arrow_schema}
        
        # Check for missing columns in Arrow schema
        missing_in_arrow = iceberg_field_names - arrow_field_names
        if missing_in_arrow:
            self.logger.warning(f"Arrow schema missing columns present in Iceberg table: {missing_in_arrow}")
        
        # Check for extra columns in Arrow schema 
        extra_in_arrow = arrow_field_names - iceberg_field_names
        if extra_in_arrow:
            raise ValueError(f"Arrow schema contains columns not present in Iceberg table: {extra_in_arrow}. "
                           f"Enable schema_evolution=True to automatically add new columns.")
        
        self.logger.debug("Schema validation passed in strict mode")
    
    def _evolve_schema_if_needed(self, iceberg_table: IcebergTable, iceberg_schema: IcebergSchema, arrow_schema: pa.Schema) -> None:
        """Evolve the Iceberg table schema to accommodate new Arrow schema fields"""
        try:
            iceberg_field_names = {field.name for field in iceberg_schema.fields}
            arrow_field_names = {field.name for field in arrow_schema}
            
            # Find new columns in Arrow schema
            new_columns = arrow_field_names - iceberg_field_names
            
            if not new_columns:
                self.logger.debug("No schema changes needed - schemas are compatible")
                return
            
            self.logger.info(f"Evolving schema to add {len(new_columns)} new columns: {new_columns}")
            
            # Use Iceberg's update schema API
            with iceberg_table.update_schema() as update:
                for field_name in new_columns:
                    arrow_field = arrow_schema.field(field_name)
                    iceberg_type = self._convert_arrow_type_to_iceberg(arrow_field.type)
                    
                    # Add new column (always optional in schema evolution to maintain compatibility)
                    update.add_column(
                        field_name, 
                        iceberg_type, 
                        doc=f"Added by schema evolution from Arrow field",
                        required=False
                    )
                    self.logger.debug(f"Added column '{field_name}' with type {iceberg_type}")
                    
            self.logger.info(f"Successfully evolved schema - added columns: {new_columns}")
            
        except Exception as e:
            raise RuntimeError(f"Failed to evolve table schema: {str(e)}")
    
    def _convert_arrow_type_to_iceberg(self, arrow_type: pa.DataType) -> Any:
        """Convert Arrow data type to equivalent Iceberg type"""
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
            element_type = self._convert_arrow_type_to_iceberg(arrow_type.value_type)
            return ListType(1, element_type, element_required=False)
        elif pa.types.is_struct(arrow_type):
            nested_fields = []
            for i, field in enumerate(arrow_type):
                field_type = self._convert_arrow_type_to_iceberg(field.type)
                nested_fields.append(NestedField(i + 1, field.name, field_type, required=not field.nullable))
            return StructType(nested_fields)
        else:
            # Fallback to string for unsupported types
            self.logger.warning(f"Unsupported Arrow type {arrow_type}, converting to StringType")
            return StringType()
    
    def _perform_load_operation(self, iceberg_table: IcebergTable, arrow_table: pa.Table, mode: LoadMode) -> int:
        """Perform the actual load operation based on mode"""
        if mode == LoadMode.APPEND:
            iceberg_table.append(arrow_table)
            return arrow_table.num_rows
            
        elif mode == LoadMode.OVERWRITE:
            iceberg_table.overwrite(arrow_table)
            return arrow_table.num_rows
            
        elif mode in (LoadMode.UPSERT, LoadMode.MERGE):
            # For UPSERT/MERGE operations, use PyIceberg's automatic matching
            try:
                self.logger.info(f"Performing {mode.value} operation with automatic column matching")
                
                # Use PyIceberg's upsert method with default settings
                upsert_result = iceberg_table.upsert(arrow_table)
                
                self.logger.info(f"Upsert operation completed successfully")
                return arrow_table.num_rows
                
            except Exception as e:
                self.logger.error(f"UPSERT/MERGE operation failed: {str(e)}. Falling back to APPEND mode.")
                iceberg_table.append(arrow_table)
                return arrow_table.num_rows
            
        else:
            raise ValueError(f"Unsupported load mode: {mode}")
    
    def _get_load_metadata(self, iceberg_table: IcebergTable, arrow_table: pa.Table, duration: float) -> Dict[str, Any]:
        """Get metadata about the load operation"""
        metadata = {
            'operation': 'load_table',
            'rows_loaded': arrow_table.num_rows,
            'columns': len(arrow_table.schema),
            'duration_seconds': duration,
            'namespace': self.storage_config.namespace,
            'partition_columns': [],
        }
        
        if self.enable_statistics:
            try:
                table_scan = iceberg_table.scan()
                metadata.update({
                    'table_size_bytes': sum(f.file_size_in_bytes for f in iceberg_table.current_snapshot().data_manifests) if iceberg_table.current_snapshot() else 0,
                    'snapshot_id': iceberg_table.current_snapshot().snapshot_id if iceberg_table.current_snapshot() else None,
                })
            except Exception as e:
                self.logger.debug(f"Failed to get table statistics: {str(e)}")
        
        return metadata

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get comprehensive table information including schema, files, and metadata"""
        try:
            # Load the table
            table_identifier = f"{self.storage_config.namespace}.{table_name}"
            try:
                iceberg_table = self._catalog.load_table(table_identifier)
            except (NoSuchTableError, NoSuchIcebergTableError):
                return {
                    'exists': False,
                    'error': f'Table {table_name} not found'
                }
            
            # Get basic table info
            info = {
                'exists': True,
                'table_name': table_name,
                'namespace': self.storage_config.namespace,
                'columns': [],
                'partition_columns': [],
                'num_files': 0,
                'size_bytes': 0,
                'snapshot_id': None,
                'schema': None
            }
            
            # Cache schema and spec to avoid redundant calls
            schema = iceberg_table.schema()
            spec = iceberg_table.spec()
            
            # Get schema information
            if schema:
                arrow_schema = schema.as_arrow()
                info['schema'] = arrow_schema
                info['columns'] = [field.name for field in arrow_schema]
            
            # Get partition information
            if spec:
                partition_fields = []
                for partition_field in spec.fields:
                    # Map source_id to actual column name using schema
                    try:
                        source_field = schema.find_field(partition_field.source_id)
                        partition_fields.append(source_field.name)
                    except Exception as e:
                        self.logger.warning(f"Could not find source field for partition {partition_field.name}: {e}")
                        # Skip this partition field if we can't resolve it
                        continue
                info['partition_columns'] = partition_fields
            
            # Get snapshot information
            current_snapshot = iceberg_table.current_snapshot()
            if current_snapshot:
                info['snapshot_id'] = current_snapshot.snapshot_id
                
                # Get file count and size if statistics are enabled
                if self.enable_statistics:
                    try:
                        manifests = current_snapshot.data_manifests if hasattr(current_snapshot, 'data_manifests') else []
                        total_files = 0
                        total_size = 0
                        
                        for manifest in manifests:
                            if hasattr(manifest, 'added_files_count'):
                                total_files += manifest.added_files_count or 0
                            if hasattr(manifest, 'file_size_in_bytes'):
                                total_size += manifest.file_size_in_bytes or 0
                        
                        info['num_files'] = total_files
                        info['size_bytes'] = total_size
                        
                    except Exception as e:
                        self.logger.debug(f"Could not get file statistics: {e}")
                        info['num_files'] = 0
                        info['size_bytes'] = 0
            
            return info
            
        except Exception as e:
            self.logger.error(f"Failed to get table info for {table_name}: {e}")
            return {
                'exists': False,
                'error': str(e),
                'table_name': table_name
            }