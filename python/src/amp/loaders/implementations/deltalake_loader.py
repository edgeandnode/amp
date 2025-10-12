# src/amp/loaders/implementations/deltalake_loader.py

import os
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

import pyarrow as pa
from arro3 import compute

try:
    from deltalake import DeltaTable, write_deltalake
    from deltalake.exceptions import DeltaError, TableNotFoundError

    DELTALAKE_AVAILABLE = True
except ImportError:
    DELTALAKE_AVAILABLE = False

from ..base import DataLoader, LoadMode, LoadResult


class DeltaWriteMode(Enum):
    """Delta Lake write modes"""
    APPEND = 'append'
    OVERWRITE = 'overwrite'
    MERGE = 'merge'
    ERROR = 'error'
    IGNORE = 'ignore'


@dataclass
class DeltaStorageConfig:
    """Configuration for Delta Lake storage backend"""

    # Storage location
    table_path: str

    # Storage backend options (S3, Azure, GCS, local)
    storage_options: Dict[str, str] = field(default_factory=dict)

    # Partitioning configuration
    partition_by: Optional[List[str]] = None

    # Optimization settings
    optimize_after_write: bool = True
    vacuum_after_write: bool = False

    # Schema evolution settings
    schema_evolution: bool = True
    merge_schema: bool = True

    # Performance settings
    file_size_hint: Optional[int] = None  # Target file size in bytes
    max_rows_per_file: Optional[int] = None
    max_rows_per_group: Optional[int] = None


class DeltaLakeLoader(DataLoader):
    """
    High-performance Delta Lake loader with zero-copy Arrow integration.

    Features:
    - Direct Arrow Table → Delta Lake integration (zero-copy)
    - ACID transactions with automatic versioning
    - Schema evolution and automatic merging
    - Partition management and optimization
    - Multiple storage backends (S3, Azure, GCS, local)
    - Automatic optimization and maintenance
    - Comprehensive error handling
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

        if not DELTALAKE_AVAILABLE:
            raise ImportError("Delta Lake support requires 'deltalake' package. Install with: pip install deltalake")

        # Core configuration
        self.storage_config = DeltaStorageConfig(table_path=config['table_path'], storage_options=config.get('storage_options', {}), partition_by=config.get('partition_by'), optimize_after_write=config.get('optimize_after_write', True), vacuum_after_write=config.get('vacuum_after_write', False), schema_evolution=config.get('schema_evolution', True), merge_schema=config.get('merge_schema', True), file_size_hint=config.get('file_size_hint'), max_rows_per_file=config.get('max_rows_per_file'), max_rows_per_group=config.get('max_rows_per_group'))

        # Performance settings
        self.batch_size = config.get('batch_size', 10000)
        self.enable_statistics = config.get('enable_statistics', True)

        # Table instance
        self._delta_table = None
        self._table_exists = False

        # Storage backend detection
        self._detect_storage_backend()

    def _detect_storage_backend(self) -> None:
        """Detect and configure storage backend"""
        parsed_path = urlparse(self.storage_config.table_path)

        if parsed_path.scheme == 's3':
            self.storage_backend = 'S3'
            self.logger.info(f'Detected S3 storage backend: {self.storage_config.table_path}')

            # Set default S3 options if not provided
            if 'AWS_S3_ALLOW_UNSAFE_RENAME' not in self.storage_config.storage_options:
                self.storage_config.storage_options['AWS_S3_ALLOW_UNSAFE_RENAME'] = 'true'

        elif parsed_path.scheme in ['az', 'abfs', 'abfss']:
            self.storage_backend = 'Azure'
            self.logger.info(f'Detected Azure storage backend: {self.storage_config.table_path}')

        elif parsed_path.scheme in ['gs', 'gcs']:
            self.storage_backend = 'GCS'
            self.logger.info(f'Detected GCS storage backend: {self.storage_config.table_path}')

        elif parsed_path.scheme in ['', 'file']:
            self.storage_backend = 'Local'
            self.logger.info(f'Detected local storage backend: {self.storage_config.table_path}')

            # Ensure local directory exists
            Path(self.storage_config.table_path).parent.mkdir(parents=True, exist_ok=True)

        else:
            self.storage_backend = 'Unknown'
            self.logger.warning(f'Unknown storage backend: {parsed_path.scheme}')

    def connect(self) -> None:
        """Initialize Delta Lake connection and table"""
        try:
            # Check if table exists
            try:
                self._delta_table = DeltaTable(self.storage_config.table_path, storage_options=self.storage_config.storage_options)
                self._table_exists = True

                # Get table information
                table_info = self._get_table_info()
                self.logger.info('Connected to existing Delta table:')
                self.logger.info(f'  Version: {table_info.get("version", "unknown")}')
                self.logger.info(f'  Files: {table_info.get("num_files", "unknown")}')
                self.logger.info(f'  Size: {table_info.get("size_bytes", "unknown")} bytes')
                self.logger.info(f'  Partitions: {table_info.get("partition_columns", [])}')

            except (TableNotFoundError, DeltaError, OSError):
                self._table_exists = False
                self.logger.info(f'Table does not exist, will create on first write: {self.storage_config.table_path}')

            # Validate storage options
            self._validate_storage_options()

            self._is_connected = True
            self.logger.info('Delta Lake loader connected successfully')

        except Exception as e:
            self.logger.error(f'Failed to connect to Delta Lake: {str(e)}')
            raise

    def disconnect(self) -> None:
        """Clean up Delta Lake connection"""
        if self._delta_table:
            self._delta_table = None

        self._is_connected = False
        self.logger.info('Delta Lake loader disconnected')

    def load_batch(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> LoadResult:
        """Load Arrow RecordBatch to Delta Lake"""
        start_time = time.time()

        try:
            # Convert batch to table
            table = pa.Table.from_batches([batch])

            # Use the table loading method
            result = self.load_table(table, table_name, **kwargs)

            # Update metadata to reflect batch operation
            result.metadata.update({'operation': 'load_batch', 'batch_size': batch.num_rows, 'schema_fields': len(batch.schema)})

            return result

        except Exception as e:
            self.logger.error(f'Failed to load batch: {str(e)}')
            return LoadResult(rows_loaded=0, duration=time.time() - start_time, table_name=table_name, loader_type='delta_lake', success=False, error=str(e))

    def load_table(self, table: pa.Table, table_name: str, **kwargs) -> LoadResult:
        """Load Arrow Table to Delta Lake using zero-copy operations"""
        start_time = time.time()

        try:
            # Determine write mode
            mode = kwargs.get('mode', LoadMode.APPEND)
            delta_mode = self._convert_load_mode(mode)

            # Prepare write options
            write_options = self._prepare_write_options(kwargs)

            # Pre-write optimizations
            if self.storage_config.partition_by:
                self._validate_partition_columns(table, self.storage_config.partition_by)

            # Write to Delta Lake (zero-copy operation)
            self.logger.info(f'Writing {table.num_rows} rows to Delta Lake (mode: {delta_mode.value})')

            write_deltalake(
                table_or_uri=self.storage_config.table_path,
                data=table,  # Direct Arrow Table - zero-copy!
                mode=delta_mode.value,
                partition_by=self.storage_config.partition_by,
                schema_mode='merge' if self.storage_config.merge_schema else 'strict',
                storage_options=self.storage_config.storage_options,
                **write_options,
            )

            # Refresh table reference
            self._refresh_table_reference()

            # Post-write optimizations
            optimization_results = self._perform_post_write_optimizations(table.num_rows)

            duration = time.time() - start_time

            # Get updated table info
            table_info = self._get_table_info()

            return LoadResult(rows_loaded=table.num_rows, duration=duration, table_name=table_name, loader_type='delta_lake', success=True, metadata={'operation': 'load_table', 'write_mode': delta_mode.value, 'table_version': table_info.get('version', 0), 'total_files': table_info.get('num_files', 0), 'total_size_bytes': table_info.get('size_bytes', 0), 'partition_columns': self.storage_config.partition_by, 'schema_fields': len(table.schema), 'table_size_mb': round(table.nbytes / 1024 / 1024, 2), 'storage_backend': self.storage_backend, 'optimization_results': optimization_results, 'throughput_rows_per_sec': round(table.num_rows / duration, 2) if duration > 0 else 0})

        except Exception as e:
            self.logger.error(f'Failed to load table: {str(e)}')
            return LoadResult(rows_loaded=0, duration=time.time() - start_time, table_name=table_name, loader_type='delta_lake', success=False, error=str(e))

    def _convert_load_mode(self, mode: LoadMode) -> DeltaWriteMode:
        """Convert LoadMode to Delta Lake write mode"""
        mapping = {
            LoadMode.APPEND: DeltaWriteMode.APPEND,
            LoadMode.OVERWRITE: DeltaWriteMode.OVERWRITE,
            LoadMode.MERGE: DeltaWriteMode.MERGE,
            LoadMode.UPSERT: DeltaWriteMode.MERGE,
        }

        return mapping.get(mode, DeltaWriteMode.APPEND)

    def _prepare_write_options(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare write options for Delta Lake"""
        options = {}

        # File size optimization
        if self.storage_config.file_size_hint:
            options['file_size_hint'] = self.storage_config.file_size_hint

        if self.storage_config.max_rows_per_file:
            options['max_rows_per_file'] = self.storage_config.max_rows_per_file

        if self.storage_config.max_rows_per_group:
            options['max_rows_per_group'] = self.storage_config.max_rows_per_group

        # Custom options from kwargs
        for key in ['engine', 'writer_properties', 'large_dtypes']:
            if key in kwargs:
                options[key] = kwargs[key]

        return options

    def _validate_partition_columns(self, table: pa.Table, partition_columns: List[str]) -> None:
        """Validate that partition columns exist in the table"""
        table_columns = set(table.schema.names)
        missing_columns = set(partition_columns) - table_columns

        if missing_columns:
            raise ValueError(f'Partition columns {missing_columns} not found in table. Available columns: {table_columns}')

    def _refresh_table_reference(self) -> None:
        """Refresh the Delta table reference after write"""
        try:
            # Force refresh of the table reference
            self._delta_table = DeltaTable(self.storage_config.table_path, storage_options=self.storage_config.storage_options)
            self._table_exists = True

            # Verify the table is accessible
            try:
                # Try to get basic info to ensure table is valid
                version = self._delta_table.version()
                self.logger.debug(f'Table refreshed successfully, version: {version}')
            except Exception as e:
                self.logger.warning(f'Table refresh verification failed: {e}')

        except Exception as e:
            self.logger.error(f'Failed to refresh table reference: {e}')
            # Don't set _table_exists = False here as the table might still exist

    def _perform_post_write_optimizations(self, rows_written: int) -> Dict[str, Any]:
        """Perform post-write optimizations with robust API handling"""
        optimization_results = {}

        try:
            if self.storage_config.optimize_after_write and self._delta_table:
                self.logger.info('Running Delta Lake optimization...')

                # Optimize (compaction) - handle different API versions
                optimize_start = time.time()
                optimize_metrics = self._safe_optimize_table()
                optimize_duration = time.time() - optimize_start

                optimization_results['optimize'] = {'duration_seconds': round(optimize_duration, 2), 'metrics': optimize_metrics, 'status': 'completed' if optimize_metrics else 'no_return_value'}

                self.logger.info(f'Optimization completed in {optimize_duration:.2f}s')

            if self.storage_config.vacuum_after_write and self._delta_table:
                self.logger.info('Running Delta Lake vacuum...')

                # Vacuum (cleanup old files)
                vacuum_start = time.time()
                vacuum_metrics = self._safe_vacuum_table()
                vacuum_duration = time.time() - vacuum_start

                optimization_results['vacuum'] = {'duration_seconds': round(vacuum_duration, 2), 'files_deleted': len(vacuum_metrics) if vacuum_metrics else 0, 'status': 'completed'}

                self.logger.info(f'Vacuum completed in {vacuum_duration:.2f}s')

        except Exception as e:
            self.logger.warning(f'Post-write optimization failed: {e}')
            optimization_results['error'] = str(e)

        return optimization_results

    def _safe_optimize_table(self) -> Dict[str, Union[str, bool]]:
        """Safely optimize table handling different API versions"""
        if not self._delta_table:
            return {}

        try:
            optimize_attr = getattr(self._delta_table, 'optimize', None)

            if optimize_attr is None:
                self.logger.warning('No optimize attribute available')
                return {}

            # Try different API patterns in order of preference

            # Pattern 1: optimize.compact() (deltalake >= 0.10.0)
            if hasattr(optimize_attr, 'compact'):
                self.logger.debug('Using optimize.compact() API')
                result = optimize_attr.compact()
                return result if result else {'api': 'compact', 'status': 'completed'}

            # Pattern 2: optimize() method (deltalake < 0.10.0)
            elif callable(optimize_attr):
                self.logger.debug('Using optimize() API')
                result = optimize_attr()
                return result if result else {'api': 'optimize', 'status': 'completed'}

            # Pattern 3: optimize as object with other methods
            else:
                self.logger.debug('Optimize attribute is not callable, checking for other methods')

                # Check for other potential methods
                for method_name in ['run', 'execute', 'apply']:
                    if hasattr(optimize_attr, method_name):
                        method = getattr(optimize_attr, method_name)
                        if callable(method):
                            self.logger.debug(f'Using optimize.{method_name}() API')
                            result = method()
                            return result if result else {'api': method_name, 'status': 'completed'}

                # If no methods found, log available methods
                available_methods = [attr for attr in dir(optimize_attr) if not attr.startswith('_') and callable(getattr(optimize_attr, attr))]
                self.logger.warning(f'No recognized optimize methods. Available: {available_methods}')
                return {'api': 'unknown', 'status': 'no_method_found'}

        except Exception as e:
            self.logger.error(f'Optimize operation failed: {e}')
            return {'error': str(e), 'status': 'failed'}

    def _safe_vacuum_table(self, retention_hours: int = 168) -> List[str]:
        """Safely vacuum table handling different API versions"""
        if not self._delta_table:
            return []

        try:
            # Most vacuum APIs are fairly consistent
            vacuum_result = self._delta_table.vacuum(retention_hours=retention_hours)
            return vacuum_result if vacuum_result else []

        except Exception as e:
            self.logger.error(f'Vacuum operation failed: {e}')
            return []

    def _get_table_info(self) -> Dict[str, Union[int, List[str], Optional[Any], str]]:
        """Get comprehensive table information"""
        if not self._delta_table:
            return {'version': 0, 'num_files': 0, 'size_bytes': 0, 'partition_columns': []}

        try:
            version = self._delta_table.version()

            files = self._delta_table.file_uris()

            total_size = 0
            try:
                if hasattr(self._delta_table, 'get_add_actions'):
                    add_actions = self._delta_table.get_add_actions(flatten=True)
                    size_column = add_actions.column('size_bytes')

                    self.logger.info(f'size_column: {size_column}')
                    total_size = compute.sum(size_column).as_py()
                    self.logger.info(f'Calculated size using get_add_actions: {total_size} bytes')
                else:
                    if self.storage_backend == 'Local':
                        for file_path in files:
                            try:
                                full_path = Path(self.storage_config.table_path) / file_path
                                if full_path.exists():
                                    total_size += full_path.stat().st_size
                            except Exception:
                                pass
                        self.logger.debug(f'Calculated size from filesystem: {total_size} bytes')
                    else:
                        self.logger.debug(f'File size calculation not available for {self.storage_backend} storage')
            except Exception as e:
                self.logger.warning(f'Failed to calculate total file size: {e}')
                total_size = 0

            return {'version': version, 'num_files': len(files), 'size_bytes': total_size, 'partition_columns': self.storage_config.partition_by or [], 'schema': self._delta_table.schema() if hasattr(self._delta_table, 'schema') else None}

        except Exception as e:
            self.logger.warning(f'Failed to get table info: {e}')
            return {'version': 0, 'num_files': 0, 'size_bytes': 0, 'partition_columns': [], 'error': str(e)}

    def _validate_storage_options(self) -> None:
        """Validate storage options for the detected backend"""
        required_options = {'S3': ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'], 'Azure': ['AZURE_STORAGE_ACCOUNT_NAME', 'AZURE_STORAGE_ACCOUNT_KEY'], 'GCS': ['GOOGLE_SERVICE_ACCOUNT']}

        if self.storage_backend in required_options:
            missing_options = []
            for option in required_options[self.storage_backend]:
                if option not in self.storage_config.storage_options and option not in os.environ:
                    missing_options.append(option)

            if missing_options:
                self.logger.warning(f'Missing {self.storage_backend} credentials: {missing_options}. Ensure they are provided in storage_options or environment variables.')

    def get_table_schema(self) -> Optional[pa.Schema]:
        """Get the current table schema"""
        if not self._delta_table:
            self.logger.warning('No Delta table available for schema retrieval')
            return None

        try:
            delta_schema = self._delta_table.schema()

            if hasattr(delta_schema, 'to_pyarrow'):
                return delta_schema.to_pyarrow()
            else:
                # Fallback: try to get schema from Arrow table
                arrow_table = self._delta_table.to_pyarrow_table()
                return arrow_table.schema

        except Exception as e:
            self.logger.error(f'Failed to get table schema: {e}')

            # Final fallback: try to get schema from Arrow table directly
            try:
                arrow_table = self._delta_table.to_pyarrow_table()
                return arrow_table.schema
            except Exception as e2:
                self.logger.error(f'Failed to get schema from Arrow table: {e2}')
                return None

    def get_table_history(self, limit: int = 10) -> List[Dict[str, Union[int, str, Dict[str, Any], bool, None]]]:
        """Get table history/version information"""
        if not self._delta_table:
            return []

        try:
            history = self._delta_table.history(limit=limit)
            return [{'version': entry.get('version'), 'timestamp': entry.get('timestamp'), 'operation': entry.get('operation', 'WRITE'), 'operationParameters': entry.get('operationParameters', {}), 'readVersion': entry.get('readVersion'), 'isBlindAppend': entry.get('isBlindAppend')} for entry in history]
        except Exception as e:
            self.logger.error(f'Failed to get table history: {e}')
            return []

    def get_table_stats(self) -> Dict[str, Union[str, int, List, Dict[str, Any], bool]]:
        """Get comprehensive table statistics"""
        if not self._delta_table:
            return {'error': 'Table not connected'}

        try:
            stats = self._get_table_info()

            # Add additional statistics
            stats.update({'storage_backend': self.storage_backend, 'table_path': self.storage_config.table_path, 'partition_columns': self.storage_config.partition_by, 'optimization_settings': {'optimize_after_write': self.storage_config.optimize_after_write, 'vacuum_after_write': self.storage_config.vacuum_after_write, 'schema_evolution': self.storage_config.schema_evolution}})

            # Get recent history
            stats['recent_history'] = self.get_table_history(limit=5)

            return stats

        except Exception as e:
            self.logger.error(f'Failed to get table stats: {e}')
            return {'error': str(e)}

    def optimize_table(self) -> Dict[str, Union[bool, float, Dict[str, Any], str]]:
        """Manually optimize the table"""
        if not self._delta_table:
            raise RuntimeError('Table not connected')

        try:
            self.logger.info('Starting manual table optimization...')
            start_time = time.time()

            # Use the same safe optimization method
            optimize_metrics = self._safe_optimize_table()

            duration = time.time() - start_time

            result = {'success': True, 'duration_seconds': round(duration, 2), 'metrics': optimize_metrics}

            self.logger.info(f'Table optimization completed in {duration:.2f}s')
            return result

        except Exception as e:
            self.logger.error(f'Table optimization failed: {e}')
            return {'success': False, 'error': str(e)}

    def vacuum_table(self, retention_hours: int = 168) -> Dict[str, Union[bool, float, int, str]]:
        """Manually vacuum the table"""
        if not self._delta_table:
            raise RuntimeError('Table not connected')

        try:
            self.logger.info(f'Starting manual table vacuum (retention: {retention_hours}h)...')
            start_time = time.time()

            # Run vacuum
            vacuum_metrics = self._delta_table.vacuum(retention_hours=retention_hours)

            duration = time.time() - start_time

            result = {'success': True, 'duration_seconds': round(duration, 2), 'files_deleted': len(vacuum_metrics), 'retention_hours': retention_hours}

            self.logger.info(f'Table vacuum completed in {duration:.2f}s')
            return result

        except Exception as e:
            self.logger.error(f'Table vacuum failed: {e}')
            return {'success': False, 'error': str(e)}

    def query_table(self, columns: Optional[List[str]] = None, limit: Optional[int] = None) -> pa.Table:
        """Query the Delta table and return Arrow table"""
        if not self._delta_table:
            raise RuntimeError('Table not connected')

        try:
            table = self._delta_table.to_pyarrow_table()

            if columns:
                table = table.select(columns)

            # Apply limit if specified
            if limit and table.num_rows > limit:
                table = table.slice(0, limit)

            return table

        except Exception as e:
            self.logger.error(f'Query failed: {e}')
            raise
