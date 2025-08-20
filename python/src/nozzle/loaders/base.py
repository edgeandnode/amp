"""
Enhanced base class for data loaders with common functionality extracted from implementations.
"""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from logging import Logger
from typing import Any, Dict, Generic, Iterator, Optional, Set, TypeVar

import pyarrow as pa


class LoadMode(Enum):
    APPEND = 'append'
    OVERWRITE = 'overwrite'
    UPSERT = 'upsert'
    MERGE = 'merge'


@dataclass
class LoadResult:
    """Result of a data loading operation"""

    rows_loaded: int
    duration: float
    ops_per_second: float
    table_name: str
    loader_type: str
    success: bool
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        if self.success:
            return f'✅ Loaded {self.rows_loaded} rows to {self.table_name} in {self.duration:.2f}s'
        else:
            return f'❌ Failed to load to {self.table_name}: {self.error}'


@dataclass
class LoadConfig:
    """Configuration for data loading operations"""

    batch_size: int = 10000
    mode: LoadMode = LoadMode.APPEND
    create_table: bool = True
    schema_evolution: bool = False
    max_retries: int = 3
    retry_delay: float = 1.0


# Type variable for configuration classes
TConfig = TypeVar('TConfig')


class DataLoader(ABC, Generic[TConfig]):
    """
    Enhanced abstract base class for all data loaders.
    
    Improvements:
    - Generic configuration type support
    - Built-in table tracking
    - Common batch processing logic
    - Standardized error handling
    - Connection state management
    - Schema conversion utilities
    """

    # Class-level attributes for loader metadata
    SUPPORTED_MODES: Set[LoadMode] = {LoadMode.APPEND, LoadMode.OVERWRITE}
    REQUIRES_SCHEMA_MATCH: bool = True
    SUPPORTS_TRANSACTIONS: bool = False
    
    def __init__(self, config: Dict[str, Any]) -> None:

        self.logger: Logger = logging.getLogger(f'{self.__class__.__name__}')
        self._connection: Optional[Any] = None
        self._is_connected: bool = False
        self._created_tables: Set[str] = set()  # Track created tables
        
        # Parse configuration into typed format
        self.config: TConfig = self._parse_config(config)
        
        # Validate configuration
        self._validate_config()

    @property
    def is_connected(self) -> bool:
        """Check if the loader is connected to the target system."""
        return self._is_connected

    def _parse_config(self, config: Dict[str, Any]) -> TConfig:
        """
        Parse configuration into loader-specific format.
        Generic implementation that works with dataclass configs.
        Override only if you need custom parsing logic.
        """
        # For loaders that don't use typed configs, just return the dict
        if not hasattr(self, '__orig_bases__'):
            return config  # type: ignore
            
        # Get the actual config type from the generic parameter
        for base in self.__orig_bases__:
            if hasattr(base, '__args__') and base.__args__:
                config_type = base.__args__[0]
                # Check if it's a real type (not TypeVar)
                if hasattr(config_type, '__name__'):
                    try:
                        return config_type(**config)
                    except TypeError as e:
                        raise ValueError(f"Invalid {self.__class__.__name__} configuration: {e}")
        
        # Fallback for non-generic loaders
        return config  # type: ignore

    def _validate_config(self) -> None:
        """
        Validate configuration parameters.
        Override to add loader-specific validation.
        """
        required_fields = self._get_required_config_fields()
        
        # Handle both dict and dataclass config objects
        if is_dataclass(self.config):            
            config_field_names = {f.name for f in fields(self.config)}
            missing_fields = [field for field in required_fields if field not in config_field_names]
        else:  # dict
            missing_fields = [field for field in required_fields if field not in self.config]
        
        if missing_fields:
            raise ValueError(f"Missing required configuration fields: {missing_fields}")

    def _get_required_config_fields(self) -> list[str]:
        """
        Return list of required configuration fields.
        Override in subclasses.
        """
        return []

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the target system"""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the target system"""
        pass

    def close(self) -> None:
        """Alias for disconnect() for backward compatibility"""
        self.disconnect()

    @abstractmethod
    def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        """
        Implementation-specific batch loading logic.
        Returns number of rows loaded.
        """
        pass

    def load_batch(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> LoadResult:
        """Load a single Arrow RecordBatch with common error handling and timing"""
        start_time = time.time()
        
        try:
            # Ensure connection
            if not self._is_connected:
                self.connect()
            
            # Validate mode
            mode = kwargs.get('mode', LoadMode.APPEND)
            if mode not in self.SUPPORTED_MODES:
                raise ValueError(f"Unsupported mode {mode}. Supported modes: {self.SUPPORTED_MODES}")
            
            # Handle table creation
            if kwargs.get('create_table', True) and table_name not in self._created_tables:
                if hasattr(self, '_create_table_from_schema'):
                    self._create_table_from_schema(batch.schema, table_name)
                    self._created_tables.add(table_name)
                else:
                    self.logger.warning(f'Table {table_name} not created automatically. '
                                        f'This loader does not yet implement "_create_table_from_schema". '
                                        f'Please create any tables needed before running the loader. ')
            
            # Handle overwrite mode
            if mode == LoadMode.OVERWRITE and hasattr(self, '_clear_table'):
                self._clear_table(table_name)
            
            # Perform the actual load
            rows_loaded = self._load_batch_impl(batch, table_name, **kwargs)
            
            duration = time.time() - start_time
            
            return LoadResult(
                rows_loaded=rows_loaded,
                duration=duration,
                ops_per_second=round(rows_loaded / duration, 2),
                table_name=table_name,
                loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                success=True,
                metadata=self._get_batch_metadata(batch, duration, **kwargs)
            )
            
        except Exception as e:
            self.logger.error(f'Failed to load batch: {str(e)}')
            return LoadResult(
                rows_loaded=0,
                duration=time.time() - start_time,
                ops_per_second=0,
                table_name=table_name,
                loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                success=False,
                error=str(e)
            )

    def load_table(self, table: pa.Table, table_name: str, **kwargs) -> LoadResult:
        """Load a complete Arrow Table with automatic batching"""
        start_time = time.time()
        batch_size = kwargs.get('batch_size', getattr(self, 'batch_size', 10000))
        
        rows_loaded = 0
        batch_count = 0
        errors = []
        
        try:
            # Process table in batches
            for batch in table.to_batches(max_chunksize=batch_size):
                result = self.load_batch(batch, table_name, **kwargs)
                
                if result.success:
                    rows_loaded += result.rows_loaded
                    batch_count += 1
                else:
                    errors.append(result.error)
                    if len(errors) > 5:  # Stop after too many errors
                        break
            
            duration = time.time() - start_time
            
            return LoadResult(
                rows_loaded=rows_loaded,
                duration=duration,
                ops_per_second=round(rows_loaded / duration, 2),
                table_name=table_name,
                loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                success=len(errors) == 0,
                error="; ".join(errors[:3]) if errors else None,
                metadata=self._get_table_metadata(table, duration, batch_count, **kwargs)
            )
            
        except Exception as e:
            self.logger.error(f'Failed to load table: {str(e)}')
            return LoadResult(
                rows_loaded=rows_loaded,
                duration=time.time() - start_time,
                ops_per_second=round(rows_loaded / duration, 2),
                table_name=table_name,
                loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                success=False,
                error=str(e)
            )

    def load_stream(self, batch_iterator: Iterator[pa.RecordBatch], table_name: str, **kwargs) -> Iterator[LoadResult]:
        """Load data from a stream of batches"""
        if not self._is_connected:
            self.connect()

        rows_loaded = 0
        start_time = time.time()
        batch_count = 0

        try:
            for batch in batch_iterator:
                batch_count += 1
                result = self.load_batch(batch, table_name, **kwargs)

                if result.success:
                    rows_loaded += result.rows_loaded
                    self.logger.info(f'Loaded batch {batch_count}: {result.rows_loaded} rows in {result.duration:.2f}s')
                else:
                    self.logger.error(f'Failed to load batch {batch_count}: {result.error}')

                yield result

        except Exception as e:
            self.logger.error(f'Stream loading failed after {batch_count} batches: {str(e)}')
            duration = time.time() - start_time
            yield LoadResult(
                rows_loaded=rows_loaded,
                duration=duration,
                ops_per_second=round(rows_loaded / duration, 2),
                table_name=table_name, 
                loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                success=False, 
                error=str(e), 
                metadata={'batches_processed': batch_count}
            )

    def _get_batch_metadata(self, batch: pa.RecordBatch, duration: float, **kwargs) -> Dict[str, Any]:
        """Get standard metadata for batch operations"""
        metadata = {
            'operation': 'load_batch',  # Required by tests
            'batch_size': batch.num_rows,
            'schema_fields': len(batch.schema),
            'throughput_rows_per_sec': round(batch.num_rows / duration, 2) if duration > 0 else 0
        }
        
        # Add any kwargs that might be useful as metadata
        for key in ['mode', 'batch_size', 'table_name']:
            if key in kwargs:
                metadata[key] = kwargs[key]
        
        # Allow loaders to add their specific metadata
        loader_metadata = self._get_loader_batch_metadata(batch, duration, **kwargs)
        metadata.update(loader_metadata)
                
        return metadata

    def _get_table_metadata(self, table: pa.Table, duration: float, batch_count: int, **kwargs) -> Dict[str, Any]:
        """Get standard metadata for table operations"""
        metadata = {
            'operation': 'load_table',
            'batches_processed': batch_count,
            'rows_loaded': table.num_rows,
            'columns': len(table.schema),
            'avg_batch_size': round(table.num_rows / batch_count, 2) if batch_count > 0 else 0,
            'table_size_bytes': table.nbytes,
            'table_size_mb': round(table.nbytes / 1024 / 1024, 2),
            'throughput_rows_per_sec': round(table.num_rows / duration, 2) if duration > 0 else 0
        }
        
        # Allow loaders to add their specific metadata
        loader_metadata = self._get_loader_table_metadata(table, duration, batch_count, **kwargs)
        metadata.update(loader_metadata)
        
        return metadata
    
    def _get_loader_batch_metadata(self, batch: pa.RecordBatch, duration: float, **kwargs) -> Dict[str, Any]:
        """Override in subclasses to add loader-specific batch metadata"""
        return {}
    
    def _get_loader_table_metadata(self, table: pa.Table, duration: float, batch_count: int, **kwargs) -> Dict[str, Any]:
        """Override in subclasses to add loader-specific table metadata"""
        return {}

    def __enter__(self) -> 'DataLoader':
        self.connect()
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[Any]) -> None:
        self.disconnect()

    # Optional methods that subclasses can implement
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a table in the target system"""
        raise NotImplementedError(f"{self.__class__.__name__} does not implement get_table_info()")

    def get_table_schema(self, table_name: str) -> Optional[pa.Schema]:
        """Get the schema of an existing table"""
        raise NotImplementedError(f"{self.__class__.__name__} does not implement get_table_schema()")

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the target system"""
        try:
            info = self.get_table_info(table_name)
            return info is not None
        except NotImplementedError:
            return table_name in self._created_tables

    def health_check(self) -> Dict[str, Any]:
        """Perform a health check on the connection"""
        return {
            'healthy': self._is_connected,
            'loader_type': self.__class__.__name__
        }
