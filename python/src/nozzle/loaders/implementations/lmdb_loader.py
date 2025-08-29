# nozzle/loaders/implementations/lmdb_loader.py

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import lmdb
import pyarrow as pa

from ..base import DataLoader, LoadMode


@dataclass
class LMDBConfig:
    """Configuration for LMDB loader with sensible defaults"""
    
    # Connection settings
    db_path: str = "./lmdb_data"
    map_size: int = 10 * 1024**3  # 10GB default, user can override
    
    # Database organization
    database_name: Optional[str] = None  # Sub-database within environment
    create_if_missing: bool = True
    
    # Key generation
    key_column: Optional[str] = None  # Column to use as key
    key_pattern: str = "{id}"  # Pattern like "{table}:{id}" or "{block}:{index}"
    composite_key_columns: Optional[List[str]] = None  # For multi-column keys
    
    # Serialization
    compression: Optional[str] = None  # zstd, lz4, snappy (future enhancement)
    
    # Performance tuning
    writemap: bool = True  # Direct memory writes
    sync: bool = True  # Sync to disk on commit
    readahead: bool = False  # Disable for random access
    transaction_size: int = 10000  # Number of puts per transaction
    max_dbs: int = 100  # Maximum number of named databases
    
    # Memory management
    max_readers: int = 126
    max_spare_txns: int = 1
    lock: bool = True


class LMDBLoader(DataLoader[LMDBConfig]):
    """
    High-performance LMDB data loader optimized for read-heavy workloads.
    
    Features:
    - Zero-copy Arrow serialization
    - Flexible key generation strategies
    - Multiple named databases support
    - Efficient transaction batching
    - Memory-mapped I/O for fast reads
    - Configurable database organization
    """
    
    # Declare loader capabilities
    SUPPORTED_MODES = {LoadMode.APPEND, LoadMode.OVERWRITE}
    REQUIRES_SCHEMA_MATCH = False
    SUPPORTS_TRANSACTIONS = True
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        self.env: Optional[lmdb.Environment] = None
        self.dbs: Dict[str, Any] = {}  # Cache opened databases
    
    
    def _get_required_config_fields(self) -> list[str]:
        """Return required configuration fields"""
        return []  # LMDB has sensible defaults for all fields
        
    def connect(self) -> None:
        """Open LMDB environment and prepare databases"""
        try:
            # Create directory if it doesn't exist
            if self.config.create_if_missing:
                Path(self.config.db_path).mkdir(parents=True, exist_ok=True)
            
            # Open LMDB environment
            self.env = lmdb.open(
                self.config.db_path,
                map_size=self.config.map_size,
                max_dbs=self.config.max_dbs,
                max_readers=self.config.max_readers,
                max_spare_txns=self.config.max_spare_txns,
                writemap=self.config.writemap,
                readahead=self.config.readahead,
                lock=self.config.lock,
                sync=self.config.sync,
                metasync=self.config.sync
            )
            
            # Get environment info
            stat = self.env.stat()
            info = self.env.info()
            
            self.logger.info(f"Connected to LMDB at {self.config.db_path}")
            self.logger.info(f"Map size: {info['map_size'] / 1024**3:.2f} GB")
            self.logger.info(f"Entries: {stat['entries']:,}")
            self.logger.info(f"Database size: {stat['psize'] * stat['leaf_pages'] / 1024**2:.2f} MB")
            
            self._is_connected = True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to LMDB: {str(e)}")
            raise
    
    def disconnect(self) -> None:
        """Close LMDB environment"""
        if self.env:
            self.env.close()
            self.env = None
            self.dbs.clear()
        self._is_connected = False
        self.logger.info("Disconnected from LMDB")
    
    def _get_or_create_db(self, name: Optional[str] = None) -> Any:
        """Get or create a named database"""
        if name is None:
            return None  # Use main database
        
        if name not in self.dbs:
            # Open named database
            self.dbs[name] = self.env.open_db(name.encode(), create=self.config.create_if_missing)
        
        return self.dbs[name]
    
    def _convert_to_bytes(self, value: Any) -> bytes:
        """Convert a value to bytes efficiently"""
        if isinstance(value, bytes):
            return value
        elif isinstance(value, (int, float)):
            return str(value).encode('utf-8')
        elif isinstance(value, str):
            return value.encode('utf-8')
        else:
            # For complex types, use string representation
            return str(value).encode('utf-8')
    
    def _generate_key(self, batch: pa.RecordBatch, row_idx: int, table_name: str) -> bytes:
        """Generate key for a single row using configured strategy, working directly with Arrow data"""
        # Single column key
        if self.config.key_column:
            col_idx = batch.schema.get_field_index(self.config.key_column)
            if col_idx == -1:
                raise ValueError(f"Key column '{self.config.key_column}' not found in data")
            
            key_value = batch.column(col_idx)[row_idx].as_py()
            return self._convert_to_bytes(key_value)
        
        # Composite key
        elif self.config.composite_key_columns:
            key_parts = []
            for col_name in self.config.composite_key_columns:
                col_idx = batch.schema.get_field_index(col_name)
                if col_idx == -1:
                    raise ValueError(f"Composite key column '{col_name}' not found in data")
                
                value = batch.column(col_idx)[row_idx].as_py()
                key_parts.append(str(value))
            
            return ":".join(key_parts).encode('utf-8')
        
        # Pattern-based key
        else:
            # Extract only the fields needed for the pattern
            row_dict = {'table': table_name, 'index': row_idx}
            
            # Try to extract fields mentioned in the pattern
            import re
            pattern_fields = re.findall(r'\{(\w+)\}', self.config.key_pattern)
            
            for field_name in pattern_fields:
                if field_name not in ['table', 'index']:
                    col_idx = batch.schema.get_field_index(field_name)
                    if col_idx != -1:
                        row_dict[field_name] = batch.column(col_idx)[row_idx].as_py()
            
            try:
                key_value = self.config.key_pattern.format(**row_dict)
                return key_value.encode('utf-8')
            except KeyError as e:
                # Fallback to hash-based key
                self.logger.warning(f"Key pattern failed: {e}. Using hash-based key.")
                # Create a hash from all values in the row
                row_data = []
                for i in range(len(batch.schema)):
                    row_data.append(str(batch.column(i)[row_idx].as_py()))
                data_hash = hashlib.md5(":".join(row_data).encode()).hexdigest()
                return f"{table_name}:{data_hash}".encode('utf-8')
    
    def _serialize_arrow_batch(self, batch: pa.RecordBatch) -> bytes:
        """
        Serialize Arrow RecordBatch to bytes using IPC format.
        
        NOTE: Arrow IPC has significant overhead for single-row batches (~3.5KB per row)
        due to schema metadata, headers, and alignment. For small datasets, this can
        result in 10-40x storage overhead vs raw data.
        
        Future optimizations to consider:
        - Batch multiple rows per LMDB entry to amortize IPC overhead
        - Use alternative serialization (msgpack/pickle) for individual rows
        - Store schema separately and use more compact data-only serialization
        - Hybrid approach with configurable batch sizes per entry
        """
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        return sink.getvalue().to_pybytes()
    
    def _clear_data(self, table_name: str) -> None:
        """Clear all data for a table (for OVERWRITE mode)"""
        try:
            db = self._get_or_create_db(self.config.database_name)
            
            # Clear all entries by iterating through and deleting
            with self.env.begin(write=True, db=db) as txn:
                cursor = txn.cursor()
                # Delete all key-value pairs
                if cursor.first():
                    while True:
                        if not cursor.delete():
                            break
                        if not cursor.next():
                            break
                            
                self.logger.info(f"Cleared all data for table '{table_name}'")
        except Exception as e:
            self.logger.error(f"Error in _clear_data: {e}")
            raise
    
    def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        """Implementation-specific batch loading logic for LMDB"""
        # Get or create database
        db = self._get_or_create_db(self.config.database_name)
        
        rows_loaded = 0
        errors = []
        
        # Process all rows in chunked transactions
        txn_size = self.config.transaction_size
        for txn_start in range(0, batch.num_rows, txn_size):
            txn_end = min(txn_start + txn_size, batch.num_rows)
            
            with self.env.begin(write=True, db=db) as txn:
                for row_idx in range(txn_start, txn_end):
                    try:
                        # Generate key directly from Arrow data
                        key = self._generate_key(batch, row_idx, table_name)
                        
                        # Slice single row from batch - no Python conversion!
                        row_batch = batch.slice(row_idx, 1)
                        
                        # Serialize the row batch
                        value = self._serialize_arrow_batch(row_batch)
                        
                        # Get mode from kwargs
                        mode = kwargs.get('mode', LoadMode.APPEND)
                        
                        # Write to LMDB
                        if not txn.put(key, value, overwrite=(mode != LoadMode.APPEND)):
                            if mode == LoadMode.APPEND:
                                errors.append(f"Key already exists: {key.decode('utf-8')}")
                        else:
                            rows_loaded += 1
                        
                    except Exception as e:
                        error_msg = f"Error processing row {row_idx}: {str(e)}"
                        self.logger.error(error_msg)
                        errors.append(error_msg)
                        if len(errors) > 100:  # Reasonable error limit
                            raise Exception(f"Too many errors ({len(errors)}), aborting") from e
        
        if errors:
            # Store errors for metadata collection
            self._last_batch_errors = errors
            self.logger.warning(f"Completed with {len(errors)} errors")
            
            # If no rows were successfully loaded and we have errors, this is a failure
            if rows_loaded == 0:
                error_summary = errors[:5]  # Show first 5 errors
                if len(errors) > 5:
                    error_summary.append(f"... and {len(errors) - 5} more errors")
                raise Exception(f"Failed to load any rows. Errors: {'; '.join(error_summary)}")
        
        return rows_loaded
    
    def _create_table_from_schema(self, schema: pa.Schema, table_name: str) -> None:
        """Create table from Arrow schema - LMDB doesn't require explicit table creation"""
        # LMDB is schemaless, so we don't need to create tables explicitly
        self.logger.info(f"LMDB is schemaless, no table creation needed for '{table_name}'")
    
    def _clear_table(self, table_name: str) -> None:
        """Clear table for overwrite mode"""
        self._clear_data(table_name)
    
    def _get_loader_batch_metadata(self, batch: pa.RecordBatch, duration: float, **kwargs) -> Dict[str, Any]:
        """Get LMDB-specific metadata for batch operation"""
        stat = self.env.stat()
        metadata = {
            'database': self.config.database_name,
            'total_entries': stat['entries'],
            'db_size_mb': stat['psize'] * stat['leaf_pages'] / 1024**2,
            'transaction_size': self.config.transaction_size
        }
        
        # Add error information if available
        if hasattr(self, '_last_batch_errors'):
            metadata['errors_count'] = len(self._last_batch_errors)
            delattr(self, '_last_batch_errors')
            
        return metadata
    
    def _get_loader_table_metadata(self, table: pa.Table, duration: float, batch_count: int, **kwargs) -> Dict[str, Any]:
        """Get LMDB-specific metadata for table operation"""
        stat = self.env.stat()
        return {
            'database': self.config.database_name,
            'total_entries': stat['entries'],
            'db_size_mb': stat['psize'] * stat['leaf_pages'] / 1024**2,
            'table_size_mb': round(table.nbytes / 1024 / 1024, 2),
            'transaction_size': self.config.transaction_size
        }

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get information about LMDB database"""
        try:
            stat = self.env.stat()
            info = self.env.info()
            
            return {
                'table_name': table_name,
                'database': self.config.database_name,
                'total_entries': stat['entries'],
                'db_size_bytes': stat['psize'] * stat['leaf_pages'],
                'db_size_mb': stat['psize'] * stat['leaf_pages'] / 1024**2,
                'map_size_gb': info['map_size'] / 1024**3,
                'last_page': stat['ms_last_pgno'],
                'depth': stat['ms_depth']
            }
        except Exception as e:
            self.logger.error(f"Failed to get table info: {e}")
            return None