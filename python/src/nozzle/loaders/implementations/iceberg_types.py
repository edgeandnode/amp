# Type stubs and aliases for better IDE support with PyIceberg
"""
This module provides explicit type definitions for better IDE intellisense 
when working with PyIceberg types.
"""

from typing import Any, Optional, Protocol, runtime_checkable

import pyarrow as pa

try:
    from pyiceberg.catalog import Catalog as IcebergCatalog
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import Table as IcebergTable
    from pyiceberg.table import UpsertResult
except ImportError:
    # Fallback protocols when PyIceberg is not available
    
    @runtime_checkable
    class IcebergTable(Protocol):
        def append(self, df: pa.Table) -> None: ...
        def overwrite(self, df: pa.Table) -> None: ...
        def upsert(self, df: pa.Table) -> 'UpsertResult': ...
        def schema(self) -> 'IcebergSchema': ...
    
    @runtime_checkable 
    class IcebergCatalog(Protocol):
        def load_table(self, identifier: str) -> IcebergTable: ...
        def create_table(self, identifier: str, schema: Any, **kwargs: Any) -> IcebergTable: ...
        def create_namespace(self, namespace: str) -> None: ...
    
    @runtime_checkable
    class IcebergSchema(Protocol):
        def to_arrow(self) -> pa.Schema: ...
    
    @runtime_checkable
    class UpsertResult(Protocol):
        added_files: Optional[Any]
        deleted_files: Optional[Any]


# Export commonly used types
__all__ = [
    'IcebergTable',
    'IcebergCatalog', 
    'IcebergSchema',
    'UpsertResult'
]