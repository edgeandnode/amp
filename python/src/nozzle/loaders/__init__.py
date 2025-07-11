"""
Data Loader Framework for Flight SQL Client

This package provides a unified interface for loading Flight SQL query results
into various storage systems with zero-copy performance optimizations.

Usage:
    from client import Client

    client = Client("grpc://localhost:8080")

    # Chaining interface
    result = client.sql("SELECT * FROM events") \
        .load("postgresql", "events_copy", connection="my_pg")

    # Alternative consumption
    table = client.sql("SELECT * FROM events").to_arrow()

    for batch in client.sql("SELECT * FROM large_table").stream():
        # Process batch
        pass
"""

from .base import DataLoader, LoadConfig, LoadMode, LoadResult
from .registry import LoaderRegistry, create_loader, get_available_loaders, get_loader_class

# Trigger auto-discovery on import
LoaderRegistry._ensure_auto_discovery()

__all__ = ['DataLoader', 'LoadResult', 'LoadConfig', 'LoadMode', 'LoaderRegistry', 'get_loader_class', 'create_loader', 'get_available_loaders']

__version__ = '0.1.0'
