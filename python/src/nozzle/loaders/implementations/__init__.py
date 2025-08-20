# loaders/implementations/__init__.py
"""
Data loader implementations
"""

# Import all loader implementations for auto-discovery
try:
    from .postgresql_loader import PostgreSQLLoader
except ImportError:
    PostgreSQLLoader = None

try:
    from .redis_loader import RedisLoader
except ImportError:
    RedisLoader = None

try:
    from .deltalake_loader import DeltaLakeLoader
except ImportError:
    DeltaLakeLoader = None

try:
    from .iceberg_loader import IcebergLoader
except ImportError:
    IcebergLoader = None

try:
    from .snowflake_loader import SnowflakeLoader
except ImportError:
    SnowflakeLoader = None

try:
    from .lmdb_loader import LMDBLoader
except ImportError:
    LMDBLoader = None

# Add any other loaders here
# try:
#     from .snowflake_loader import SnowflakeLoader
# except ImportError:
#     SnowflakeLoader = None

__all__ = []

# Add available loaders to __all__
if PostgreSQLLoader:
    __all__.append('PostgreSQLLoader')
if RedisLoader:
    __all__.append('RedisLoader')
if DeltaLakeLoader:
    __all__.append('DeltaLakeLoader')
if IcebergLoader:
    __all__.append('IcebergLoader')
if SnowflakeLoader:
    __all__.append('SnowflakeLoader')
if LMDBLoader:
    __all__.append('LMDBLoader')
