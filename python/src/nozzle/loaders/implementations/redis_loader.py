# nozzle/loaders/implementations/redis_loader.py

import hashlib
import json
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

import pyarrow as pa
import redis

from ..base import DataLoader, LoadMode


class RedisDataStructure(Enum):
    """Redis data structure types for different use cases.
    
    - HASH: key-value access, field queries, general purpose
    - STRING: JSON storage, full-text search, simple caching
    - STREAM: time-series data, event logging, pub/sub
    - SET: unique values, membership testing, intersections
    - SORTED_SET: ranked data, leaderboards, range queries
    - LIST: ordered data, queues, timeline data
    - JSON: complex nested data (requires RedisJSON module)
    """
    HASH = 'hash'
    STRING = 'string'
    STREAM = 'stream'
    SET = 'set'
    SORTED_SET = 'sorted_set'
    LIST = 'list'
    JSON = 'json'


@dataclass
class RedisConfig:
    """Configuration for Redis loader"""
    
    # Connection settings
    host: str = 'localhost'
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    socket_connect_timeout: int = 5
    socket_timeout: int = 5
    max_connections: int = 20
    decode_responses: bool = False  # Keep as bytes for binary data
    connection_params: Dict[str, Any] = None
    
    # Data structure configuration
    data_structure: str = 'hash'
    key_pattern: str = '{table}:{id}'
    ttl: Optional[int] = None
    
    # Performance configuration
    batch_size: int = 1000
    pipeline_size: int = 1000
    
    # Data structure specific configuration
    score_field: Optional[str] = None  # For sorted sets
    unique_field: Optional[str] = None  # For sets

    def __post_init__(self):
        if self.connection_params is None:
            self.connection_params = {}


class RedisLoader(DataLoader[RedisConfig]):
    """
    Optimized Redis data loader combining zero-copy operations with flexible data structures.

    Features:
    - Multiple Redis data structures support
    - Zero-copy Arrow operations where possible
    - Efficient pipeline batching
    - Flexible key pattern generation
    - Comprehensive error handling
    - Connection pooling
    - Binary data support
    """

    # Declare loader capabilities
    SUPPORTED_MODES = {LoadMode.APPEND, LoadMode.OVERWRITE}
    REQUIRES_SCHEMA_MATCH = False
    SUPPORTS_TRANSACTIONS = False

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # Core Redis configuration
        self.redis_client = None
        self.connection_pool = None
        
        # Convert data_structure string to enum
        self.data_structure = RedisDataStructure(self.config.data_structure)
    
    
    def _get_required_config_fields(self) -> list[str]:
        """Return required configuration fields"""
        return ['host']  # Only host is truly required, others have defaults

    def connect(self) -> None:
        """Establish optimized connection to Redis with pooling"""
        try:
            # Create connection pool for efficient reuse
            self.connection_pool = redis.ConnectionPool(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                decode_responses=self.config.decode_responses,
                socket_connect_timeout=self.config.socket_connect_timeout,
                socket_timeout=self.config.socket_timeout,
                max_connections=self.config.max_connections,
                **self.config.connection_params
            )

            # Create Redis client with connection pool
            self.redis_client = redis.Redis(connection_pool=self.connection_pool)

            # Test connection and get server info
            info = self.redis_client.info()
            self.logger.info(f'Connected to Redis {info["redis_version"]} at {self.config.host}:{self.config.port}')
            self.logger.info(f'Memory usage: {info.get("used_memory_human", "unknown")}')
            self.logger.info(f'Connected clients: {info.get("connected_clients", "unknown")}')
            self.logger.info(f'Data structure: {self.data_structure.value}')

            # Log data structure specific optimizations
            if self.data_structure == RedisDataStructure.HASH:
                self.logger.info('Optimized for key-value access and field queries')
            elif self.data_structure == RedisDataStructure.STREAM:
                self.logger.info('Optimized for time-series and event data')
            elif self.data_structure == RedisDataStructure.SORTED_SET:
                self.logger.info(f'Optimized for ranked data (score field: {self.config.score_field})')

            self._is_connected = True

        except Exception as e:
            self.logger.error(f'Failed to connect to Redis: {str(e)}')
            raise

    def disconnect(self) -> None:
        """Clean up Redis connections"""
        if self.connection_pool:
            self.connection_pool.disconnect()
            self.connection_pool = None
        if self.redis_client:
            self.redis_client.close()
            self.redis_client = None
        self._is_connected = False
        self.logger.info('Disconnected from Redis')

    def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        """Implementation-specific batch loading logic"""
        return self._load_batch_optimized(batch, table_name)
    
    def _create_table_from_schema(self, schema: pa.Schema, table_name: str) -> None:
        """Create table from Arrow schema - Redis doesn't require explicit table creation"""
        # Redis is schemaless and creates keys/structures on demand
        # No action needed here as Redis will create the data structures
        # automatically when data is inserted
        self.logger.debug(f"Redis will create data structures for '{table_name}' on first write")
    
    def _clear_table(self, table_name: str) -> None:
        """Clear table for overwrite mode"""
        self._clear_data(table_name)

    def _load_batch_optimized(self, batch: pa.RecordBatch, table_name: str) -> int:
        """Optimized batch loading with zero-copy operations"""

        # Convert Arrow batch to Python dict for efficient access (zero-copy to native types)
        data_dict = batch.to_pydict()

        # Route to appropriate loading method
        if self.data_structure == RedisDataStructure.HASH:
            return self._load_as_hashes_optimized(data_dict, batch.num_rows, table_name)
        elif self.data_structure == RedisDataStructure.STRING:
            return self._load_as_strings_optimized(data_dict, batch.num_rows, table_name)
        elif self.data_structure == RedisDataStructure.STREAM:
            return self._load_as_stream_optimized(data_dict, batch.num_rows, table_name)
        elif self.data_structure == RedisDataStructure.SET:
            return self._load_as_set_optimized(data_dict, batch.num_rows, table_name)
        elif self.data_structure == RedisDataStructure.SORTED_SET:
            return self._load_as_sorted_set_optimized(data_dict, batch.num_rows, table_name)
        elif self.data_structure == RedisDataStructure.LIST:
            return self._load_as_list_optimized(data_dict, batch.num_rows, table_name)
        elif self.data_structure == RedisDataStructure.JSON:
            return self._load_as_json_optimized(data_dict, batch.num_rows, table_name)
        else:
            raise ValueError(f'Unsupported data structure: {self.data_structure}')

    def _load_as_hashes_optimized(self, data_dict: Dict[str, List], num_rows: int, table_name: str) -> int:
        """Optimized hash loading with efficient pipeline usage"""

        # Use pipeline for batch operations
        pipe = self.redis_client.pipeline()
        commands_in_pipe = 0

        for i in range(num_rows):
            # Generate key efficiently
            key = self._generate_key_optimized(data_dict, i, table_name)

            # Build hash mapping efficiently
            hash_data = {}
            for field_name, values in data_dict.items():
                value = values[i]

                # Skip null values
                if value is None:
                    continue

                # Handle different data types efficiently
                if isinstance(value, bytes):
                    hash_data[field_name] = value
                elif isinstance(value, (int, float)):
                    hash_data[field_name] = str(value).encode('utf-8')
                elif isinstance(value, bool):
                    hash_data[field_name] = b'1' if value else b'0'
                else:
                    hash_data[field_name] = str(value).encode('utf-8')

            # Add to pipeline if we have data
            if hash_data:
                pipe.hset(key, mapping=hash_data)
                commands_in_pipe += 1

                if self.config.ttl:
                    pipe.expire(key, self.config.ttl)
                    commands_in_pipe += 1

                # Execute pipeline when it gets large
                if commands_in_pipe >= self.config.pipeline_size:
                    pipe.execute()
                    pipe = self.redis_client.pipeline()
                    commands_in_pipe = 0

        # Execute remaining commands
        if commands_in_pipe > 0:
            pipe.execute()

        return num_rows

    def _load_as_strings_optimized(self, data_dict: Dict[str, List], num_rows: int, table_name: str) -> int:
        """Optimized string (JSON) loading"""

        pipe = self.redis_client.pipeline()
        commands_in_pipe = 0

        for i in range(num_rows):
            key = self._generate_key_optimized(data_dict, i, table_name)

            # Build row object efficiently
            row_data = {}
            for field_name, values in data_dict.items():
                value = values[i]
                if value is not None:
                    if isinstance(value, bytes):
                        row_data[field_name] = value.hex()
                    else:
                        row_data[field_name] = value

            # Convert to JSON and store
            if row_data:
                json_str = json.dumps(row_data, default=str)
                pipe.set(key, json_str)
                commands_in_pipe += 1

                if self.config.ttl:
                    pipe.expire(key, self.config.ttl)
                    commands_in_pipe += 1

                # Execute pipeline when it gets large
                if commands_in_pipe >= self.config.pipeline_size:
                    pipe.execute()
                    pipe = self.redis_client.pipeline()
                    commands_in_pipe = 0

        # Execute remaining commands
        if commands_in_pipe > 0:
            pipe.execute()

        return num_rows

    def _load_as_stream_optimized(self, data_dict: Dict[str, List], num_rows: int, table_name: str) -> int:
        """Optimized stream loading for time-series data"""

        stream_key = f'{table_name}:stream'
        pipe = self.redis_client.pipeline()
        commands_in_pipe = 0

        for i in range(num_rows):
            # Build stream entry efficiently
            entry_data = {}
            for field_name, values in data_dict.items():
                value = values[i]
                if value is not None:
                    if isinstance(value, bytes):
                        entry_data[field_name] = value.hex()
                    else:
                        entry_data[field_name] = str(value)

            # Add to stream
            if entry_data:
                pipe.xadd(stream_key, entry_data)
                commands_in_pipe += 1

                # Execute pipeline when it gets large
                if commands_in_pipe >= self.config.pipeline_size:
                    pipe.execute()
                    pipe = self.redis_client.pipeline()
                    commands_in_pipe = 0

        # Set TTL on stream if specified
        if self.config.ttl and commands_in_pipe >= 0:
            pipe.expire(stream_key, self.config.ttl)
            commands_in_pipe += 1

        # Execute remaining commands
        if commands_in_pipe > 0:
            pipe.execute()

        return num_rows

    def _load_as_set_optimized(self, data_dict: Dict[str, List], num_rows: int, table_name: str) -> int:
        """Optimized set loading for unique values"""

        set_key = f'{table_name}:set'
        pipe = self.redis_client.pipeline()
        members = []

        for i in range(num_rows):
            # Build unique member representation
            if self.config.unique_field and self.config.unique_field in data_dict:
                # Use specific field for uniqueness
                value = data_dict[self.config.unique_field][i]
                if value is not None:
                    member = str(value)
                    members.append(member)
            else:
                # Use entire row as member
                row_data = {}
                for field_name, values in data_dict.items():
                    value = values[i]
                    if value is not None:
                        if isinstance(value, bytes):
                            row_data[field_name] = value.hex()
                        else:
                            row_data[field_name] = value

                if row_data:
                    member = json.dumps(row_data, sort_keys=True, default=str)
                    members.append(member)

        # Add all members to set in one operation
        if members:
            pipe.sadd(set_key, *members)
            if self.config.ttl:
                pipe.expire(set_key, self.config.ttl)
            pipe.execute()

        return num_rows

    def _load_as_sorted_set_optimized(self, data_dict: Dict[str, List], num_rows: int, table_name: str) -> int:
        """Optimized sorted set loading for ranked data"""

        zset_key = f'{table_name}:zset'
        pipe = self.redis_client.pipeline()
        mapping = {}

        for i in range(num_rows):
            # Determine score
            if self.config.score_field and self.config.score_field in data_dict:
                score_value = data_dict[self.config.score_field][i]
                score = float(score_value) if score_value is not None else float(i)
            else:
                score = float(i)

            # Build member representation
            row_data = {}
            for field_name, values in data_dict.items():
                value = values[i]
                if value is not None:
                    if isinstance(value, bytes):
                        row_data[field_name] = value.hex()
                    else:
                        row_data[field_name] = value

            if row_data:
                member = json.dumps(row_data, sort_keys=True, default=str)
                mapping[member] = score

        # Add all members to sorted set in one operation
        if mapping:
            pipe.zadd(zset_key, mapping)
            if self.config.ttl:
                pipe.expire(zset_key, self.config.ttl)
            pipe.execute()

        return num_rows

    def _load_as_list_optimized(self, data_dict: Dict[str, List], num_rows: int, table_name: str) -> int:
        """Optimized list loading for ordered data"""

        list_key = f'{table_name}:list'
        pipe = self.redis_client.pipeline()
        items = []

        for i in range(num_rows):
            # Build list item
            row_data = {}
            for field_name, values in data_dict.items():
                value = values[i]
                if value is not None:
                    if isinstance(value, bytes):
                        row_data[field_name] = value.hex()
                    else:
                        row_data[field_name] = value

            if row_data:
                item = json.dumps(row_data, default=str)
                items.append(item)

        # Add all items to list in one operation
        if items:
            pipe.rpush(list_key, *items)
            if self.config.ttl:
                pipe.expire(list_key, self.config.ttl)
            pipe.execute()

        return num_rows

    def _load_as_json_optimized(self, data_dict: Dict[str, List], num_rows: int, table_name: str) -> int:
        """Optimized JSON loading (requires RedisJSON module)"""

        pipe = self.redis_client.pipeline()
        commands_in_pipe = 0

        for i in range(num_rows):
            key = self._generate_key_optimized(data_dict, i, table_name)

            # Build JSON object
            json_data = {}
            for field_name, values in data_dict.items():
                value = values[i]
                if value is not None:
                    if isinstance(value, bytes):
                        json_data[field_name] = value.hex()
                    else:
                        json_data[field_name] = value

            # Store as JSON
            if json_data:
                try:
                    # Try RedisJSON first
                    pipe.json().set(key, '$', json_data)
                except AttributeError:
                    # Fall back to regular string storage
                    pipe.set(key, json.dumps(json_data, default=str))

                commands_in_pipe += 1

                if self.config.ttl:
                    pipe.expire(key, self.config.ttl)
                    commands_in_pipe += 1

                # Execute pipeline when it gets large
                if commands_in_pipe >= self.config.pipeline_size:
                    pipe.execute()
                    pipe = self.redis_client.pipeline()
                    commands_in_pipe = 0

        # Execute remaining commands
        if commands_in_pipe > 0:
            pipe.execute()

        return num_rows

    def _generate_key_optimized(self, data_dict: Dict[str, List], row_index: int, table_name: str) -> str:
        """Optimized key generation with better error handling"""

        try:
            key = self.config.key_pattern

            # Replace table placeholder
            key = key.replace('{table}', table_name)

            # Replace field placeholders
            for field_name, values in data_dict.items():
                placeholder = f'{{{field_name}}}'
                if placeholder in key:
                    value = values[row_index]
                    if value is not None:
                        if isinstance(value, bytes):
                            # Use hash for binary data to keep keys short
                            key = key.replace(placeholder, hashlib.md5(value).hexdigest()[:8])
                        else:
                            key = key.replace(placeholder, str(value))
                    else:
                        key = key.replace(placeholder, 'null')

            # Handle remaining {id} placeholder
            if '{id}' in key:
                if 'id' in data_dict:
                    id_value = data_dict['id'][row_index]
                    key = key.replace('{id}', str(id_value) if id_value is not None else str(row_index))
                else:
                    key = key.replace('{id}', str(row_index))

            return key

        except Exception as e:
            # Fallback to simple key generation
            self.logger.warning(f'Key generation failed, using fallback: {e}')
            return f'{table_name}:{row_index}'

    def _clear_data(self, table_name: str) -> None:
        """Optimized data clearing for overwrite mode"""

        try:
            if self.data_structure in [RedisDataStructure.HASH, RedisDataStructure.STRING, RedisDataStructure.JSON]:
                # For key-based structures, delete matching keys
                pattern = self.config.key_pattern.replace('{table}', table_name)
                # Replace common placeholders with wildcards
                for placeholder in ['{id}', '{block_num}', '{block_hash}', '{user_id}']:
                    pattern = pattern.replace(placeholder, '*')

                # Use SCAN for efficient key deletion
                keys_deleted = 0
                for key in self.redis_client.scan_iter(match=pattern, count=1000):
                    self.redis_client.delete(key)
                    keys_deleted += 1

                if keys_deleted > 0:
                    self.logger.info(f'Deleted {keys_deleted} existing keys')

            else:
                # For collection-based structures, delete the main key
                collection_key = f'{table_name}:{self.data_structure.value}'
                if self.redis_client.exists(collection_key):
                    self.redis_client.delete(collection_key)
                    self.logger.info(f'Deleted existing collection: {collection_key}')

        except Exception as e:
            self.logger.warning(f'Failed to clear existing data: {e}')

    def get_comprehensive_stats(self, table_name: str) -> Dict[str, Any]:
        """Get comprehensive statistics about stored data"""

        try:
            stats = {
                'table_name': table_name, 
                'data_structure': self.data_structure.value, 
                'key_pattern': self.config.key_pattern, 
                'ttl': self.config.ttl, 
                'connection_info': 
                    {
                        'host': self.config.host, 
                        'port': self.config.port, 
                        'db': self.config.db
                    }
            }

            if self.data_structure in [RedisDataStructure.HASH, RedisDataStructure.STRING, RedisDataStructure.JSON]:
                # Key-based structures
                pattern = self.config.key_pattern.replace('{table}', table_name).replace('{id}', '*')
                keys = list(self.redis_client.scan_iter(match=pattern, count=1000))
                stats['key_count'] = len(keys)

                if keys and self.data_structure == RedisDataStructure.HASH:
                    # Sample first hash for field info
                    sample_key = keys[0]
                    fields = self.redis_client.hkeys(sample_key)
                    stats['sample_fields'] = [f.decode('utf-8') if isinstance(f, bytes) else f for f in fields]

                # Calculate approximate memory usage
                if len(keys) > 0:
                    sample_keys = keys[: min(10, len(keys))]
                    total_memory = sum(self.redis_client.memory_usage(key) or 0 for key in sample_keys)
                    stats['estimated_memory_bytes'] = int(total_memory * len(keys) / len(sample_keys))
                    stats['estimated_memory_mb'] = round(stats['estimated_memory_bytes'] / 1024 / 1024, 2)

            elif self.data_structure == RedisDataStructure.STREAM:
                stream_key = f'{table_name}:stream'
                if self.redis_client.exists(stream_key):
                    info = self.redis_client.xinfo_stream(stream_key)
                    stats['stream_length'] = info['length']
                    stats['stream_groups'] = info['groups']
                    stats['memory_usage_bytes'] = self.redis_client.memory_usage(stream_key) or 0
                    stats['memory_usage_mb'] = round(stats['memory_usage_bytes'] / 1024 / 1024, 2)

            elif self.data_structure == RedisDataStructure.SET:
                set_key = f'{table_name}:set'
                if self.redis_client.exists(set_key):
                    stats['set_size'] = self.redis_client.scard(set_key)
                    stats['memory_usage_bytes'] = self.redis_client.memory_usage(set_key) or 0
                    stats['memory_usage_mb'] = round(stats['memory_usage_bytes'] / 1024 / 1024, 2)

            elif self.data_structure == RedisDataStructure.SORTED_SET:
                zset_key = f'{table_name}:zset'
                if self.redis_client.exists(zset_key):
                    stats['zset_size'] = self.redis_client.zcard(zset_key)
                    stats['memory_usage_bytes'] = self.redis_client.memory_usage(zset_key) or 0
                    stats['memory_usage_mb'] = round(stats['memory_usage_bytes'] / 1024 / 1024, 2)

                    # Get score range
                    if stats['zset_size'] > 0:
                        min_score = self.redis_client.zrange(zset_key, 0, 0, withscores=True)
                        max_score = self.redis_client.zrange(zset_key, -1, -1, withscores=True)
                        if min_score:
                            stats['min_score'] = min_score[0][1]
                        if max_score:
                            stats['max_score'] = max_score[0][1]

            elif self.data_structure == RedisDataStructure.LIST:
                list_key = f'{table_name}:list'
                if self.redis_client.exists(list_key):
                    stats['list_length'] = self.redis_client.llen(list_key)
                    stats['memory_usage_bytes'] = self.redis_client.memory_usage(list_key) or 0
                    stats['memory_usage_mb'] = round(stats['memory_usage_bytes'] / 1024 / 1024, 2)

            return stats

        except Exception as e:
            self.logger.error(f'Failed to get comprehensive stats: {e}')
            return {'error': str(e), 'table_name': table_name}

    def health_check(self) -> Dict[str, Any]:
        """Perform health check on Redis connection and configuration"""

        try:
            # Test basic connectivity
            ping_result = self.redis_client.ping()

            # Get server info
            info = self.redis_client.info()

            # Get configuration info
            config_info = self.redis_client.config_get('*')

            # Test a simple operation
            test_key = f'health_check:{int(time.time())}'
            self.redis_client.set(test_key, 'test_value', ex=5)
            test_value = self.redis_client.get(test_key)
            self.redis_client.delete(test_key)

            return {
                'healthy': True, 
                'ping': ping_result, 
                'redis_version': info.get('redis_version'), 
                'used_memory_human': info.get('used_memory_human'), 
                'connected_clients': info.get('connected_clients'), 
                'total_commands_processed': info.get('total_commands_processed'), 
                'keyspace_hits': info.get('keyspace_hits'), 
                'keyspace_misses': info.get('keyspace_misses'), 
                'hit_rate': round(info.get('keyspace_hits', 0) / max(info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0), 1) * 100, 2), 
                'uptime_in_seconds': info.get('uptime_in_seconds'), 
                'test_operation': test_value == b'test_value' if not self.config.decode_responses else test_value == 'test_value', 
                'max_memory': config_info.get('maxmemory', 'unlimited'), 
                'eviction_policy': config_info.get('maxmemory-policy', 'noeviction')
            }

        except Exception as e:
            return {'healthy': False, 'error': str(e)}
    
    def _get_loader_batch_metadata(self, batch: pa.RecordBatch, duration: float, **kwargs) -> Dict[str, Any]:
        """Get Redis-specific metadata for batch operation"""
        metadata = {
            'data_structure': self.data_structure.value
        }
        return metadata
    
    def _get_loader_table_metadata(self, table: pa.Table, duration: float, batch_count: int, **kwargs) -> Dict[str, Any]:
        """Get Redis-specific metadata for table operation"""
        metadata = {
            'data_structure': self.data_structure.value
        }
        return metadata
