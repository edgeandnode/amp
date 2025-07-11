# nozzle/loaders/implementations/redis_loader.py

import hashlib
import json
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

import pyarrow as pa
import redis

from ..base import DataLoader, LoadMode, LoadResult


class RedisDataStructure(Enum):
    """Redis data structure types with performance characteristics"""

    HASH = 'hash'  # Best for: key-value access, field queries, general purpose
    STRING = 'string'  # Best for: JSON storage, full-text search, simple caching
    STREAM = 'stream'  # Best for: time-series data, event logging, pub/sub
    SET = 'set'  # Best for: unique values, membership testing, intersections
    SORTED_SET = 'sorted_set'  # Best for: ranked data, leaderboards, range queries
    LIST = 'list'  # Best for: ordered data, queues, timeline data
    JSON = 'json'  # Best for: complex nested data (requires RedisJSON module)


@dataclass
class RedisConnectionConfig:
    """Configuration for Redis connection with sensible defaults"""

    host: str = 'localhost'
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    socket_connect_timeout: int = 5
    socket_timeout: int = 5
    max_connections: int = 20
    decode_responses: bool = False  # Keep as bytes for binary data
    connection_params: Dict[str, Any] = None

    def __post_init__(self):
        if self.connection_params is None:
            self.connection_params = {}


class RedisLoader(DataLoader):
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

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

        # Core Redis configuration
        self.redis_client = None
        self.connection_pool = None

        # Data structure configuration
        self.data_structure = RedisDataStructure(config.get('data_structure', 'hash'))
        self.key_pattern = config.get('key_pattern', '{table}:{id}')
        self.ttl = config.get('ttl', None)

        # Performance configuration
        self.batch_size = config.get('batch_size', 1000)
        self.pipeline_size = config.get('pipeline_size', 1000)

        # Data structure specific configuration
        self.score_field = config.get('score_field', None)  # For sorted sets
        self.unique_field = config.get('unique_field', None)  # For sets

        # Connection configuration
        self.conn_config = RedisConnectionConfig(host=config.get('host', 'localhost'), port=config.get('port', 6379), db=config.get('db', 0), password=config.get('password'), socket_connect_timeout=config.get('connect_timeout', 5), socket_timeout=config.get('socket_timeout', 5), max_connections=config.get('max_connections', 20), decode_responses=config.get('decode_responses', False), connection_params=config.get('connection_params', {}))

    def connect(self) -> None:
        """Establish optimized connection to Redis with pooling"""
        try:
            # Create connection pool for efficient reuse
            self.connection_pool = redis.ConnectionPool(host=self.conn_config.host, port=self.conn_config.port, db=self.conn_config.db, password=self.conn_config.password, decode_responses=self.conn_config.decode_responses, socket_connect_timeout=self.conn_config.socket_connect_timeout, socket_timeout=self.conn_config.socket_timeout, max_connections=self.conn_config.max_connections, **self.conn_config.connection_params)

            # Create Redis client with connection pool
            self.redis_client = redis.Redis(connection_pool=self.connection_pool)

            # Test connection and get server info
            info = self.redis_client.info()
            self.logger.info(f'Connected to Redis {info["redis_version"]} at {self.conn_config.host}:{self.conn_config.port}')
            self.logger.info(f'Memory usage: {info.get("used_memory_human", "unknown")}')
            self.logger.info(f'Connected clients: {info.get("connected_clients", "unknown")}')
            self.logger.info(f'Data structure: {self.data_structure.value}')

            # Log data structure specific optimizations
            if self.data_structure == RedisDataStructure.HASH:
                self.logger.info('Optimized for key-value access and field queries')
            elif self.data_structure == RedisDataStructure.STREAM:
                self.logger.info('Optimized for time-series and event data')
            elif self.data_structure == RedisDataStructure.SORTED_SET:
                self.logger.info(f'Optimized for ranked data (score field: {self.score_field})')

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

    def load_batch(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> LoadResult:
        """Load Arrow RecordBatch using zero-copy operations where possible"""
        start_time = time.time()

        try:
            # Handle load modes
            mode = kwargs.get('mode', LoadMode.APPEND)
            if mode == LoadMode.OVERWRITE:
                self._clear_data(table_name)

            # Use optimized loading method based on data structure
            rows_loaded = self._load_batch_optimized(batch, table_name)

            duration = time.time() - start_time

            return LoadResult(rows_loaded=rows_loaded, duration=duration, table_name=table_name, loader_type='redis', success=True, metadata={'batch_size': batch.num_rows, 'data_structure': self.data_structure.value, 'schema_fields': len(batch.schema), 'key_pattern': self.key_pattern, 'ttl': self.ttl, 'ops_per_second': round(batch.num_rows / duration, 2) if duration > 0 else 0})

        except Exception as e:
            self.logger.error(f'Failed to load batch: {str(e)}')
            return LoadResult(rows_loaded=0, duration=time.time() - start_time, table_name=table_name, loader_type='redis', success=False, error=str(e))

    def load_table(self, table: pa.Table, table_name: str, **kwargs) -> LoadResult:
        """Load complete Arrow Table with optimized batching"""
        start_time = time.time()

        try:
            # Handle load modes
            mode = kwargs.get('mode', LoadMode.APPEND)
            if mode == LoadMode.OVERWRITE:
                self._clear_data(table_name)

            # Process table in optimized batches
            batch_size = kwargs.get('batch_size', self.batch_size)
            total_rows = 0
            batch_count = 0

            for batch in table.to_batches(max_chunksize=batch_size):
                rows_loaded = self._load_batch_optimized(batch, table_name)
                total_rows += rows_loaded
                batch_count += 1

            duration = time.time() - start_time

            return LoadResult(rows_loaded=total_rows, duration=duration, table_name=table_name, loader_type='redis', success=True, metadata={'total_rows': table.num_rows, 'data_structure': self.data_structure.value, 'schema_fields': len(table.schema), 'batches_processed': batch_count, 'avg_batch_size': round(total_rows / batch_count, 2) if batch_count > 0 else 0, 'ops_per_second': round(total_rows / duration, 2) if duration > 0 else 0, 'table_size_mb': round(table.nbytes / 1024 / 1024, 2)})

        except Exception as e:
            self.logger.error(f'Failed to load table: {str(e)}')
            return LoadResult(rows_loaded=0, duration=time.time() - start_time, table_name=table_name, loader_type='redis', success=False, error=str(e))

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

                if self.ttl:
                    pipe.expire(key, self.ttl)
                    commands_in_pipe += 1

                # Execute pipeline when it gets large
                if commands_in_pipe >= self.pipeline_size:
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

                if self.ttl:
                    pipe.expire(key, self.ttl)
                    commands_in_pipe += 1

                # Execute pipeline when it gets large
                if commands_in_pipe >= self.pipeline_size:
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
                if commands_in_pipe >= self.pipeline_size:
                    pipe.execute()
                    pipe = self.redis_client.pipeline()
                    commands_in_pipe = 0

        # Set TTL on stream if specified
        if self.ttl and commands_in_pipe >= 0:
            pipe.expire(stream_key, self.ttl)
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
            if self.unique_field and self.unique_field in data_dict:
                # Use specific field for uniqueness
                value = data_dict[self.unique_field][i]
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
            if self.ttl:
                pipe.expire(set_key, self.ttl)
            pipe.execute()

        return num_rows

    def _load_as_sorted_set_optimized(self, data_dict: Dict[str, List], num_rows: int, table_name: str) -> int:
        """Optimized sorted set loading for ranked data"""

        zset_key = f'{table_name}:zset'
        pipe = self.redis_client.pipeline()
        mapping = {}

        for i in range(num_rows):
            # Determine score
            if self.score_field and self.score_field in data_dict:
                score_value = data_dict[self.score_field][i]
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
            if self.ttl:
                pipe.expire(zset_key, self.ttl)
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
            if self.ttl:
                pipe.expire(list_key, self.ttl)
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

                if self.ttl:
                    pipe.expire(key, self.ttl)
                    commands_in_pipe += 1

                # Execute pipeline when it gets large
                if commands_in_pipe >= self.pipeline_size:
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
            key = self.key_pattern

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
                pattern = self.key_pattern.replace('{table}', table_name)
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
            stats = {'table_name': table_name, 'data_structure': self.data_structure.value, 'key_pattern': self.key_pattern, 'ttl': self.ttl, 'connection_info': {'host': self.conn_config.host, 'port': self.conn_config.port, 'db': self.conn_config.db}}

            if self.data_structure in [RedisDataStructure.HASH, RedisDataStructure.STRING, RedisDataStructure.JSON]:
                # Key-based structures
                pattern = self.key_pattern.replace('{table}', table_name).replace('{id}', '*')
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

            return {'healthy': True, 'ping': ping_result, 'redis_version': info.get('redis_version'), 'used_memory_human': info.get('used_memory_human'), 'connected_clients': info.get('connected_clients'), 'total_commands_processed': info.get('total_commands_processed'), 'keyspace_hits': info.get('keyspace_hits'), 'keyspace_misses': info.get('keyspace_misses'), 'hit_rate': round(info.get('keyspace_hits', 0) / max(info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0), 1) * 100, 2), 'uptime_in_seconds': info.get('uptime_in_seconds'), 'test_operation': test_value == b'test_value' if not self.conn_config.decode_responses else test_value == 'test_value', 'max_memory': config_info.get('maxmemory', 'unlimited'), 'eviction_policy': config_info.get('maxmemory-policy', 'noeviction')}

        except Exception as e:
            return {'healthy': False, 'error': str(e)}


# Example usage and testing
def create_comprehensive_test_data() -> pa.Table:
    """Create comprehensive test data for all Redis data structures"""
    import random
    from datetime import datetime, timedelta

    import pandas as pd

    # Create 1000 rows of diverse data
    base_time = datetime.now()

    data = {
        'id': range(1000),
        'user_id': [f'user_{i % 100}' for i in range(1000)],  # 100 unique users
        'session_id': [f'session_{i % 200}' for i in range(1000)],  # 200 unique sessions
        'score': [random.randint(1, 1000) for _ in range(1000)],  # For sorted sets
        'timestamp': [(base_time + timedelta(seconds=i)).timestamp() for i in range(1000)],
        'event_type': [random.choice(['login', 'purchase', 'view', 'click', 'logout']) for _ in range(1000)],
        'amount': [round(random.uniform(10.0, 1000.0), 2) for _ in range(1000)],
        'category': [random.choice(['electronics', 'clothing', 'books', 'sports', 'home']) for _ in range(1000)],
        'active': [random.choice([True, False]) for _ in range(1000)],
        'metadata': [json.dumps({'key': f'value_{i}', 'index': i}) for i in range(1000)],
        'binary_data': [f'binary_data_{i}'.encode('utf-8') for i in range(1000)],
    }

    df = pd.DataFrame(data)
    return pa.Table.from_pandas(df)


def comprehensive_redis_test():
    """Comprehensive test of the optimized Redis loader"""
    import logging

    logging.basicConfig(level=logging.INFO)

    # Test configurations for different data structures
    test_configs = [{'name': 'Optimized Hash Storage', 'config': {'host': 'localhost', 'port': 6379, 'password': 'mypassword', 'db': 1, 'data_structure': 'hash', 'key_pattern': 'user:{user_id}:event:{id}', 'ttl': 3600, 'batch_size': 100, 'pipeline_size': 500}}, {'name': 'Optimized Stream Storage', 'config': {'host': 'localhost', 'port': 6379, 'password': 'mypassword', 'db': 2, 'data_structure': 'stream', 'ttl': 3600, 'batch_size': 100}}, {'name': 'Optimized Sorted Set Storage', 'config': {'host': 'localhost', 'port': 6379, 'password': 'mypassword', 'db': 3, 'data_structure': 'sorted_set', 'score_field': 'score', 'ttl': 3600, 'batch_size': 100}}]

    # Create comprehensive test data
    print('📊 Creating comprehensive test data...')
    test_table = create_comprehensive_test_data()
    print(f'Created table with {test_table.num_rows} rows and {len(test_table.schema)} columns')

    for test_config in test_configs:
        print(f'\n🧪 Testing {test_config["name"]}...')
        print('=' * 60)

        try:
            loader = RedisLoader(test_config['config'])

            with loader:
                # Health check
                health = loader.health_check()
                print(f'🏥 Health check: {"✅ Healthy" if health["healthy"] else "❌ Unhealthy"}')
                if health['healthy']:
                    print(f'   Redis version: {health["redis_version"]}')
                    print(f'   Memory usage: {health["used_memory_human"]}')
                    print(f'   Hit rate: {health["hit_rate"]}%')

                # Performance test
                print('⚡ Performance test...')
                start_time = time.time()
                result = loader.load_table(test_table, 'performance_test', mode=LoadMode.OVERWRITE)
                end_time = time.time()

                print(f'✅ {result}')
                print(f'   Operations per second: {result.metadata.get("ops_per_second", 0)}')
                print(f'   Average batch size: {result.metadata.get("avg_batch_size", 0)}')

                # Statistics
                stats = loader.get_comprehensive_stats('performance_test')
                print(f'📊 Statistics: {stats}')

        except Exception as e:
            print(f'❌ {test_config["name"]} failed: {e}')
            continue

    print('\n🎉 Comprehensive Redis loader testing completed!')


if __name__ == '__main__':
    comprehensive_redis_test()
