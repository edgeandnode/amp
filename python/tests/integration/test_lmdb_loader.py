# tests/integration/test_lmdb_loader.py
"""
Integration tests for LMDB loader implementation.
These tests use a local LMDB instance.
"""

import os
import shutil
import time
import tempfile
from datetime import datetime

import pyarrow as pa
import pytest

try:
    import lmdb
    from src.nozzle.loaders.base import LoadMode
    from src.nozzle.loaders.implementations.lmdb_loader import LMDBLoader
except ImportError:
    pytest.skip('LMDB or nozzle modules not available', allow_module_level=True)


@pytest.fixture
def lmdb_test_dir():
    """Create and cleanup temporary directory for LMDB databases"""
    temp_dir = tempfile.mkdtemp(prefix='lmdb_test_')
    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def lmdb_config(lmdb_test_dir):
    """LMDB configuration for testing"""
    return {
        'db_path': os.path.join(lmdb_test_dir, 'test.lmdb'),
        'map_size': 100 * 1024**2,  # 100MB for tests
        'transaction_size': 1000
    }


@pytest.fixture
def test_table_name():
    """Generate unique table name for each test"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    return f'test_table_{timestamp}'


@pytest.fixture
def sample_test_data():
    """Create sample test data"""
    data = {
        'id': list(range(100)),
        'name': [f'name_{i}' for i in range(100)],
        'value': [i * 1.5 for i in range(100)],
        'active': [i % 2 == 0 for i in range(100)]
    }
    return pa.Table.from_pydict(data)


@pytest.fixture
def blockchain_test_data():
    """Create blockchain-like test data"""
    data = {
        'block_number': list(range(1000, 2000)),
        'block_hash': [f'0x{i:064x}' for i in range(1000, 2000)],
        'timestamp': [1600000000 + i * 12 for i in range(1000)],
        'transaction_count': [i % 100 + 1 for i in range(1000)],
        'gas_used': [21000 + i * 1000 for i in range(1000)]
    }
    return pa.Table.from_pydict(data)


class TestLMDBLoader:
    """Test LMDB loader implementation"""
    
    def test_connection(self, lmdb_config):
        """Test basic connection to LMDB"""
        loader = LMDBLoader(lmdb_config)
        
        # Should not be connected initially
        assert not loader.is_connected
        
        # Connect
        loader.connect()
        assert loader.is_connected
        assert loader.env is not None
        
        # Check that database was created
        assert os.path.exists(lmdb_config['db_path'])
        
        # Disconnect
        loader.disconnect()
        assert not loader.is_connected
    
    def test_load_table_basic(self, lmdb_config, sample_test_data, test_table_name):
        """Test basic table loading"""
        loader = LMDBLoader(lmdb_config)
        loader.connect()
        
        # Load data
        result = loader.load_table(sample_test_data, test_table_name)
        
        assert result.success
        assert result.rows_loaded == 100
        assert result.table_name == test_table_name
        assert result.loader_type == 'lmdb'
        
        # Verify data was stored
        with loader.env.begin() as txn:
            cursor = txn.cursor()
            count = sum(1 for _ in cursor)
            assert count == 100
        
        loader.disconnect()
    
    def test_key_column_strategy(self, lmdb_config, sample_test_data, test_table_name):
        """Test key generation using specific column"""
        config = {**lmdb_config, 'key_column': 'id'}
        loader = LMDBLoader(config)
        loader.connect()
        
        result = loader.load_table(sample_test_data, test_table_name)
        assert result.success
        
        # Verify keys were generated correctly
        with loader.env.begin() as txn:
            # Check specific keys
            for i in range(10):
                key = str(i).encode('utf-8')
                value = txn.get(key)
                assert value is not None
        
        loader.disconnect()
    
    def test_key_pattern_strategy(self, lmdb_config, blockchain_test_data, test_table_name):
        """Test pattern-based key generation"""
        config = {
            **lmdb_config,
            'key_pattern': '{table}:block:{block_number}'
        }
        loader = LMDBLoader(config)
        loader.connect()
        
        result = loader.load_table(blockchain_test_data, test_table_name)
        assert result.success
        
        # Verify pattern-based keys
        with loader.env.begin() as txn:
            key = f'{test_table_name}:block:1000'.encode('utf-8')
            value = txn.get(key)
            assert value is not None
        
        loader.disconnect()
    
    def test_composite_key_strategy(self, lmdb_config, sample_test_data, test_table_name):
        """Test composite key generation"""
        config = {
            **lmdb_config,
            'composite_key_columns': ['name', 'id']
        }
        loader = LMDBLoader(config)
        loader.connect()
        
        result = loader.load_table(sample_test_data, test_table_name)
        assert result.success
        
        # Verify composite keys
        with loader.env.begin() as txn:
            key = 'name_0:0'.encode('utf-8')
            value = txn.get(key)
            assert value is not None
        
        loader.disconnect()
    
    def test_named_database(self, lmdb_config, sample_test_data):
        """Test using named databases"""
        config = {
            **lmdb_config,
            'database_name': 'blocks',
            'key_column': 'id'
        }
        loader = LMDBLoader(config)
        loader.connect()
        
        # Load to named database
        result = loader.load_table(sample_test_data, 'any_table')
        assert result.success
        
        # Verify data is in named database
        db = loader.env.open_db(b'blocks')
        with loader.env.begin(db=db) as txn:
            cursor = txn.cursor()
            count = sum(1 for _ in cursor)
            assert count == 100
        
        loader.disconnect()
    
    def test_load_modes(self, lmdb_config, sample_test_data, test_table_name):
        """Test different load modes"""
        config = {**lmdb_config, 'key_column': 'id'}
        loader = LMDBLoader(config)
        loader.connect()
        
        # Initial load
        result1 = loader.load_table(sample_test_data, test_table_name)
        assert result1.success
        assert result1.rows_loaded == 100
        
        # Append mode (should fail for duplicate keys)
        result2 = loader.load_table(sample_test_data, test_table_name, mode=LoadMode.APPEND)
        assert not result2.success  # Should fail due to duplicate keys
        assert result2.rows_loaded == 0  # No new rows added
        assert 'Key already exists' in str(result2.error)
        
        # Overwrite mode
        result3 = loader.load_table(sample_test_data, test_table_name, mode=LoadMode.OVERWRITE)
        assert result3.success
        assert result3.rows_loaded == 100
        
        loader.disconnect()
    
    def test_transaction_batching(self, lmdb_test_dir, blockchain_test_data, test_table_name):
        """Test transaction batching performance"""
        # Small transactions
        config1 = {
            'db_path': os.path.join(lmdb_test_dir, 'test1.lmdb'),
            'map_size': 100 * 1024**2,
            'transaction_size': 100,
            'key_column': 'block_number'
        }
        loader1 = LMDBLoader(config1)
        loader1.connect()
        
        start = time.time()
        result1 = loader1.load_table(blockchain_test_data, test_table_name)
        time1 = time.time() - start
        
        loader1.disconnect()
        
        # Large transactions - use different database
        config2 = {
            'db_path': os.path.join(lmdb_test_dir, 'test2.lmdb'),
            'map_size': 100 * 1024**2,
            'transaction_size': 1000,
            'key_column': 'block_number'
        }
        loader2 = LMDBLoader(config2)
        loader2.connect()
        
        start = time.time()
        result2 = loader2.load_table(blockchain_test_data, test_table_name)
        time2 = time.time() - start
        
        loader2.disconnect()
        
        # Both should succeed
        assert result1.success
        assert result2.success
        
        # Larger transactions should generally be faster
        # (though this might not always be true in small test datasets)
        print(f"Small txn time: {time1:.3f}s, Large txn time: {time2:.3f}s")
    
    def test_byte_key_handling(self, lmdb_config):
        """Test handling of byte array keys"""
        # Create data with byte keys
        data = {
            'key': [b'key1', b'key2', b'key3'],
            'value': [1, 2, 3]
        }
        table = pa.Table.from_pydict(data)
        
        config = {**lmdb_config, 'key_column': 'key'}
        loader = LMDBLoader(config)
        loader.connect()
        
        result = loader.load_table(table, 'byte_test')
        assert result.success
        assert result.rows_loaded == 3
        
        # Verify byte keys work correctly
        with loader.env.begin() as txn:
            assert txn.get(b'key1') is not None
            assert txn.get(b'key2') is not None
            assert txn.get(b'key3') is not None
        
        loader.disconnect()
    
    def test_large_batch_loading(self, lmdb_config):
        """Test loading large batches"""
        # Create larger dataset
        size = 50000
        data = {
            'id': list(range(size)),
            'data': [f'x' * 100 for _ in range(size)]  # 100 chars per row
        }
        table = pa.Table.from_pydict(data)
        
        config = {
            **lmdb_config,
            'map_size': 500 * 1024**2,  # 500MB
            'transaction_size': 5000,
            'key_column': 'id'
        }
        loader = LMDBLoader(config)
        loader.connect()
        
        start = time.time()
        result = loader.load_table(table, 'large_test')
        duration = time.time() - start
        
        assert result.success
        assert result.rows_loaded == size
        
        # Check performance metrics
        throughput = result.metadata['throughput_rows_per_sec']
        print(f"Loaded {size} rows in {duration:.2f}s ({throughput:.0f} rows/sec)")
        assert throughput > 10000  # Should handle at least 10k rows/sec
        
        loader.disconnect()
    
    def test_error_handling(self, lmdb_config, sample_test_data):
        """Test error handling"""
        # Test with invalid key column
        config = {**lmdb_config, 'key_column': 'nonexistent_column'}
        loader = LMDBLoader(config)
        loader.connect()
        
        result = loader.load_table(sample_test_data, 'error_test')
        assert not result.success
        assert 'not found in data' in result.error
        
        loader.disconnect()
    
    def test_data_persistence(self, lmdb_config, sample_test_data, test_table_name):
        """Test that data persists across connections"""
        config = {**lmdb_config, 'key_column': 'id'}
        
        # First connection - write data
        loader1 = LMDBLoader(config)
        loader1.connect()
        result = loader1.load_table(sample_test_data, test_table_name)
        assert result.success
        loader1.disconnect()
        
        # Second connection - verify data
        loader2 = LMDBLoader(config)
        loader2.connect()
        
        with loader2.env.begin() as txn:
            # Check a few keys
            for i in range(10):
                key = str(i).encode('utf-8')
                value = txn.get(key)
                assert value is not None
                
                # Deserialize and verify it's valid Arrow data
                import pyarrow as pa
                reader = pa.ipc.open_stream(value)
                batch = reader.read_next_batch()
                assert batch.num_rows == 1
        
        loader2.disconnect()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])