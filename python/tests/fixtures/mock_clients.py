# tests/fixtures/mock_clients.py
"""
Mock clients and components for testing the data loader framework.
Fixed import paths for nozzle project structure.
"""

from typing import Any, Dict, Optional
from unittest.mock import Mock

import pyarrow as pa

# Import with proper error handling
try:
    from src.nozzle.loaders.base import DataLoader, LoadResult
except ImportError:
    # Provide fallback implementations for when modules aren't available
    from abc import ABC, abstractmethod
    from dataclasses import dataclass
    from typing import Any

    @dataclass
    class LoadResult:
        rows_loaded: int
        duration: float
        table_name: str
        loader_type: str
        success: bool
        error: Optional[str] = None
        metadata: Dict[str, Any] = None

    class DataLoader(ABC):
        def __init__(self, config: Dict[str, Any]):
            self.config = config
            self._is_connected = False

        @abstractmethod
        def connect(self) -> None:
            pass

        @abstractmethod
        def disconnect(self) -> None:
            pass

        @abstractmethod
        def load_batch(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> LoadResult:
            pass

        @abstractmethod
        def load_table(self, table: pa.Table, table_name: str, **kwargs) -> LoadResult:
            pass

        def __enter__(self):
            self.connect()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.disconnect()


class MockFlightSQLClient:
    """Mock Flight SQL client for testing"""

    def __init__(self, url: str):
        self.url = url
        self.conn = Mock()
        self._test_data = None

    def set_test_data(self, table: pa.Table):
        """Set test data to return from queries"""
        self._test_data = table

    def get_sql(self, query: str, read_all: bool = False):
        """Mock get_sql method"""
        if self._test_data is None:
            # Return empty table if no test data set
            self._test_data = pa.table({'id': [1, 2, 3], 'name': ['a', 'b', 'c']})

        if read_all:
            return self._test_data
        else:
            # Return batches for streaming
            return self._test_data.to_batches(max_chunksize=100)


class MockDataLoader(DataLoader):
    """Mock data loader for testing framework components"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.load_calls = []
        self.should_fail = False
        self.fail_message = 'Mock failure'

    def connect(self) -> None:
        self._is_connected = True

    def disconnect(self) -> None:
        self._is_connected = False

    def load_batch(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> LoadResult:
        self.load_calls.append(('batch', table_name, batch.num_rows))

        if self.should_fail:
            return LoadResult(rows_loaded=0, duration=0.1, table_name=table_name, loader_type='mock', success=False, error=self.fail_message)

        return LoadResult(rows_loaded=batch.num_rows, duration=0.1, table_name=table_name, loader_type='mock', success=True)

    def load_table(self, table: pa.Table, table_name: str, **kwargs) -> LoadResult:
        self.load_calls.append(('table', table_name, table.num_rows))

        if self.should_fail:
            return LoadResult(rows_loaded=0, duration=0.1, table_name=table_name, loader_type='mock', success=False, error=self.fail_message)

        return LoadResult(rows_loaded=table.num_rows, duration=0.1, table_name=table_name, loader_type='mock', success=True)


class MockConnectionManager:
    """Mock connection manager for testing"""

    def __init__(self):
        self.connections = {}

    def add_connection(self, name: str, loader: str, config: Dict[str, Any]):
        self.connections[name] = {'loader': loader, 'config': config}

    def get_connection(self, name: str) -> Dict[str, Any]:
        if name in self.connections:
            return self.connections[name]['config']

        # Mock environment variable lookup
        if name == 'postgresql':
            return {'host': 'localhost', 'database': 'test'}

        raise ValueError(f"Connection '{name}' not found")

    def get_connection_info(self, name: str) -> Dict[str, Any]:
        """Get connection information including loader type and config"""
        if name in self.connections:
            return self.connections[name]

        # Mock environment variable lookup
        if name == 'postgresql':
            return {'loader': 'postgresql', 'config': {'host': 'localhost', 'database': 'test'}}

        raise ValueError(f"Connection '{name}' not found")

    def list_connections(self) -> Dict[str, str]:
        return {name: conn['loader'] for name, conn in self.connections.items()}


class MockArrowTable:
    """Mock Arrow table for testing when pyarrow is not available"""

    def __init__(self, num_rows: int = 100, schema=None):
        self.num_rows = num_rows
        self.schema = schema or MockArrowSchema()
        self.nbytes = num_rows * 100  # Mock byte size

    def to_batches(self, max_chunksize: int = 1000):
        """Mock batch generation"""
        for i in range(0, self.num_rows, max_chunksize):
            batch_size = min(max_chunksize, self.num_rows - i)
            yield MockArrowBatch(batch_size, self.schema)


class MockArrowBatch:
    """Mock Arrow batch for testing"""

    def __init__(self, num_rows: int, schema=None):
        self.num_rows = num_rows
        self.schema = schema or MockArrowSchema()


class MockArrowSchema:
    """Mock Arrow schema for testing"""

    def __init__(self, fields=None):
        self.fields = fields or [MockArrowField('id'), MockArrowField('name')]

    def __len__(self):
        return len(self.fields)

    def __iter__(self):
        return iter(self.fields)

    def field(self, name: str):
        for field in self.fields:
            if field.name == name:
                return field
        raise KeyError(f"Field '{name}' not found")


class MockArrowField:
    """Mock Arrow field for testing"""

    def __init__(self, name: str, type_=None, nullable: bool = True):
        self.name = name
        self.type = type_ or MockArrowType()
        self.nullable = nullable


class MockArrowType:
    """Mock Arrow type for testing"""

    def __init__(self, name: str = 'string'):
        self.name = name

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return isinstance(other, MockArrowType) and self.name == other.name


# Factory functions for creating test data
def create_mock_table(num_rows: int = 100) -> MockArrowTable:
    """Create a mock Arrow table"""
    return MockArrowTable(num_rows)


def create_mock_batch(num_rows: int = 100) -> MockArrowBatch:
    """Create a mock Arrow batch"""
    return MockArrowBatch(num_rows)


def create_mock_schema(field_names: list = None) -> MockArrowSchema:
    """Create a mock Arrow schema"""
    if field_names is None:
        field_names = ['id', 'name', 'value']

    fields = [MockArrowField(name) for name in field_names]
    return MockArrowSchema(fields)
