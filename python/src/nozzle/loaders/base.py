from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Iterator
from dataclasses import dataclass, field
from enum import Enum
import pyarrow as pa
import time
import logging


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
    table_name: str
    loader_type: str
    success: bool
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __str__(self):
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


class DataLoader(ABC):
    """Abstract base class for all data loaders"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self._connection = None
        self._is_connected = False

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the target system"""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the target system"""
        pass

    @abstractmethod
    def load_batch(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> LoadResult:
        """Load a single Arrow RecordBatch"""
        pass

    @abstractmethod
    def load_table(self, table: pa.Table, table_name: str, **kwargs) -> LoadResult:
        """Load a complete Arrow Table"""
        pass

    def load_stream(self, batch_iterator: Iterator[pa.RecordBatch], table_name: str, **kwargs) -> Iterator[LoadResult]:
        """Load data from a stream of batches"""
        if not self._is_connected:
            self.connect()

        total_rows = 0
        start_time = time.time()

        try:
            for batch in batch_iterator:
                result = self.load_batch(batch, table_name, **kwargs)

                if result.success:
                    total_rows += result.rows_loaded
                    self.logger.info(f'Loaded batch: {result.rows_loaded} rows in {result.duration:.2f}s')
                else:
                    self.logger.error(f'Failed to load batch: {result.error}')

                yield result

        except Exception as e:
            self.logger.error(f'Stream loading failed: {str(e)}')
            yield LoadResult(rows_loaded=0, duration=time.time() - start_time, table_name=table_name, loader_type=self.__class__.__name__, success=False, error=str(e))

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
