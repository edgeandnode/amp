from abc import ABC, abstractmethod
from typing import List, Optional, Dict
from dataclasses import dataclass, field
from .contracts import Contracts
from .event import Event
from .contract import Contract
import datafusion
from datafusion import functions as f
from .dataset_registry import DatasetRegistry, Dataset, Table
from .schedule import Schedule
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from nozzle.client import Client, process_query
from pathlib import Path

DEFAULT_BLOCK_RANGE = 100_000
NOZZLE_URL = "grpc://34.122.177.97:80"
BASE_FIREHOSE_TABLE = "eth_firehose.logs"
DATA_DIR = Path("data")


@dataclass
class View(ABC):
    events: List[Event]
    start_block: Optional[int] = None
    end_block: Optional[int] = None
    force_refresh: bool = False
    _ctx: datafusion.SessionContext = field(init=False, default_factory=datafusion.SessionContext)
    _preprocessed: bool = field(init=False, default=False)
    _event_tables: Dict[str, str] = field(init=False, default_factory=dict)
    

    def __post_init__(self):
        self.set_block_range()
        self.validate_inputs()
        DATA_DIR.mkdir(exist_ok=True)
        self._dataset = Dataset(self.__class__.__name__)
        self.schedule = self.schedule

    def set_block_range(self):
        if self.start_block is None and self.end_block is None:
            raise ValueError("Either start_block or end_block must be specified")
        
        if self.end_block is None:
            self.end_block = self.start_block + DEFAULT_BLOCK_RANGE
        elif self.start_block is None:
            self.start_block = max(1, self.end_block - DEFAULT_BLOCK_RANGE)

    def validate_inputs(self):
        if not isinstance(self.start_block, int) or not isinstance(self.end_block, int):
            raise ValueError("start_block and end_block must be integers")
        
        if self.start_block >= self.end_block:
            raise ValueError("start_block must be less than end_block")

        if not self.events:
            raise ValueError("At least one event must be specified")

        for event in self.events:
            if not isinstance(event, Event):
                raise ValueError(f"Invalid event: {event}. Must be an Event object.")

    def preprocess_data(self):
        if self._preprocessed:
            return

        client = self.nozzle_client()
        for event in self.events:
            event_dir = DATA_DIR / self.__class__.__name__ / event.name.lower()
            event_dir.mkdir(parents=True, exist_ok=True)
            
            output_path = event_dir / f"blocks_{self.start_block}_{self.end_block}.parquet"
            table_name = f"preprocessed_{event.name.lower()}_{self.start_block}_{self.end_block}"
            
            if os.path.exists(output_path) and not self.force_refresh:
                print(f"Using existing preprocessed data for {event.name} from {output_path}")
                df = pd.read_parquet(output_path)
            else:
                print(f"Fetching data for {event.name} from remote server")
                query = self.build_event_query(event)
                df = process_query(client, query)
                df.to_parquet(output_path)
                table = Table(table_name, columns=['column1', 'column2'], 
                          start_block=self.start_block, end_block=self.end_block)
                self._dataset.add_table(table)
            print(f"Preprocessed data for {event.name} saved to {output_path}")
            

            self._ctx.register_parquet(table_name, output_path)
            self._event_tables[event.name] = table_name
            DatasetRegistry().register_dataset(self._dataset)
            print(f"Table {table_name} registered with {len(df)} rows")

        self._preprocessed = True


    def build_event_query(self, event: Event) -> str:
        columns = [
            "block_num",
            "timestamp",
            "tx_hash",
            "log_index",
            "address",
            f"'{event.signature}' as event_signature"
        ]
        
        decoded_columns = []
        for param in event.parameters:
            param_name = param.name
            param_type = param.type
            decoded_columns.append(f"decoded.{param_name} as {param_name}")
        
        decoded_columns_str = ", ".join(decoded_columns)
        columns_str = ", ".join(columns + [decoded_columns_str])
        
        return f"""
        SELECT {columns_str}
        FROM (
            SELECT *,
                evm_decode(topic1, topic2, topic3, data, '{event.signature}') as decoded
            FROM {BASE_FIREHOSE_TABLE}
            WHERE address = arrow_cast(x'{str(event.contract.address)[2:]}', 'FixedSizeBinary(20)')
            AND topic0 = evm_topic('{event.signature}')
            AND block_num BETWEEN {self.start_block} AND {self.end_block}
        )
        """

    def get_sql_type(self, eth_type: str) -> str:
        if eth_type.startswith('uint') or eth_type.startswith('int'):
            return 'DECIMAL(76, 0)'
        elif eth_type == 'address':
            return 'VARCHAR'
        elif eth_type == 'bool':
            return 'BOOLEAN'
        else:
            return 'VARCHAR'

    def nozzle_client(self):
        return Client(NOZZLE_URL)

    @abstractmethod
    def get_query(self) -> str:
        pass

    def execute(self) -> pa.Table:
        self.preprocess_data()  # Ensure data is preprocessed
        query = self.get_query()
        df = self._ctx.sql(query)
        print(df)
        return df
    
    def extract_table_names(query):
        # Implement SQL parsing to extract table names
        pass

    def execute_query(query):
        # Implement query execution
        pass
