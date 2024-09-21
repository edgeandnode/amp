from abc import ABC, abstractmethod
from typing import List, Optional, Dict
from dataclasses import dataclass, field
from .contracts import Contracts
from .event import Event
from .contract import Contract
import datafusion
from datafusion import SessionContext, DataFrame, functions as f
from .event_registry import RegisteredEvent
from .schedule import Schedule
import pyarrow as pa
import pyarrow.parquet as pq
import os
from nozzle.client import Client, process_query
from pathlib import Path
from typing import Callable, Union
import random
import uuid
from datetime import datetime
import numpy as np
import pandas as pd
from functools import wraps
from .tables import Tables
from .table_registry import table_registry, RegisteredTable

DEFAULT_BLOCK_RANGE = 100_000
NOZZLE_URL = "grpc://34.122.177.97:80"
BASE_FIREHOSE_TABLE = "eth_firehose.logs"
DATA_DIR = Path("data")

@dataclass
class View(ABC):
    description: str
    events: Optional[List[RegisteredEvent]] = None
    input_tables: Optional[List[RegisteredTable]] = None
    event_descriptions: Optional[Dict[RegisteredEvent, str]] = None
    start_block: Optional[int] = None
    end_block: Optional[int] = None
    num_blocks: Optional[int] = None
    force_refresh: bool = False
    # materialized: bool = True TODO: implement non-materialized views (data not persisted, but query plans can be composed with other views)
    tables = Tables()
    _ctx: datafusion.SessionContext = tables._ctx
    _preprocessed: bool = field(init=False, default=False)
    _event_tables: Dict[str, str] = field(init=False, default_factory=dict)
    

    def __post_init__(self):
        # Must include either events or input_tables
        if not self.events and not self.input_tables:
            raise ValueError("Either events or input_tables must be specified in the view to have any data to query")
        self.name: self.__class__.__name__
        self.set_block_range()
        self.validate_inputs()
        DATA_DIR.mkdir(exist_ok=True)
        # self.schedule = self.schedule TODO: implement scheduling functionality

    def set_block_range(self):
        if self.start_block is None and self.end_block is None:
            raise ValueError("Either start_block or end_block must be specified")
        
        if self.end_block is None:
            if self.num_blocks is None:
                self.end_block = self.start_block + DEFAULT_BLOCK_RANGE
            else:
                self.end_block = self.start_block + self.num_blocks
        elif self.start_block is None:
            if self.num_blocks is None: 
                self.start_block = max(1, self.end_block - DEFAULT_BLOCK_RANGE)
            else:
                self.start_block = self.end_block - self.num_blocks

    def validate_inputs(self):
        if not isinstance(self.start_block, int) or not isinstance(self.end_block, int):
            raise ValueError("start_block and end_block must be integers")
        
        if self.start_block >= self.end_block:
            raise ValueError("start_block must be less than end_block")

        if not self.events:
            raise ValueError("At least one event must be specified")

        for event in self.events:
            if not isinstance(event, RegisteredEvent):
                raise ValueError(f"Invalid event: {event}. Must be an RegisteredEvent object.")

    def preprocess_data(self, run: bool = False):
        if self._preprocessed:
            return

        # New code
        if self.events:
            for event in self.events:
                event_dir = DATA_DIR / event.contract.name / event.name
                if not event_dir.exists():
                    event_dir.mkdir(parents=True)                
                output_path = event_dir / f"{self.start_block}_{self.end_block}.parquet"
                print('output_path', output_path)
                print(self.force_refresh)
                print(run)
                print(os.path.exists(output_path))
                
                table_name = f"preprocessed_event_{event.contract.name.lower()}_{event.name.lower()}_{self.start_block}_{self.end_block}"
                datafusion_tables = self._ctx.tables()
                print(datafusion_tables)
                if table_name in datafusion_tables and os.path.exists(output_path) and not self.force_refresh and not run:
                    print(f"Using existing preprocessed data for {event.name} from {output_path}")
                    table = pq.read_table(output_path)
                else:
                    client = self.nozzle_client()
                    print(f"Preprocessing data for {event.name} events from remote server")
                    query = self.build_event_query(event, run)
                    table = process_query(client, query)
                    # Generate schema from pyarrow table
                    schema = table.schema
                    pq.write_table(table, output_path)
                    print(f"Table {table_name} registered with {table.num_rows} rows")
                    print(f"Preprocessed data for {event.contract.name} {event.name} events saved to {output_path}")
                    table_description = f"Preprocessed {event.contract.name} {event.name} event data from blocks {self.start_block} to {self.end_block}"
                    field_descriptions = {
                        'block_num': 'The block number',
                        'timestamp': 'The timestamp of the block',
                        'tx_hash': 'The transaction hash',
                        'log_index': 'The event log index, which is unique within a block',
                        'address': 'The address of the contract that emitted the event',
                        'event_signature': 'The event signature, which is a string that includes the name and type of each parameter'
                    }
                    # for param in event.parameters:
                    #     param_name = param.name
                        # field_descriptions[param_name] = self.event_descriptions[event][param_name]
                    self.tables.register_table(table_name, output_path, schema, table_description, field_descriptions)
                    self._event_tables[event.name] = table_name
            
        self._preprocessed = True


    def build_event_query(self, event: Event, run: bool = False) -> str:
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
            {f"" if run else "LIMIT 1000"}
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
    
    def create_dummy_data_table(schema: pa.lib.Schema, num_rows: int = 5) -> pa.Table:
        # Creates a dummy table with random data to test the schema
        # TODO: create a more comprehensive set of dummy data types
        dummy_data = {}
        
        for field in schema:
            if pa.types.is_boolean(field.type):
                dummy_data[field.name] = np.random.choice([True, False], num_rows)
            elif pa.types.is_integer(field.type):
                dummy_data[field.name] = np.random.randint(-100, 100, num_rows)
            elif pa.types.is_floating(field.type):
                # Cast to float to avoid issues with numpy
                dummy_data[field.name] = np.random.uniform(0, 1, num_rows)
            elif pa.types.is_string(field.type):
                dummy_data[field.name] = [f"str_{i}" for i in range(num_rows)]
            elif pa.types.is_timestamp(field.type):
                dummy_data[field.name] = np.array([np.datetime64('now') + np.timedelta64(i, 'D') for i in range(num_rows)])
            elif pa.types.is_date(field.type):
                dummy_data[field.name] = np.array([np.datetime64('today') + np.timedelta64(i, 'D') for i in range(num_rows)])
            elif pa.types.is_list(field.type):
                value_type = field.type.value_type
                if pa.types.is_integer(value_type):
                    dummy_data[field.name] = [[np.random.randint(-10, 10) for _ in range(3)] for _ in range(num_rows)]
                elif pa.types.is_string(value_type):
                    dummy_data[field.name] = [[f"item_{j}" for j in range(3)] for _ in range(num_rows)]
                else:
                    dummy_data[field.name] = [[None] * 3 for _ in range(num_rows)]
            elif pa.types.is_struct(field.type):
                dummy_data[field.name] = [{'a': 1, 'b': 2} for _ in range(num_rows)]
            else:
                dummy_data[field.name] = [None] * num_rows
        # Create a pyarrow DataFrame from a dictionary
        dummy_table = pa.Table.from_pydict(dummy_data)
        # Coerce dummy data table to match the schema if possible
        dummy_table = dummy_table.cast(schema)
        # Check whether the dataframe schema is compatible with the input schema
        if dummy_table.schema != schema:
            raise ValueError("Schema is not compatible with the dummy data")
        return dummy_table
                
    def nozzle_client(self):
        return Client(NOZZLE_URL)
    
    # Validate the DataFusion query of the subclass implementation, must avoid decorator is not defined error
    def validate_query(self, result: Union[str, Callable[[DataFrame], DataFrame]]) -> Union[str, Callable[[DataFrame], DataFrame]]:
        if isinstance(result, str):
            # It's a SQL query string
            try:
                # Create logical plan from the SQL query
                df = self._ctx.sql(result)
                df.logical_plan()
            except Exception as e:
                raise self.InvalidDataFusionQueryError(f"Invalid DataFusion SQL query: {str(e)}")
        elif callable(result):
            # It's a series of DataFrame operations
            try:
                # Create a dummy Table from the schema
                dummy_table = self.create_dummy_data_table(self.schema())
                df = result(dummy_table)
                df.logical_plan()
            except Exception as e:
                raise self.InvalidDataFusionQueryError(f"Invalid DataFusion DataFrame operations: {str(e)}")
        else:
            raise self.InvalidDataFusionQueryError("Function must return either a string SQL query or a callable that operates on a DataFrame")

    class InvalidDataFusionQueryError(Exception):
        pass

    @abstractmethod
    def query(self) -> Union[str, Callable[[DataFrame], DataFrame]]:
        pass

    # Require subclasses to create a schema that can be used to check the output of an
    # execute result against the expected schema
    @abstractmethod
    def schema(self) -> pa.lib.Schema:
        pass

    def execute(self, run: bool = False) -> pa.Table:
        # TODO: make this work with session context and table registry
        if run:
            print("Running full run (on all specified data)")
        else:
            print("Running test run (event preprocessing queries limited to 1000 records)")
        self.preprocess_data(run)  # Ensure data is preprocessed
        # Validate the DataFusion query of the subclass implementation
        # self.validate_query(self.query())
        print(self.query())
        query = self.query()
        view_name = self.__class__.__name__
        output_dir = DATA_DIR / view_name
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{self.start_block}_{self.end_block}.parquet"
        table_name = f"{view_name}_{self.start_block}_{self.end_block}"
        table = self.tables.execute_query(query, view_name)
        schema = table.schema
        pq.write_table(table, output_path)
        print(f"{table_name} run data saved to {output_path}")
        self.tables.register_table(table_name, output_path, schema, f"{self.description}. {view_name} view output from blocks {self.start_block} to {self.end_block}", self.field_descriptions())
        print(f"Table {table_name} registered with {table.num_rows} rows")
        print(table.take(list(range(min(5, table.num_rows)))).to_pandas().head())
        return table
    
    def extract_table_names(query: str) -> List[str]:
        table_names = []
        for word in query.split():
            if word.upper().startswith('FROM'):
                table_name = word.upper().split('FROM')[1].split(' ')[0].strip()
                table_names.append(table_name)
        return table_names
    
    def field_descriptions(self) -> Dict[str, str]:
        """
        Returns a dictionary of field names and their descriptions.
        Override this method in subclasses to provide field descriptions.
        """
        print('schema ', self.schema())
        return {field.name: field.metadata.get(b'description', b'').decode() 
                for field in self.schema()}
    
    @staticmethod
    def adjust_schema(table: pa.Table) -> pa.Table:
        new_schema = []
        for field in table.schema:
            if pa.types.is_decimal(field.type) and field.type.precision > 38:
                # Convert high-precision decimal to uint64
                new_field = pa.field(field.name, pa.uint64(), field.metadata)
            elif pa.types.is_integer(field.type) and field.type.bit_width > 64:
                # Convert large integers to uint64
                new_field = pa.field(field.name, pa.uint64(), field.metadata)
            else:
                new_field = field
            new_schema.append(new_field)
        
        new_schema = pa.schema(new_schema)
        return table.cast(new_schema)

