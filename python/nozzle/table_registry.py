from typing import Dict
from .table import Table
from dataclasses import dataclass
import pyarrow as pa
from datafusion import SessionContext
from pathlib import Path

class RegisteredTable(Table):
    pass

@dataclass
class TableRegistry:

    @property
    def preprocessed_event_weth9_transfer_20700000_20800000(self) -> Dict[str, any]:
        return {
            'name': 'preprocessed_event_weth9_transfer_20700000_20800000',
            'schema': pa.schema([
                pa.field('block_num', pa.int64(), metadata={'description': 'The block number'}),
                pa.field('timestamp', pa.timestamp('ns', tz='UTC'), metadata={'description': 'The timestamp of the block'}),
                pa.field('tx_hash', pa.string(), metadata={'description': 'The transaction hash'}),
                pa.field('log_index', pa.int64(), metadata={'description': 'The event log index, which is unique within a block'}),
                pa.field('address', pa.string(), metadata={'description': 'The address of the contract that emitted the event'}),
                pa.field('event_signature', pa.string(), metadata={'description': 'The event signature, which is a string that includes the name and type of each parameter'}),
                pa.field('src', pa.string(), metadata={'description': ''}),
                pa.field('dst', pa.string(), metadata={'description': ''}),
                pa.field('wad', pa.decimal128(20, 0), metadata={'description': ''}),
            ]),
            'description': 'Preprocessed WETH9 Transfer event data from blocks 20700000 to 20800000',
            'parquet_files': [Path('data/WETH9/Transfer/20700000_20800000.parquet')],
        }

    @property
    def preprocessed_event_weth9_approval_20700000_20800000(self) -> Dict[str, any]:
        return {
            'name': 'preprocessed_event_weth9_approval_20700000_20800000',
            'schema': pa.schema([
                pa.field('block_num', pa.int64(), metadata={'description': 'The block number'}),
                pa.field('timestamp', pa.timestamp('ns', tz='UTC'), metadata={'description': 'The timestamp of the block'}),
                pa.field('tx_hash', pa.string(), metadata={'description': 'The transaction hash'}),
                pa.field('log_index', pa.int64(), metadata={'description': 'The event log index, which is unique within a block'}),
                pa.field('address', pa.string(), metadata={'description': 'The address of the contract that emitted the event'}),
                pa.field('event_signature', pa.string(), metadata={'description': 'The event signature, which is a string that includes the name and type of each parameter'}),
                pa.field('src', pa.string(), metadata={'description': ''}),
                pa.field('guy', pa.string(), metadata={'description': ''}),
                pa.field('wad', pa.decimal128(76, 0), metadata={'description': ''}),
            ]),
            'description': 'Preprocessed WETH9 Approval event data from blocks 20700000 to 20800000',
            'parquet_files': [Path('data/WETH9/Approval/20700000_20800000.parquet')],
        }


table_registry = TableRegistry()

def initialize_session() -> SessionContext:
    ctx = SessionContext()
    for table_info in [getattr(table_registry, attr) for attr in dir(table_registry) if isinstance(getattr(table_registry, attr), property)]:
        for file in table_info['parquet_files']:
            ctx.register_parquet(table_info['name'], str(file), schema=table_info['schema'])
    return ctx