import pyarrow as pa
from typing import List
from pathlib import Path
from .registered_table import RegisteredTable

class RegisteredTable:
    name: str
    schema: pa.Schema
    description: str
    parquet_files: List[Path]
    min_block: int
    max_block: int


class preprocessed_event_dai_approval(RegisteredTable):
    name = 'preprocessed_event_dai_approval'
    schema = pa.schema([
        pa.field('block_num', pa.int64(), metadata=None),
        pa.field('timestamp', pa.timestamp('ns', tz='UTC'), metadata=None),
        pa.field('tx_hash', pa.string(), metadata=None),
        pa.field('log_index', pa.int64(), metadata=None),
        pa.field('address', pa.string(), metadata=None),
        pa.field('event_signature', pa.string(), metadata=None),
        pa.field('src', pa.string(), metadata=None),
        pa.field('guy', pa.string(), metadata=None),
        pa.field('wad', pa.decimal256(76, 0), metadata=None),
    ])
    description = 'Preprocessed Dai Approval event data'
    parquet_files = [Path('data/Dai/Approval/17500000_17600000.parquet')]
    min_block = 17500000
    max_block = 17600000
    class columns:
        block_num = 'block_num'
        timestamp = 'timestamp'
        tx_hash = 'tx_hash'
        log_index = 'log_index'
        address = 'address'
        event_signature = 'event_signature'
        src = 'src'
        guy = 'guy'
        wad = 'wad'

class preprocessed_event_dai_transfer(RegisteredTable):
    name = 'preprocessed_event_dai_transfer'
    schema = pa.schema([
        pa.field('block_num', pa.int64(), metadata=None),
        pa.field('timestamp', pa.timestamp('ns', tz='UTC'), metadata=None),
        pa.field('tx_hash', pa.string(), metadata=None),
        pa.field('log_index', pa.int64(), metadata=None),
        pa.field('address', pa.string(), metadata=None),
        pa.field('event_signature', pa.string(), metadata=None),
        pa.field('src', pa.string(), metadata=None),
        pa.field('dst', pa.string(), metadata=None),
        pa.field('wad', pa.decimal128(27, 0), metadata=None),
    ])
    description = 'Preprocessed Dai Transfer event data'
    parquet_files = [Path('data/Dai/Transfer/17500000_17600000.parquet')]
    min_block = 17500000
    max_block = 17600000
    class columns:
        block_num = 'block_num'
        timestamp = 'timestamp'
        tx_hash = 'tx_hash'
        log_index = 'log_index'
        address = 'address'
        event_signature = 'event_signature'
        src = 'src'
        dst = 'dst'
        wad = 'wad'

class daily_dai_transfer_counts_view(RegisteredTable):
    name = 'daily_dai_transfer_counts_view'
    schema = pa.schema([
        pa.field('sender', pa.string(), metadata=None),
        pa.field('day', pa.timestamp('ns', tz='UTC'), metadata=None),
        pa.field('transfer_count', pa.int64(), metadata=None),
        pa.field('avg_value', pa.decimal128(35, 8), metadata=None),
    ])
    description = 'This is a table of DAI transfer counts by day. daily_dai_transfer_counts_view view output from blocks 17500000 to 17600000'
    parquet_files = [Path('data/daily_dai_transfer_counts_view/17500000_17600000.parquet')]
    min_block = 17500000
    max_block = 17600000
    class columns:
        sender = 'sender'
        day = 'day'
        transfer_count = 'transfer_count'
        avg_value = 'avg_value'

class preprocessed_event_uniswapv2factory_paircreated(RegisteredTable):
    name = 'preprocessed_event_uniswapv2factory_paircreated'
    schema = pa.schema([
        pa.field('block_num', pa.int64(), metadata=None),
        pa.field('timestamp', pa.timestamp('ns', tz='UTC'), metadata=None),
        pa.field('tx_hash', pa.string(), metadata=None),
        pa.field('log_index', pa.int64(), metadata=None),
        pa.field('address', pa.string(), metadata=None),
        pa.field('event_signature', pa.string(), metadata=None),
        pa.field('token0', pa.string(), metadata=None),
        pa.field('token1', pa.string(), metadata=None),
        pa.field('pair', pa.string(), metadata=None),
    ])
    description = 'Preprocessed UniswapV2Factory PairCreated event data'
    parquet_files = [Path('data/UniswapV2Factory/PairCreated/17500000_17600000.parquet')]
    min_block = 17500000
    max_block = 17600000
    class columns:
        block_num = 'block_num'
        timestamp = 'timestamp'
        tx_hash = 'tx_hash'
        log_index = 'log_index'
        address = 'address'
        event_signature = 'event_signature'
        token0 = 'token0'
        token1 = 'token1'
        pair = 'pair'


class TableRegistry:

    preprocessed_event_dai_approval = preprocessed_event_dai_approval()

    preprocessed_event_dai_transfer = preprocessed_event_dai_transfer()

    daily_dai_transfer_counts_view = daily_dai_transfer_counts_view()

    preprocessed_event_uniswapv2factory_paircreated = preprocessed_event_uniswapv2factory_paircreated()


    @classmethod
    def get_table(cls, name: str) -> RegisteredTable:
        return getattr(cls, name)

    @classmethod
    def list_tables(cls) -> List[str]:
        return [attr for attr in dir(cls) if isinstance(getattr(cls, attr), RegisteredTable)]