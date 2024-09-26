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
    name = "preprocessed_event_dai_approval"
    schema = pa.schema([
        pa.field("block_num", pa.int64(), metadata=None),
        pa.field("timestamp", pa.timestamp('ns', tz='UTC'), metadata=None),
        pa.field("tx_hash", pa.string(), metadata=None),
        pa.field("log_index", pa.int64(), metadata=None),
        pa.field("address", pa.string(), metadata=None),
        pa.field("event_signature", pa.string(), metadata=None),
        pa.field("src", pa.string(), metadata=None),
        pa.field("guy", pa.string(), metadata=None),
        pa.field("wad", pa.decimal256(76, 0), metadata=None),
    ])
    description = "Preprocessed Dai Approval event data"
    parquet_files = [Path("data/Dai/Approval/17000000_18000000.parquet")]
    min_block = 17000000
    max_block = 18000000
    
    class columns:
        block_num = "block_num"
        timestamp = "timestamp"
        tx_hash = "tx_hash"
        log_index = "log_index"
        address = "address"
        event_signature = "event_signature"
        src = "src"
        guy = "guy"
        wad = "wad"

class preprocessed_event_dai_transfer(RegisteredTable):
    name = "preprocessed_event_dai_transfer"
    schema = pa.schema([
        pa.field("block_num", pa.int64(), metadata=None),
        pa.field("timestamp", pa.timestamp('ns', tz='UTC'), metadata=None),
        pa.field("tx_hash", pa.string(), metadata=None),
        pa.field("log_index", pa.int64(), metadata=None),
        pa.field("address", pa.string(), metadata=None),
        pa.field("event_signature", pa.string(), metadata=None),
        pa.field("src", pa.string(), metadata=None),
        pa.field("dst", pa.string(), metadata=None),
        pa.field("wad", pa.decimal128(27, 0), metadata=None),
    ])
    description = "Preprocessed Dai Transfer event data"
    parquet_files = [Path("data/Dai/Transfer/17000000_18000000.parquet")]
    min_block = 17000000
    max_block = 18000000
    
    class columns:
        block_num = "block_num"
        timestamp = "timestamp"
        tx_hash = "tx_hash"
        log_index = "log_index"
        address = "address"
        event_signature = "event_signature"
        src = "src"
        dst = "dst"
        wad = "wad"

class preprocessed_event_lusdtoken_approval(RegisteredTable):
    name = "preprocessed_event_lusdtoken_approval"
    schema = pa.schema([
        pa.field("block_num", pa.int64(), metadata=None),
        pa.field("timestamp", pa.timestamp('ns', tz='UTC'), metadata=None),
        pa.field("tx_hash", pa.string(), metadata=None),
        pa.field("log_index", pa.int64(), metadata=None),
        pa.field("address", pa.string(), metadata=None),
        pa.field("event_signature", pa.string(), metadata=None),
        pa.field("owner", pa.string(), metadata=None),
        pa.field("spender", pa.string(), metadata=None),
        pa.field("value", pa.decimal256(68, 0), metadata=None),
    ])
    description = "Preprocessed LUSDToken Approval event data"
    parquet_files = [Path("data/LUSDToken/Approval/17000000_18000000.parquet")]
    min_block = 17000000
    max_block = 18000000
    
    class columns:
        block_num = "block_num"
        timestamp = "timestamp"
        tx_hash = "tx_hash"
        log_index = "log_index"
        address = "address"
        event_signature = "event_signature"
        owner = "owner"
        spender = "spender"
        value_ = "value"

class preprocessed_event_lusdtoken_transfer(RegisteredTable):
    name = "preprocessed_event_lusdtoken_transfer"
    schema = pa.schema([
        pa.field("block_num", pa.int64(), metadata=None),
        pa.field("timestamp", pa.timestamp('ns', tz='UTC'), metadata=None),
        pa.field("tx_hash", pa.string(), metadata=None),
        pa.field("log_index", pa.int64(), metadata=None),
        pa.field("address", pa.string(), metadata=None),
        pa.field("event_signature", pa.string(), metadata=None),
        pa.field("from", pa.string(), metadata=None),
        pa.field("to", pa.string(), metadata=None),
        pa.field("value", pa.decimal128(25, 0), metadata=None),
    ])
    description = "Preprocessed LUSDToken Transfer event data"
    parquet_files = [Path("data/LUSDToken/Transfer/17000000_18000000.parquet")]
    min_block = 17000000
    max_block = 18000000
    
    class columns:
        block_num = "block_num"
        timestamp = "timestamp"
        tx_hash = "tx_hash"
        log_index = "log_index"
        address = "address"
        event_signature = "event_signature"
        from_ = "from"
        to_ = "to"
        value_ = "value"

class daily_dai_transfer_counts_view(RegisteredTable):
    name = "daily_dai_transfer_counts_view"
    schema = pa.schema([
        pa.field("sender", pa.string(), metadata=None),
        pa.field("day", pa.timestamp('ns', tz='UTC'), metadata=None),
        pa.field("transfer_count", pa.int64(), metadata=None),
        pa.field("avg_value_dai", pa.decimal128(38, 4), metadata=None),
    ])
    description = "This is a table of DAI transfer counts by day. daily_dai_transfer_counts_view view output from blocks 17000000 to 18000000"
    parquet_files = [Path("data/daily_dai_transfer_counts_view/17000000_18000000.parquet")]
    min_block = 17000000
    max_block = 18000000
    
    class columns:
        sender = "sender"
        day = "day"
        transfer_count = "transfer_count"
        avg_value_dai = "avg_value_dai"

class daily_lusd_transfers_view(RegisteredTable):
    name = "daily_lusd_transfers_view"
    schema = pa.schema([
        pa.field("sender", pa.string(), metadata=None),
        pa.field("day", pa.timestamp('ns', tz='UTC'), metadata=None),
        pa.field("transfer_count", pa.int64(), metadata=None),
        pa.field("avg_value_lusd", pa.decimal128(38, 4), metadata=None),
    ])
    description = "Daily LUSD transfers by wallet. daily_lusd_transfers_view view output from blocks 15000000 to 15100000"
    parquet_files = [Path("data/daily_lusd_transfers_view/15000000_15100000.parquet")]
    min_block = 15000000
    max_block = 15100000
    
    class columns:
        sender = "sender"
        day = "day"
        transfer_count = "transfer_count"
        avg_value_lusd = "avg_value_lusd"

class preprocessed_event_weth9_approval(RegisteredTable):
    name = "preprocessed_event_weth9_approval"
    schema = pa.schema([
        pa.field("block_num", pa.int64(), metadata=None),
        pa.field("timestamp", pa.timestamp('ns', tz='UTC'), metadata=None),
        pa.field("tx_hash", pa.string(), metadata=None),
        pa.field("log_index", pa.int64(), metadata=None),
        pa.field("address", pa.string(), metadata=None),
        pa.field("event_signature", pa.string(), metadata=None),
        pa.field("src", pa.string(), metadata=None),
        pa.field("guy", pa.string(), metadata=None),
        pa.field("wad", pa.decimal256(53, 0), metadata=None),
    ])
    description = "Preprocessed WETH9 Approval event data"
    parquet_files = [Path("data/WETH9/Approval/17000000_17010000.parquet")]
    min_block = 17000000
    max_block = 17010000
    
    class columns:
        block_num = "block_num"
        timestamp = "timestamp"
        tx_hash = "tx_hash"
        log_index = "log_index"
        address = "address"
        event_signature = "event_signature"
        src = "src"
        guy = "guy"
        wad = "wad"

class preprocessed_event_weth9_transfer(RegisteredTable):
    name = "preprocessed_event_weth9_transfer"
    schema = pa.schema([
        pa.field("block_num", pa.int64(), metadata=None),
        pa.field("timestamp", pa.timestamp('ns', tz='UTC'), metadata=None),
        pa.field("tx_hash", pa.string(), metadata=None),
        pa.field("log_index", pa.int64(), metadata=None),
        pa.field("address", pa.string(), metadata=None),
        pa.field("event_signature", pa.string(), metadata=None),
        pa.field("src", pa.string(), metadata=None),
        pa.field("dst", pa.string(), metadata=None),
        pa.field("wad", pa.decimal128(23, 0), metadata=None),
    ])
    description = "Preprocessed WETH9 Transfer event data"
    parquet_files = [Path("data/WETH9/Transfer/17000000_17010000.parquet")]
    min_block = 17000000
    max_block = 17010000
    
    class columns:
        block_num = "block_num"
        timestamp = "timestamp"
        tx_hash = "tx_hash"
        log_index = "log_index"
        address = "address"
        event_signature = "event_signature"
        src = "src"
        dst = "dst"
        wad = "wad"

class daily_weth_transfers_view(RegisteredTable):
    name = "daily_weth_transfers_view"
    schema = pa.schema([
        pa.field("sender", pa.string(), metadata=None),
        pa.field("day", pa.timestamp('ns', tz='UTC'), metadata=None),
        pa.field("transfer_count", pa.int64(), metadata=None),
        pa.field("avg_value_dai", pa.decimal128(37, 4), metadata=None),
    ])
    description = "WETH transfers at the wallet level by day. daily_weth_transfers_view view output from blocks 17000000 to 17100000"
    parquet_files = [Path("data/daily_weth_transfers_view/17000000_17100000.parquet")]
    min_block = 17000000
    max_block = 17100000
    
    class columns:
        sender = "sender"
        day = "day"
        transfer_count = "transfer_count"
        avg_value_dai = "avg_value_dai"


class TableRegistry:

    preprocessed_event_dai_approval = preprocessed_event_dai_approval()

    preprocessed_event_dai_transfer = preprocessed_event_dai_transfer()

    preprocessed_event_lusdtoken_approval = preprocessed_event_lusdtoken_approval()

    preprocessed_event_lusdtoken_transfer = preprocessed_event_lusdtoken_transfer()

    daily_dai_transfer_counts_view = daily_dai_transfer_counts_view()

    daily_lusd_transfers_view = daily_lusd_transfers_view()

    preprocessed_event_weth9_approval = preprocessed_event_weth9_approval()

    preprocessed_event_weth9_transfer = preprocessed_event_weth9_transfer()

    daily_weth_transfers_view = daily_weth_transfers_view()


    @classmethod
    def get_table(cls, name: str) -> RegisteredTable:
        return getattr(cls, name)

    @classmethod
    def list_tables(cls) -> List[str]:
        return [attr for attr in dir(cls) if isinstance(getattr(cls, attr), RegisteredTable)]