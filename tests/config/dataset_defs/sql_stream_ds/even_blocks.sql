select *
from eth_firehose_stream.blocks
where
    block_num % 2 = 0
