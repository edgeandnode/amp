select hash
from eth_firehose.blocks
where
    block_num % 2 = 0
