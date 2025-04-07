select *
from eth_firehose.blocks
where
    timestamp >= '2015-02-01'
    and timestamp < '2015-03-01'
