# Schema
Auto-generated file. See `schema_to_markdown` in `common/src/catalog/mod.rs`.
## blocks
````
+--------------------+---------------------------------------+-------------+
| column_name        | data_type                             | is_nullable |
+--------------------+---------------------------------------+-------------+
| block_num          | UInt64                                | NO          |
| timestamp          | Timestamp                             | NO          |
| hash               | Binary32                              | NO          |
| parent_hash        | Binary32                              | NO          |
| ommers_hash        | Binary32                              | NO          |
| miner              | Binary20                              | NO          |
| state_root         | Binary32                              | NO          |
| transactions_root  | Binary32                              | NO          |
| receipt_root       | Binary32                              | NO          |
| logs_bloom         | Binary                                | NO          |
| difficulty         | UInt126                               | NO          |
| gas_limit          | UInt64                                | NO          |
| gas_used           | UInt64                                | NO          |
| extra_data         | Binary                                | NO          |
| mix_hash           | Binary32                              | NO          |
| nonce              | UInt64                                | NO          |
| base_fee_per_gas   | UInt126                               | YES         |
| withdrawals_root   | Binary32                              | YES         |
| blob_gas_used      | UInt64                                | YES         |
| excess_blob_gas    | UInt64                                | YES         |
| parent_beacon_root | Binary32                              | YES         |
+--------------------+---------------------------------------+-------------+
````
## transactions
````
+--------------------------+---------------------------------------+-------------+
| column_name              | data_type                             | is_nullable |
+--------------------------+---------------------------------------+-------------+
| block_hash               | Binary32                              | NO          |
| block_num                | UInt64                                | NO          |
| timestamp                | Timestamp                             | NO          |
| tx_index                 | UInt32                                | NO          |
| tx_hash                  | Binary32                              | NO          |
| to                       | Binary                                | NO          |
| nonce                    | UInt64                                | NO          |
| gas_price                | UInt126                               | YES         |
| gas_limit                | UInt64                                | NO          |
| value                    | UInt126                               | YES         |
| input                    | Binary                                | NO          |
| v                        | Binary                                | NO          |
| r                        | Binary                                | NO          |
| s                        | Binary                                | NO          |
| gas_used                 | UInt64                                | NO          |
| type                     | Int32                                 | NO          |
| max_fee_per_gas          | UInt126                               | YES         |
| max_priority_fee_per_gas | UInt126                               | YES         |
| from                     | Binary20                              | NO          |
| status                   | Int32                                 | NO          |
| return_data              | Binary                                | NO          |
| public_key               | Binary                                | NO          |
| begin_ordinal            | UInt64                                | NO          |
| end_ordinal              | UInt64                                | NO          |
+--------------------------+---------------------------------------+-------------+
````
## calls
````
+---------------+---------------------------------------+-------------+
| column_name   | data_type                             | is_nullable |
+---------------+---------------------------------------+-------------+
| block_hash    | Binary32                              | NO          |
| block_num     | UInt64                                | NO          |
| timestamp     | Timestamp                             | NO          |
| tx_index      | UInt32                                | NO          |
| tx_hash       | Binary32                              | NO          |
| index         | UInt32                                | NO          |
| parent_index  | UInt32                                | NO          |
| depth         | UInt32                                | NO          |
| call_type     | Int32                                 | NO          |
| caller        | Binary20                              | NO          |
| address       | Binary20                              | NO          |
| value         | UInt126                               | YES         |
| gas_limit     | UInt64                                | NO          |
| gas_consumed  | UInt64                                | NO          |
| return_data   | Binary                                | NO          |
| input         | Binary                                | NO          |
| selfdestruct  | Boolean                               | NO          |
| executed_code | Boolean                               | NO          |
| begin_ordinal | UInt64                                | NO          |
| end_ordinal   | UInt64                                | NO          |
+---------------+---------------------------------------+-------------+
````
## logs
````
+-------------+---------------------------------------+-------------+
| column_name | data_type                             | is_nullable |
+-------------+---------------------------------------+-------------+
| block_hash  | Binary32                              | NO          |
| block_num   | UInt64                                | NO          |
| timestamp   | Timestamp                             | NO          |
| tx_hash     | Binary32                              | NO          |
| tx_index    | UInt32                                | NO          |
| log_index   | UInt32                                | NO          |
| address     | Binary20                              | NO          |
| topic0      | Binary32                              | YES         |
| topic1      | Binary32                              | YES         |
| topic2      | Binary32                              | YES         |
| topic3      | Binary32                              | YES         |
| data        | Binary                                | NO          |
+-------------+---------------------------------------+-------------+
````
