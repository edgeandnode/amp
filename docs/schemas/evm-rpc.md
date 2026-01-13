# Schema
Auto-generated file. See `to_markdown` in `crates/core/datasets-raw/src/schema.rs`.

## blocks
````
+--------------------+-------------------------------+-------------+
| column_name        | data_type                     | is_nullable |
+--------------------+-------------------------------+-------------+
| _block_num         | UInt64                        | NO          |
| block_num          | UInt64                        | NO          |
| timestamp          | Timestamp(Nanosecond, +00:00) | NO          |
| hash               | FixedSizeBinary(32)           | NO          |
| parent_hash        | FixedSizeBinary(32)           | NO          |
| ommers_hash        | FixedSizeBinary(32)           | NO          |
| miner              | FixedSizeBinary(20)           | NO          |
| state_root         | FixedSizeBinary(32)           | NO          |
| transactions_root  | FixedSizeBinary(32)           | NO          |
| receipt_root       | FixedSizeBinary(32)           | NO          |
| logs_bloom         | Binary                        | NO          |
| difficulty         | Decimal128(38, 0)             | NO          |
| total_difficulty   | Decimal128(38, 0)             | YES         |
| gas_limit          | UInt64                        | NO          |
| gas_used           | UInt64                        | NO          |
| extra_data         | Binary                        | NO          |
| mix_hash           | FixedSizeBinary(32)           | NO          |
| nonce              | UInt64                        | NO          |
| base_fee_per_gas   | Decimal128(38, 0)             | YES         |
| withdrawals_root   | FixedSizeBinary(32)           | YES         |
| blob_gas_used      | UInt64                        | YES         |
| excess_blob_gas    | UInt64                        | YES         |
| parent_beacon_root | FixedSizeBinary(32)           | YES         |
+--------------------+-------------------------------+-------------+
````
## transactions
````
+--------------------------+-------------------------------------------------------------------------------------+-------------+
| column_name              | data_type                                                                           | is_nullable |
+--------------------------+-------------------------------------------------------------------------------------+-------------+
| _block_num               | UInt64                                                                              | NO          |
| block_hash               | FixedSizeBinary(32)                                                                 | NO          |
| block_num                | UInt64                                                                              | NO          |
| timestamp                | Timestamp(Nanosecond, +00:00)                                                       | NO          |
| tx_index                 | UInt32                                                                              | NO          |
| tx_hash                  | FixedSizeBinary(32)                                                                 | NO          |
| to                       | FixedSizeBinary(20)                                                                 | YES         |
| nonce                    | UInt64                                                                              | NO          |
| gas_price                | Decimal128(38, 0)                                                                   | YES         |
| gas_limit                | UInt64                                                                              | NO          |
| value                    | Utf8                                                                                | NO          |
| input                    | Binary                                                                              | NO          |
| r                        | FixedSizeBinary(32)                                                                 | NO          |
| s                        | FixedSizeBinary(32)                                                                 | NO          |
| v_parity                 | Boolean                                                                             | NO          |
| chain_id                 | UInt64                                                                              | YES         |
| gas_used                 | UInt64                                                                              | NO          |
| type                     | Int32                                                                               | NO          |
| max_fee_per_gas          | Decimal128(38, 0)                                                                   | YES         |
| max_priority_fee_per_gas | Decimal128(38, 0)                                                                   | YES         |
| max_fee_per_blob_gas     | Decimal128(38, 0)                                                                   | YES         |
| from                     | FixedSizeBinary(20)                                                                 | NO          |
| status                   | Boolean                                                                             | NO          |
| state_root               | FixedSizeBinary(32)                                                                 | YES         |
| access_list              | List(Struct(address: FixedSizeBinary(20), storage_keys: List(FixedSizeBinary(32)))) | YES         |
| blob_versioned_hashes    | List(FixedSizeBinary(32))                                                           | YES         |
+--------------------------+-------------------------------------------------------------------------------------+-------------+
````
## logs
````
+-------------+-------------------------------+-------------+
| column_name | data_type                     | is_nullable |
+-------------+-------------------------------+-------------+
| _block_num  | UInt64                        | NO          |
| block_hash  | FixedSizeBinary(32)           | NO          |
| block_num   | UInt64                        | NO          |
| timestamp   | Timestamp(Nanosecond, +00:00) | NO          |
| tx_hash     | FixedSizeBinary(32)           | NO          |
| tx_index    | UInt32                        | NO          |
| log_index   | UInt32                        | NO          |
| address     | FixedSizeBinary(20)           | NO          |
| topic0      | FixedSizeBinary(32)           | YES         |
| topic1      | FixedSizeBinary(32)           | YES         |
| topic2      | FixedSizeBinary(32)           | YES         |
| topic3      | FixedSizeBinary(32)           | YES         |
| data        | Binary                        | NO          |
+-------------+-------------------------------+-------------+
````
