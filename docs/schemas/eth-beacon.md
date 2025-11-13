# Schema
Auto-generated file. See `to_markdown` in `crates/core/datasets-raw/src/schema.rs`.

## blocks
````
+-------------------------+---------------------+-------------+
| column_name             | data_type           | is_nullable |
+-------------------------+---------------------+-------------+
| _block_num              | UInt64              | NO          |
| block_num               | UInt64              | NO          |
| version                 | Utf8                | YES         |
| signature               | FixedSizeBinary(96) | YES         |
| proposer_index          | UInt64              | YES         |
| parent_root             | FixedSizeBinary(32) | YES         |
| state_root              | FixedSizeBinary(32) | YES         |
| randao_reveal           | FixedSizeBinary(96) | YES         |
| eth1_data_deposit_root  | FixedSizeBinary(32) | YES         |
| eth1_data_deposit_count | UInt64              | YES         |
| eth1_data_block_hash    | FixedSizeBinary(32) | YES         |
| graffiti                | FixedSizeBinary(32) | YES         |
+-------------------------+---------------------+-------------+
````
