# Schema
Auto-generated file. See `schema_to_markdown` in `crates/core/common/src/catalog/mod.rs`.
## blocks
````
+-------------------------+---------------------+-------------+
| column_name             | data_type           | is_nullable |
+-------------------------+---------------------+-------------+
| _block_num              | UInt64              | NO          |
| block_num               | UInt64              | NO          |
| version                 | Utf8                | NO          |
| signature               | FixedSizeBinary(96) | NO          |
| proposer_index          | UInt64              | NO          |
| parent_root             | FixedSizeBinary(32) | NO          |
| state_root              | FixedSizeBinary(32) | NO          |
| randao_reveal           | FixedSizeBinary(96) | NO          |
| eth1_data_deposit_root  | FixedSizeBinary(32) | NO          |
| eth1_data_deposit_count | UInt64              | NO          |
| eth1_data_block_hash    | FixedSizeBinary(32) | NO          |
| graffiti                | FixedSizeBinary(32) | NO          |
+-------------------------+---------------------+-------------+
````
