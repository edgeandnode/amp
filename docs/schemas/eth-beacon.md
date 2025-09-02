# Schema
Auto-generated file. See `schema_to_markdown` in `common/src/catalog/mod.rs`.
## blocks
````
+---------------------+---------------------+-------------+
| column_name         | data_type           | is_nullable |
+---------------------+---------------------+-------------+
| _block_num          | UInt64              | NO          |
| block_num           | UInt64              | NO          |
| version             | Utf8                | NO          |
| proposer_index      | UInt64              | NO          |
| parent_root         | FixedSizeBinary(32) | NO          |
| state_root          | FixedSizeBinary(32) | NO          |
| execution_block_num | UInt64              | NO          |
+---------------------+---------------------+-------------+
````
