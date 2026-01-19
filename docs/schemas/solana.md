# Schema
Auto-generated file. See `to_markdown` in `crates/core/datasets-raw/src/schema.rs`.

## block_headers
````
+---------------------+-----------+-------------+
| column_name         | data_type | is_nullable |
+---------------------+-----------+-------------+
| _block_num          | UInt64    | NO          |
| slot                | UInt64    | NO          |
| parent_slot         | UInt64    | NO          |
| block_hash          | Utf8      | NO          |
| previous_block_hash | Utf8      | NO          |
| block_height        | UInt64    | YES         |
| block_time          | Int64     | YES         |
+---------------------+-----------+-------------+
````
## transactions
````
+---------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+
| column_name               | data_type                                                                                                                                                                         | is_nullable |
+---------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+
| _block_num                | UInt64                                                                                                                                                                            | NO          |
| slot                      | UInt64                                                                                                                                                                            | NO          |
| tx_index                  | UInt32                                                                                                                                                                            | NO          |
| signatures                | List(Utf8)                                                                                                                                                                        | NO          |
| status                    | Boolean                                                                                                                                                                           | YES         |
| fee                       | UInt64                                                                                                                                                                            | YES         |
| pre_balances              | List(UInt64)                                                                                                                                                                      | YES         |
| post_balances             | List(UInt64)                                                                                                                                                                      | YES         |
| log_messages              | List(Utf8)                                                                                                                                                                        | YES         |
| pre_token_balances        | List(Struct(account_index: UInt8, mint: Utf8, ui_token_amount: Struct(ui_amount: Float64, decimals: UInt8, amount: Utf8, ui_amount_string: Utf8), owner: Utf8, program_id: Utf8)) | YES         |
| post_token_balances       | List(Struct(account_index: UInt8, mint: Utf8, ui_token_amount: Struct(ui_amount: Float64, decimals: UInt8, amount: Utf8, ui_amount_string: Utf8), owner: Utf8, program_id: Utf8)) | YES         |
| rewards                   | List(Struct(pubkey: Utf8, lamports: Int64, post_balance: UInt64, reward_type: Utf8, commission: UInt8))                                                                           | YES         |
| loaded_addresses_writable | List(Utf8)                                                                                                                                                                        | YES         |
| loaded_addresses_readonly | List(Utf8)                                                                                                                                                                        | YES         |
| return_data_program_id    | Utf8                                                                                                                                                                              | YES         |
| return_data_data          | List(UInt8)                                                                                                                                                                       | YES         |
| compute_units_consumed    | UInt64                                                                                                                                                                            | YES         |
| cost_units                | UInt64                                                                                                                                                                            | YES         |
+---------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+
````
## messages
````
+-----------------------+--------------------------------------------------------------------------------------------------------------------+-------------+
| column_name           | data_type                                                                                                          | is_nullable |
+-----------------------+--------------------------------------------------------------------------------------------------------------------+-------------+
| _block_num            | UInt64                                                                                                             | NO          |
| slot                  | UInt64                                                                                                             | NO          |
| tx_index              | UInt32                                                                                                             | NO          |
| header                | Struct(num_required_signatures: UInt8, num_readonly_signed_accounts: UInt8, num_readonly_unsigned_accounts: UInt8) | NO          |
| address_table_lookups | List(Struct(account_key: Utf8, writable_indexes: List(UInt8), readonly_indexes: List(UInt8)))                      | YES         |
| account_keys          | List(Utf8)                                                                                                         | NO          |
| recent_block_hash     | Utf8                                                                                                               | NO          |
+-----------------------+--------------------------------------------------------------------------------------------------------------------+-------------+
````
## instructions
````
+--------------------+-------------+-------------+
| column_name        | data_type   | is_nullable |
+--------------------+-------------+-------------+
| _block_num         | UInt64      | NO          |
| slot               | UInt64      | NO          |
| tx_index           | UInt32      | NO          |
| program_id_index   | UInt8       | NO          |
| accounts           | List(UInt8) | NO          |
| data               | List(UInt8) | NO          |
| inner_index        | UInt32      | YES         |
| inner_stack_height | UInt32      | YES         |
+--------------------+-------------+-------------+
````
