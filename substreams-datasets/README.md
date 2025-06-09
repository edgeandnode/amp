# Dataset Configuration

A Substreams base dataset is configured through a dataset definition file and a provider file with
the connection details. The [example_config](example_config) documents the necessary fields.

## Example

To dump substreams output you need to provide substreams .SPKG package and output module under the
`manifest` and `module` fields of the dataset definition.

The following types of substreams modules can be used:

- `db_out`-like modules emitting `DatabaseChanges` output. For this type of module to work, the substreams .SPKG manifest must contain `sink` section with embedded `schema.sql` schema. Corresponding Arrow schema is inferred from `schema.sql`. See this manifest as an example: [WETH token contract](https://github.com/pinax-network/weth-substreams/blob/main/substreams.sql.yaml#L50-L57).
  An example dataset using WETH ERC20 token smart contract:

```toml
kind = "substreams"
name = "weth"
module = "db_out"
manifest = "=https://spkg.io/pinax-network/weth-v0.1.0.spkg"
```

With that file in your `dataset_defs_dir`, you can dump the data:
  ```bash
  cargo run --release -p dump -- --dataset=weth -s=18000000 -e=18100000
  ```

- Arbitrary map modules with repeated messages in the output. Schema in this case is inferred from the Protobuf message descriptors included in the substreams .SPKG package.
  For example, a module with the following output:
  ```proto
  message Events {
    repeated Transfer transfers = 1;
    repeated Mint mints = 2;
    repeated Burn burns = 3;
  }
  ```
  will produce `transfers`, `mints`, and `burns` parquet files, where each column matches the field of the corresponding event message type. All non-repeated fields in the module output are dropped from the schema.
