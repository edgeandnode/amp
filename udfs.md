# Nozzle UDFs

```sql
T evm_decode(
    FixedSizeBinary(20) topic1,
    FixedSizeBinary(20) topic2,
    FixedSizeBinary(20) topic3,
    Binary data,
    Utf8 signature
)
```

Decodes and EVM event log. The signature parameter is the Solidity signature of the event.
The return type of `evm_decode` is the SQL version of the return type specified in the signature.

```sql
FixedSizeBinary(32) evm_topic(Utf8 signature)
```

Returns the topic hash of the event signature. This is the first topic that will show up in the log
when the event is emitted. The topic hash is the keccak256 hash of the event signature.

```sql
(Binary, Utf8) ${dataset}.eth_call(
    FixedSizeBinary(20) from, # optional
    FixedSizeBinary(20) to,
    BIGINT UNSIGNED gas, # optional
    Decimal(38, 0) gas_price, #optional
    Decimal(38, 0) value, # optional
    Binary input_data, # optional
    Utf8 block, # block number or tag (e.g. "1", "32", "latest")
)
```

This function executes an `eth_call` JSON-RPC against the provider of the specified EVM-RPC dataset. For example:

```sql
SELECT example_evm_rpc.eth_call(
    from,
    to,
    CAST(300000000 AS BIGINT UNSIGNED),
    CAST(0 AS DECIMAL(38, 0)),
    value,
    input,
    CAST(block_num as STRING))
FROM example_evm_rpc.transactions
LIMIT 10
```

Returns a tuple of the return value of the call and the error message (if any, or empty string if no error).

```sql
Binary attestation_hash(...)
```

This is an aggregate UDF which takes any number of parameters of any type. Returns a hash over all
the input parameters (columns) over all the rows.
