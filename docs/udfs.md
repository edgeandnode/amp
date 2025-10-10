# Amp UDFs

Amp provides a number of "built-in" SQL functions that the user can call to manipulate
the data they are querying.

## `evm_decode_log`

```sql
T evm_decode_log(
    FixedSizeBinary(20) topic1,
    FixedSizeBinary(20) topic2,
    FixedSizeBinary(20) topic3,
    Binary data,
    Utf8 signature
)
```

Decodes an EVM event log. The signature parameter is the Solidity signature of the event.
The return type of `evm_decode_log` is the SQL version of the return type specified in the signature.

## `evm_topic`

```sql
FixedSizeBinary(32) evm_topic(Utf8 signature)
```

Returns the topic hash of the event signature. This is the first topic that will show up in the log
when the event is emitted. The topic hash is the keccak256 hash of the event signature.

## `${dataset}.eth_call`

```sql
(Binary, Utf8) ${dataset}.eth_call(
    FixedSizeBinary(20) from, # optional
    FixedSizeBinary(20) to,
    Binary input_data, # optional
    Utf8 block, # block number or tag (e.g. "1", "32", "latest")
)
```

This function executes an `eth_call` JSON-RPC against the provider of the specified EVM-RPC dataset. For example:

```sql
SELECT example_evm_rpc.eth_call(
    from,
    to,
    input,
    CAST(block_num as STRING))
FROM example_evm_rpc.transactions
LIMIT 10
```

Returns a tuple of the return value of the call and the error message (if any, or empty string if no error).

## `attestation_hash`

```sql
Binary attestation_hash(...)
```

This is an aggregate UDF which takes any number of parameters of any type. Returns a hash over all
the input parameters (columns) over all the rows.

## `evm_decode_params`

```sql
T evm_decode_params(
    Binary input,
    Utf8 signature
)
```

Decodes the Ethereum ABI-encoded parameters of a function. For example:

```sql
SELECT evm_decode_params(input, 'function approve(address _spender, uint256 _value)') AS params
FROM eth_rpc.transactions
```

All of the function parameters and results must be named. The output of this function will be packed into a struct:

```json
[
  {
    "params": {
      "_spender": "abea9132b05a70803a4e85094fd0e1800777fbef",
      "_value": "115792089237316195423570985008687907853269984665640564039457584007913129639935"
    }
  }
]
```

## `evm_encode_params`

```sql
T evm_encode_params(
    Any args...,
    Utf8 signature
)
```

ABI-encodes the given arguments into EVM parameters for the Solidity function corresponding to `signature`. `evm_encode_params`
takes the same number of arguments as the Solidity function corresponding to `signature`, plus the last `signature` argument.
Returns a binary value.

For example:

```sql
SELECT
    evm_encode_params(
        to,
        CAST(123123123 AS DECIMAL(41, 0)),
        input,
        'function example(address _to, uint256 _value, bytes _input) public returns (bool success)'
    ) AS encoded
FROM eth_rpc.transactions
```

Results:

```json
[
  {
    "encoded": "af32b01f000000000000000000000000beefbabeea323f07c59926295205d3b7a17e8638000000000000000000000000000000000000000000000000000000000756b5b30000000000000000000000000000000000000000000000000000000000000060000000000000000000000000000000000000000000000000000000000000008f000000020000000000000000000000000000000000000000000000000000000003e366320000000000000000000000000000000000000000000000005ea06407f0408000aaaebe6fe48e54f431b0c390cfaf0b017d09d42dc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2000bb800000000000000000000000000000000000000000000000000210fa439a7fc040000000000000000000000000000000000"
  }
]
```

## `evm_encode_type`

```sql
Binary evm_encode_type(
    Any value,
    Utf8 type
)
```

Encodes the given value as a Solidity type, corresponding to the type string `type`. Returns a binary value. For example:

```sql
SELECT evm_encode_type(CAST(635 AS DECIMAL(39, 0)), 'uint256') AS uint256
```

Returns:

```json
[
  {
    "uint256": "000000000000000000000000000000000000000000000000000000000000027b"
  }
]
```

## `evm_decode_type`

```sql
T evm_decode_type(
    Binary data,
    Utf8 type
)
```

Decodes the given Solidity ABI-encoded value into an SQL value.

Example with nested data types, first ABI-encodes some data using `evm_encode_type`, then decodes it:

```sql
SELECT evm_decode_type(encoded, '(int256, int8, string[])') AS decoded
    FROM (SELECT evm_encode_type(struct(1, 2, ['str1', 'str2', 'str3']), '(int256, int8, string[])') AS encoded)
```

Returns:

```json
[
  {
    "decoded": {
      "c0": "1",
      "c1": 2,
      "c2": ["str1", "str2", "str3"]
    }
  }
]
```
