# Amp UDFs

Amp provides a number of "built-in" SQL functions that the user can call to manipulate
the data they are querying.

## `evm_encode_hex`

```sql
FixedSizeBinary(20) | FixedSizeBinary(32) evm_encode_hex(Utf8 hex_string)
```

Converts a hex-encoded string to a 20-byte binary value or 32-byte binary value. Accepts addresses with or without the `0x` prefix.

Example:

```sql
SELECT address
FROM eth_rpc.logs
WHERE address = evm_encode_hex('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
```

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

## `evm_decode_hex`

```sql
Utf8 evm_decode_hex(
    FixedSizeBinary(20) | FixedSizeBinary(32) binary
)
```

Converts a binary EVM address or bytes32 to hex string with `0x` prefix.

For example:

```sql
SELECT evm_decode_hex(address) AS address
FROM eth_rpc.logs
```

Returns:

```json
[
  {
    "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
  }
]
```

Works with both 20-byte addresses and 32-byte hashes.
Returns null if input is null.

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

## `shift_units`

```sql
Utf8 shift_units(T value, Int64 units)
```

**Supported input types for `value`:**
- `Utf8` (string)
- `Int8`, `Int16`, `Int32`, `Int64`
- `UInt8`, `UInt16`, `UInt32`, `UInt64`
- `Float32`, `Float64`
- `Decimal128`, `Decimal256`

Shifts the decimal point of a numeric value by the specified number of places:

- **Positive units**: Shifts right (multiplies by 10^units) - converts human-readable amounts to raw units
- **Negative units**: Shifts left (divides by 10^units) - converts raw units to human-readable amounts

For example:

```sql
-- Convert 1.5 ETH to wei (shift right by 18 places)
SELECT shift_units('1.5', 18) AS wei
-- Returns: "1500000000000000000"

-- Convert wei back to ETH (shift left by 18 places)
SELECT shift_units('1500000000000000000', -18) AS eth
-- Returns: "1.5"
```

More examples:

```sql
-- Convert 100 USDC (6 decimals) to raw units
SELECT shift_units('100', 6) AS usdc_raw
-- Returns: "100000000"

-- Convert raw USDC units back to human-readable
SELECT shift_units('100000000', -6) AS usdc
-- Returns: "100"

-- Convert a precise ETH amount to wei
SELECT shift_units('1.376988483056381409', 18) AS wei
-- Returns: "1376988483056381409"
```
