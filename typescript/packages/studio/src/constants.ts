import { Schema } from "effect"

// Determine API origin based on environment
// In development (pnpm run dev): use full URL with localhost
// In production (built and served via nozzle CLI): use relative path
export const API_ORIGIN = import.meta.env.MODE === "production"
  ? "/api/v1"
  : import.meta.env.VITE_API_URL || "http://localhost:1615/api/v1"

export const RESERVED_FIELDS = new Set(["from", "select", "limit", "order"])

export const UserDefinedFunction = Schema.Struct({
  name: Schema.NonEmptyTrimmedString,
  description: Schema.String,
  sql: Schema.String,
})
export type UserDefinedFunction = typeof UserDefinedFunction.Type
export const USER_DEFINED_FUNCTIONS: ReadonlyArray<UserDefinedFunction> = [
  {
    name: "evm_decode_log",
    description:
      "Decodes an EVM event log. The signature parameter is the Solidity signature of the event. The return type of `evm_decode_log` is the SQL version of the return type specified in the signature.",
    sql: `T evm_decode_log(
  FixedSizeBinary(20) topic1,
  FixedSizeBinary(20) topic2,
  FixedSizeBinary(20) topic3,
  Binary data,
  Utf8 signature
)
`,
  },
  {
    name: "evm_topic",
    description:
      "Returns the topic hash of the event signature. This is the first topic that will show up in the log when the event is emitted. The topic hash is the keccak256 hash of the event signature.",
    sql: `FixedSizeBinary(32) evm_topic(Utf8 signature)`,
  },
  {
    name: "${dataset}.eth_call",
    description:
      "This function executes an `eth_call` JSON-RPC against the provider of the specified EVM-RPC dataset. Returns a tuple of the return value of the call and the error message (if any, or empty string if no error).",
    sql: `(Binary, Utf8) {dataset}.eth_call(
  FixedSizeBinary(20) from, # optional
  FixedSizeBinary(20) to,
  Binary input_data, # optional
  Utf8 block, # block number or tag (e.g. "1", "32", "latest")
)    
`,
  },
  {
    name: "attestation_hash",
    description:
      "This is an aggregate UDF which takes any number of parameters of any type. Returns a hash over all the input parameters (columns) over all the rows.",
    sql: `Binary attestation_hash(...)`,
  },
  {
    name: "evm_decode_params",
    description:
      "Decodes the Ethereum ABI-encoded parameters of a function. All of the function parameters and results must be named. The output of this function will be packed into a struct.",
    sql: `T evm_decode_params(
  Binary input,
  Utf8 signature
)
`,
  },
  {
    name: "evm_encode_params",
    description:
      "ABI-encodes the given arguments into EVM parameters for the Solidity function corresponding to `signature`. `evm_encode_params` takes the same number of arguments as the Solidity function corresponding to `signature`, plus the last `signature` argument. Returns a binary value.",
    sql: `T evm_encode_params(
  Any args...,
  Utf8 signature
)
`,
  },
  {
    name: "evm_encode_type",
    description:
      "Encodes the given value as a Solidity type, corresponding to the type string `type`. Returns a binary value.",
    sql: `Binary evm_encode_type(
  Any value,
  Utf8 type
)
`,
  },
  {
    name: "evm_decode_type",
    description: "Decodes the given Solidity ABI-encoded value into an SQL value.",
    sql: `T evm_decode_type(
  Binary data,
  Utf8 type
)
`,
  },
]
