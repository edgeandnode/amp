/**
 * Mock UDF definitions for testing SQL intellisense features
 * Provides all 8 Amp UDF functions with realistic test data
 */

import type { UserDefinedFunction } from "../../../../src/services/sql/types"

export const mockUDFs: ReadonlyArray<UserDefinedFunction> = [
  {
    name: "evm_decode_log",
    description:
      "Decodes an EVM event log. The signature parameter is the Solidity signature of the event. The return type of `evm_decode_log` is the SQL version of the return type specified in the signature.",
    sql: "evm_decode_log(topic1, topic2, topic3, data, signature)",
    parameters: ["topic1", "topic2", "topic3", "data", "signature"],
    example:
      "SELECT evm_decode_log(topics[1], topics[2], topics[3], data, 'Transfer(address indexed from, address indexed to, uint256 value)') FROM anvil.logs",
  },
  {
    name: "evm_topic",
    description:
      "Returns the topic hash of the event signature. This is the first topic that will show up in the log when the event is emitted. The topic hash is the keccak256 hash of the event signature.",
    sql: "evm_topic(signature)",
    parameters: ["signature"],
    example:
      "SELECT * FROM anvil.logs WHERE topics[0] = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)')",
  },
  {
    name: "${dataset}.eth_call",
    description:
      "This function executes an `eth_call` JSON-RPC against the provider of the specified EVM-RPC dataset. Returns a tuple of the return value of the call and the error message (if any, or empty string if no error).",
    sql: "${dataset}.eth_call(from, to, input_data, block_specification)",
    parameters: ["from", "to", "input_data", "block_specification"],
    example:
      "SELECT anvil.eth_call('0x0000000000000000000000000000000000000000', '0x1234567890123456789012345678901234567890', '0x70a08231', 'latest')",
  },
  {
    name: "attestation_hash",
    description:
      "This is an aggregate UDF which takes any number of parameters of any type. Returns a hash over all the input parameters (columns) over all the rows.",
    sql: "attestation_hash(column1, column2, ...)",
    parameters: ["column1", "column2"],
    example: "SELECT attestation_hash(block_number, transaction_hash) FROM anvil.transactions",
  },
  {
    name: "evm_decode_params",
    description:
      "Decodes the Ethereum ABI-encoded parameters of a function. All of the function parameters and results must be named. The output of this function will be packed into a struct.",
    sql: "evm_decode_params(input_data, signature)",
    parameters: ["input_data", "signature"],
    example: "SELECT evm_decode_params(input, 'transfer(address to, uint256 amount)') FROM anvil.transactions",
  },
  {
    name: "evm_encode_params",
    description:
      "ABI-encodes the given arguments into EVM parameters for the Solidity function corresponding to `signature`. `evm_encode_params` takes the same number of arguments as the Solidity function corresponding to `signature`, plus the last `signature` argument. Returns a binary value.",
    sql: "evm_encode_params(arg1, arg2, ..., signature)",
    parameters: ["arg1", "arg2", "signature"],
    example:
      "SELECT evm_encode_params('0x1234567890123456789012345678901234567890', 1000, 'transfer(address to, uint256 amount)')",
  },
  {
    name: "evm_encode_type",
    description:
      "Encodes the given value as a Solidity type, corresponding to the type string `type`. Returns a binary value.",
    sql: "evm_encode_type(value, type)",
    parameters: ["value", "type"],
    example: "SELECT evm_encode_type(1000, 'uint256')",
  },
  {
    name: "evm_decode_type",
    description: "Decodes the given Solidity ABI-encoded value into an SQL value.",
    sql: "evm_decode_type(data, type)",
    parameters: ["data", "type"],
    example: "SELECT evm_decode_type(data, 'uint256') FROM anvil.logs WHERE LENGTH(data) = 32",
  },
] as const

/**
 * Minimal UDF set for testing basic functionality
 */
export const mockUDFsMinimal: ReadonlyArray<UserDefinedFunction> = [
  {
    name: "evm_topic",
    description: "Returns the topic hash of the event signature.",
    sql: "evm_topic(signature)",
    parameters: ["signature"],
    example: "SELECT evm_topic('Transfer(address,address,uint256)')",
  },
] as const

/**
 * Empty UDF set for testing graceful degradation
 */
export const mockUDFsEmpty: ReadonlyArray<UserDefinedFunction> = [] as const

/**
 * Helper function to get UDF by name
 */
export function getUDFByName(udfs: ReadonlyArray<UserDefinedFunction>, name: string): UserDefinedFunction | undefined {
  return udfs.find((udf) => udf.name === name)
}

/**
 * Helper function to get all UDF names
 */
export function getAllUDFNames(udfs: ReadonlyArray<UserDefinedFunction>): Array<string> {
  return udfs.map((udf) => udf.name)
}

/**
 * Helper function to filter UDFs by name pattern
 */
export function filterUDFsByPattern(
  udfs: ReadonlyArray<UserDefinedFunction>,
  pattern: string,
): ReadonlyArray<UserDefinedFunction> {
  const regex = new RegExp(pattern, "i")
  return udfs.filter((udf) => regex.test(udf.name))
}
