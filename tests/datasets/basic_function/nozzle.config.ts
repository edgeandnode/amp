import { defineDataset, functionSource } from "nozzl"

export default defineDataset(() => ({
  name: "basic_function",
  version: "0.1.0",
  dependencies: {
    mainnet: {
      owner: "graphprotocol",
      name: "mainnet",
      version: "0.1.0",
    },
  },
  tables: {},
  functions: {
    "testString": {
      inputTypes: [],
      outputType: "Utf8",
      source: functionSource("test_string.js"),
    },
  },
}))
