import { defineDataset } from "@edgeandnode/amp"

export default defineDataset((ctx) => ({
  name: "basic_function",
  network: "mainnet",
  version: "0.1.0",
  dependencies: {
    mainnet: {
      name: "mainnet",
      version: "0.0.0",
    },
  },
  tables: {},
  functions: {
    "testString": {
      inputTypes: [],
      outputType: "Utf8",
      source: ctx.functionSource("test_string.js"),
    },
  },
}))
