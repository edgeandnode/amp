export const abi = [
  {
    type: "event",
    name: "Incremented",
    inputs: [{ name: "count", type: "uint256", indexed: false }],
  },
  {
    type: "event",
    name: "Decremented",
    inputs: [{ name: "count", type: "uint256", indexed: false }],
  },
  {
    type: "function",
    name: "count",
    inputs: [],
    outputs: [{ name: "", type: "uint256" }],
    stateMutability: "view",
  },
  {
    type: "function",
    name: "increment",
    inputs: [],
    outputs: [],
    stateMutability: "nonpayable",
  },
  {
    type: "function",
    name: "decrement",
    inputs: [],
    outputs: [],
    stateMutability: "nonpayable",
  },
] as const
