import { defineDataset, eventTables } from "@edgeandnode/amp"
import { abi } from "./abi.ts"

export default defineDataset(() => ({
  namespace: "edgeandnode",
  name: "amp_demo",
  network: "anvil",
  dependencies: {
    anvil_rpc: "_/anvil_rpc@0.0.0",
  },
  tables: eventTables(abi, "anvil_rpc"),
}))
