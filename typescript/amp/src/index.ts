import type { TableDefinition } from "@edgeandnode/amp/Model"
import { type Abi, type AbiEvent, formatAbiItem } from "abitype"

import type { Context } from "./ConfigLoader.ts"
import type * as Model from "./Model.ts"

export type { Context } from "./ConfigLoader.ts"

export * as Admin from "./api/Admin.ts"
export * as ArrowFlight from "./api/ArrowFlight.ts"
export * as ApiError from "./api/Error.ts"
export * as JsonLines from "./api/JsonLines.ts"
export * as Arrow from "./Arrow.ts"
export * as Model from "./Model.ts"
export * as StudioModel from "./studio/Model.ts"

export const defineDataset = (fn: (context: Context) => typeof Model.DatasetConfig.Encoded) => {
  return (context: Context) => fn(context)
}

export const eventQuery = (abi: AbiEvent) => {
  const signature = formatAbiItem(abi).replace(/^event /, "")
  // TODO: make this configurable?
  const logsColumns = ["block_hash", "tx_hash", "address", "block_num", "timestamp"]
  // TODO: prevent collision with potential `event` column in list.
  const eventColumn = `evm_decode_log(topic1, topic2, topic3, data, '${signature}') AS event`
  const logsQuery = `
    SELECT ${[...logsColumns, eventColumn].join(", ")}
    FROM anvil.logs
    WHERE topic0 = evm_topic('${signature}')
  `

  const eventColumns = abi.inputs.map((input) => input.name)
    .filter((name) => name !== undefined)
    .map((name) => `e.event['${name}'] AS ${camelToSnake(name)}`)

  const eventsQuery = `
    SELECT ${[...logsColumns, ...eventColumns].join(", ")}
    FROM (${logsQuery}) AS e
  `

  return eventsQuery
}

export const camelToSnake = (str: string) => str.replace(/([a-zA-Z])(?=[A-Z])/g, "$1_").toLowerCase()

export const eventTableName = (abi: AbiEvent) => camelToSnake(abi.name)
export const eventTable = (abi: AbiEvent) => ({
  sql: eventQuery(abi),
})

export const eventTables = (abi: Abi): Record<string, TableDefinition> => {
  const events = abi.filter((item) => item.type === "event").map((item) => [eventTableName(item), eventTable(item)])
  return Object.fromEntries(events)
}
