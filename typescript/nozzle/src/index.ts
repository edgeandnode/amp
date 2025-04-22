import { Schema } from "effect"
import * as Model from "./Model.js"

export * as Api from "./Api.js"
export * as Arrow from "./Arrow.js"
export * as ArrowFlight from "./ArrowFlight.js"
export * as JsonLines from "./JsonLines.js"
export * as Model from "./Model.js"

export const defineDataset = (fn: () => Model.DatasetDefinition) => {
  return Schema.decodeUnknownSync(Model.DatasetDefinition)(fn())
}
