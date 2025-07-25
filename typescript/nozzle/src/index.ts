import type { Context } from "./ConfigLoader.ts"
import type * as Model from "./Model.ts"

export type { Context } from "./ConfigLoader.ts"

export * as Api from "./Api.js"
export * as Arrow from "./Arrow.js"
export * as ArrowFlight from "./ArrowFlight.js"
export * as JsonLines from "./JsonLines.js"
export * as Model from "./Model.js"
export * as StudioModel from "./Studio/Model.js"

export const defineDataset = (fn: (context: Context) => Model.DatasetDefinition) => {
  return (context: Context) => fn(context)
}
