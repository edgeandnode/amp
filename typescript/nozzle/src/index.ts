import type { Context } from "./ConfigLoader.ts"
import type * as Model from "./Model.ts"

export type { Context } from "./ConfigLoader.ts"

export * as Api from "./Api.ts"
export * as Arrow from "./Arrow.ts"
export * as ArrowFlight from "./ArrowFlight.ts"
export * as JsonLines from "./JsonLines.ts"
export * as Model from "./Model.ts"

export const defineDataset = (fn: (context: Context) => Model.DatasetDefinition) => {
  return (context: Context) => fn(context)
}
