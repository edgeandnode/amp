import type { Context } from "./ConfigLoader.ts"
import type * as Model from "./Model.ts"

export type { Context } from "./ConfigLoader.ts"

export * as Admin from "./api/Admin.ts"
export * as ArrowFlight from "./api/ArrowFlight.ts"
export * as ApiError from "./api/Error.ts"
export * as JsonLines from "./api/JsonLines.ts"
export * as Arrow from "./Arrow.ts"
export * as Model from "./Model.ts"
export * as StudioModel from "./Studio/Model.js"

export const defineDataset = (fn: (context: Context) => Model.DatasetDefinition) => {
  return (context: Context) => fn(context)
}
