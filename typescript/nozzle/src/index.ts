import { readFileSync } from "fs"
import type { Context } from "./ConfigLoader.js"
import * as Model from "./Model.js"

export type { Context } from "./ConfigLoader.js"

export * as Api from "./Api.js"
export * as Arrow from "./Arrow.js"
export * as ArrowFlight from "./ArrowFlight.js"
export * as JsonLines from "./JsonLines.js"
export * as Model from "./Model.js"

export const defineDataset = (fn: (context: Context) => Model.DatasetDefinition) => {
  return (context: Context) => fn(context)
}

/// Reads the source code file at `path`
export function functionSource(path: string) {
  const func = new Model.FunctionSource({
    source: readFileSync(path, "utf8"),
    filename: path.split("/").pop() as string,
  })
  return func
}
