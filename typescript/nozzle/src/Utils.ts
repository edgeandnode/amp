import { Cause, Effect, Function, HashSet } from "effect"
import { minimatch } from "minimatch"
import * as path from "node:path"
import * as Constants from "./Constants.js"

export const logCauseWith: {
  <E>(cause: Cause.Cause<E>, message: string): Effect.Effect<void>
  (message: string): <E>(cause: Cause.Cause<E>) => Effect.Effect<void>
} = Function.dual(2, (cause: Cause.Cause<any>, message: string) => Effect.logError(message, prettyCause(cause)))

export const prettyCause = <E>(cause: Cause.Cause<E>): string => {
  if (Cause.isInterruptedOnly(cause)) {
    return "All fibers interrupted without errors."
  }

  const stack = Cause.prettyErrors<E>(cause).flatMap((error) => {
    const output = (error.stack ?? "").split("\n").filter((line) => !line.trim().startsWith("at ")) ?? []
    return error.cause ? [output, ...renderCause(error.cause as Cause.PrettyError)] : [output]
  }).filter((lines) => lines.length > 0)

  if (stack.length <= 1) {
    return stack[0]?.join("\n") ?? ""
  }

  return stack.map((lines, index, array) => {
    const prefix = index === 0 ? "┌ " : index === array.length - 1 && lines.length === 1 ? "└ " : "├ "
    const output = lines.map((line, index) => `${index === 0 ? prefix : "│ "}${line}`).join("\n")
    return index === array.length - 1 ? output : `${output}\n│`
  }).join("\n")
}

const renderCause = (cause: Cause.PrettyError): Array<Array<string>> => {
  const output = (cause.stack ?? "").split("\n").filter((line) => !line.trim().startsWith("at ")) ?? []
  return cause.cause ? [output, ...renderCause(cause.cause as Cause.PrettyError)] : [output]
}

/**
 * Checks if a relative path should be included (not excluded) based on the exclude patterns.
 * Uses the minimatch library for proper glob pattern matching.
 *
 * @param relativePath The relative path to check
 * @param excludePatterns The set of patterns to match against
 * @returns true if the path should be included (does not match any exclude pattern), false if it should be excluded
 */
export function foundryOutputPathIncluded(
  relativePath: string,
  excludePatterns: HashSet.HashSet<string> = Constants.foundryOutputExludeList,
) {
  // Only include .json files
  if (!/\.(json)$/.test(path.extname(relativePath))) {
    return false
  }
  return !HashSet.some(excludePatterns, (pattern) => minimatch(relativePath, pattern))
}
