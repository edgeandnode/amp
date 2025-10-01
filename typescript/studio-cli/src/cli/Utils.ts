import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Function from "effect/Function"

/**
 * Logs a cause with a message.
 *
 * @param cause - The cause to log.
 * @param message - The message to log.
 */
export const logCauseWith: {
  <E>(cause: Cause.Cause<E>, message: string): Effect.Effect<void>
  (message: string): <E>(cause: Cause.Cause<E>) => Effect.Effect<void>
} = Function.dual(2, (cause: Cause.Cause<any>, message: string) => Effect.logError(message, prettyCause(cause)))

/**
 * Pretty prints a cause.
 *
 * @param cause - The cause to pretty print.
 * @returns The pretty printed cause.
 */
export const prettyCause = <E>(cause: Cause.Cause<E>): string => {
  if (Cause.isInterruptedOnly(cause)) {
    return "All fibers interrupted without errors."
  }

  const stack = Cause.prettyErrors<E>(cause)
    .flatMap((error) => {
      const output = (error.stack ?? "").split("\n").filter((line) => !line.trim().startsWith("at ")) ?? []
      return error.cause ? [output, ...renderCause(error.cause as Cause.PrettyError)] : [output]
    })
    .filter((lines) => lines.length > 0)

  if (stack.length <= 1) {
    return stack[0]?.join("\n") ?? ""
  }

  return stack
    .map((lines, index, array) => {
      const prefix = index === 0 ? "┌ " : index === array.length - 1 && lines.length === 1 ? "└ " : "├ "
      const output = lines.map((line, index) => `${index === 0 ? prefix : "│ "}${line}`).join("\n")
      return index === array.length - 1 ? output : `${output}\n│`
    })
    .join("\n")
}

/**
 * Renders a cause.
 *
 * @param cause - The cause to render.
 * @returns The rendered cause.
 */
const renderCause = (cause: Cause.PrettyError): Array<Array<string>> => {
  const output = (cause.stack ?? "").split("\n").filter((line) => !line.trim().startsWith("at ")) ?? []
  return cause.cause ? [output, ...renderCause(cause.cause as Cause.PrettyError)] : [output]
}
