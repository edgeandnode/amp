import { Cause } from "effect"

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
