import * as NodeSink from "@effect/platform-node/NodeSink"
import * as Socket from "@effect/platform/Socket"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Function from "effect/Function"
import * as Match from "effect/Match"
import * as Schedule from "effect/Schedule"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Net from "node:net"

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

/**
 * Creates an effect that waits for a port to be open.
 *
 * Retries indefinitely until the port is open.
 *
 * @param port - The port to wait for.
 * @returns An effect that yields once the port is open.
 */
export const waitForPort = Effect.fn(function*(port: number) {
  return yield* Effect.async<Net.Socket, Socket.SocketError, never>((resume, signal) => {
    const connection = Net.createConnection({ port, signal })
    connection.on("connect", () => {
      connection.removeAllListeners()
      resume(Effect.succeed(connection))
    })
    connection.on("error", (cause) => {
      connection.removeAllListeners()
      resume(Effect.fail(new Socket.SocketGenericError({ reason: "Open", cause })))
    })
  }).pipe(
    Effect.retry({
      schedule: Schedule.spaced("100 millis"),
      while: (cause) => Socket.isSocketError(cause) && cause.reason === "Open",
    }),
    Effect.tap((connection) => Effect.try(() => connection.destroy()).pipe(Effect.ignore)),
    Effect.asVoid,
  )
})

/**
 * Adds a line prefix to a stream.
 *
 * @param stream - The stream to add the line prefix to.
 * @param prefix - The line prefix to add.
 * @returns The stream with the line prefix added.
 */
export const withLinePrefix: {
  <E, R>(stream: Stream.Stream<Uint8Array<ArrayBufferLike>, E, R>, prefix: string): Stream.Stream<string, E, R>
  (prefix: string): <E, R>(stream: Stream.Stream<Uint8Array<ArrayBufferLike>, E, R>) => Stream.Stream<string, E, R>
} = Function.dual(2, <E, R>(stream: Stream.Stream<Uint8Array<ArrayBufferLike>, E, R>, prefix: string) => {
  return stream.pipe(
    Stream.decodeText("utf-8"),
    Stream.splitLines,
    Stream.map((line) => `${prefix} ${line}\n`),
  )
})

/**
 * Forwards the given stdout and stderr streams to the respective node sinks.
 *
 * @param options.which - Which of the streams to forward. Defaults to "none".
 * @param options.stdout - The stdout stream to forward to the node sink.
 * @param options.stderr - The stderr stream to forward to the node sink.
 */
export const intoNodeSink = <E, E2, R, R2>({
  stderr = Stream.empty,
  stdout = Stream.empty,
  which = "none",
}: {
  which?: "stdout" | "stderr" | "both" | "none" | undefined
  stdout?: Stream.Stream<string | Uint8Array<ArrayBufferLike>, E, R> | undefined
  stderr?: Stream.Stream<string | Uint8Array<ArrayBufferLike>, E2, R2> | undefined
}): Effect.Effect<void, never, R | R2 | Scope.Scope> => {
  return Match.value(which).pipe(
    Match.when("none", () => Effect.void),
    Match.when("stdout", () => stdout.pipe(Stream.run(NodeSink.stdout), Effect.forkScoped)),
    Match.when("stderr", () => stderr.pipe(Stream.run(NodeSink.stderr), Effect.forkScoped)),
    Match.when("both", () =>
      Effect.all([
        stdout.pipe(Stream.run(NodeSink.stdout), Effect.forkScoped),
        stderr.pipe(Stream.run(NodeSink.stderr), Effect.forkScoped),
      ])),
    Match.exhaustive,
    Effect.asVoid,
  )
}
