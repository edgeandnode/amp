import { createConnectTransport } from "@connectrpc/connect-web"
import { Effect, Exit, Schema } from "effect"
import { ArrowFlight } from "nozzl"

const program = (query: string) =>
  Effect.gen(function*() {
    const flight = yield* ArrowFlight.ArrowFlight
    const table = yield* flight.table(query)
    const schema = ArrowFlight.generateSchema(table.schema)
    const result = yield* Schema.encodeUnknown(Schema.Array(schema))([...table])
    return result
  })

// TODO: Type resolution issue here.
const transport = createConnectTransport({ baseUrl: "/nozzle" }) as any

export async function runQuery(query: string, controller = new AbortController()) {
  const runnable = program(query).pipe(Effect.provide(ArrowFlight.layer(transport)))

  const responseExit = await Effect.runPromiseExit(runnable, { signal: controller.signal })

  if (Exit.isSuccess(responseExit)) {
    return responseExit.value
  } else if (!Exit.isInterrupted(responseExit)) {
    console.error(responseExit.cause)
    throw responseExit.cause
  }
}
