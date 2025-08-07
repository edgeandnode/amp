import { createConnectTransport } from "@connectrpc/connect-web"
import { Cause, Effect, Exit, Schema } from 'effect'
import { Arrow, ArrowFlight } from "nozzl"

export const transport = createConnectTransport({ baseUrl: "/nozzle" })

const program = <T = ReadonlyArray<any>>(query: string) => Effect.gen(function*() {
  const flight = yield* ArrowFlight.ArrowFlight
  const table = yield* flight.table(query)
  const schema = Arrow.generateSchema(table.schema)
  const result = yield* Schema.encodeUnknown(Schema.Array(schema))([...table])
  return result as T
}).pipe(
  Effect.provide(ArrowFlight.ArrowFlight.withTransport(transport))
)

export class NozzleQueryRunner {
  async query<T = ReadonlyArray<any>>(query: string): Promise<ReadonlyArray<T>> {
    return Effect.runPromiseExit(program(query)).then((exit) => {
      if (Exit.isSuccess(exit)) {
        return exit.value
      }
      throw new Error(exit.cause.pipe(Cause.pretty))
    })
  }
}
