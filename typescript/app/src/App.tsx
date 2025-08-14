import { createConnectTransport } from "@connectrpc/connect-web"
import { Atom, Result, useAtomValue } from "@effect-atom/atom-react"
import { Cause, Effect, Schedule, Schema, Stream } from "effect"
import { Arrow, ArrowFlight } from "nozzl"

// TODO: Type resolution issue here.
const transport = createConnectTransport({ baseUrl: "/nozzle" }) as any
const runtime = Atom.runtime(ArrowFlight.layer(transport))

const fetch = Effect.gen(function*() {
  const flight = yield* ArrowFlight.ArrowFlight
  const table = yield* flight.table`SELECT * FROM example.counts ORDER BY block_num DESC LIMIT 100`
  const schema = Arrow.generateSchema(table.schema)
  const result = yield* Schema.encodeUnknown(Schema.Array(schema))([...table])
  return result
})

const poll = runtime.atom(Stream.repeatEffectWithSchedule(fetch, Schedule.spaced("1 second")))

const App = () => {
  const atom = useAtomValue(poll)
  return Result.match(atom, {
    onInitial: () => <div>Loading ...</div>,
    onFailure: (error) => <div>Error: {Cause.pretty(error.cause)}</div>,
    onSuccess: (success) => (
      <div>
        <ul>
          {success.value.map((row) => <li key={row.id}>{JSON.stringify(row)}</li>)}
        </ul>
      </div>
    ),
  })
}

export default App
