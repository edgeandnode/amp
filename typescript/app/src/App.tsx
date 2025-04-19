import { createConnectTransport } from "@connectrpc/connect-web"
import { Effect, Schema } from "effect"
import { ArrowFlight } from "nozzl"
import { useEffect, useState } from "react"

const program = Effect.gen(function*() {
  const flight = yield* ArrowFlight.ArrowFlight
  const table = yield* flight.table("SELECT * FROM transfers_eth_mainnet.erc20_transfers LIMIT 100")
  const schema = ArrowFlight.generateSchema(table.schema)
  const result = yield* Schema.encodeUnknown(Schema.Array(schema))([...table])
  return result
})

// TODO: Type resolution issue here.
const transport = createConnectTransport({ baseUrl: "/nozzle" }) as any
const runnable = program.pipe(Effect.provide(ArrowFlight.layer(transport)))

function App() {
  const [rows, setRows] = useState<ReadonlyArray<any>>([])

  useEffect(() => {
    const controller = new AbortController()
    Effect.runPromise(runnable, { signal: controller.signal }).then((_) => setRows(_))
    return () => controller.abort()
  }, [setRows])

  return (
    <ul>
      {rows.map((row) => <li key={row.id}>{JSON.stringify(row)}</li>)}
    </ul>
  )
}

export default App
