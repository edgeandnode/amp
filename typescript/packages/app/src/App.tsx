import { createConnectTransport } from "@connectrpc/connect-web"
import { Atom, Result, useAtomValue } from "@effect-atom/atom-react"
import { Cause, Effect, Schedule, Schema, Stream } from "effect"
import { Arrow, ArrowFlight } from "nozzl"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./components/ui/table.tsx"

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

// Helper function to format hash values
const formatHash = (hash: string) => {
  return `${hash.slice(0, 8)}...${hash.slice(-6)}`
}

// Helper function to format timestamp
const formatTimestamp = (timestamp: string | number) => {
  const date = new Date(timestamp)
  return date.toLocaleString()
}

// Helper function to copy text to clipboard
const copyToClipboard = (text: string) => {
  navigator.clipboard.writeText(text).catch(() => {})
}

const App = () => {
  const atom = useAtomValue(poll)

  return (
    <div className="container mx-auto py-8 px-4">
      <div className="mb-8">
        <h1 className="text-3xl font-bold mb-2">Nozzle Data Explorer</h1>
      </div>

      {Result.match(atom, {
        onInitial: () => (
          <div className="flex items-center justify-center py-16">
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 border-2 border-primary border-t-transparent rounded-full animate-spin" />
              <span className="text-muted-foreground">Loading data...</span>
            </div>
          </div>
        ),
        onFailure: (error) => (
          <div className="border border-destructive/50 bg-destructive/10 text-destructive rounded-lg p-4">
            <h3 className="font-medium mb-2">Error loading data</h3>
            <pre className="text-sm opacity-80 overflow-auto">
              {Cause.pretty(error.cause)}
            </pre>
          </div>
        ),
        onSuccess: (success) => (
          <div className="border rounded-lg">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Block Number</TableHead>
                  <TableHead>Timestamp</TableHead>
                  <TableHead>Address</TableHead>
                  <TableHead>Transaction Hash</TableHead>
                  <TableHead>Block Hash</TableHead>
                  <TableHead>Count</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {success.value.length === 0 ?
                  (
                    <TableRow>
                      <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                        No data available
                      </TableCell>
                    </TableRow>
                  ) :
                  (
                    success.value.map((row: any, index: number) => (
                      <TableRow key={`${row.block_num}-${row.tx_hash}-${index}`}>
                        <TableCell className="font-mono">
                          {row.block_num?.toString()}
                        </TableCell>
                        <TableCell>
                          {formatTimestamp(row.timestamp)}
                        </TableCell>
                        <TableCell>
                          <button
                            className="font-mono text-sm hover:bg-muted px-2 py-1 rounded transition-colors"
                            onClick={() =>
                              copyToClipboard(row.address)}
                            title="Click to copy full address"
                          >
                            {formatHash(row.address)}
                          </button>
                        </TableCell>
                        <TableCell>
                          <button
                            className="font-mono text-sm hover:bg-muted px-2 py-1 rounded transition-colors"
                            onClick={() => copyToClipboard(row.tx_hash)}
                            title="Click to copy full transaction hash"
                          >
                            {formatHash(row.tx_hash)}
                          </button>
                        </TableCell>
                        <TableCell>
                          <button
                            className="font-mono text-sm hover:bg-muted px-2 py-1 rounded transition-colors"
                            onClick={() => copyToClipboard(row.block_hash)}
                            title="Click to copy full block hash"
                          >
                            {formatHash(row.block_hash)}
                          </button>
                        </TableCell>
                        <TableCell className="font-medium">
                          {row.count?.toString()}
                        </TableCell>
                      </TableRow>
                    ))
                  )}
              </TableBody>
            </Table>
          </div>
        ),
      })}
    </div>
  )
}

export default App
