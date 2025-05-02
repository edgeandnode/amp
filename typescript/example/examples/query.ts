import { Console, Effect, Pretty, Stream } from "effect"
import { JsonLines } from "nozzl"
import { Counts } from "./schema.js"

const pretty = Pretty.make(Counts)

const program = Effect.gen(function*() {
  const jsonl = yield* JsonLines.JsonLines
  const stream = jsonl.stream(Counts)`
    SELECT * FROM example.counts LIMIT 100
  `
  yield* Stream.runForEach(stream, (value) => Console.log(pretty(value)))
})

Effect.runPromise(program.pipe(Effect.provide(JsonLines.JsonLines.Default)))
