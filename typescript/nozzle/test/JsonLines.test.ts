import { expect, it } from "@effect/vitest"
import { Chunk, Effect, Schema, Stream } from "effect"
import * as JsonLines from "../src/JsonLines.js"

it.layer(JsonLines.JsonLines.Default)((it) => {
  it.effect("should respond to a sql query", () =>
    Effect.gen(function*() {
      const jsonl = yield* JsonLines.JsonLines
      const schema = Schema.Struct({ a: Schema.Number, b: Schema.Number, c: Schema.Number, d: Schema.Number })
      const stream = jsonl.stream(schema)`
        SELECT * FROM VALUES (1, 2, 3, 4), (5, 6, 7, 8) AS TableLiteral(a, b, c, d)
      `

      const result = yield* stream.pipe(Stream.runCollect).pipe(Effect.map(Chunk.toArray))
      expect(result).toStrictEqual([
        { a: 1, b: 2, c: 3, d: 4 },
        { a: 5, b: 6, c: 7, d: 8 }
      ])
    }))

  it.effect("should decode errors in case of a bad request", () =>
    Effect.gen(function*() {
      const jsonl = yield* JsonLines.JsonLines
      const schema = Schema.Struct({ a: Schema.Number, b: Schema.Number, c: Schema.Number, d: Schema.Number })
      const stream = jsonl.stream(schema)`
        THIS IS A BAD QUERY
      `

      const result = yield* stream.pipe(Stream.runDrain).pipe(Effect.flip)
      expect(result).toBeInstanceOf(JsonLines.JsonLinesError)
      expect(result.message).toContain("Expected: an SQL statement, found: THIS")
    }))
})
