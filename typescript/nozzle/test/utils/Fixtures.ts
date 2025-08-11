import * as FileSystem from "@effect/platform/FileSystem"
import * as Path from "@effect/platform/Path"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"

/**
 * Service definition for the fixtures service.
 *
 * The `Fixtures` service can load fixtures from the fixtures directory
 * and decode them with a given schema.
 *
 * Useful for conveniently loading files for testing.
 */
export class Fixtures extends Context.Tag("Nozzle/Fixtures")<Fixtures, {
  /**
   * Loads a fixture from the fixtures directory.
   *
   * @param name - The name of the fixture to load.
   * @param schema - The schema to decode the fixture with.
   * @returns An effect that yields the decoded fixture.
   */
  readonly load: <A, I, R>(name: string, schema: Schema.Schema<A, I, R>) => Effect.Effect<A, never, R>
}>() {}

/**
 * Default layer for the fixtures service.
 */
export const layer = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const dir = path.join(import.meta.dirname, "..", "fixtures")

  const load = Effect.fn(function*<A, I, R>(name: string, schema: Schema.Schema<A, I, R>) {
    return yield* fs.readFileString(path.join(dir, name), "utf-8").pipe(
      Effect.map((_) => JSON.parse(_)),
      Effect.flatMap(Schema.decodeUnknown(schema)),
      Effect.orDie,
    )
  })

  return { load }
}).pipe(Layer.effect(Fixtures))
