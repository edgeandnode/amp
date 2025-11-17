import * as KeyValueStore from "@effect/platform/KeyValueStore"
import * as Path from "@effect/platform/Path"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as NodeOS from "node:os"

/**
 * Provides a persistent key-value store backed by the local filesystem for
 * caching data that is frequently accessed by Amp components (e.g. the access
 * token used by the command-line interface).
 *
 * Cache files are stored in the user's home directory at `~/.amp/cache/`. This
 * location is automatically created if it doesn't exist when the cache is first
 * accessed.
 *
 * @example
 * ```ts
 * import { LocalCache } from "@edgeandnode/amp/LocalCache"
 *
 * const program = Effect.gen(function*() {
 *   const kv = yield* KeyValueStore.KeyValueStore
 *
 *   // Store a value
 *   yield* kv.set("myKey", "myValue")
 *
 *   // Retrieve a value
 *   const value = yield* kv.get("myKey")
 * }).pipe(Effect.provide(LocalKeyValueCache))
 * ```
 *
 * @see {@link https://effect.website/docs/guides/layers | Effect Layers Documentation}
 * @see {@link https://effect.website/docs/platform/key-value-store | KeyValueStore Documentation}
 */
export const LocalCache = Effect.gen(function*() {
  const path = yield* Path.Path

  const homeDirectory = NodeOS.homedir()
  const ampCachePath = path.join(homeDirectory, ".amp", "cache")

  return KeyValueStore.layerFileSystem(ampCachePath)
}).pipe(Layer.unwrapEffect)
