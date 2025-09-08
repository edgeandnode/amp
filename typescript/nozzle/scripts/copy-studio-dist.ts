import { FileSystem, Path } from "@effect/platform"
import { NodeContext } from "@effect/platform-node"
import { Effect } from "effect"

const program = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path

  const src = path.resolve("../", "studio", "dist")
  const dest = path.resolve("./", "dist", "studio", "dist")

  yield* fs
    .makeDirectory(dest, { recursive: true })
    .pipe(Effect.andThen(() => fs.copy(src, dest, { overwrite: true })))

  return yield* Effect.logInfo("[Build] Copied studio/dist to dist/studio/dist")
}).pipe(Effect.provide(NodeContext.layer))

Effect.runPromise(program).catch(console.error)
