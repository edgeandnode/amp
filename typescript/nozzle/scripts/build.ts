import * as path from "node:path"
import { NodeFileSystem } from "@effect/platform-node"
import { FileSystem } from "@effect/platform"
import { Effect, pipe } from "effect"

const read = pipe(
  FileSystem.FileSystem,
  Effect.flatMap(fileSystem => fileSystem.readFileString("dist/package.json")),
  Effect.map(_ => JSON.parse(_)),
  Effect.map(json => {
    json.bin = "./dist/esm/cli/main.js"
    return json
  }),
)

const to = path.join("dist", "package.json")
const write = (pkg: object) =>
  pipe(
    FileSystem.FileSystem,
    Effect.flatMap(fileSystem =>
      fileSystem.writeFileString(to, JSON.stringify(pkg, null, 2))
    ),
  )

const program = pipe(
  Effect.sync(() => console.log(`copying package.json to ${to}...`)),
  Effect.flatMap(() => read),
  Effect.flatMap(write),
  Effect.provide(NodeFileSystem.layer),
)

Effect.runPromise(program)
