/* eslint-disable @typescript-eslint/no-empty-object-type */
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import { FileSystem } from "@effect/platform/FileSystem"
import * as Schema from "effect/Schema"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"

export class PackageJson extends Schema.Class<PackageJson>("PackageJson")({
  name: Schema.String,
  version: Schema.String,
  description: Schema.String,
  type: Schema.String,
  private: Schema.optionalWith(Schema.Boolean, { default: () => false }),
  publishConfig: Schema.optional(Schema.Struct({
    provenance: Schema.optionalWith(Schema.Boolean, { default: () => false }),
  })),
  license: Schema.String,
  author: Schema.optional(
    Schema.Union(
      Schema.String,
      Schema.Struct({
        name: Schema.String,
        email: Schema.String,
        url: Schema.optional(Schema.String),
      }),
    ),
  ),
  repository: Schema.Union(
    Schema.String,
    Schema.Struct({
      type: Schema.String,
      url: Schema.String,
      directory: Schema.optional(Schema.String),
    }),
  ),
  homepage: Schema.optional(Schema.String),
  sideEffects: Schema.optionalWith(Schema.Array(Schema.String), {
    default: () => [],
  }),
  dependencies: Schema.optional(
    Schema.Record({ key: Schema.String, value: Schema.String }),
  ),
  peerDependencies: Schema.optional(
    Schema.Record({ key: Schema.String, value: Schema.String }),
  ),
  peerDependenciesMeta: Schema.optional(
    Schema.Record({
      key: Schema.String,
      value: Schema.Struct({ optional: Schema.Boolean }),
    }),
  ),
  optionalDependencies: Schema.optional(
    Schema.Record({ key: Schema.String, value: Schema.String }),
  ),
  gitHead: Schema.optional(Schema.String),
}) {
  static readonly decode = Schema.decodeUnknown(this)
}

const make = Effect.gen(function* () {
  const fs = yield* FileSystem

  const packageJson = fs.readFileString("./package.json").pipe(
    Effect.map(_ => JSON.parse(_)),
    Effect.flatMap(PackageJson.decode),
    Effect.withSpan("PackageContext/packageJson"),
  )

  return yield* packageJson
})

export interface PackageContext extends Effect.Effect.Success<typeof make> {}
export const PackageContext = Context.GenericTag<PackageContext>(
  "@effect/build-tools/PackageContext",
)
export const PackageContextLive = Layer.effect(PackageContext, make).pipe(
  Layer.provide(NodeFileSystem.layer),
)