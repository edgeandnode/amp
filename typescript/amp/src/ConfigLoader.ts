import * as FileSystem from "@effect/platform/FileSystem"
import * as Path from "@effect/platform/Path"
import type * as Cause from "effect/Cause"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import * as Match from "effect/Match"
import * as Predicate from "effect/Predicate"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as fs from "node:fs"
import * as path from "node:path"
import * as ManifestBuilder from "./ManifestBuilder.ts"
import * as Model from "./Model.ts"

export class Context {
  public definitionPath: string

  constructor(definitionPath: string) {
    this.definitionPath = definitionPath
  }

  /// Reads a file relative to the directory of the dataset definition
  functionSource(relativePath: string): Model.FunctionSource {
    const baseDir = path.dirname(this.definitionPath)
    const fullPath = path.join(baseDir, relativePath)

    let source: string
    try {
      source = fs.readFileSync(fullPath, "utf8")
    } catch (err: any) {
      throw new Error(
        `Failed to read function source at ${fullPath}: ${err.message}`,
      )
    }

    const func = new Model.FunctionSource({
      source,
      filename: path.basename(fullPath),
    })
    return func
  }
}

export class ConfigLoaderError extends Data.TaggedError("ConfigLoaderError")<{
  readonly cause?: unknown
  readonly message?: string
}> {}

export class ConfigLoader extends Effect.Service<ConfigLoader>()(
  "Amp/ConfigLoader",
  {
    dependencies: [ManifestBuilder.ManifestBuilder.Default],
    effect: Effect.gen(function*() {
      const path = yield* Path.Path
      const fs = yield* FileSystem.FileSystem
      const builder = yield* ManifestBuilder.ManifestBuilder

      const jiti = yield* Effect.tryPromise({
        try: () =>
          import("jiti").then(({ createJiti }) =>
            createJiti(import.meta.url, {
              moduleCache: false,
              tryNative: false,
            })
          ),
        catch: (cause) => new ConfigLoaderError({ cause }),
      }).pipe(Effect.cached)

      const loadTypeScript = Effect.fnUntraced(function*(file: string) {
        return yield* Effect.tryMapPromise(jiti, {
          try: (jiti) =>
            jiti.import<(context: Context) => Model.DatasetConfig>(file, {
              default: true,
            }),
          catch: (cause) => cause,
        }).pipe(
          Effect.map((callback) => callback(new Context(file))),
          Effect.flatMap(Schema.decodeUnknown(Model.DatasetConfig)),
          Effect.mapError(
            (cause) =>
              new ConfigLoaderError({
                cause,
                message: `Failed to load config file ${file}`,
              }),
          ),
        )
      })

      const loadJavaScript = Effect.fnUntraced(function*(file: string) {
        return yield* Effect.tryPromise({
          try: () =>
            import(file).then(
              (module) => module.default as (context: Context) => Model.DatasetConfig,
            ),
          catch: (cause) => cause,
        }).pipe(
          Effect.map((callback) => callback(new Context(file))),
          Effect.flatMap(Schema.decodeUnknown(Model.DatasetConfig)),
          Effect.mapError(
            (cause) =>
              new ConfigLoaderError({
                cause,
                message: `Failed to load config file ${file}`,
              }),
          ),
        )
      })

      const loadJson = Effect.fnUntraced(function*(file: string) {
        return yield* Effect.tryMap(fs.readFileString(file), {
          try: (content) => JSON.parse(content),
          catch: (cause) => cause,
        }).pipe(
          Effect.flatMap(Schema.decodeUnknown(Model.DatasetConfig)),
          Effect.mapError(
            (cause) =>
              new ConfigLoaderError({
                cause,
                message: `Failed to load config file ${file}`,
              }),
          ),
        )
      })

      const fileMatcher = Match.type<string>().pipe(
        Match.when(
          (_) => /\.(ts|mts|cts)$/.test(path.extname(_)),
          (_) => loadTypeScript(_),
        ),
        Match.when(
          (_) => /\.(js|mjs|cjs)$/.test(path.extname(_)),
          (_) => loadJavaScript(_),
        ),
        Match.when(
          (_) => /\.(json)$/.test(path.extname(_)),
          (_) => loadJson(_),
        ),
        Match.orElse(
          (_) =>
            new ConfigLoaderError({
              message: `Unsupported file extension ${path.extname(_)}`,
            }),
        ),
      )

      const load = Effect.fnUntraced(function*(file: string) {
        const resolved = path.resolve(file)
        return yield* fileMatcher(resolved)
      })

      const build = Effect.fnUntraced(function*(file: string) {
        const config = yield* load(file)
        return yield* builder.build(config).pipe(
          Effect.mapError(
            (cause) =>
              new ConfigLoaderError({
                cause,
                message: `Failed to build config file ${file}`,
              }),
          ),
        )
      })

      const find = Effect.fnUntraced(function*(cwd: string = ".") {
        const candidates = [
          path.resolve(cwd, `amp.config.ts`),
          path.resolve(cwd, `amp.config.mts`),
          path.resolve(cwd, `amp.config.cts`),
          path.resolve(cwd, `amp.config.js`),
          path.resolve(cwd, `amp.config.mjs`),
          path.resolve(cwd, `amp.config.cjs`),
          path.resolve(cwd, `amp.config.json`),
        ]
        return yield* Effect.findFirst(candidates, (_) => fs.exists(_).pipe(Effect.orElseSucceed(() => false)))
      })

      const watch = <E, R>(
        file: string,
        options?: {
          onError?: (
            cause: Cause.Cause<ConfigLoaderError>,
          ) => Effect.Effect<void, E, R>
        },
      ): Stream.Stream<
        ManifestBuilder.ManifestBuildResult,
        ConfigLoaderError | E,
        R
      > => {
        const resolved = path.resolve(file)
        const open = load(resolved).pipe(
          Effect.tapErrorCause(options?.onError ?? (() => Effect.void)),
          Effect.either,
        )

        const updates = fs.watch(resolved).pipe(
          Stream.buffer({ capacity: 1, strategy: "sliding" }),
          Stream.mapError(
            (cause) =>
              new ConfigLoaderError({
                cause,
                message: "Failed to watch config file",
              }),
          ),
          Stream.filter(Predicate.isTagged("Update")),
          Stream.mapEffect(() => open),
        )

        const build = (config: Model.DatasetConfig) =>
          builder.build(config).pipe(
            Effect.mapError(
              (cause) =>
                new ConfigLoaderError({
                  cause,
                  message: `Failed to build config file ${file}`,
                }),
            ),
            Effect.tapErrorCause(options?.onError ?? (() => Effect.void)),
            Effect.either,
          )

        return Stream.fromEffect(open).pipe(
          Stream.concat(updates),
          Stream.filterMap(Either.getRight),
          Stream.changesWith(Schema.equivalence(Model.DatasetConfig)),
          Stream.mapEffect((config) => build(config)),
          Stream.filterMap(Either.getRight),
          Stream.changesWith(
            (a, b) =>
              Schema.equivalence(Model.DatasetMetadata)(
                a.metadata,
                b.metadata,
              ) &&
              Schema.equivalence(Model.DatasetDerived)(a.manifest, b.manifest),
          ),
        ) as Stream.Stream<
          ManifestBuilder.ManifestBuildResult,
          ConfigLoaderError | E,
          R
        >
      }

      return { load, find, watch, build }
    }),
  },
) {}
