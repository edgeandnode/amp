import { FileSystem, Path } from "@effect/platform"
import type { Cause } from "effect"
import { Data, Effect, Either, Match, Predicate, Schema, Stream } from "effect"
import * as ManifestBuilder from "./ManifestBuilder.js"
import * as Model from "./Model.js"

export interface Context {
  file: string
}

export class ConfigLoaderError extends Data.TaggedError("ConfigLoaderError")<{
  readonly cause?: unknown
  readonly message?: string
}> {}

export class ConfigLoader extends Effect.Service<ConfigLoader>()("Nozzle/ConfigLoader", {
  dependencies: [ManifestBuilder.ManifestBuilder.Default],
  effect: Effect.gen(function*() {
    const path = yield* Path.Path
    const fs = yield* FileSystem.FileSystem
    const builder = yield* ManifestBuilder.ManifestBuilder

    const jiti = yield* Effect.tryPromise({
      try: () =>
        import("jiti").then(({ createJiti }) => createJiti(import.meta.url, { moduleCache: false, tryNative: false })),
      catch: (cause) => new ConfigLoaderError({ cause }),
    }).pipe(Effect.cached)

    const loadTypeScript = Effect.fnUntraced(function*(file: string) {
      return yield* Effect.tryMapPromise(jiti, {
        try: (jiti) => jiti.import<((context: Context) => Model.DatasetDefinition)>(file, { default: true }),
        catch: (cause) => cause,
      }).pipe(
        Effect.map((callback) => callback({ file })),
        Effect.flatMap(Schema.decodeUnknown(Model.DatasetDefinition)),
        Effect.mapError((cause) => new ConfigLoaderError({ cause, message: `Failed to load config file ${file}` })),
      )
    })

    const loadJavaScript = Effect.fnUntraced(function*(file: string) {
      return yield* Effect.tryPromise({
        try: () => import(file).then((module) => module.default as (context: Context) => Model.DatasetDefinition),
        catch: (cause) => cause,
      }).pipe(
        Effect.map((callback) => callback({ file })),
        Effect.flatMap(Schema.decodeUnknown(Model.DatasetDefinition)),
        Effect.mapError((cause) => new ConfigLoaderError({ cause, message: `Failed to load config file ${file}` })),
      )
    })

    const loadJson = Effect.fnUntraced(function*(file: string) {
      return yield* Effect.tryMap(fs.readFileString(file), {
        try: (content) => JSON.parse(content),
        catch: (cause) => cause,
      }).pipe(
        Effect.flatMap(Schema.decodeUnknown(Model.DatasetDefinition)),
        Effect.mapError((cause) => new ConfigLoaderError({ cause, message: `Failed to load config file ${file}` })),
      )
    })

    const load = Effect.fnUntraced(function*(file: string) {
      const resolved = path.resolve(file)
      return yield* Match.value(path.extname(resolved)).pipe(
        Match.when((_) => /\.(ts|mts|cts)$/.test(_), () => loadTypeScript(resolved)),
        Match.when((_) => /\.(js|mjs|cjs)$/.test(_), () => loadJavaScript(resolved)),
        Match.when((_) => /\.(json)$/.test(_), () => loadJson(resolved)),
        Match.orElse((_) => new ConfigLoaderError({ message: `Unsupported file extension ${_}` })),
      )
    })

    const build = Effect.fnUntraced(function*(file: string) {
      const config = yield* load(file)
      return yield* builder.build(config).pipe(
        Effect.mapError((cause) => new ConfigLoaderError({ cause, message: `Failed to build config file ${file}` })),
      )
    })

    const find = Effect.fnUntraced(function*(cwd: string = ".") {
      const candidates = [
        path.resolve(cwd, `nozzle.config.ts`),
        path.resolve(cwd, `nozzle.config.mts`),
        path.resolve(cwd, `nozzle.config.cts`),
        path.resolve(cwd, `nozzle.config.js`),
        path.resolve(cwd, `nozzle.config.mjs`),
        path.resolve(cwd, `nozzle.config.cjs`),
        path.resolve(cwd, `nozzle.config.json`),
      ]
      return yield* Effect.findFirst(candidates, (_) => fs.exists(_).pipe(Effect.orElseSucceed(() => false)))
    })

    const watch = <E, R>(file: string, options?: {
      onError?: (cause: Cause.Cause<ConfigLoaderError>) => Effect.Effect<void, E, R>
    }): Stream.Stream<Model.DatasetManifest, ConfigLoaderError | E, R> => {
      const resolved = path.resolve(file)
      const open = load(resolved).pipe(Effect.tapErrorCause(options?.onError ?? (() => Effect.void)), Effect.either)

      const updates = fs.watch(resolved).pipe(
        Stream.buffer({ capacity: 1, strategy: "sliding" }),
        Stream.mapError((cause) => new ConfigLoaderError({ cause, message: "Failed to watch config file" })),
        Stream.filter(Predicate.isTagged("Update")),
        Stream.mapEffect(() => open),
      )

      const build = (config: Model.DatasetDefinition) =>
        builder.build(config).pipe(
          Effect.mapError((cause) => new ConfigLoaderError({ cause, message: `Failed to build config file ${file}` })),
          Effect.tapErrorCause(options?.onError ?? (() => Effect.void)),
        ).pipe(Effect.either)

      return Stream.fromEffect(open).pipe(
        Stream.concat(updates),
        Stream.filterMap(Either.getRight),
        Stream.changesWith(Schema.equivalence(Model.DatasetDefinition)),
        Stream.mapEffect((config) => build(config)),
        Stream.filterMap(Either.getRight),
        Stream.changesWith(Schema.equivalence(Model.DatasetManifest)),
      )
    }

    return { load, find, watch, build }
  }),
}) {}
