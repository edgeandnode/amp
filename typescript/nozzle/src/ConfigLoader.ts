import { Path, FileSystem } from "@effect/platform";
import { Data, Effect, Match, Option, Schema } from "effect";
import * as Model from "./Model.js";

export class ConfigLoaderError extends Data.TaggedError("ConfigLoaderError")<{
  readonly cause?: unknown;
  readonly message?: string;
}> {}

export class ConfigLoader extends Effect.Service<ConfigLoader>()("Nozzle/ConfigLoader", {
  dependencies: [],
  effect: Effect.gen(function* () {
    const path = yield* Path.Path;
    const fs = yield* FileSystem.FileSystem;

    const jiti = yield* Effect.tryPromise({
      try: () => import('jiti').then(({ createJiti }) => createJiti(import.meta.url)),
      catch: (cause) => new ConfigLoaderError({ cause }),
    }).pipe(Effect.cached);

    const loadTypeScript = Effect.fnUntraced(function* (file: string) {
      return yield* jiti.pipe(Effect.flatMap(jiti => Effect.tryPromise({
        try: () => jiti.import(file, { default: true }),
        catch: (cause) => cause,
      }))).pipe(
        Effect.flatMap(Schema.decodeUnknown(Model.DatasetDefinition)),
        Effect.mapError((cause) => new ConfigLoaderError({ cause, message: `Failed to load config file ${file}` }))
      );
    });

    const loadJavaScript = Effect.fnUntraced(function* (file: string) {
      return yield* Effect.tryPromise({
        try: () => import(file).then((module) => module.default),
        catch: (cause) => cause,
      }).pipe(
        Effect.flatMap(Schema.decodeUnknown(Model.DatasetDefinition)),
        Effect.mapError((cause) => new ConfigLoaderError({ cause, message: `Failed to load config file ${file}` }))
      )
    });

    const loadJson = Effect.fnUntraced(function* (file: string) {
      return yield* fs.readFileString(file).pipe(
        Effect.flatMap((content) => Effect.try({
          try: () => JSON.parse(content),
          catch: (cause) => cause,
        })),
        Effect.flatMap(Schema.decodeUnknown(Model.DatasetDefinition)),
        Effect.mapError((cause) => new ConfigLoaderError({ cause, message: `Failed to load config file ${file}` })),
      )
    });

    const load = Effect.fnUntraced(function* (file: string) {
      const resolved = path.resolve(file);
      return yield* Match.value(path.extname(resolved)).pipe(
        Match.when((_) => /\.(ts|mts|cts)$/.test(_), () => loadTypeScript(resolved)),
        Match.when((_) => /\.(js|mjs|cjs)$/.test(_), () => loadJavaScript(resolved)),
        Match.when((_) => /\.(json)$/.test(_), () => loadJson(resolved)),
        Match.orElse((_) => Effect.fail(new ConfigLoaderError({ message: `Unsupported file extension ${_}` }))),
      )
    });

    const find = Effect.fnUntraced(function* (cwd: string = ".") {
      const candidates = [
        path.resolve(cwd, `nozzle.config.ts`),
        path.resolve(cwd, `nozzle.config.mts`),
        path.resolve(cwd, `nozzle.config.cts`),
        path.resolve(cwd, `nozzle.config.js`),
        path.resolve(cwd, `nozzle.config.mjs`),
        path.resolve(cwd, `nozzle.config.cjs`),
        path.resolve(cwd, `nozzle.config.json`),
      ];

      const match = yield* Effect.findFirst(candidates, (_) => fs.exists(_).pipe(Effect.orElseSucceed(() => false)));
      return yield* Option.match(match, {
        onNone: () => Effect.succeed(Option.none<Model.DatasetDefinition>()),
        onSome: (file) => load(file).pipe(Effect.map(Option.some)),
      });
    });

    return { load, find };
  }),
}) {}
