import { Effect } from "effect";
import { FileSystem } from "@effect/platform";

export const importFile = Effect.fn("importFile")((file: string) => Effect.tryPromise({
  try: () => import(file).then((m) => m.default),
  catch: () => new Error(`Failed to load dataset definition ${file}`),
}))

export const readJson = Effect.fn("readJson")((file: string) => Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const json = yield* fs.readFileString(file);
  return yield* Effect.try({
    try: () => JSON.parse(json),
    catch: () => new Error(`Failed to parse JSON from ${file}`),
  })
}))
