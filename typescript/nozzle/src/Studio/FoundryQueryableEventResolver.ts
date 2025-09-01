import { FileSystem, Path } from "@effect/platform"
import { NodeFileSystem } from "@effect/platform-node"
import { Array, Cause, Console, Effect, Either, Option, pipe, Schema, Stream } from "effect"
import { load } from "js-toml"

import * as Model from "./Model.js"
import * as Utils from "./Utils.js"

const FoundryTomlConfig = Schema.Struct({
  profile: Schema.Struct({
    default: Schema.Struct({
      src: Schema.NonEmptyTrimmedString,
      out: Schema.NonEmptyTrimmedString,
      libs: Schema.Array(Schema.NonEmptyTrimmedString),
      test: Schema.optional(Schema.NullishOr(Schema.NonEmptyTrimmedString)),
      script: Schema.optional(Schema.NullishOr(Schema.NonEmptyTrimmedString)),
      cache_path: Schema.optional(
        Schema.NullishOr(Schema.NonEmptyTrimmedString),
      ),
    }),
  }),
})
type FoundryTomlConfig = typeof FoundryTomlConfig.Type
const FoundryTomlConfigDecoder = Schema.decodeUnknownEither(FoundryTomlConfig)

class FoundryOutputAbiFunction extends Schema.Class<FoundryOutputAbiFunction>(
  "Nozzle/cli/studio/models/FoundryOutputAbiFunction",
)({
  type: Schema.Literal("function"),
  name: Schema.NonEmptyTrimmedString,
  inputs: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      type: Schema.String,
      internalType: Schema.String,
    }),
  ),
  outputs: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      type: Schema.String,
      internalType: Schema.String,
    }),
  ),
  stateMutability: Schema.String,
}) {}
class FoundryOutputAbiEvent extends Schema.Class<FoundryOutputAbiEvent>(
  "Nozzle/cli/studio/models/FoundryOutputAbiEvent",
)({
  type: Schema.Literal("event"),
  name: Schema.NonEmptyTrimmedString,
  inputs: Schema.Array(
    Schema.Struct({
      name: Schema.NonEmptyTrimmedString,
      type: Schema.NonEmptyTrimmedString,
      indexed: Schema.Boolean,
      internalType: Schema.String,
    }),
  ),
  anonymous: Schema.Boolean,
}) {
  get signature(): string {
    const params = pipe(
      this.inputs,
      Array.map((input) =>
        input.indexed
          ? `${input.type} indexed ${input.name}`
          : `${input.type} ${input.name}`
      ),
      Array.join(", "),
    )
    return `${this.name}(${params})`
  }
}
const FoundOuputAbiObject = Schema.Union(
  FoundryOutputAbiFunction,
  FoundryOutputAbiEvent,
  // handle unknown types
  Schema.Record({ key: Schema.String, value: Schema.Any }),
)
class FoundryOutput extends Schema.Class<FoundryOutput>(
  "Nozzle/cli/studio/models/FoundryOutput",
)({
  abi: Schema.Array(FoundOuputAbiObject),
  // parse the metadata to get the contract source
  metadata: Schema.Struct({
    compiler: Schema.Struct({
      version: Schema.String,
    }),
    language: Schema.String,
    output: Schema.Struct({
      abi: Schema.Array(FoundOuputAbiObject),
      devdoc: Schema.Struct({
        kind: Schema.String,
        methods: Schema.Object,
        version: Schema.NonNegativeInt,
      }),
      userdoc: Schema.Struct({
        kind: Schema.String,
        methods: Schema.Any,
        version: Schema.NonNegativeInt,
      }),
    }),
    settings: Schema.Record({ key: Schema.String, value: Schema.Any }),
    version: Schema.NonNegativeInt,
    sources: Schema.Record({
      // name of the file the output was built from
      key: Schema.NonEmptyTrimmedString,
      value: Schema.Any,
    }),
  }),
}) {}
const FoundryOutputDecoder = Schema.decodeUnknownEither(
  Schema.parseJson(FoundryOutput),
)

function abiOutputIsEvent(item: any): item is FoundryOutputAbiEvent {
  if (typeof item !== "object") {
    return false
  } else if (!Object.hasOwn(item, "type")) {
    return false
  }
  const type = item.type

  return type === "event"
}

export class FoundryQueryableEventResolver extends Effect.Service<FoundryQueryableEventResolver>()(
  "Nozzle/studio/services/FoundryQueryableEventResolver",
  {
    dependencies: [NodeFileSystem.layer],
    effect: Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path

      /**
       * Check if the directory the cli tool is being ran in has a foundry.toml config file.
       * If true, then the QueryableEvents will be resolved by introspecting the foundry output ABIs.
       */
      const modeIsLocalFoundry = Effect.fnUntraced(function*(
        cwd: string = ".",
      ) {
        return yield* fs
          .exists(path.resolve(cwd, "foundry.toml"))
          .pipe(Effect.orElseSucceed(() => false))
      })
      /**
       * If the cli is running in local foundy mode, this reads the foundry.toml config and parses it.
       * @returns the parsed foundry.toml config
       */
      const fetchAndParseFoundryConfigToml = (cwd: string = ".") =>
        Effect.gen(function*() {
          const isLocalFoundry = yield* modeIsLocalFoundry(cwd)
          if (!isLocalFoundry) {
            return Option.none<FoundryTomlConfig>()
          }
          const config = yield* fs
            .readFileString(path.resolve(cwd, "foundry.toml"))
            .pipe(
              Effect.map((config) => Option.some(config)),
              Effect.orElseSucceed(() => Option.none<string>()),
            )

          return Option.match(config, {
            onNone() {
              return Option.none<FoundryTomlConfig>()
            },
            onSome(found) {
              const parsed = load(found)
              const decoded = FoundryTomlConfigDecoder(parsed)
              return Either.match(decoded, {
                onLeft() {
                  // failure parsing the foundry config. return Option.none
                  // todo: error handling, where does it belong??
                  return Option.none<FoundryTomlConfig>()
                },
                onRight(right) {
                  return Option.some(right)
                },
              })
            },
          })
        })

      const decodeEventsFromAbi = (outpath: string, filepath: string) =>
        Effect.gen(function*() {
          const resolvedPath = path.resolve(outpath, filepath)
          const rawAbi = yield* fs.readFileString(resolvedPath)
          const decoded = FoundryOutputDecoder(rawAbi)

          return Either.match(decoded, {
            onLeft() {
              return [] as Array<Model.QueryableEvent>
            },
            onRight(right) {
              const sources = Object.keys(right.metadata.sources)
              return pipe(
                right.abi,
                Array.filter(abiOutputIsEvent),
                Array.map((event) =>
                  Model.QueryableEvent.make({
                    name: event.name,
                    source: sources,
                    signature: event.signature,
                    params: Array.map(event.inputs, (input) => ({
                      name: input.name,
                      datatype: input.type,
                      indexed: input.indexed,
                    })),
                  })
                ),
              )
            },
          })
        })

      /**
       * Derive a list of [QueryableEvent](./Model.js) from the current foundry compiled output ABIs.
       * @param outpath the foundry contracts output path. example contracts/out
       * @returns a stream of [QueryableEvent](./Model.js)
       */
      const currentEventsStream = (outpath: string) =>
        Stream.fromIterableEffect(
          fs.readDirectory(outpath, { recursive: true }),
        ).pipe(
          Stream.filter((filepath) => Utils.foundryOutputPathIncluded(filepath)),
          Stream.mapEffect((filepath) => decodeEventsFromAbi(outpath, filepath)),
          Stream.tapErrorCause((cause) =>
            Console.error("failure deriving current events stream", {
              cause: Cause.pretty(cause),
            })
          ),
        )
      /**
       * Derive a list of [QueryableEvent](./Model.js) after watching file changes from the foundry contracts compiled output.
       * @param outpath the foundry contracts output path. example contracts/out
       * @returns a stream of [QueryableEvent](./Model.js)
       */
      const watchEventStreamUpdates = (outpath: string) =>
        fs.watch(outpath, { recursive: true }).pipe(
          Stream.buffer({ capacity: 1, strategy: "sliding" }),
          Stream.map((event) => event.path),
          Stream.filter((filepath) => Utils.foundryOutputPathIncluded(filepath)),
          Stream.mapEffect((filepath) => decodeEventsFromAbi(outpath, filepath)),
          Stream.tapErrorCause((cause) =>
            Console.error("failure deriving watch events stream", {
              cause: Cause.pretty(cause),
            })
          ),
        )

      const queryableEventsStream = (cwd: string = ".") =>
        Effect.gen(function*() {
          const parsedFoundryConfig = yield* fetchAndParseFoundryConfigToml(cwd)
          if (Option.isNone(parsedFoundryConfig)) {
            return Stream.empty
          }
          const foundryConfig = parsedFoundryConfig.value
          const outpath = path.resolve(cwd, foundryConfig.profile.default.out)

          return currentEventsStream(outpath).pipe(
            Stream.concat(watchEventStreamUpdates(outpath)),
            Stream.filter((events) => Array.isNonEmptyArray(events)),
            Stream.map((events) =>
              Model.QueryableEventStream.make({
                events,
              })
            ),
            Stream.map((stream) => {
              const jsonData = JSON.stringify(stream)
              const sseData = `data: ${jsonData}\n\n`
              return new TextEncoder().encode(sseData)
            }),
          )
        })

      return {
        queryableEventsStream,
        metadata() {
          return Effect.succeed([
            Model.DatasetMetadata.make(
            {
            metadata_columns: [
              { name: "address", description: "The 0x address that invoked the transaction", dataType: "address" },
              { name: "block_num", description: "The block # when the transaction occurred", dataType: "bigint" },
              {
                name: "timestamp",
                description: "The timestamp,z in unix-seconds, when the transaction occurred",
                dataType: "bigint",
              },
            ],
            source: "anvil.logs",
          }),
            Model.DatasetMetadata.make(
            {
            metadata_columns: [
              { name: "block_num", description: "The block # when the transaction occurred", dataType: "bigint" },
              {
                name: "timestamp",
                description: "The timestamp,z in unix-seconds, when the transaction occurred",
                dataType: "bigint",
              },
              { name: "to", description: "The 0x address that sent the tokens", dataType: "address" },
              { name: "from", description: "The 0x address that received the tokens", dataType: "address" },
              { name: "value", description: "Amount transferred", dataType: "bigint" },             
            ],
            source: "erc20token.transfers",
          }),
        ])
        },
      } as const
    }),
  },
) {}
export const layer = FoundryQueryableEventResolver.Default
