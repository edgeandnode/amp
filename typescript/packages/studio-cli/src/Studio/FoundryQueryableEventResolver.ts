import { FileSystem, Path } from "@effect/platform"
import { NodeFileSystem } from "@effect/platform-node"
import { Array, Cause, Chunk, Console, Effect, Either, Option, pipe, Schema, Stream } from "effect"
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
        const candidates = [
          path.resolve(cwd, `foundry.toml`),
          path.resolve(cwd, "contracts", `foundry.toml`),
        ]
        return yield* Effect.findFirst(candidates, (_) => fs.exists(_).pipe(Effect.orElseSucceed(() => false)))
      })
      /**
       * If the cli is running in local foundy mode, this reads the foundry.toml config and parses it.
       * @returns the parsed foundry.toml config along with the directory containing it
       */
      const fetchAndParseFoundryConfigToml = Effect.fn("FetchAndParseFoundryConfig")(function*(cwd: string = ".") {
        const isLocalFoundry = yield* modeIsLocalFoundry(cwd)
        if (Option.isNone(isLocalFoundry)) {
          return Option.none<{ config: FoundryTomlConfig; configDir: string }>()
        }
        const foundryPath = isLocalFoundry.value
        const foundryDir = path.dirname(foundryPath)

        const config = yield* fs
          .readFileString(foundryPath)
          .pipe(
            Effect.map((config) => Option.some(config)),
            Effect.orElseSucceed(() => Option.none<string>()),
          )

        return Option.match(config, {
          onNone() {
            return Option.none<{ config: FoundryTomlConfig; configDir: string }>()
          },
          onSome(found) {
            const parsed = load(found)
            const decoded = FoundryTomlConfigDecoder(parsed)
            return Either.match(decoded, {
              onLeft() {
                // failure parsing the foundry config. return Option.none
                // todo: error handling, where does it belong??
                return Option.none<{ config: FoundryTomlConfig; configDir: string }>()
              },
              onRight(right) {
                return Option.some({ config: right, configDir: foundryDir })
              },
            })
          },
        })
      })

      const decodeEventsFromAbi = Effect.fn("DecodeAbiEvents")(function*(outpath: string, filepath: string) {
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

      const queryableEventsStream = Effect.fn("BuildQueryableEventsStream")(function*(cwd: string = ".") {
        const parsedFoundryConfig = yield* fetchAndParseFoundryConfigToml(cwd)
        if (Option.isNone(parsedFoundryConfig)) {
          return Stream.empty
        }
        const { config: foundryConfig, configDir } = parsedFoundryConfig.value
        const outpath = path.resolve(configDir, foundryConfig.profile.default.out)

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
        events: Effect.fn("QueryableEventsParser")(function*(cwd: string = ".") {
          const parsedFoundryConfig = yield* fetchAndParseFoundryConfigToml(cwd)
          if (Option.isNone(parsedFoundryConfig)) {
            return Chunk.empty<Model.QueryableEvent>()
          }
          const { config: foundryConfig, configDir } = parsedFoundryConfig.value
          const outpath = path.resolve(configDir, foundryConfig.profile.default.out)

          // Read directory and process files directly
          const files = yield* fs.readDirectory(outpath, { recursive: true })
          const filteredFiles = Array.filter(files, (filepath) => Utils.foundryOutputPathIncluded(filepath))

          // Decode events from each ABI file
          const eventsArrays = yield* Effect.all(
            Array.map(filteredFiles, (filepath) => decodeEventsFromAbi(outpath, filepath)),
            { concurrency: "unbounded" },
          )

          // Flatten all events into a single Chunk
          return Chunk.fromIterable(eventsArrays.flat())
        }),
        queryableEventsStream,
        sources() {
          return Effect.succeed([
            Model.DatasetSource.make({
              metadata_columns: [
                { name: "address", datatype: "address" },
                { name: "block_num", datatype: "bigint" },
                { name: "block_hash", datatype: "bytes32" },
                { name: "timestamp", datatype: "bigint" },
                { name: "tx_hash", datatype: "bytes32" },
                { name: "tx_index", datatype: "int" },
                { name: "log_index", datatype: "int" },
                { name: "topic0", datatype: "unknown" },
                { name: "topic1", datatype: "unknown" },
                { name: "topic2", datatype: "unknown" },
                { name: "topic3", datatype: "unknown" },
                { name: "data", datatype: "string" },
              ],
              source: "anvil.logs",
            }),
            Model.DatasetSource.make({
              source: "anvil.transactions",
              metadata_columns: [
                { name: "block_num", datatype: "bigint" },
                { name: "block_hash", datatype: "bytes32" },
                { name: "timestamp", datatype: "bigint" },
                { name: "tx_hash", datatype: "bytes32" },
                { name: "tx_index", datatype: "int" },
                { name: "to", datatype: "address" },
                { name: "from", datatype: "address" },
                { name: "nonce", datatype: "bigint" },
                { name: "gas_price", datatype: "bigint" },
                { name: "gas_limit", datatype: "bigint" },
                { name: "gas_used", datatype: "bigint" },
                { name: "max_fee_per_gas", datatype: "Uint32Array" },
                { name: "max_priority_fee_per_gas", datatype: "Uint32Array" },
                { name: "max_fee_per_blob_gas", datatype: "Uint32Array" },
                { name: "value", datatype: "Uint32Array" },
                { name: "input", datatype: "string" },
                { name: "v", datatype: "string" },
                { name: "s", datatype: "string" },
                { name: "r", datatype: "string" },
                { name: "type", datatype: "int" },
                { name: "status", datatype: "boolean" },
              ],
            }),
            Model.DatasetSource.make({
              source: "anvil.blocks",
              metadata_columns: [
                { name: "block_num", datatype: "string" },
                { name: "timestamp", datatype: "bigint" },
                { name: "hash", datatype: "bytes32" },
                { name: "parent_hash", datatype: "bytes32" },
                { name: "ommers_hash", datatype: "bytes32" },
                { name: "miner", datatype: "address" },
                { name: "state_root", datatype: "bytes32" },
                { name: "transactions_root", datatype: "bytes32" },
                { name: "receipt_root", datatype: "bytes32" },
                { name: "logs_bloom", datatype: "string" },
                { name: "difficulty", datatype: "Uint32Array" },
                { name: "gas_limit", datatype: "string" },
                { name: "gas_used", datatype: "string" },
                { name: "extra_data", datatype: "string" },
                { name: "mix_hash", datatype: "bytes32" },
                { name: "nonce", datatype: "string" },
                { name: "base_fee_per_gas", datatype: "Uint32Array" },
                { name: "withdrawals_root", datatype: "bytes32" },
                { name: "blob_gas_used", datatype: "string" },
                { name: "excess_blob_gas", datatype: "string" },
                { name: "parent_beacon_root", datatype: "bytes32" },
              ],
            }),
          ])
        },
      } as const
    }),
  },
) {}
export const layer = FoundryQueryableEventResolver.Default
