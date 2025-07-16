import { Command, Options } from "@effect/cli"
import {
  FileSystem,
  HttpApi,
  HttpApiBuilder,
  HttpApiEndpoint,
  HttpApiError,
  HttpApiGroup,
  HttpApiScalar,
  HttpApiSchema,
  HttpMiddleware,
  HttpRouter,
  HttpServer,
  HttpServerResponse,
  OpenApi,
  Path,
} from "@effect/platform"
import { NodeFileSystem, NodeHttpServer } from "@effect/platform-node"
import {
  Array as EffectArray,
  Console,
  Data,
  Effect,
  Either,
  Layer,
  Option,
  pipe,
  Schema,
  Stream,
  String as EffectString,
  Struct,
} from "effect"
import { load } from "js-toml"
import { createServer } from "node:http"
import { fileURLToPath } from "node:url"
import open, { type AppName, apps } from "open"

import * as Model from "../../Model.js"
import * as Utils from "../../Utils.js"

const FoundryTomlConfig = Schema.Struct({
  profile: Schema.Struct({
    default: Schema.Struct({
      src: Schema.NonEmptyTrimmedString,
      out: Schema.NonEmptyTrimmedString,
      libs: Schema.Array(Schema.NonEmptyTrimmedString),
      test: Schema.optional(Schema.NullishOr(Schema.NonEmptyTrimmedString)),
      script: Schema.optional(Schema.NullishOr(Schema.NonEmptyTrimmedString)),
      cache_path: Schema.optional(Schema.NullishOr(Schema.NonEmptyTrimmedString)),
    }),
  }),
})
type FoundryTomlConfig = typeof FoundryTomlConfig.Type
const FoundryTomlConfigDecoder = Schema.decodeUnknownEither(FoundryTomlConfig)

class FoundryOutputAbiFunction
  extends Schema.Class<FoundryOutputAbiFunction>("Nozzle/cli/studio/models/FoundryOutputAbiFunction")({
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
  })
{}
class FoundryOutputAbiEvent
  extends Schema.Class<FoundryOutputAbiEvent>("Nozzle/cli/studio/models/FoundryOutputAbiEvent")({
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
  })
{
  get signature(): string {
    const params = pipe(
      this.inputs,
      EffectArray.map((input) => input.indexed ? `${input.type} indexed ${input.name}` : `${input.type} ${input.name}`),
      EffectArray.join(", "),
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
class FoundryOutput extends Schema.Class<FoundryOutput>("Nozzle/cli/studio/models/FoundryOutput")({
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
const FoundryOutputDecoder = Schema.decodeUnknownEither(Schema.parseJson(FoundryOutput))

function abiOutputIsEvent(item: any): item is FoundryOutputAbiEvent {
  if (typeof item !== "object") {
    return false
  } else if (!Object.hasOwn(item, "type")) {
    return false
  }
  const type = item.type

  return type === "event"
}

/**
 * Goal of this resolver is to build an array of [QueryableEvent](../../Model.ts) from the watched source of Smart Contract events.
 *
 * <bold>Phase 1</bold>: checks if the user is using foundry in the directory where the cli is ran. if yes, resolves the `foundry.toml` file, parses it, and finds the output ABIs.
 * These ABIs are then parsed to return a list of available Events.
 * The output files are watched, and then changes are emitted on a stream through the api.
 */
class QueryableEventResolver
  extends Effect.Service<QueryableEventResolver>()("Nozzle/cli/studio/services/QueryableEventResolver", {
    dependencies: [NodeFileSystem.layer],
    effect: Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path

      /**
       * Check if the directory the cli tool is being ran in has a foundry.toml config file.
       * If true, then the QueryableEvents will be resolved by introspecting the foundry output ABIs.
       */
      const modeIsLocalFoundry = Effect.fnUntraced(function*(cwd: string = ".") {
        return yield* fs.exists(path.resolve(cwd, "foundry.toml")).pipe(Effect.orElseSucceed(() => false))
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
          const config = yield* fs.readFileString(path.resolve(cwd, "foundry.toml")).pipe(
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
      /**
       * Builds a stream of changes from watching the foundry output.
       * The output of foundry is JSON files with the ABI from the compiled contracts code.
       * This function will watch this out directory (declared in the foundry.toml config -> out),
       * and for each .json file in the subdirectories - excluding the ignore list - parse the ABI
       * and create a stream of the events from these ABIs.
       *
       * @returns a stream of QueryableEventStream objects encoded to be sent on the streaming response api endpoint
       */
      const watchFoundryABIChanges = (cwd: string = ".") =>
        Effect.gen(function*() {
          const parsedFoundryConfig = yield* fetchAndParseFoundryConfigToml(cwd)
          if (Option.isNone(parsedFoundryConfig)) {
            return Stream.empty
          }
          const foundryConfig = parsedFoundryConfig.value
          const out = foundryConfig.profile.default.out
          const outPath = path.resolve(cwd, out)

          // Build a QueryableEventStream from the _current_ state of the foundry output files.
          // This will get concatenated to the updates stream of events
          const currentEventsStream = Stream.fromIterableEffect(fs.readDirectory(outPath, { recursive: true })).pipe(
            Stream.filter((filePath) => Utils.foundryOutputPathIncluded(filePath)),
            Stream.mapEffect((filePath) =>
              Effect.gen(function*() {
                const resolvedPath = path.resolve(outPath, filePath)
                const rawAbi = yield* fs.readFileString(resolvedPath)
                const decoded = FoundryOutputDecoder(rawAbi)

                return Either.match(decoded, {
                  onLeft() {
                    return Model.QueryableEventStream.make({ events: [] })
                  },
                  onRight(right) {
                    const sources = Object.keys(right.metadata.sources)
                    const events = pipe(
                      right.abi,
                      EffectArray.filter(abiOutputIsEvent),
                      EffectArray.map((event) =>
                        Model.QueryableEvent.make({
                          name: event.name,
                          source: sources,
                          signature: event.signature,
                          params: EffectArray.map(event.inputs, (input) => ({
                            name: input.name,
                            datatype: input.type,
                            indexed: input.indexed,
                          })),
                        })
                      ),
                    )
                    return Model.QueryableEventStream.make({ events })
                  },
                })
              })
            ),
            Stream.map((stream) => {
              const jsonData = JSON.stringify(stream)
              const sseData = `data: ${jsonData}\n\n`
              return new TextEncoder().encode(sseData)
            }),
          )

          // Watches for changes on the foundry out directory.
          // When the files change, this emits an event on the stream.
          const updates = fs.watch(outPath, { recursive: true }).pipe(
            Stream.buffer({ capacity: 1, strategy: "sliding" }),
            Stream.map((event) => event.path),
            // Filter out ignored directories/files and only watch .json files
            Stream.filter((watchFilePath) => Utils.foundryOutputPathIncluded(watchFilePath)),
            // derive the ABI -> events from the path
            Stream.mapEffect((watchFilePath) =>
              Effect.gen(function*() {
                const filePath = path.resolve(outPath, watchFilePath)
                const rawAbi = yield* fs.readFileString(filePath)
                const decoded = FoundryOutputDecoder(rawAbi)

                return Either.match(decoded, {
                  onLeft() {
                    return Model.QueryableEventStream.make({ events: [] })
                  },
                  onRight(right) {
                    const sources = Object.keys(right.metadata.sources)
                    const events = pipe(
                      right.abi,
                      EffectArray.filter(abiOutputIsEvent),
                      EffectArray.map((event) =>
                        Model.QueryableEvent.make({
                          name: event.name,
                          source: sources,
                          signature: event.signature,
                          params: EffectArray.map(event.inputs, (input) => ({
                            name: input.name,
                            datatype: input.type,
                            indexed: input.indexed,
                          })),
                        })
                      ),
                    )
                    return Model.QueryableEventStream.make({ events })
                  },
                })
              })
            ),
            // maps the QueryableEventStream to an SSE encoded event to send on the Stream response api
            Stream.map((event) => {
              const jsonData = JSON.stringify(event)
              const sseData = `data: ${jsonData}\n\n`
              return new TextEncoder().encode(sseData)
            }),
          )

          // concat together current derived events with the file watcher stream
          return Stream.concat(currentEventsStream, updates)
        })

      return {
        watchFoundryABIChanges,
      } as const
    }),
  })
{}

class NozzleStudioApiRouter extends HttpApiGroup.make("NozzleStudioApi").add(
  HttpApiEndpoint.get("QueryableEventStream")`/events/stream`
    .addSuccess(
      Schema.String.pipe(HttpApiSchema.withEncoding({
        kind: "Json",
        contentType: "text/event-stream",
      })),
    )
    .addError(HttpApiError.InternalServerError)
    .annotateContext(OpenApi.annotations({
      title: "Queryable Smart Contract events stream",
      version: "v1",
      description:
        "Listens to file changes on the smart contracts/abis and emits updates of the available events to query",
    })),
).prefix("/v1") {}

class NozzleStudioApi extends HttpApi.make("NozzleStudioApi").add(NozzleStudioApiRouter).prefix("/api") {}

const NozzleStudioApiLive = HttpApiBuilder.group(
  NozzleStudioApi,
  "NozzleStudioApi",
  (handlers) =>
    handlers.handle(
      "QueryableEventStream",
      () =>
        Effect.gen(function*() {
          const resolver = yield* QueryableEventResolver

          const stream = yield* resolver.watchFoundryABIChanges().pipe(
            Effect.catchAll(() => new HttpApiError.InternalServerError()),
          )

          return yield* HttpServerResponse.stream(stream, { contentType: "text/event-stream" }).pipe(
            HttpServerResponse.setHeaders({
              "Content-Type": "text/event-stream",
              "Cache-Control": "no-cache",
              "Connection": "keep-alive",
            }),
          )
        }),
    ),
)
const NozzleStudioApiLayer = Layer.merge(HttpApiBuilder.middlewareCors(), HttpApiScalar.layer({ path: "/api/docs" }))
  .pipe(
    Layer.provideMerge(HttpApiBuilder.api(NozzleStudioApi)),
    Layer.provide(QueryableEventResolver.Default),
    Layer.provide(NozzleStudioApiLive),
  )
const ApiLive = HttpApiBuilder.httpApp.pipe(
  Effect.provide(Layer.mergeAll(NozzleStudioApiLayer, HttpApiBuilder.Router.Live, HttpApiBuilder.Middleware.layer)),
)

const DatasetWorksFileRouter = Effect.gen(function*() {
  const path = yield* Path.Path

  const __filename = fileURLToPath(import.meta.url)
  const __dirname = path.dirname(__filename)
  /**
   * This resolves an issue when running the cli in dev mode locally vs published mode.
   * In local dev mode, the __dirname will end with `commands` as this file will be ran from the ./commands directory.
   * When running in the compiled dist mode, the __dirname will end with `dist`.
   *
   * @todo clean this up and figure out a better way to derive
   */
  const isLocal = EffectString.endsWith("commands")(__dirname)
  const datasetWorksClientDist = isLocal
    ? path.resolve(__dirname, "..", "..", "..", "..", "studio", "dist")
    : path.resolve(__dirname, "studio", "dist")

  return HttpRouter.empty.pipe(
    HttpRouter.get(
      "/",
      HttpServerResponse.file(path.join(datasetWorksClientDist, "index.html")).pipe(
        Effect.orElse(() => HttpServerResponse.empty({ status: 404 })),
      ),
    ),
    HttpRouter.get(
      "/assets/:file",
      Effect.gen(function*() {
        const file = yield* HttpRouter.params.pipe(Effect.map(Struct.get("file")), Effect.map(Option.fromNullable))

        if (Option.isNone(file)) {
          return HttpServerResponse.empty({ status: 404 })
        }

        const assets = path.join(datasetWorksClientDist, "assets")
        const normalized = path.normalize(path.join(assets, ...file.value.split("/")))
        if (!normalized.startsWith(assets)) {
          return HttpServerResponse.empty({ status: 404 })
        }

        return yield* HttpServerResponse.file(normalized)
      }).pipe(Effect.orElse(() => HttpServerResponse.empty({ status: 404 }))),
    ),
  )
})

const Server = Effect.all({
  api: ApiLive,
  files: DatasetWorksFileRouter,
}).pipe(
  Effect.map(({ api, files }) =>
    HttpRouter.empty.pipe(HttpRouter.mount("/", files), HttpRouter.mountApp("/api", api, { includePrefix: true }))
  ),
  Effect.map((router) => HttpServer.serve(HttpMiddleware.logger)(router)),
  Layer.unwrapEffect,
)

export const studio = Command.make("studio", {
  args: {
    port: Options.integer("port").pipe(
      Options.withAlias("p"),
      Options.withDefault(3000),
      Options.withDescription("The port to run the nozzle dataset studio server on. Default 3000"),
    ),
    open: Options.boolean("open").pipe(
      Options.withDescription("If true, opens the nozzle dataset studio in your browser"),
      Options.withDefault(true),
    ),
    browser: Options.choice("browser", [
      "chrome",
      "firefox",
      "edge",
      "safari",
      "arc",
      "browser",
      "browserPrivate",
    ]).pipe(
      Options.withAlias("b"),
      Options.withDescription(
        "Broweser to open the nozzle dataset studio app in. Default is your default selected browser",
      ),
      Options.withDefault("browser"),
    ),
  },
}).pipe(
  Command.withDescription("Opens the nozzle dataset studio visualization tool"),
  Command.withHandler(({ args }) =>
    Effect.gen(function*() {
      yield* Server.pipe(
        HttpServer.withLogAddress,
        Layer.provide(NodeHttpServer.layer(createServer, { port: args.port })),
        Layer.tap(() =>
          Effect.gen(function*() {
            if (args.open) {
              return yield* openBrowser(args.port, args.browser).pipe(
                Effect.tapErrorCause((cause) =>
                  Console.warn(
                    `Failure opening nozzle dataset studio in your browser. Open at http://localhost:${args.port}`,
                    {
                      cause,
                    },
                  )
                ),
                Effect.orElseSucceed(() => Effect.void),
              )
            }
            return Effect.void
          })
        ),
        Layer.tap(() => Console.log(`🎉 nozzle dataset studio started and running at http://localhost:${args.port}`)),
        Layer.launch,
      )
    })
  ),
  Command.provide(QueryableEventResolver.Default),
)

const openBrowser = (port: number, browser: AppName | "arc" | "safari" | "browser" | "browserPrivate") =>
  Effect.async<void, OpenBrowserError>((resume) => {
    const url = `http://localhost:${port}`

    const launch = (appOpts?: { name: string | ReadonlyArray<string> }) =>
      open(url, appOpts ? { app: appOpts } : undefined).then((subprocess) => {
        subprocess.on("spawn", () => resume(Effect.void))
        subprocess.on("error", (err) => resume(Effect.fail(new OpenBrowserError({ cause: err }))))
      })

    const mapBrowserName = (b: typeof browser): string | ReadonlyArray<string> | undefined => {
      switch (b) {
        case "chrome":
          return apps.chrome // cross-platform alias from open
        case "firefox":
          return apps.firefox
        case "edge":
          return apps.edge
        case "safari":
          return "Safari"
        case "arc":
          return "Arc"
        default:
          return undefined
      }
    }

    switch (browser) {
      case "browser":
        launch()
        break
      case "browserPrivate":
        launch({ name: apps.browserPrivate })
        break
      default: {
        const mapped = mapBrowserName(browser)
        if (mapped) {
          launch({ name: mapped }).catch(() => launch())
          break
        }
        launch()
        break
      }
    }
  })

export class OpenBrowserError extends Data.TaggedError("Nozzle/cli/studio/errors/OpenBrowserError")<{
  readonly cause: unknown
}> {}
