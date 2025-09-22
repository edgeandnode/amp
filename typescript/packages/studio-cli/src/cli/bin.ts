#!/usr/bin/env node

import { createGrpcTransport } from "@connectrpc/connect-node"
import { Command, Options, ValidationError } from "@effect/cli"
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
  PlatformConfigProvider,
} from "@effect/platform"
import { NodeContext, NodeHttpServer, NodeRuntime } from "@effect/platform-node"
import {
  Cause,
  Chunk,
  Config,
  Console,
  Data,
  Effect,
  Layer,
  Logger,
  LogLevel,
  Option,
  Schema,
  Stream,
  String,
  Struct,
} from "effect"
import { createServer } from "node:http"
import { fileURLToPath } from "node:url"
import { Arrow, ArrowFlight } from "nozzl"
import * as Admin from "nozzl/api/Admin"
import * as ConfigLoader from "nozzl/ConfigLoader"
import * as ManifestContext from "nozzl/ManifestContext"
import open, { type AppName, apps } from "open"

import { FoundryQueryableEventResolver, Model as StudioModel } from "../Studio/index.ts"
import { adminUrl, configFile, flightUrl } from "./common.ts"
import { prettyCause } from "./Utils.ts"

class AmpStudioApiRouter extends HttpApiGroup.make("AmpStudioApi")
  .add(
    HttpApiEndpoint.get("AmpConfigStream")`/config/stream`
      .addSuccess(
        Schema.String.pipe(
          HttpApiSchema.withEncoding({
            kind: "Json",
            contentType: "text/event-stream",
          }),
        ),
      ).annotateContext(
        OpenApi.annotations({
          title: "Amp Config",
          description: "Watches the amp config and emits a stream of events of changes to the file",
          version: "v1",
        }),
      ),
  )
  .add(
    HttpApiEndpoint.get("QueryableEventStream")`/events/stream`
      .addSuccess(
        Schema.String.pipe(
          HttpApiSchema.withEncoding({
            kind: "Json",
            contentType: "text/event-stream",
          }),
        ),
      )
      .addError(HttpApiError.InternalServerError)
      .annotateContext(
        OpenApi.annotations({
          title: "Queryable Smart Contract events stream",
          version: "v1",
          description:
            "Listens to file changes on the smart contracts/abis and emits updates of the available events to query",
        }),
      ),
  )
  .add(
    HttpApiEndpoint.get("Sources")`/sources`
      .addSuccess(Schema.Array(StudioModel.DatasetSource))
      .annotateContext(
        OpenApi.annotations({
          title: "Dataset sources",
          version: "v1",
          description: "Provides the sources the user can query",
        }),
      ),
  )
  .add(
    HttpApiEndpoint.get("DefaultQuery")`/query/default`
      .addSuccess(
        StudioModel.DefaultQuery.annotations({
          examples: [
            {
              title: "SELECT ... counts",
              query: `SELECT c.block_hash, c.tx_hash, c.address, c.block_num, c.timestamp, c.event['count'] as count
 FROM (
  SELECT block_hash, tx_hash, block_num, timestamp, address, evm_decode_log(topic1, topic2, topic3, data, 'Count(uint256 count)') as event
  FROM anvil.logs
  WHERE topic0 = evm_topic('Count(uint256 count)')
 ) as c`,
            },
            {
              title: "SELECT ... count",
              query:
                `SELECT tx_hash, block_num, evm_decode_log(topic1, topic2, topic3, data, 'Count(uint256 count)') as event
FROM anvil.logs
WHERE topic0 = evm_topic('Count(uint256 count)');`,
            },
            {
              title: "SELECT ... anvil.logs",
              query: `SELECT * FROM anvil.logs LIMIT 10`,
            },
          ],
        }),
      )
      .addError(HttpApiError.InternalServerError)
      .annotateContext(
        OpenApi.annotations({
          title: "Default query",
          version: "v1",
          description:
            "Builds a default query to hydrate in the initial tab on the playground. Derived in order of: 1. parsed nozzle config, 2. parsed contract artifacts, 3. dataset sources",
        }),
      ),
  )
  .add(
    HttpApiEndpoint.post("Query")`/query`
      .setPayload(
        Schema.Union(
          Schema.Struct({ query: Schema.NonEmptyTrimmedString }),
          Schema.parseJson(Schema.Struct({ query: Schema.NonEmptyTrimmedString })),
        ),
      )
      .addSuccess(Schema.Array(Schema.Any))
      .addError(HttpApiError.InternalServerError)
      .annotateContext(
        OpenApi.annotations({
          title: "Perform Query",
          version: "v1",
          description: "Runs the submitted query payload through the ArrowFlight service",
        }),
      ),
  )
  .prefix("/v1")
{}

class AmpStudioApi extends HttpApi.make("AmpStudioApi")
  .add(AmpStudioApiRouter)
  .prefix("/api")
{}

const AmpStudioApiLive = HttpApiBuilder.group(
  AmpStudioApi,
  "AmpStudioApi",
  (handlers) =>
    Effect.gen(function*() {
      const loader = yield* ConfigLoader.ConfigLoader
      const resolver = yield* FoundryQueryableEventResolver.FoundryQueryableEventResolver
      const flight = yield* ArrowFlight.ArrowFlight

      return handlers
        .handle("AmpConfigStream", () =>
          Effect.gen(function*() {
            const stream: Stream.Stream<Uint8Array<ArrayBuffer>, HttpApiError.InternalServerError, never> =
              yield* loader.find().pipe(
                Effect.map(Option.match({
                  onNone() {
                    return Stream.make([1]).pipe(
                      Stream.map(() => {
                        const jsonData = JSON.stringify({})
                        const sseData = `data: ${jsonData}\n\n`
                        return new TextEncoder().encode(sseData)
                      }),
                    )
                  },
                  onSome(nozzleConfigPath) {
                    return loader.watch<HttpApiError.InternalServerError, never>(nozzleConfigPath, {
                      onError(cause) {
                        console.error("Failure while watching the nozzle config", nozzleConfigPath, cause)
                        return new HttpApiError.InternalServerError()
                      },
                    }).pipe(
                      Stream.map((manifest) => {
                        const jsonData = JSON.stringify(manifest)
                        const sseData = `data: ${jsonData}\n\n`
                        return new TextEncoder().encode(sseData)
                      }),
                      Stream.mapError(() => new HttpApiError.InternalServerError()),
                    )
                  },
                })),
              )
            return yield* HttpServerResponse.stream(stream, {
              contentType: "text/event-stream",
            }).pipe(
              HttpServerResponse.setHeaders({
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                Connection: "keep-alive",
              }),
            )
          }))
        .handle("QueryableEventStream", () =>
          Effect.gen(function*() {
            const stream = yield* resolver
              .queryableEventsStream()
              .pipe(
                Effect.catchAll(() => new HttpApiError.InternalServerError()),
              )

            return yield* HttpServerResponse.stream(stream, {
              contentType: "text/event-stream",
            }).pipe(
              HttpServerResponse.setHeaders({
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                Connection: "keep-alive",
              }),
            )
          }))
        .handle("Sources", () => resolver.sources())
        .handle("DefaultQuery", () =>
          Effect.gen(function*() {
            // 1. Check for nozzle.config existence
            const configPath = yield* loader.find()

            if (Option.isSome(configPath)) {
              // Found nozzle.config, try to build it
              const manifest = yield* loader.build(configPath.value).pipe(
                Effect.mapError(() => new HttpApiError.InternalServerError()),
              )

              // Get the first table from the manifest
              const tables = Object.entries(manifest.tables)
              if (tables.length > 0) {
                const [tableName, table] = tables[0]
                return {
                  title: `SELECT ... ${tableName}`,
                  query: table.input.sql.trim(),
                }
              }
            }

            // 2. Check if events can be derived from the QueryableEvents
            const events = yield* resolver.events().pipe(
              Effect.mapError(() => new HttpApiError.InternalServerError()),
            )

            if (!Chunk.isEmpty(events)) {
              const firstEvent = Chunk.unsafeHead(events)
              // Generate a query for the first event
              const selectFields = firstEvent.params
                .map((param) => `event['${param.name}'] as ${param.name}`)
                .join(", ")

              const query = `SELECT 
  block_hash, 
  tx_hash, 
  address, 
  block_num, 
  timestamp, 
  ${selectFields}
FROM (
  SELECT 
    block_hash, 
    tx_hash, 
    block_num, 
    timestamp, 
    address, 
    evm_decode_log(topic1, topic2, topic3, data, '${firstEvent.signature}') as event
  FROM anvil.logs
  WHERE topic0 = evm_topic('${firstEvent.signature}')
) as decoded`

              return {
                title: `SELECT ... ${firstEvent.name}`,
                query,
              }
            }

            // 3. Use the first dataset source
            const sources = yield* resolver.sources()
            if (sources.length > 0) {
              const firstSource = sources[0]
              return {
                title: `SELECT ... ${firstSource.source}`,
                query: `SELECT * FROM ${firstSource.source} LIMIT 100`,
              }
            }

            // Fallback if nothing is available
            return {
              title: "No default query available",
              query: "-- No datasets or events found",
            }
          }))
        .handle("Query", ({ payload }) =>
          flight.table(payload.query).pipe(
            Effect.flatMap((table) => {
              const schema = Arrow.generateSchema(table.schema)
              return Effect.succeed([table, schema] as const)
            }),
            Effect.flatMap(([table, schema]) => Schema.encodeUnknown(Schema.Array(schema))([...table])),
            Effect.tapErrorCause((cause) => Effect.logError("Failure performing query", Cause.pretty(cause))),
            Effect.mapError(() => new HttpApiError.InternalServerError()),
          ))
    }),
)
const AmpStudioApiLayer = Layer.merge(
  HttpApiBuilder.middlewareCors({
    allowedMethods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Accept", "Cache-Control", "Connection", "Authorization", "X-Requested-With"],
  }),
  HttpApiScalar.layer({ path: "/api/docs" }),
).pipe(
  Layer.provideMerge(HttpApiBuilder.api(AmpStudioApi)),
  Layer.provide(FoundryQueryableEventResolver.layer),
  Layer.provide(AmpStudioApiLive),
)
const ApiLive = HttpApiBuilder.httpApp.pipe(
  Effect.provide(
    Layer.mergeAll(
      AmpStudioApiLayer,
      HttpApiBuilder.Router.Live,
      HttpApiBuilder.Middleware.layer,
    ),
  ),
)

const StudioFileRouter = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path

  const __filename = fileURLToPath(import.meta.url)
  const __dirname = path.dirname(__filename)

  const possibleStudioPaths = [
    // attempt the compiled dist output on build
    path.resolve(__dirname, "studio", "dist"),
    // attempt local dev mode as a fallback
    path.resolve(__dirname, "..", "..", "..", "studio", "dist"),
  ]
  const findStudioDist = Effect.fnUntraced(function*() {
    return yield* Effect.findFirst(possibleStudioPaths, (_) => fs.exists(_).pipe(Effect.orElseSucceed(() => false)))
  })
  const studioClientDist = yield* findStudioDist().pipe(
    // default to first path
    Effect.map((maybe) => Option.getOrElse(maybe, () => possibleStudioPaths[0])),
  )

  return HttpRouter.empty.pipe(
    HttpRouter.get(
      "/",
      HttpServerResponse.file(path.join(studioClientDist, "index.html")).pipe(
        Effect.orElse(() => HttpServerResponse.empty({ status: 404 })),
      ),
    ),
    HttpRouter.get(
      "/manifest.json",
      HttpServerResponse.file(path.join(studioClientDist, "manifest.json")).pipe(
        Effect.orElse(() => HttpServerResponse.empty({ status: 404 })),
      ),
    ),
    // serves the favicon dir files
    HttpRouter.get(
      "/favicon/:file",
      Effect.gen(function*() {
        const file = yield* HttpRouter.params.pipe(
          Effect.map(Struct.get("file")),
          Effect.map(Option.fromNullable),
        )

        if (Option.isNone(file)) {
          return HttpServerResponse.empty({ status: 404 })
        }

        const assets = path.join(studioClientDist, "favicon")
        const normalized = path.normalize(
          path.join(assets, ...file.value.split("/")),
        )
        if (!normalized.startsWith(assets)) {
          return HttpServerResponse.empty({ status: 404 })
        }

        return yield* HttpServerResponse.file(normalized)
      }).pipe(Effect.orElse(() => HttpServerResponse.empty({ status: 404 }))),
    ),
    // serves the logo dir files
    HttpRouter.get(
      "/logo/:file",
      Effect.gen(function*() {
        const file = yield* HttpRouter.params.pipe(
          Effect.map(Struct.get("file")),
          Effect.map(Option.fromNullable),
        )

        if (Option.isNone(file)) {
          return HttpServerResponse.empty({ status: 404 })
        }

        const assets = path.join(studioClientDist, "logo")
        const normalized = path.normalize(
          path.join(assets, ...file.value.split("/")),
        )
        if (!normalized.startsWith(assets)) {
          return HttpServerResponse.empty({ status: 404 })
        }

        return yield* HttpServerResponse.file(normalized)
      }).pipe(Effect.orElse(() => HttpServerResponse.empty({ status: 404 }))),
    ),
    HttpRouter.get(
      "/assets/:file",
      Effect.gen(function*() {
        const file = yield* HttpRouter.params.pipe(
          Effect.map(Struct.get("file")),
          Effect.map(Option.fromNullable),
        )

        if (Option.isNone(file)) {
          return HttpServerResponse.empty({ status: 404 })
        }

        const assets = path.join(studioClientDist, "assets")
        const normalized = path.normalize(
          path.join(assets, ...file.value.split("/")),
        )
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
  files: StudioFileRouter,
}).pipe(
  Effect.map(({ api, files }) =>
    HttpRouter.empty.pipe(
      HttpRouter.mount("/", files),
      HttpRouter.mountApp("/api", api, { includePrefix: true }),
    )
  ),
  Effect.map((router) => HttpServer.serve(HttpMiddleware.logger)(router)),
  Layer.unwrapEffect,
)

const levels = LogLevel.allLevels.map((value) => String.toLowerCase(value.label)) as Array<Lowercase<LogLevel.Literal>>
const studio = Command.make("studio", {
  args: {
    logs: Options.choice("logs", levels).pipe(
      Options.withFallbackConfig(Config.string("NOZZLE_LOG_LEVEL").pipe(Config.withDefault("info"))),
      Options.withDescription("The log level to use"),
      Options.map((value) => LogLevel.fromLiteral(String.capitalize(value) as LogLevel.Literal)),
    ),
    port: Options.integer("port").pipe(
      Options.withAlias("p"),
      Options.withDefault(1615),
      Options.withDescription(
        "The port to run the nozzle dataset studio server on. Default 1615",
      ),
    ),
    open: Options.boolean("open").pipe(
      Options.withDescription(
        "If true, opens the nozzle dataset studio in your browser",
      ),
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
        "Browser to open the amp dataset studio app in. Default is your default selected browser",
      ),
      Options.withDefault("browser"),
    ),
    config: configFile.pipe(Options.optional),
    flightUrl,
    adminUrl,
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
                    `Failure opening amp dataset studio in your browser. Open at http://localhost:${args.port}`,
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
        Layer.tap(() =>
          Console.log(
            `🎉 amp dataset studio started and running at http://localhost:${args.port}`,
          )
        ),
        Layer.launch,
      )
    })
  ),
  Command.provide(ConfigLoader.ConfigLoader.Default),
  Command.provide(FoundryQueryableEventResolver.layer),
  Command.provide(({ args }) => ManifestContext.layerFromConfigFile(args.config)),
  Command.provide(({ args }) => Admin.layer(`${args.adminUrl}`)),
  Command.provide(({ args }) => ArrowFlight.layer(createGrpcTransport({ baseUrl: `${args.flightUrl}` }))),
  Command.provide(({ args }) => Logger.minimumLogLevel(args.logs)),
)

const cli = Command.run(studio, {
  name: "Studio CLI",
  version: "v0.0.1",
})

const layer = Layer.provideMerge(PlatformConfigProvider.layerDotEnvAdd(".env"), NodeContext.layer)

const runnable = Effect.suspend(() => cli(process.argv)).pipe(
  Effect.provide(layer),
  Effect.tapErrorCause((cause) => {
    const squashed = Cause.squash(cause)
    // Command validation errors are already printed by @effect/cli.
    if (ValidationError.isValidationError(squashed)) {
      return Effect.void
    }

    return Console.error(prettyCause(cause))
  }),
)

Effect.suspend(() => runnable).pipe(NodeRuntime.runMain({ disableErrorReporting: true }))

const openBrowser = (
  port: number,
  browser: AppName | "arc" | "safari" | "browser" | "browserPrivate",
) =>
  Effect.async<void, OpenBrowserError>((resume) => {
    const url = `http://localhost:${port}`

    const launch = (appOpts?: { name: string | ReadonlyArray<string> }) =>
      open(url, appOpts ? { app: appOpts } : undefined).then((subprocess) => {
        subprocess.on("spawn", () => resume(Effect.void))
        subprocess.on("error", (err) => resume(Effect.fail(new OpenBrowserError({ cause: err }))))
      })

    const mapBrowserName = (
      b: typeof browser,
    ): string | ReadonlyArray<string> | undefined => {
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

export class OpenBrowserError extends Data.TaggedError(
  "Nozzle/cli/studio/errors/OpenBrowserError",
)<{
  readonly cause: unknown
}> {}
