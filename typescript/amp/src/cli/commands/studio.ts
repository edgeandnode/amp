import { createGrpcTransport } from "@connectrpc/connect-node"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer"
import * as FileSystem from "@effect/platform/FileSystem"
import * as HttpApi from "@effect/platform/HttpApi"
import * as HttpApiBuilder from "@effect/platform/HttpApiBuilder"
import * as HttpApiEndpoint from "@effect/platform/HttpApiEndpoint"
import * as HttpApiError from "@effect/platform/HttpApiError"
import * as HttpApiGroup from "@effect/platform/HttpApiGroup"
import * as HttpApiScalar from "@effect/platform/HttpApiScalar"
import * as HttpApiSchema from "@effect/platform/HttpApiSchema"
import * as HttpMiddleware from "@effect/platform/HttpMiddleware"
import * as HttpRouter from "@effect/platform/HttpRouter"
import * as HttpServer from "@effect/platform/HttpServer"
import * as HttpServerResponse from "@effect/platform/HttpServerResponse"
import * as OpenApi from "@effect/platform/OpenApi"
import * as Path from "@effect/platform/Path"
import * as ApacheArrow from "apache-arrow"
import * as Cause from "effect/Cause"
import * as Chunk from "effect/Chunk"
import * as Console from "effect/Console"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Struct from "effect/Struct"
import { createServer } from "node:http"
import { fileURLToPath } from "node:url"
import open, { type AppName, apps } from "open"
import * as Admin from "../../api/Admin.ts"
import * as ArrowFlight from "../../api/ArrowFlight.ts"
import * as Arrow from "../../Arrow.ts"
import * as ConfigLoader from "../../ConfigLoader.ts"
import * as ManifestBuilder from "../../ManifestBuilder.ts"
import { FoundryQueryableEventResolver, Model as StudioModel } from "../../studio/index.ts"
import { adminUrl, configFile, flightUrl } from "../common.ts"

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
            "Builds a default query to hydrate in the initial tab on the playground. Derived in order of: 1. parsed amp config, 2. parsed contract artifacts, 3. dataset sources",
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
                Effect.tapErrorCause(() => Console.error("Failure finding amp.config from builder")),
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
                  onSome(ampConfigPath) {
                    return loader.watch<HttpApiError.InternalServerError, never>(ampConfigPath, {
                      onError: (cause) =>
                        Effect.gen(function*() {
                          yield* Console.error("Failure while watching the amp config", ampConfigPath, cause)
                          return yield* new HttpApiError.InternalServerError()
                        }),
                    }).pipe(
                      Stream.tapErrorCause((cause) =>
                        Console.error("Failure watching config file", Cause.pretty(cause))
                      ),
                      Stream.mapEffect((manifest) =>
                        Effect.gen(function*() {
                          // Encode the manifest using Effect Schema to properly serialize dependencies
                          const encoded = yield* Schema.encode(ManifestBuilder.ManifestBuildResult)(manifest)
                          const jsonData = JSON.stringify(encoded)
                          const sseData = `data: ${jsonData}\n\n`
                          return new TextEncoder().encode(sseData)
                        })
                      ),
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
            // Strategy 1: Try amp.config - get first table
            const tryConfigQuery = loader.find().pipe(
              Effect.flatMap(
                Option.match({
                  onNone: () => Effect.fail("NoConfig" as const),
                  onSome: (configPath) =>
                    loader.build(configPath).pipe(
                      Effect.map((buildResult) => {
                        const reference = `"${buildResult.metadata.namespace || "_"}/${buildResult.metadata.name}@dev"`
                        const tables = Object.entries(buildResult.manifest.tables)
                        return tables.length > 0
                          ? Option.some({
                            title: `SELECT ... ${tables[0][0]}`,
                            query: `SELECT * FROM ${reference}."${tables[0][0]}"`,
                          })
                          : Option.none()
                      }),
                      Effect.flatMap(
                        Option.match({
                          onNone: () => Effect.fail("NoTables" as const),
                          onSome: Effect.succeed,
                        }),
                      ),
                      Effect.tapErrorCause((cause) => Effect.logError("Failure building Config", Cause.pretty(cause))),
                      Effect.catchAll(() => Effect.fail("ConfigError" as const)),
                    ),
                }),
              ),
            )

            // Strategy 2: Try events - generate query for first event
            const tryEventQuery = resolver.events().pipe(
              Effect.flatMap((events) =>
                Chunk.head(events).pipe(
                  Option.match({
                    onNone: () => Effect.fail("NoEvents" as const),
                    onSome: (firstEvent) => {
                      const selectFields = firstEvent.params
                        .map((param) => `event['${param.name}'] as ${param.name}`)
                        .join(", ")

                      return Effect.succeed({
                        title: `SELECT ... ${firstEvent.name}`,
                        query: `SELECT
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
) as decoded`,
                      })
                    },
                  }),
                )
              ),
              Effect.tapErrorCause((cause) => Effect.logError("Failure resolving events", Cause.pretty(cause))),
              Effect.catchAll(() => Effect.fail("EventError" as const)),
            )

            // Strategy 3: Use the first dataset source
            const trySourceQuery = resolver.sources().pipe(
              Effect.flatMap((sources) =>
                sources.length > 0
                  ? Effect.succeed({
                    title: `SELECT ... ${sources[0].source}`,
                    query: `SELECT * FROM ${sources[0].source} LIMIT 100`,
                  })
                  : Effect.fail("NoSources" as const)
              ),
            )

            // Fallback if nothing is available
            const fallbackQuery = Effect.succeed({
              title: "No default query available",
              query: "-- No datasets or events found",
            })

            // Chain strategies with orElse for clean fallback
            return yield* tryConfigQuery.pipe(
              Effect.orElse(() => tryEventQuery),
              Effect.orElse(() => trySourceQuery),
              Effect.orElse(() => fallbackQuery),
            )
          }))
        .handle("Query", ({ payload }) =>
          flight.query(payload.query).pipe(
            Stream.mapEffect(Effect.fn(function*(batch) {
              const table = new ApacheArrow.Table(batch)
              const schema = Schema.Array(Arrow.generateSchema(batch.schema))
              const data = yield* Schema.encode(schema)(table.toArray())
              return data
            })),
            Stream.runCollect,
            Effect.map(Chunk.toReadonlyArray),
            Effect.map((_) => _.flat()),
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
    // attempt the compiled dist output on build (published package)
    // from dist/cli/commands -> dist/studio/dist
    path.resolve(__dirname, "..", "..", "studio", "dist"),
    // attempt local dev mode as a fallback
    // from src/cli/commands -> typescript/studio/dist
    path.resolve(__dirname, "..", "..", "..", "..", "studio", "dist"),
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

export const studio = Command.make("studio", {
  args: {
    port: Options.integer("port").pipe(
      Options.withAlias("p"),
      Options.withDefault(1615),
      Options.withDescription(
        "The port to run the amp dataset studio server on. Default 1615",
      ),
    ),
    open: Options.boolean("open").pipe(
      Options.withDescription(
        "If true, opens the amp dataset studio in your browser",
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
  Command.withDescription("Opens the amp dataset studio visualization tool"),
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
  Command.provide(({ args }) => Admin.layer(`${args.adminUrl}`)),
  Command.provide(({ args }) => ArrowFlight.layer(createGrpcTransport({ baseUrl: `${args.flightUrl}` }))),
)

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
  "Amp/cli/studio/errors/OpenBrowserError",
)<{
  readonly cause: unknown
}> {}
