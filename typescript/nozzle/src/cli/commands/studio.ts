import { Command, Options } from "@effect/cli"
import {
  HttpApi,
  HttpApiBuilder,
  HttpApiEndpoint,
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
import { NodeHttpServer } from "@effect/platform-node"
import { Console, Data, Effect, Layer, Option, Schedule, Schema, Stream, String as EffectString, Struct } from "effect"
import { createServer } from "node:http"
import { fileURLToPath } from "node:url"
import open, { type AppName, apps } from "open"

export class QueryableEvent extends Schema.Class<QueryableEvent>("Nozzle/models/events/QueryableEvent")({
  name: Schema.NonEmptyTrimmedString.annotations({
    identifier: "QueryableEvent.name",
    description: "Parsed event name",
    examples: ["Count", "Transfer"],
  }),
  params: Schema.Array(Schema.Struct({
    name: Schema.NonEmptyTrimmedString.annotations({
      identifier: "QueryableEvent.params.name",
      description: "Name of the emitted event param",
    }),
    datatype: Schema.NonEmptyTrimmedString.annotations({
      identifier: "QueryableEvent.params.datatype",
      description: "Type of the emitted event param",
      examples: ["uint256", "bytes32", "address"],
    }),
    indexed: Schema.NullOr(Schema.Boolean).annotations({
      identifier: "QueryableEvent.params.indexed",
      description: "If true, the emitted parameter is indexed",
    }),
  })).annotations({
    identifier: "QueryableEvent.params",
    description: "The parameters emitted with the event",
    examples: [[{ name: "count", datatype: "uint256", indexed: false }], [
      {
        name: "from",
        datatype: "address",
        indexed: true,
      },
      {
        name: "to",
        datatype: "address",
        indexed: true,
      },
      {
        name: "value",
        datatype: "uint256",
        indexed: null,
      },
    ]],
  }),
  signature: Schema.NonEmptyTrimmedString.annotations({
    identifier: "QueryableEvent.signature",
    description: "The event signature, including the event params.",
    examples: ["Count(uint256 count)", "Transfer(address indexed from, address indexed to, uint256 value)"],
  }),
  source: Schema.NonEmptyTrimmedString.annotations({
    identifier: "QueryableEvent.source",
    description: "Smart Contract source where the event comes from",
    examples: ["contracts/src/Counter.sol"],
  }),
}) {}
export class QueryableEventStream
  extends Schema.Class<QueryableEventStream>("Nozzle/models/events/QueryableEventStream")({
    events: Schema.Array(QueryableEvent),
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
    .annotateContext(OpenApi.annotations({
      title: "Queryable Smart Contract events stream",
      version: "v1",
      description:
        "Listens to file changes on the smart contracts/abis and emits updates of the available events to query",
    })),
).prefix("/v1") {}

class NozzleStudioApi extends HttpApi.make("NozzleStudioApi").add(NozzleStudioApiRouter).prefix("/api") {}

const queryableEventStream = Stream.make(
  QueryableEventStream.make({
    events: [
      QueryableEvent.make({
        name: "Count",
        params: [{ name: "count", datatype: "uint256", indexed: false }],
        signature: "Count(uint256 count)",
        source: "./contracts/src/Counter.sol",
      }),
      QueryableEvent.make({
        name: "Transfer",
        params: [
          { name: "from", datatype: "address", indexed: true },
          { name: "to", datatype: "address", indexed: true },
          { name: "value", datatype: "uint256", indexed: false },
        ],
        signature: "Transfer(address indexed from, address indexed to, uint256 value)",
        source: "./contracts/src/Counter.sol",
      }),
    ],
  }),
).pipe(
  Stream.schedule(Schedule.spaced("500 millis")),
  Stream.map((item) => {
    const jsonData = JSON.stringify(item)
    const sseData = `data: ${jsonData}\n\n`
    return new TextEncoder().encode(sseData)
  }),
)

const NozzleStudioApiLive = HttpApiBuilder.group(
  NozzleStudioApi,
  "NozzleStudioApi",
  (handlers) =>
    handlers.handle(
      "QueryableEventStream",
      () =>
        Effect.gen(function*() {
          return yield* HttpServerResponse.stream(queryableEventStream).pipe(
            HttpServerResponse.setHeaders({
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

export class OpenBrowserError extends Data.TaggedError("/nozzl/errors/OpenBrowserError")<{
  readonly cause: unknown
}> {}
