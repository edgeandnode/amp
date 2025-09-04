import { createGrpcTransport } from "@connectrpc/connect-node"
import { Command, Options } from "@effect/cli"
import {
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
import { NodeHttpServer } from "@effect/platform-node"
import {
  Cause,
  Config,
  Console,
  Data,
  Effect,
  Layer,
  Option,
  Schema,
  Stream,
  String as EffectString,
  Struct,
} from "effect"
import { createServer } from "node:http"
import { fileURLToPath } from "node:url"
import open, { type AppName, apps } from "open"

import * as ArrowFlight from "../../api/ArrowFlight.ts"
import * as Registry from "../../api/Registry.ts"
import * as Arrow from "../../Arrow.ts"
import * as ConfigLoader from "../../ConfigLoader.ts"
import { FoundryQueryableEventResolver, Model as StudioModel } from "../../Studio/index.js"

class NozzleStudioApiRouter extends HttpApiGroup.make("NozzleStudioApi")
  .add(
    HttpApiEndpoint.get("NozzleConfigStream")`/config/stream`
      .addSuccess(
        Schema.String.pipe(
          HttpApiSchema.withEncoding({
            kind: "Json",
            contentType: "text/event-stream",
          }),
        ),
      ).annotateContext(
        OpenApi.annotations({
          title: "Nozzle Config",
          description: "Watches the nozzle config and emits a stream of events of changes to the file",
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
    HttpApiEndpoint.get("Metadata")`/metadata`
      .addSuccess(Schema.Array(StudioModel.DatasetMetadata))
      .annotateContext(
        OpenApi.annotations({
          title: "Metadata about the nozzle datasets",
          version: "v1",
          description:
            "Provides metadata about how to query the dataset. Returns an array of dataset metadata, where each item contains metadata columns and the event source to query",
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
  .add(
    HttpApiEndpoint.post("QueryStream")`/query/stream`
      .setPayload(
        Schema.Union(
          Schema.Struct({ query: Schema.NonEmptyTrimmedString }),
          Schema.parseJson(Schema.Struct({ query: Schema.NonEmptyTrimmedString })),
        ),
      )
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
          title: "Dataset query stream",
          version: "v1",
          description: "Performs the Dataset query and returns a stream of the data",
        }),
      ),
  )
  .prefix("/v1")
{}

class NozzleStudioApi extends HttpApi.make("NozzleStudioApi")
  .add(NozzleStudioApiRouter)
  .prefix("/api")
{}

const NozzleStudioApiLive = HttpApiBuilder.group(
  NozzleStudioApi,
  "NozzleStudioApi",
  (handlers) =>
    Effect.gen(function*() {
      const loader = yield* ConfigLoader.ConfigLoader
      const resolver = yield* FoundryQueryableEventResolver.FoundryQueryableEventResolver
      const flight = yield* ArrowFlight.ArrowFlight

      return handlers
        .handle("NozzleConfigStream", () =>
          Effect.gen(function*() {
            const stream: Stream.Stream<Uint8Array<ArrayBuffer>, HttpApiError.InternalServerError, never> =
              yield* loader.find().pipe(
                Effect.map(Option.match({
                  onNone() {
                    console.log("No nozzle config found :{")
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
        .handle("Metadata", () => resolver.metadata())
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
        .handle("QueryStream", ({ payload }) =>
          Effect.gen(function*() {
            const stream = flight.stream(payload.query).pipe(
              Stream.mapEffect((batch) =>
                Effect.gen(function*() {
                  const schema = Arrow.generateSchema(batch.schema)
                  const data = batch.toArray()
                  const encodedData = yield* Schema.encodeUnknown(Schema.Array(schema))(data)
                  // Format as SSE
                  const jsonData = JSON.stringify(encodedData)
                  const sseData = `data: ${jsonData}\n\n`
                  return new TextEncoder().encode(sseData)
                })
              ),
              Stream.tapErrorCause((cause) => Console.error("Failure performing query stream", Cause.pretty(cause))),
              Stream.catchAll(() => new HttpApiError.InternalServerError()),
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
    }),
)
const NozzleStudioApiLayer = Layer.merge(
  HttpApiBuilder.middlewareCors(),
  HttpApiScalar.layer({ path: "/api/docs" }),
).pipe(
  Layer.provideMerge(HttpApiBuilder.api(NozzleStudioApi)),
  Layer.provide(FoundryQueryableEventResolver.layer),
  Layer.provide(NozzleStudioApiLive),
)
const ApiLive = HttpApiBuilder.httpApp.pipe(
  Effect.provide(
    Layer.mergeAll(
      NozzleStudioApiLayer,
      HttpApiBuilder.Router.Live,
      HttpApiBuilder.Middleware.layer,
    ),
  ),
)

const StudioFileRouter = Effect.gen(function*() {
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
  const studioClientDist = isLocal
    ? path.resolve(__dirname, "..", "..", "..", "..", "studio", "dist")
    : path.resolve(__dirname, "studio", "dist")

  return HttpRouter.empty.pipe(
    HttpRouter.get(
      "/",
      HttpServerResponse.file(path.join(studioClientDist, "index.html")).pipe(
        Effect.orElse(() => HttpServerResponse.empty({ status: 404 })),
      ),
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
      Options.withDefault(3000),
      Options.withDescription(
        "The port to run the nozzle dataset studio server on. Default 3000",
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
        "Browser to open the nozzle dataset studio app in. Default is your default selected browser",
      ),
      Options.withDefault("browser"),
    ),
    flight: Options.text("flight-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_ARROW_FLIGHT_URL").pipe(Config.withDefault("http://localhost:1602")),
      ),
      Options.withDescription("The Arrow Flight URL to use for the query"),
      Options.withSchema(Schema.URL),
    ),
    registry: Options.text("registry-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_REGISTRY_URL").pipe(Config.withDefault("http://localhost:1611")),
      ),
      Options.withDescription("The url of the Nozzle registry server"),
      Options.withSchema(Schema.URL),
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
        Layer.tap(() =>
          Console.log(
            `🎉 nozzle dataset studio started and running at http://localhost:${args.port}`,
          )
        ),
        Layer.launch,
      )
    })
  ),
  Command.provide(({ args }) =>
    ConfigLoader.ConfigLoader.Default.pipe(Layer.provide(Registry.layer(`${args.registry}`)))
  ),
  Command.provide(FoundryQueryableEventResolver.layer),
  Command.provide(({ args }) => ArrowFlight.layer(createGrpcTransport({ baseUrl: `${args.flight}` }))),
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
  "Nozzle/cli/studio/errors/OpenBrowserError",
)<{
  readonly cause: unknown
}> {}
