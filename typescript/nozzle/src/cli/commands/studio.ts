import { Command, Options } from "@effect/cli"
import { HttpMiddleware, HttpRouter, HttpServer, HttpServerResponse, Path } from "@effect/platform"
import { NodeHttpServer } from "@effect/platform-node"
import { Console, Data, Effect, Layer, Option, String as EffectString, Struct } from "effect"
import { createServer } from "node:http"
import { fileURLToPath } from "node:url"
import open, { type AppName, apps } from "open"

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
  files: DatasetWorksFileRouter,
}).pipe(
  Effect.map(({ files }) => HttpRouter.empty.pipe(HttpRouter.mount("/", files))),
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
