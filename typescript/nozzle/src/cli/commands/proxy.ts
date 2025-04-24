import { connectNodeAdapter, createGrpcTransport } from "@connectrpc/connect-node"
import { Args, Command, Options } from "@effect/cli"
import { Config, Effect } from "effect"
import { createServer, type Server } from "node:http"
import * as ArrowFlight from "../../ArrowFlight.js"

export const proxy = Command.make("proxy", {
  args: {
    port: Args.integer({ name: "port" }).pipe(
      Args.withDefault(8080),
      Args.withDescription("The port to listen on"),
    ),
    admin: Options.text("admin-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_ADMIN_URL").pipe(Config.withDefault("http://localhost:1610")),
      ),
      Options.withDescription("The admin URL to use for the proxy"),
    ),
    flight: Options.text("flight-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_ARROW_FLIGHT_URL").pipe(Config.withDefault("http://localhost:1602")),
      ),
      Options.withDescription("The Arrow Flight URL to use for the proxy"),
    ),
  },
}, ({ args }) =>
  Effect.gen(function*() {
    const flight = yield* ArrowFlight.ArrowFlight
    const adapter = connectNodeAdapter({
      routes: (router) => {
        router.service(ArrowFlight.Flight.FlightService, {
          doGet: (request) => flight.client.doGet(request),
          doPut: (request) => flight.client.doPut(request),
          doAction: (request) => flight.client.doAction(request),
          doExchange: (request) => flight.client.doExchange(request),
          getSchema: (request) => flight.client.getSchema(request),
          getFlightInfo: (request) => flight.client.getFlightInfo(request),
          listActions: (request) => flight.client.listActions(request),
          listFlights: (request) => flight.client.listFlights(request),
          pollFlightInfo: (request) => flight.client.pollFlightInfo(request),
        })
      },
    })

    const acquire = Effect.async<Server>((resume) => {
      const server = createServer(adapter)
      server.listen(args.port, () => resume(Effect.succeed(server)))
    }).pipe(
      Effect.tap(() => Effect.log(`Proxy server listening on port ${args.port}`)),
    )

    const release = (server: Server) =>
      Effect.async((resume) => {
        server.close(() => resume(Effect.void))
      }).pipe(
        Effect.tap(() => Effect.log(`Proxy server closed on port ${args.port}`)),
      )

    yield* Effect.acquireRelease(acquire, release).pipe(Effect.zip(Effect.never))
  }).pipe(
    Effect.scoped,
    Effect.provide(ArrowFlight.layer(createGrpcTransport({ baseUrl: args.flight }))),
  )).pipe(
    Command.withDescription("Launches a Connect proxy for the Arrow Flight server"),
  )
