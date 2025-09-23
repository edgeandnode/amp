import { connectNodeAdapter, createGrpcTransport } from "@connectrpc/connect-node"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { createServer, type Server } from "node:http"
import * as ArrowFlight from "../../api/ArrowFlight.ts"
import { flightUrl } from "../common.ts"

export const proxy = Command.make("proxy", {
  args: {
    port: Options.integer("port").pipe(
      Options.withDescription("The port to listen on"),
      Options.withSchema(Schema.Int.pipe(Schema.between(1, 65535))),
      Options.withDefault(8080),
    ),
    flightUrl,
  },
}).pipe(
  Command.withDescription("Launches a Connect proxy for the Arrow Flight server"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
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
      }).pipe(Effect.tap(() => Effect.log(`Proxy server listening on port ${args.port}`)))

      const release = (server: Server) =>
        Effect.async((resume) => {
          server.close(() => resume(Effect.void))
        }).pipe(Effect.tap(() => Effect.log(`Proxy server closed on port ${args.port}`)))

      yield* Effect.acquireRelease(acquire, release).pipe(Effect.zip(Effect.never))
    }, Effect.scoped),
  ),
  Command.provide(({ args }) => ArrowFlight.layer(createGrpcTransport({ baseUrl: `${args.flightUrl}` }))),
)
