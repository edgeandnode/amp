import { anyPack, AnySchema } from "@bufbuild/protobuf/wkt";
import { create, toBinary, toJson } from "@bufbuild/protobuf";
import { Args, Command } from "@effect/cli";
import { Effect, Function, Option, Stream } from "effect";
import * as Api from "../../Api.js";
import * as Proto from "../../Proto.js";

export const query = Command.make("query", {
  args: {
    query: Args.text({ name: "query" }).pipe(
      Args.withDescription("The SQL query string"),
    ),
    limit: Args.integer({ name: "limit" }).pipe(
      Args.withDefault(10),
      Args.withDescription("The number of rows to return"),
    ),
  }
}, ({ args }) => Effect.gen(function* () {
  const flight = yield* Api.ArrowFlight;
  const cmd = create(Proto.FlightSql.CommandStatementQuerySchema, {
    query: args.query,
  });

  // TODO: Why is it necessary to pack the command into an Any?
  const any = anyPack(Proto.FlightSql.CommandStatementQuerySchema, cmd);
  const descriptor = create(Proto.Flight.FlightDescriptorSchema, {
    type: Proto.Flight.FlightDescriptor_DescriptorType.CMD,
    cmd: toBinary(AnySchema, any),
  });
  
  const info = yield* Effect.tryPromise((_) => flight.getFlightInfo(descriptor, { signal: _ }));
  const ticket = yield* Option.fromNullable(info.endpoint[0]?.ticket).pipe(Option.match({
    onNone: () => Effect.dieMessage("No flight ticket found"),
    onSome: (ticket) => Effect.succeed(ticket),
  }));
  
  const request = yield* Effect.async<AsyncIterable<Proto.Flight.FlightData>>((resume, signal) => {
    resume(Effect.succeed(flight.doGet(ticket, { signal })))
  });

  const stream = Stream.fromAsyncIterable(request, Function.identity);
  yield* Stream.runForEach(stream, (data) => {
    const json = toJson(Proto.Flight.FlightDataSchema, data);
    return Effect.log(json);
  });
})).pipe(
  Command.withDescription("Run a Nozzle SQL query"),
  Command.provide(Api.ArrowFlight.Default),
);
