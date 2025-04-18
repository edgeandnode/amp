import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { FetchHttpClient } from "@effect/platform";
import { Chunk, Config, Data, Effect, Option, Stream } from "effect";
import { AnySchema } from "@bufbuild/protobuf/wkt";
import { create, toBinary } from "@bufbuild/protobuf";
import { anyPack } from "@bufbuild/protobuf/wkt";
import { RecordBatchReader, Table } from "apache-arrow";
import * as Proto from "./Proto.js";

export class ArrowFlightError extends Data.TaggedError("ArrowFlightError")<{
  cause: unknown;
}> {}

export class ArrowFlight extends Effect.Service<ArrowFlight>()("Nozzle/Api/ArrowFlight", {
  dependencies: [FetchHttpClient.layer],
  effect: Effect.gen(function* () {
    const url = yield* Config.string("NOZZLE_ARROW_FLIGHT_URL").pipe(Effect.orDie);
    const transport = createGrpcTransport({
      baseUrl: url,
    });

    const client = createClient(Proto.Flight.FlightService, transport);
    const stream = (query: string) => Effect.gen(function* () {
      const cmd = create(Proto.FlightSql.CommandStatementQuerySchema, {
        query,
      });
    
      // TODO: Why is it necessary to pack the command into an Any?
      const any = anyPack(Proto.FlightSql.CommandStatementQuerySchema, cmd);
      const descriptor = create(Proto.Flight.FlightDescriptorSchema, {
        type: Proto.Flight.FlightDescriptor_DescriptorType.CMD,
        cmd: toBinary(AnySchema, any),
      });
    
      const info = yield* Effect.tryPromise({
        try: (_) => client.getFlightInfo(descriptor, { signal: _ }),
        catch: (cause) => new ArrowFlightError({ cause }),
      });

      const ticket = yield* Option.fromNullable(info.endpoint[0]?.ticket).pipe(Option.match({
        onNone: () => new ArrowFlightError({ cause: new Error("No flight ticket found") }),
        onSome: (ticket) => Effect.succeed(ticket),
      }));
    
      const request = yield* Effect.async<AsyncIterable<Proto.Flight.FlightData>>((resume, signal) => {
        resume(Effect.succeed(client.doGet(ticket, { signal })))
      });
    
      const reader = yield* Effect.tryPromise({
        catch: (cause) => new ArrowFlightError({ cause }),
        try: () => RecordBatchReader.from({
          [Symbol.asyncIterator]: async function* () {
            for await (const data of request) {
              // The data length needs to be padded to multiple of 8 bytes.
              const padding = data.dataBody.length % 8 === 0 ? 0 : 8 - (data.dataBody.length % 8);
              const length = 8 + data.dataHeader.length + padding + data.dataBody.length;
              const buf = new ArrayBuffer(length);
              const view = new DataView(buf);
              
              // Write header length and continuation token.
              view.setUint32(0, 0xFFFFFFFF, true);
              view.setUint32(4, data.dataHeader.length, true);
              
              // Write header and body data.
              new Uint8Array(buf, 8, data.dataHeader.length).set(data.dataHeader);
              new Uint8Array(buf, 8 + data.dataHeader.length + padding, data.dataBody.length).set(data.dataBody);
    
              yield new Uint8Array(buf);
            }
          }
        } as AsyncIterable<Uint8Array>)
      })
    
      return Stream.fromAsyncIterable(reader, (cause) => new ArrowFlightError({ cause }));
    }).pipe(Stream.unwrap);

    const table = (query: string) => Stream.runCollect(stream(query)).pipe(
      Effect.map((batches) => new Table(Chunk.toArray(batches)))
    )

    return { client, stream, table };
  }),
}) {}
