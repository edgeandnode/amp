import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { FetchHttpClient } from "@effect/platform";
import { Chunk, Config, Data, DateTime, Effect, Option, Schema, Stream, Types } from "effect";
import { AnySchema } from "@bufbuild/protobuf/wkt";
import { create, toBinary } from "@bufbuild/protobuf";
import { anyPack } from "@bufbuild/protobuf/wkt";
import { DataType, RecordBatchReader, Table, Field as ArrowSchemaField, Schema as ArrowSchema, Type, TypeMap } from "apache-arrow";
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
        try: () => RecordBatchReader.from<TypeMap>({
          [Symbol.asyncIterator]: async function* () {
            for await (const data of request) {
              // The data length needs to be padded to multiple of 8 bytes.
              const padding = data.dataBody.length % 8 === 0 ? 0 : 8 - (data.dataBody.length % 8);

              // Create buffer and write metadata prefix.
              const length = 8 + data.dataHeader.length + padding + data.dataBody.length;
              const buf = new ArrayBuffer(length);
              const view = new DataView(buf);
              view.setUint32(0, 0xFFFFFFFF, true); // Continuation token
              view.setUint32(4, data.dataHeader.length, true); // Header length

              // Copy header and body into buffer.
              const bytes = new Uint8Array(buf);
              bytes.set(data.dataHeader, 8);
              bytes.set(data.dataBody, 8 + data.dataHeader.length + padding);
    
              yield buf;
            }
          }
        } as AsyncIterable<ArrayBuffer>)
      })
    
      return Stream.fromAsyncIterable(reader, (cause) => new ArrowFlightError({ cause }));
    }).pipe(Stream.unwrap);

    const table = (query: string) => Stream.runCollect(stream(query)).pipe(
      Effect.map((batches) => new Table(Chunk.toArray(batches)))
    )

    return { client, stream, table };
  }),
}) {}

export const generateSchema = <T extends TypeMap>(schema: ArrowSchema<T>): Schema.Schema.AnyNoContext => {
  return Schema.Struct(generateFields(schema.fields)) as any;
}

const generateFields = <T extends DataType>(fields: Array<ArrowSchemaField<T>>) => {
  const output: Types.Mutable<Schema.Struct.Fields> = {};
  for (const field of fields) {
    output[field.name] = generateField(field);
  }

  return output as Schema.Struct.Fields;
}

const generateField = <T extends DataType>(field: ArrowSchemaField<T>): Schema.Schema.AnyNoContext => {
  return field.nullable ? Schema.NullOr(generateType(field.type)) : generateType(field.type);
}

const generateType = <T extends DataType>(type: T): Schema.Schema.AnyNoContext => {
  switch (type.typeId) {
    case Type.FixedSizeBinary: return Schema.Uint8ArrayFromHex;
    case Type.Binary: return Schema.Uint8ArrayFromHex;
    case Type.Utf8: return Schema.String;
    case Type.Int: return Schema.Union(Schema.BigInt, Schema.Int);
    case Type.Int8: return Schema.BigInt;
    case Type.Int16: return Schema.BigInt;
    case Type.Int32: return Schema.BigInt;
    case Type.Int64: return Schema.BigInt;
    case Type.Uint8: return Schema.NonNegativeInt;
    case Type.Uint16: return Schema.NonNegativeInt;
    case Type.Uint32: return Schema.NonNegativeInt;
    case Type.Uint64: return Schema.NonNegativeInt;
    case Type.Float: return Schema.Number;
    case Type.Float16: return Schema.Unknown; // TODO: Implement
    case Type.Float32: return Schema.Unknown; // TODO: Implement
    case Type.Float64: return Schema.Unknown; // TODO: Implement
    case Type.Bool: return Schema.Boolean;
    case Type.Decimal: return Schema.Unknown; // TODO: Implement
    case Type.DateDay: return Schema.Unknown; // TODO: Implement
    case Type.Date: return Schema.Unknown; // TODO: Implement
    case Type.Time: return Schema.Unknown; // TODO: Implement
    case Type.Timestamp: return Schema.DateTimeUtc.pipe(Schema.transform(Schema.Number, {
      decode: (_) => DateTime.toEpochMillis(_),
      encode: (_) => DateTime.unsafeMake(_),
      strict: true,
    }));
    case Type.Interval: return Schema.Unknown; // TODO: Implement
    case Type.Duration: return Schema.Unknown; // TODO: Implement
    case Type.FixedSizeList: return Schema.Array(generateField(type.children[0]!.type));
    case Type.List: return Schema.Array(generateField(type.children[0]!.type));
    case Type.Struct: return Schema.Struct(generateFields(type.children)) as any;
    case Type.Union: return Schema.Union(...type.children.map((_) => generateField(_.type)));
    case Type.Map: return Schema.Record({ key: generateField(type.children[0]!.type), value: generateField(type.children[1]!.type) });
    case Type.LargeUtf8: return Schema.String;
    case Type.LargeBinary: return Schema.Uint8ArrayFromHex;
    case Type.Dictionary: return Schema.String;
    case Type.DenseUnion: return Schema.Union(...type.children.map((_) => generateField(_.type)));
    case Type.SparseUnion: return Schema.Union(...type.children.map((_) => generateField(_.type)));
    case Type.IntervalDayTime: return Schema.Unknown; // TODO: Implement
    case Type.IntervalYearMonth: return Schema.Unknown; // TODO: Implement
    case Type.DurationSecond: return Schema.Unknown; // TODO: Implement
    case Type.DurationMillisecond: return Schema.Unknown; // TODO: Implement
    case Type.DurationMicrosecond: return Schema.Unknown; // TODO: Implement
    case Type.DurationNanosecond: return Schema.Unknown; // TODO: Implement
    case Type.Null: return Schema.Null;
    default: throw new Error(`Unsupported type ${type.typeId}`);
  }
}
