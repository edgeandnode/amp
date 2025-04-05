import { Api } from "@nozzle/nozzle";
import { Effect, Console, Stream, Schema } from "effect";
import { Erc20Transfers } from "./schema.js";

const program = Effect.gen(function* () {
  const api = yield* Api.Api;
  const result = yield* api.jsonl.query({
    payload: "SELECT * FROM transfers_eth_mainnet.erc20_transfers LIMIT 10",
  });

  const stream = Stream.make(result).pipe(
    Stream.splitLines,
    Stream.map((_) => JSON.parse(_)),
    Stream.flatMap(Schema.decodeUnknown(Erc20Transfers)),
  );

  const values = yield* Stream.runCollect(stream);
  yield* Console.log(values);
});

Effect.runPromise(program.pipe(Effect.provide(Api.Api.Default)));
