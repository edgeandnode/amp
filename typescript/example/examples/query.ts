import { Api } from "@nozzle/nozzle";
import { Effect, Stream, Schema, Pretty } from "effect";
import { Erc20Transfers } from "./schema.js";

const pretty = Pretty.make(Erc20Transfers);

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

  yield* Stream.runForEach(stream, (value) => Effect.log(pretty(value)));
});

Effect.runPromise(program.pipe(Effect.provide(Api.Api.Default)));
