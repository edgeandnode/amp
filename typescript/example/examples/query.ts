import { JsonLines } from "nozzl";
import { Effect, Stream, Pretty, Console } from "effect";
import { Erc20Transfers } from "./schema.js";

const pretty = Pretty.make(Erc20Transfers);

const program = Effect.gen(function* () {
  const jsonl = yield* JsonLines.JsonLines;
  const stream = jsonl.stream(Erc20Transfers, "SELECT * FROM transfers_eth_mainnet.erc20_transfers LIMIT 100")
  yield* Stream.runForEach(stream, (value) => Console.log(pretty(value)));
});

Effect.runPromise(program.pipe(Effect.provide(JsonLines.JsonLines.Default)));
