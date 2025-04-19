import { Schema } from "effect"

export class Erc20Transfers extends Schema.Class<Erc20Transfers>("Nozzle/Schema/Erc20Transfers")({
  block_num: Schema.NonNegativeInt,
  timestamp: Schema.DateTimeUtc,
  from: Schema.String.pipe(Schema.optionalWith({ nullable: true })),
  to: Schema.String.pipe(Schema.optionalWith({ nullable: true })),
  value: Schema.String.pipe(Schema.optionalWith({ nullable: true }))
}) {}
