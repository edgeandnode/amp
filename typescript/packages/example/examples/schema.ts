import { Schema } from "effect"

export class Counts extends Schema.Class<Counts>("Nozzle/Schema/Counts")({
  address: Schema.String,
  block_num: Schema.NonNegativeInt,
  timestamp: Schema.DateTimeUtc,
  count: Schema.String.pipe(Schema.optionalWith({ nullable: true })),
}) {}

export class Transfers extends Schema.Class<Transfers>("Nozzle/Schema/Transfers")({
  block_num: Schema.NonNegativeInt,
  timestamp: Schema.DateTimeUtc,
  from: Schema.String.pipe(Schema.optionalWith({ nullable: true })),
  to: Schema.String.pipe(Schema.optionalWith({ nullable: true })),
  value: Schema.String.pipe(Schema.optionalWith({ nullable: true })),
}) {}
