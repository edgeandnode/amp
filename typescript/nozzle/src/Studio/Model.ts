import * as Schema from "effect/Schema"

export class QueryableEvent extends Schema.Class<QueryableEvent>(
  "Nozzle/studio/models/QueryableEvent",
)({
  name: Schema.NonEmptyTrimmedString.annotations({
    identifier: "QueryableEvent.name",
    description: "Parsed event name",
    examples: ["Count", "Transfer"],
  }),
  params: Schema.Array(
    Schema.Struct({
      name: Schema.NonEmptyTrimmedString.annotations({
        identifier: "QueryableEvent.params.name",
        description: "Name of the emitted event param",
      }),
      datatype: Schema.NonEmptyTrimmedString.annotations({
        identifier: "QueryableEvent.params.datatype",
        description: "Type of the emitted event param",
        examples: ["uint256", "bytes32", "address"],
      }),
      indexed: Schema.NullOr(Schema.Boolean).annotations({
        identifier: "QueryableEvent.params.indexed",
        description: "If true, the emitted parameter is indexed",
      }),
    }),
  ).annotations({
    identifier: "QueryableEvent.params",
    description: "The parameters emitted with the event",
    examples: [
      [{ name: "count", datatype: "uint256", indexed: false }],
      [
        {
          name: "from",
          datatype: "address",
          indexed: true,
        },
        {
          name: "to",
          datatype: "address",
          indexed: true,
        },
        {
          name: "value",
          datatype: "uint256",
          indexed: null,
        },
      ],
    ],
  }),
  signature: Schema.NonEmptyTrimmedString.annotations({
    identifier: "QueryableEvent.signature",
    description: "The event signature, including the event params.",
    examples: [
      "Count(uint256 count)",
      "Transfer(address indexed from, address indexed to, uint256 value)",
    ],
  }),
  source: Schema.Array(
    Schema.NonEmptyTrimmedString.annotations({
      identifier: "QueryableEvent.source",
      description: "Smart Contract source where the event comes from",
      examples: ["contracts/src/Counter.sol"],
    }),
  ).pipe(
    Schema.minItems(1),
  ),
}) {}
export class QueryableEventStream extends Schema.Class<QueryableEventStream>(
  "Nozzle/studio/models/QueryableEventStream",
)({
  events: Schema.Array(QueryableEvent),
}) {}
