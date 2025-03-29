import { Schema } from "effect";

export class Dependency extends Schema.Class<Dependency>("Dependency")({
  owner: Schema.String,
  name: Schema.String,
  version: Schema.String,
}) {}

export class TableDefinition extends Schema.Class<TableDefinition>("TableDefinition")({
  sql: Schema.String,
}) {}

export class DatasetDefinition extends Schema.Class<DatasetDefinition>("DatasetDefinition")({
  name: Schema.String,
  version: Schema.String,
  readme: Schema.String.pipe(Schema.optional),
  repository: Schema.String.pipe(Schema.optional),
  dependencies: Schema.Record({
    key: Schema.String,
    value: Dependency,
  }),
  tables: Schema.Record({
    key: Schema.String,
    value: TableDefinition,
  }),
}) {}

export class ArrowSchema extends Schema.Class<ArrowSchema>("ArrowSchema")({
  fields: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      type: Schema.Any,
      nullable: Schema.Boolean,
    }),
  ),
}) {}

export class TableSchema extends Schema.Class<TableSchema>("TableSchema")({
  arrow: ArrowSchema,
}) {}

export class TableInput extends Schema.Class<TableInput>("TableInput")({
  sql: Schema.String,
}) {}

export class Table extends Schema.Class<Table>("Table")({
  input: TableInput,
  schema: TableSchema,
}) {}

export class DatasetManifest extends Schema.Class<DatasetManifest>("DatasetManifest")({
  kind: Schema.Literal("manifest"),
  name: Schema.String,
  version: Schema.String,
  dependencies: Schema.Record({
    key: Schema.String,
    value: Dependency,
  }),
  tables: Schema.Record({
    key: Schema.String,
    value: Table,
  }),
}) {}
