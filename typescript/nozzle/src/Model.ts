import { Schema } from "effect"

export class Dependency extends Schema.Class<Dependency>("Dependency")({
  owner: Schema.String,
  name: Schema.String,
  version: Schema.String,
}) {}

export class TableDefinition extends Schema.Class<TableDefinition>("TableDefinition")({
  sql: Schema.String,
}) {}

export const DatasetName = Schema.Lowercase.pipe(
  Schema.annotations({
    title: "Name",
    description: "the name of the dataset",
    examples: ["uniswap"],
  }),
)

export const DatasetVersion = Schema.String.pipe(
  Schema.pattern(/^\d+\.\d+\.\d+$/),
  Schema.annotations({
    title: "Version",
    description: "a semantic version number (e.g. \"4.1.3\")",
    examples: ["1.0.0", "1.0.1", "1.1.0"],
  }),
)

export const DatasetRepository = Schema.URL.pipe(
  Schema.annotations({
    title: "Repository",
    description: "the address of the repository",
    examples: [new URL("https://github.com/foo/bar")],
  }),
)

export const DatasetReadme = Schema.String.pipe(
  Schema.annotations({
    title: "Readme",
    description: "the documentation of the dataset",
  }),
)

export class FunctionSource extends Schema.Class<FunctionSource>("FunctionSource")({
  source: Schema.String,
  filename: Schema.String,
}) {}

export class FunctionDefinition extends Schema.Class<FunctionDefinition>("FunctionDefinition")({
  source: FunctionSource,
  inputTypes: Schema.Array(Schema.String),
  outputType: Schema.String,
}) {}

export class DatasetDefinition extends Schema.Class<DatasetDefinition>("DatasetDefinition")({
  name: DatasetName,
  version: DatasetVersion,
  readme: DatasetReadme.pipe(Schema.optional),
  repository: DatasetRepository.pipe(Schema.optional),
  dependencies: Schema.Record({
    key: Schema.String,
    value: Dependency,
  }),
  tables: Schema.Record({
    key: Schema.String,
    value: TableDefinition,
  }).pipe(Schema.optionalWith({ default: () => ({}) })),
  functions: Schema.Record({
    key: Schema.String,
    value: FunctionDefinition,
  }).pipe(Schema.optionalWith({ default: () => ({}) })),
}) {}

export class ArrowField extends Schema.Class<ArrowField>("ArrowField")({
  name: Schema.String,
  type: Schema.Any,
  nullable: Schema.Boolean,
}) {}

export class ArrowSchema extends Schema.Class<ArrowSchema>("ArrowSchema")({
  fields: Schema.Array(
    ArrowField,
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
  network: Schema.String,
}) {}

export class FunctionManifest extends Schema.Class<FunctionManifest>("FunctionManifest")({
  name: Schema.String,
  source: FunctionSource,
  inputTypes: Schema.Array(Schema.String),
  outputType: Schema.String,
}) {}

export class DatasetManifest extends Schema.Class<DatasetManifest>("DatasetManifest")({
  kind: Schema.Literal("manifest"),
  name: DatasetName,
  version: DatasetVersion,
  dependencies: Schema.Record({
    key: Schema.String,
    value: Dependency,
  }),
  tables: Schema.Record({
    key: Schema.String,
    value: Table,
  }).pipe(Schema.optionalWith({ default: () => ({}) })),
  functions: Schema.Record({
    key: Schema.String,
    value: FunctionManifest,
  }).pipe(Schema.optionalWith({ default: () => ({}) })),
}) {}
