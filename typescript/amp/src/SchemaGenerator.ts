import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as String from "effect/String"
import * as Admin from "./api/Admin.ts"
import type * as Model from "./Model.ts"

export class SchemaGeneratorError extends Data.TaggedError("SchemaGeneratorError")<{
  cause: unknown
  message: string
}> {}

export class SchemaGenerator extends Effect.Service<SchemaGenerator>()("Amp/SchemaGenerator", {
  effect: Effect.gen(function*() {
    const api = yield* Admin.Admin

    const fromTable = (schema: Model.TableSchema, name: string) => {
      const output: Array<string> = []
      output.push(`export class ${name} extends Schema.Class<${name}>("Amp/Schema/${name}")({\n`)
      for (const field of schema.arrow.fields) {
        const schemaType = convertType(field.type)
        output.push(`  ${field.name}: ${schemaType}`)
        if (field.nullable) {
          output.push(".pipe(Schema.optionalWith({ nullable: true }))")
        }
        output.push(",\n")
      }
      output.push(`}) {}\n`)
      return output.join("")
    }

    const fromManifest = (manifest: Model.DatasetManifest) => {
      const output: Array<string> = []
      output.push(`import { Schema } from "effect";\n`)

      // Only derived datasets have tables with schemas we can generate
      if (manifest.kind !== "manifest") {
        throw new Error(
          `Schema generation only supported for derived datasets (kind: "manifest"), got kind: "${manifest.kind}"`,
        )
      }

      for (const [name, table] of Object.entries(manifest.tables)) {
        output.push(`\n${fromTable(table.schema, String.capitalize(String.snakeToCamel(name)))}`)
      }
      return output.join("")
    }

    const fromSql = Effect.fn(function*(sql: string, name = "Table") {
      const schema = yield* api
        .getOutputSchema(sql, { isSqlDataset: true })
        .pipe(Effect.mapError((cause) => new SchemaGeneratorError({ cause, message: cause.message })))

      const output: Array<string> = []
      output.push(`import { Schema } from "effect";\n\n`)
      output.push(fromTable(schema.schema, name))
      return output.join("")
    })

    return { fromSql, fromTable, fromManifest }
  }),
}) {}

const convertType = (type: string) => {
  if (typeof type === "string") {
    switch (type) {
      case "Utf8":
      case "LargeUtf8":
        return "Schema.String"
      case "Int8":
        return "Schema.Int"
      case "Int16":
        return "Schema.Int"
      case "Int32":
        return "Schema.Int"
      case "Int64":
        return "Schema.Int"
      case "UInt8":
        return "Schema.NonNegativeInt"
      case "UInt16":
        return "Schema.NonNegativeInt"
      case "UInt32":
        return "Schema.NonNegativeInt"
      case "UInt64":
        return "Schema.NonNegativeInt"
      case "Float16":
        return "Schema.Number"
      case "Float32":
        return "Schema.Number"
      case "Float64":
        return "Schema.Number"
      case "Date32":
        return "Schema.Date"
      case "Boolean":
        return "Schema.Boolean"
      case "Binary":
        return "Schema.String"
    }

    return "Schema.Unknown"
  }

  if ("FixedSizeBinary" in type) {
    return "Schema.String"
  }

  if ("Time32" in type) {
    // TODO: Implement this.
    return "Schema.Unknown"
  }

  if ("Time64" in type) {
    // TODO: Implement this.
    return "Schema.Unknown"
  }

  if ("Timestamp" in type) {
    return "Schema.DateTimeUtc"
  }

  if ("Duration" in type) {
    if ((type as any).Duration === "Second") {
      return "Schema.Duration"
    }

    if ((type as any).Duration === "Millisecond") {
      return "Schema.DurationFromMillis"
    }

    if ((type as any).Duration === "Microsecond") {
      // TODO: Implement this.
      return "Schema.Unknown"
    }

    if ((type as any).Duration === "Nanosecond") {
      return "Schema.DurationFromNanos"
    }

    return "Schema.Unknown"
  }

  if ("Interval" in type) {
    if ((type as any).Interval === "MonthDayNano") {
      // TODO: Implement this.
      return "Schema.Unknown"
    }

    if ((type as any).Interval === "YearMonth") {
      // TODO: Implement this.
      return "Schema.Unknown"
    }

    if ((type as any).Interval === "DayTime") {
      // TODO: Implement this.
      return "Schema.Unknown"
    }

    return "Schema.Unknown"
  }

  if ("Decimal128" in type) {
    return "Schema.BigDecimal"
  }

  if ("Decimal256" in type) {
    return "Schema.BigDecimal"
  }

  return "Schema.Unknown"
}
