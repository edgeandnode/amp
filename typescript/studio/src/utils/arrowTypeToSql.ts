/**
 * Converts Arrow type definitions to SQL-like type strings for display in intellisense.
 * Handles both simple string types (e.g., "UInt64") and complex object types (e.g., { Timestamp: [...] }).
 *
 * Based on Arrow to PostgreSQL type mappings from arrow-to-postgres crate.
 */

type ArrowType =
  | string
  | { Timestamp: [string, string | null] }
  | { Decimal128: [number, number] }
  | { Time32: string }
  | { Time64: string }
  | { Duration: string }
  | { List: ArrowType }
  | { LargeList: ArrowType }
  | { FixedSizeList: [ArrowType, number] }
  | { FixedSizeBinary: number }

/**
 * Converts an Arrow type to a SQL-like type string for display in intellisense.
 *
 * @param arrowType - The Arrow type from DatasetManifest schema
 * @returns A SQL-like type string (e.g., "BIGINT", "TIMESTAMP", "VARCHAR")
 *
 * @example
 * arrowTypeToSql("UInt64") // "BIGINT"
 * arrowTypeToSql({ Timestamp: ["Nanosecond", null] }) // "TIMESTAMP"
 * arrowTypeToSql({ Decimal128: [38, 18] }) // "NUMERIC(38, 18)"
 */
export function arrowTypeToSql(arrowType: ArrowType): string {
  // Handle string types (simple types)
  if (typeof arrowType === "string") {
    switch (arrowType) {
      // Boolean
      case "Boolean":
        return "BOOLEAN"

      // Integer types
      case "UInt8":
        return "SMALLINT" // INT2
      case "UInt16":
        return "INTEGER" // INT4
      case "UInt32":
        return "BIGINT" // INT8
      case "UInt64":
        return "NUMERIC(20, 0)" // u64 max ~1.8e19
      case "Int8":
        return "SMALLINT" // INT2
      case "Int16":
        return "SMALLINT" // INT2
      case "Int32":
        return "INTEGER" // INT4
      case "Int64":
        return "BIGINT"

      // Float types
      // INT8
      case "Float16":
        return "REAL" // FLOAT4
      case "Float32":
        return "REAL" // FLOAT4
      case "Float64":
        return "DOUBLE PRECISION"

      // Date/Time types
      // FLOAT8
      case "Date32":
        return "DATE"

      // String types
      case "Utf8":
      case "LargeUtf8":
        return "TEXT"

      // Binary types
      case "Binary":
      case "LargeBinary":
        return "BYTEA"

      default:
        return arrowType.toUpperCase()
    }
  }

  // Handle complex types (objects)
  if (typeof arrowType === "object" && arrowType !== null) {
    // Timestamp types
    if ("Timestamp" in arrowType) {
      const [_unit, timezone] = arrowType.Timestamp
      if (timezone) {
        return `TIMESTAMP WITH TIME ZONE`
      }
      return "TIMESTAMP"
    }

    // Decimal128
    if ("Decimal128" in arrowType) {
      const [precision, scale] = arrowType.Decimal128
      return `NUMERIC(${precision}, ${scale})`
    }

    // Time32
    if ("Time32" in arrowType) {
      return "TIME"
    }

    // Time64
    if ("Time64" in arrowType) {
      return "TIME"
    }

    // Duration
    if ("Duration" in arrowType) {
      return "INTERVAL"
    }

    // List types
    if ("List" in arrowType) {
      const innerType = arrowTypeToSql(arrowType.List)
      return `${innerType}[]`
    }

    if ("LargeList" in arrowType) {
      const innerType = arrowTypeToSql(arrowType.LargeList)
      return `${innerType}[]`
    }

    if ("FixedSizeList" in arrowType) {
      const [innerType] = arrowType.FixedSizeList
      const sqlType = arrowTypeToSql(innerType)
      return `${sqlType}[]`
    }

    // FixedSizeBinary
    if ("FixedSizeBinary" in arrowType) {
      return "BYTEA"
    }
  }

  // Fallback for unknown types
  return "UNKNOWN"
}

/**
 * Formats a nullable type for display.
 *
 * @param sqlType - The SQL type string
 * @param nullable - Whether the field is nullable
 * @returns Formatted type string with NULL/NOT NULL annotation
 *
 * @example
 * formatNullableType("BIGINT", false) // "BIGINT NOT NULL"
 * formatNullableType("TEXT", true) // "TEXT"
 */
export function formatNullableType(sqlType: string, nullable: boolean): string {
  return nullable ? sqlType : `${sqlType} NOT NULL`
}
