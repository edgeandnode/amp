/**
 * Tests for Arrow type to SQL type converter
 */

import { describe, expect, it } from "vitest"

import { arrowTypeToSql, formatNullableType } from "../../src/utils/arrowTypeToSql"

describe("arrowTypeToSql", () => {
  describe("Simple Types", () => {
    it("should convert Boolean type", () => {
      expect(arrowTypeToSql("Boolean")).toBe("BOOLEAN")
    })

    it("should convert integer types", () => {
      expect(arrowTypeToSql("UInt8")).toBe("SMALLINT")
      expect(arrowTypeToSql("UInt16")).toBe("INTEGER")
      expect(arrowTypeToSql("UInt32")).toBe("BIGINT")
      expect(arrowTypeToSql("UInt64")).toBe("NUMERIC(20, 0)")
      expect(arrowTypeToSql("Int8")).toBe("SMALLINT")
      expect(arrowTypeToSql("Int16")).toBe("SMALLINT")
      expect(arrowTypeToSql("Int32")).toBe("INTEGER")
      expect(arrowTypeToSql("Int64")).toBe("BIGINT")
    })

    it("should convert float types", () => {
      expect(arrowTypeToSql("Float16")).toBe("REAL")
      expect(arrowTypeToSql("Float32")).toBe("REAL")
      expect(arrowTypeToSql("Float64")).toBe("DOUBLE PRECISION")
    })

    it("should convert string types", () => {
      expect(arrowTypeToSql("Utf8")).toBe("TEXT")
      expect(arrowTypeToSql("LargeUtf8")).toBe("TEXT")
    })

    it("should convert binary types", () => {
      expect(arrowTypeToSql("Binary")).toBe("BYTEA")
      expect(arrowTypeToSql("LargeBinary")).toBe("BYTEA")
    })

    it("should convert date types", () => {
      expect(arrowTypeToSql("Date32")).toBe("DATE")
    })

    it("should handle unknown types", () => {
      expect(arrowTypeToSql("UnknownType")).toBe("UNKNOWNTYPE")
    })
  })

  describe("Complex Types", () => {
    it("should convert Timestamp without timezone", () => {
      expect(arrowTypeToSql({ Timestamp: ["Nanosecond", null] })).toBe("TIMESTAMP")
      expect(arrowTypeToSql({ Timestamp: ["Microsecond", null] })).toBe("TIMESTAMP")
      expect(arrowTypeToSql({ Timestamp: ["Millisecond", null] })).toBe("TIMESTAMP")
      expect(arrowTypeToSql({ Timestamp: ["Second", null] })).toBe("TIMESTAMP")
    })

    it("should convert Timestamp with timezone", () => {
      expect(arrowTypeToSql({ Timestamp: ["Nanosecond", "UTC"] })).toBe("TIMESTAMP WITH TIME ZONE")
      expect(arrowTypeToSql({ Timestamp: ["Microsecond", "+00:00"] })).toBe("TIMESTAMP WITH TIME ZONE")
    })

    it("should convert Decimal128", () => {
      expect(arrowTypeToSql({ Decimal128: [38, 18] })).toBe("NUMERIC(38, 18)")
      expect(arrowTypeToSql({ Decimal128: [10, 2] })).toBe("NUMERIC(10, 2)")
      expect(arrowTypeToSql({ Decimal128: [20, 0] })).toBe("NUMERIC(20, 0)")
    })

    it("should convert Time types", () => {
      expect(arrowTypeToSql({ Time32: "Second" })).toBe("TIME")
      expect(arrowTypeToSql({ Time32: "Millisecond" })).toBe("TIME")
      expect(arrowTypeToSql({ Time64: "Microsecond" })).toBe("TIME")
    })

    it("should convert Duration", () => {
      expect(arrowTypeToSql({ Duration: "Second" })).toBe("INTERVAL")
      expect(arrowTypeToSql({ Duration: "Millisecond" })).toBe("INTERVAL")
      expect(arrowTypeToSql({ Duration: "Microsecond" })).toBe("INTERVAL")
    })

    it("should convert FixedSizeBinary", () => {
      expect(arrowTypeToSql({ FixedSizeBinary: 32 })).toBe("BYTEA")
      expect(arrowTypeToSql({ FixedSizeBinary: 20 })).toBe("BYTEA")
    })
  })

  describe("Array Types", () => {
    it("should convert List types", () => {
      expect(arrowTypeToSql({ List: "Int64" })).toBe("BIGINT[]")
      expect(arrowTypeToSql({ List: "Utf8" })).toBe("TEXT[]")
    })

    it("should convert LargeList types", () => {
      expect(arrowTypeToSql({ LargeList: "Float64" })).toBe("DOUBLE PRECISION[]")
      expect(arrowTypeToSql({ LargeList: "Boolean" })).toBe("BOOLEAN[]")
    })

    it("should convert FixedSizeList types", () => {
      expect(arrowTypeToSql({ FixedSizeList: ["Int32", 10] })).toBe("INTEGER[]")
      expect(arrowTypeToSql({ FixedSizeList: ["Utf8", 5] })).toBe("TEXT[]")
    })

    it("should convert nested array types", () => {
      expect(arrowTypeToSql({ List: { Decimal128: [10, 2] } })).toBe("NUMERIC(10, 2)[]")
    })
  })

  describe("Real-world examples from DatasetManifest", () => {
    it("should handle block_number (UInt64)", () => {
      expect(arrowTypeToSql("UInt64")).toBe("NUMERIC(20, 0)")
    })

    it("should handle timestamp with nanosecond precision", () => {
      expect(arrowTypeToSql({ Timestamp: ["Nanosecond", null] })).toBe("TIMESTAMP")
    })

    it("should handle transaction hash (Utf8)", () => {
      expect(arrowTypeToSql("Utf8")).toBe("TEXT")
    })

    it("should handle value with high precision (Decimal128)", () => {
      expect(arrowTypeToSql({ Decimal128: [38, 18] })).toBe("NUMERIC(38, 18)")
    })

    it("should handle binary data (Binary)", () => {
      expect(arrowTypeToSql("Binary")).toBe("BYTEA")
    })
  })
})

describe("formatNullableType", () => {
  it("should add NOT NULL for non-nullable fields", () => {
    expect(formatNullableType("BIGINT", false)).toBe("BIGINT NOT NULL")
    expect(formatNullableType("TEXT", false)).toBe("TEXT NOT NULL")
  })

  it("should not add annotation for nullable fields", () => {
    expect(formatNullableType("BIGINT", true)).toBe("BIGINT")
    expect(formatNullableType("TEXT", true)).toBe("TEXT")
  })
})
