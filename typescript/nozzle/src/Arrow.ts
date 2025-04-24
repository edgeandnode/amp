import type { Field as ArrowField, Schema as ArrowSchema } from "apache-arrow"
import { DataType, TimeUnit } from "apache-arrow"
import type { Types } from "effect"
import { Schema } from "effect"

export const generateSchema = (schema: ArrowSchema): Schema.Schema.AnyNoContext => {
  return Schema.Struct(generateFields(schema.fields)) as any
}

const generateFields = (fields: Array<ArrowField>) => {
  const output: Types.Mutable<Schema.Struct.Fields> = {}
  for (const field of fields) {
    output[field.name] = generateField(field)
  }

  return output as Schema.Struct.Fields
}

const generateField = (field: ArrowField): Schema.Schema.AnyNoContext => {
  const type = generateType(field.type)
  return field.nullable && !DataType.isNull(field.type) ? Schema.NullOr(type) : type
}

const generateType = (type: DataType): Schema.Schema.AnyNoContext => {
  if (DataType.isNull(type)) {
    return Schema.Null
  }

  if (DataType.isBool(type)) {
    return Schema.Boolean
  }

  if (DataType.isInt(type)) {
    if (type.bitWidth === 8 || type.bitWidth === 16 || type.bitWidth === 32) {
      return type.isSigned ? Schema.Int : Schema.NonNegativeInt
    }

    return type.isSigned ? Schema.BigInt : Schema.NonNegativeBigInt
  }

  if (DataType.isFloat(type)) {
    return Schema.Number
  }

  if (DataType.isBinary(type) || DataType.isLargeBinary(type) || DataType.isFixedSizeBinary(type)) {
    return Schema.Uint8ArrayFromHex
  }

  if (DataType.isUtf8(type) || DataType.isLargeUtf8(type)) {
    return Schema.String
  }

  if (DataType.isDate(type)) {
    return Schema.DateTimeUtc
  }

  if (DataType.isTime(type)) {
    if (type.unit === TimeUnit.SECOND || type.unit === TimeUnit.MILLISECOND) {
      return Schema.Number
    }

    return Schema.BigInt
  }

  if (DataType.isTimestamp(type)) {
    return Schema.Number
  }

  if (DataType.isDuration(type)) {
    return Schema.Never as any // TODO: Implement
  }

  if (DataType.isInterval(type)) {
    return Schema.Never as any // TODO: Implement
  }

  if (DataType.isList(type)) {
    return Schema.Array(generateType(type.valueType()))
  }

  if (DataType.isStruct(type)) {
    return Schema.Struct(generateFields(type.children)) as any
  }

  if (DataType.isUnion(type)) {
    return Schema.Union(...type.children.map((_) => generateType(_.type)))
  }

  if (DataType.isMap(type)) {
    return Schema.Record({ key: generateType(type.keyType()), value: generateType(type.valueType()) })
  }

  if (DataType.isFixedSizeList(type)) {
    return Schema.Array(generateType(type.valueType()))
  }

  if (DataType.isDictionary(type)) {
    return Schema.String
  }

  if (DataType.isDenseUnion(type) || DataType.isSparseUnion(type)) {
    return Schema.Union(...type.children.map((_) => generateType(_.type)))
  }

  if (DataType.isDecimal(type)) {
    return Schema.Any as any // TODO: Decimals appear to be currently broken in apache-arrow
  }

  throw new Error(`Unsupported type ${type.typeId}`)
}
