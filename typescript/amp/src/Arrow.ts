import type { Field as ArrowField, RecordBatch, Schema as ArrowSchema, TypeMap } from "apache-arrow"
import { DataType, Table, TimeUnit } from "apache-arrow"
import * as Schema from "effect/Schema"
import type * as Types from "effect/Types"

export const makeTable: {
  <T extends TypeMap = any>(data: RecordBatch<T>): Table<T>
  <T extends TypeMap = any>(data: Iterable<RecordBatch<T>>): Table<T>
} = (data) => new Table(data as any)

export const parseRecordBatch: {
  <A, I, R>(schema: Schema.Schema<A, I, R>, data: RecordBatch): Array<A>
  <A, I, R>(schema: Schema.Schema<A, I, R>, data: Iterable<RecordBatch>): Array<A>
} = (schema, data) => {
  const table = makeTable(data as any)
  return Schema.validateSync(getArraySchema(schema))(table.toArray())
}

const arraySchemaCache = new WeakMap<Schema.Schema.Any, Schema.Schema.Any>()
const getArraySchema = (schema: Schema.Schema.Any): Schema.Schema.Any => {
  if (arraySchemaCache.has(schema)) {
    return arraySchemaCache.get(schema)!
  }

  const generated = Schema.Array(schema).pipe(Schema.mutable)
  arraySchemaCache.set(schema, generated)
  return generated
}

const schemaCache = new WeakMap<ArrowSchema, Schema.Schema.AnyNoContext>()
export const generateSchema = (schema: ArrowSchema): Schema.Schema.AnyNoContext => {
  if (schemaCache.has(schema)) {
    return schemaCache.get(schema)!
  }

  const generated = Schema.Struct(generateFields(schema.fields)) as any
  schemaCache.set(schema, generated)
  return generated
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
