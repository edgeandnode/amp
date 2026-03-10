function no_params_no_ret() {
  return 36
}

function throws() {
  throw new Error("test exception")
}

function param_types(
  nul,
  bool,
  f16,
  f32,
  f64,
  i8,
  i16,
  u8,
  u16,
  u32,
  u64,
  i32,
  i64,
  str,
  bytes,
  decimal128,
  decimal256,
  date32,
  date64,
  time32s,
  time32ms,
  time64us,
  time64ns,
  timestamp,
  duration,
  interval_ym,
  interval_dt,
  null_f16,
  null_date32,
  null_timestamp,
  null_interval_dt,
) {
  assert_eq(nul, null)
  assert_eq(bool, true)
  assert_eq(f16, 1.5)
  assert_eq(f32, 32.5)
  assert_eq(f64, 64.5)

  // Signed integers take their min value
  assert_eq(i8, -128)
  assert_eq(i16, -32768)
  assert_eq(i32, -2147483648)
  assert_eq(i64, BigInt("-9223372036854775808"))

  // Unsigned integers take their max value
  assert_eq(u8, 255)
  assert_eq(u16, 65535)
  assert_eq(u32, 4294967295)
  assert_eq(u64, BigInt("18446744073709551615"))

  assert_eq(str, "data 🇧🇷🇵🇹")

  assert_eq(uint8ArrayToHex(bytes), "c944e90c64b2c07662a292be6244bdf05cda44a7")

  assert_eq(decimal128, BigInt("170141183460469231731687303715884105727"))
  assert_eq(
    decimal256,
    BigInt(
      "57896044618658097711785492504343953926634992332820282019728792003956564819967",
    ),
  )

  assert_eq(date32, 19000)

  assert_eq(date64, BigInt("1700000000000"))

  assert_eq(time32s, 3661)
  assert_eq(time32ms, 3661500)
  assert_eq(time64us, BigInt("3661500000"))
  assert_eq(time64ns, BigInt("3661500000000"))

  assert_eq(timestamp, BigInt("1700000000"))

  assert_eq(duration, BigInt("86400000"))

  assert_eq(interval_ym, 14)

  assert_eq(interval_dt.days, 5)
  assert_eq(interval_dt.milliseconds, 3000)

  assert_eq(null_f16, null)
  assert_eq(null_date32, null)
  assert_eq(null_timestamp, null)
  assert_eq(null_interval_dt, null)
}

// Returns an object symmetric to the parameters to `param_types`, on field for each parameter, and
// values the same as the expected values in `param_types`
function return_types() {
  return {
    nul: null,
    bool: true,
    f32: 32.5,
    f64: 64.5,
    i8: -128,
    i16: -32768,
    u8: 255,
    u16: 65535,
    u32: 4294967295,
    u64: BigInt("18446744073709551615"),
    i32: -2147483648,
    i64: BigInt("-9223372036854775808"),
    str: "data 🇧🇷🇵🇹",

    // TypedArray not yet supported
    // bytes: uint8ArrayFromHex("c944e90c64b2c07662a292be6244bdf05cda44a7"),

    decimal128: BigInt("170141183460469231731687303715884105727"),
    decimal256: BigInt(
      "57896044618658097711785492504343953926634992332820282019728792003956564819967",
    ),
  }
}

function uint8ArrayToHex(uint8Array) {
  return Array.from(uint8Array)
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("")
}

function uint8ArrayFromHex(hex) {
  return new Uint8Array(
    hex.match(/.{1,2}/g).map((byte) => Number.parseInt(byte, 16)),
  )
}

function format(value) {
  if (typeof value === "bigint") return `${value}n`
  if (typeof value === "string") return `"${value}"`
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

function assert_eq(actual, expected) {
  const isEqual = typeof actual === "bigint" && typeof expected === "bigint"
    ? actual === expected
    : actual === expected

  if (!isEqual) {
    throw new Error(
      `assert_eq failed:\n  actual:   ${format(actual)}\n  expected: ${format(expected)}`,
    )
  }
}

function obj_param(obj) {
  assert_eq(obj.a, 1)
  assert_eq(obj.b, "perf")
  assert_eq(obj.c, true)
}

function list_param(list) {
  assert_eq(list.length, 3)
  assert_eq(list[0], "1")
  assert_eq(list[1], "2")
  assert_eq(list[2], "3")
}

function map_param(map) {
  assert_eq(map.x, 10)
  assert_eq(map.y, 20)
}

function dictionary_param(dict) {
  assert_eq(dict, "hello")
}
