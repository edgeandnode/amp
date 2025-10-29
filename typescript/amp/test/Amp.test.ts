import * as Anvil from "@edgeandnode/amp/Anvil"
import * as Admin from "@edgeandnode/amp/api/Admin"
import * as Errors from "@edgeandnode/amp/api/Error"
import * as JsonLines from "@edgeandnode/amp/api/JsonLines"
import * as Model from "@edgeandnode/amp/Model"
import { assertEquals, assertFailure, assertInstanceOf, assertSome, deepStrictEqual } from "@effect/vitest/utils"
import * as Array from "effect/Array"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Struct from "effect/Struct"
import * as Fixtures from "./utils/Fixtures.ts"
import * as Testing from "./utils/Testing.ts"

Testing.layer((it) => {
  it.effect(
    "run the counter script",
    Effect.fn(function*() {
      // Run the counter script to deploy the `Counter.sol` contract and generate some events.
      yield* Anvil.script("DeployCounter.s.sol:DeployCounterScript")
      yield* Anvil.script("IncrementCounter.s.sol:IncrementCounterScript")
      yield* Anvil.script("IncrementCounter.s.sol:IncrementCounterScript")
      yield* Anvil.script("IncrementCounter.s.sol:IncrementCounterScript")
    }),
  )

  it.effect(
    "register and dump the root dataset",
    Effect.fn(function*() {
      const admin = yield* Admin.Admin

      // Register and dump the root dataset.
      yield* admin.registerDataset("_", "anvil", "0.1.0", Anvil.dataset)
      const job = yield* admin.deployDataset("_", "anvil", "0.1.0", {
        endBlock: "5",
      })

      // Wait for the job to complete
      yield* Testing.waitForJobCompletion(job.jobId)

      const response = yield* admin.getDatasetVersion("_", "anvil", "dev")
      assertInstanceOf(response, Model.DatasetVersionInfo)
      deepStrictEqual(response.name, "anvil")
    }),
  )

  it.effect(
    "can fetch a root dataset",
    Effect.fn(function*() {
      const api = yield* Admin.Admin
      const result = yield* api.getDatasetVersion("_", "anvil", "dev")
      assertInstanceOf(result, Model.DatasetVersionInfo)
      assertEquals(result.namespace, "_")
      assertEquals(result.name, "anvil")
      assertEquals(result.revision, "dev")
      assertEquals(result.kind, "evm-rpc")
    }),
  )

  it.effect(
    "can fetch the manifest for an evm-rpc dataset",
    Effect.fn(function*() {
      const api = yield* Admin.Admin
      const result = yield* api.getDatasetManifest("_", "anvil", "0.1.0")
      assertEquals(result.kind, "evm-rpc")
      assertEquals(result.network, "anvil")
      deepStrictEqual(typeof result.tables, "object")
    }),
  )

  it.effect(
    "can fetch the output schema of a root dataset",
    Effect.fn(function*() {
      const api = yield* Admin.Admin
      const result = yield* api.getOutputSchema("SELECT * FROM anvil.transactions")
      assertInstanceOf(result, Model.OutputSchema)
    }),
  )

  it.effect(
    "register and dump the example dataset",
    Effect.fn(function*() {
      const admin = yield* Admin.Admin
      const fixtures = yield* Fixtures.Fixtures

      // Register and dump the example manifest.
      const dataset = yield* fixtures.load("manifest.json", Model.DatasetManifest)
      yield* admin.registerDataset("_", "example", "0.1.0", dataset)

      const job = yield* admin.deployDataset("_", "example", "0.1.0", {
        endBlock: "5",
      })

      // Wait for the job to complete
      yield* Testing.waitForJobCompletion(job.jobId)

      const response = yield* admin.getDatasetVersion("_", "example", "dev")
      assertInstanceOf(response, Model.DatasetVersionInfo)
      deepStrictEqual(response.name, "example")
    }),
  )

  it.effect(
    "can fetch the schema for a dataset version",
    Effect.fn(function*() {
      const api = yield* Admin.Admin
      const result = yield* api.getDatasetManifest("_", "example", "dev")
      assertEquals(result.kind, "manifest")
      assertEquals(result.network, undefined)
      deepStrictEqual(typeof result.tables, "object")
    }),
  )

  it.effect(
    "can fetch a list of datasets",
    Effect.fn(function*() {
      const api = yield* Admin.Admin
      const result = yield* api.getDatasets()
      const example = result.datasets.find((dataset) => dataset.name === "example")
      assertInstanceOf(example, Model.DatasetSummary)
      const anvil = result.datasets.find((dataset) => dataset.name === "anvil")
      assertInstanceOf(anvil, Model.DatasetSummary)
    }),
  )

  it.effect(
    "query the example dataset",
    Effect.fn(function*() {
      const jsonl = yield* JsonLines.JsonLines
      const schema = Schema.Struct({
        blockHash: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("block_hash")),
        blockNumber: Schema.Number.pipe(Schema.propertySignature, Schema.fromKey("block_num")),
        txHash: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("tx_hash")),
        address: Schema.String,
        count: Schema.NumberFromString,
      })

      // Query the example dataset.
      const response = yield* jsonl.query(schema)`
        SELECT tx_hash, block_hash, block_num, address, count
        FROM example.counts
        ORDER BY count DESC
        LIMIT 1
      `

      assertSome(Array.last(response).pipe(Option.map(Struct.get("count"))), 3)
    }),
  )

  it.effect(
    "handles job not found error",
    Effect.fn(function*() {
      const admin = yield* Admin.Admin
      const result = yield* admin.getJobById(999999).pipe(Effect.exit)
      const expected = new Errors.JobNotFound({
        message: "job '999999' not found",
        code: "JOB_NOT_FOUND",
      })
      assertFailure(result, Cause.fail(expected))
    }),
  )
})
