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
import * as Anvil from "./utils/Anvil.ts"
import * as Fixtures from "./utils/Fixtures.ts"
import * as Testing from "./utils/Testing.ts"

Testing.layer((it) => {
  it.effect(
    "run the counter script",
    Effect.fn(function*() {
      const anvil = yield* Anvil.Anvil

      // Run the counter script to deploy the `Counter.sol` contract and generate some events.
      yield* anvil.script("DeployCounter.s.sol:DeployCounterScript")
      yield* anvil.script("IncrementCounter.s.sol:IncrementCounterScript")
      yield* anvil.script("IncrementCounter.s.sol:IncrementCounterScript")
      yield* anvil.script("IncrementCounter.s.sol:IncrementCounterScript")
    }),
  )

  it.effect(
    "register and dump the root dataset",
    Effect.fn(function*() {
      const admin = yield* Admin.Admin

      // Register and dump the root dataset.
      const namespace = Model.DatasetNamespace.make("_")
      const name = Model.DatasetName.make("anvil")
      const version = Model.DatasetVersion.make("0.0.1")

      yield* admin.registerDataset(
        namespace,
        name,
        Anvil.dataset,
        version,
      )
      const job = yield* admin.deployDataset(
        namespace,
        name,
        version,
        {
          endBlock: "5",
        },
      )

      // Wait for the job to complete
      yield* Testing.waitForJobCompletion(job.jobId)

      const response = yield* admin.getDatasetVersion(namespace, name, Model.DatasetTag.make("dev"))
      deepStrictEqual(response.name, name)
    }),
  )

  it.effect(
    "can fetch a root dataset",
    Effect.fn(function*() {
      const api = yield* Admin.Admin
      const dataset = Model.DatasetReference.decode("anvil")
      const result = yield* api.getDatasetVersion(dataset.namespace, dataset.name, dataset.revision)
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
      const dataset = Model.DatasetReference.decode("anvil@0.0.1")
      const result = yield* api.getDatasetManifest(dataset.namespace, dataset.name, dataset.revision)
      assertEquals(result.kind, "evm-rpc")
      assertEquals(result.network, "anvil")
      assertEquals(typeof result.tables, "object")
    }),
  )

  it.effect(
    "can fetch the output schema of a root dataset",
    Effect.fn(function*() {
      const api = yield* Admin.Admin
      const request = new Admin.GetOutputSchemaPayload({
        tables: { query: "SELECT * FROM anvil.transactions" },
        dependencies: {
          anvil: Model.DatasetReference.decode("anvil@0.0.1"),
        },
      })
      const result = yield* api.getOutputSchema(request)
      assertInstanceOf(result, Admin.GetOutputSchemaResponse)
      assertInstanceOf(result.schemas.query, Model.TableSchemaWithNetworks)
    }),
  )

  it.effect(
    "register and dump the example dataset",
    Effect.fn(function*() {
      const admin = yield* Admin.Admin
      const fixtures = yield* Fixtures.Fixtures

      // Register and dump the example manifest.
      const {
        name,
        namespace,
        revision,
      } = Model.DatasetReference.decode("example@0.0.1")

      const dataset = yield* fixtures.load("manifest.json", Model.DatasetDerived)
      yield* admin.registerDataset(namespace, name, dataset, revision)
      const job = yield* admin.deployDataset(namespace, name, revision, {
        endBlock: "5",
      })

      // Wait for the job to complete
      yield* Testing.waitForJobCompletion(job.jobId)

      const response = yield* admin.getDatasetVersion(namespace, name, revision)
      deepStrictEqual(response.name, "example")
      deepStrictEqual(response.namespace, "_")
      deepStrictEqual(response.revision, "0.0.1")
    }),
  )

  it.effect(
    "can fetch the schema for a dataset version",
    Effect.fn(function*() {
      const api = yield* Admin.Admin
      const dataset = Model.DatasetReference.decode("example")
      const result = yield* api.getDatasetManifest(dataset.namespace, dataset.name, dataset.revision)
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
      deepStrictEqual(example, {
        name: Model.DatasetName.make("example"),
        namespace: Model.DatasetNamespace.make("_"),
        versions: [Model.DatasetVersion.make("0.0.1")],
        latestVersion: Model.DatasetVersion.make("0.0.1"),
      })
      const anvil = result.datasets.find((dataset) => dataset.name === "anvil")
      deepStrictEqual(anvil, {
        name: Model.DatasetName.make("anvil"),
        namespace: Model.DatasetNamespace.make("_"),
        versions: [Model.DatasetVersion.make("0.0.1")],
        latestVersion: Model.DatasetVersion.make("0.0.1"),
      })
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
        FROM "_/example@0.0.1".counts
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
