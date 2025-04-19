import { NodeContext, NodeRuntime } from "@effect/platform-node"
import { FileSystem } from "@effect/platform/FileSystem"
import * as Array from "effect/Array"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Order from "effect/Order"
import * as Record from "effect/Record"
import * as String from "effect/String"
import { FsUtils, FsUtilsLive } from "./lib/FsUtils.js"
import type { PackageJson } from "./lib/Package.js"
import { PackageContext, PackageContextLive } from "./lib/Package.js"

const program = Effect.gen(function*() {
  const fsUtils = yield* FsUtils
  const fs = yield* FileSystem
  const ctx = yield* PackageContext

  const modules = yield* fsUtils.glob("*.ts", {
    nodir: true,
    cwd: "src",
    ignore: ["index.ts"]
  }).pipe(
    Effect.map(Array.map(String.replace(/\.ts$/, ""))),
    Effect.map(Array.sort(Order.string)),
    Effect.withSpan("Pack-v2/discoverModules")
  )

  const buildPackageJson = Effect.sync(() => {
    const out: Record<string, any> = {
      name: ctx.name,
      version: ctx.version,
      description: ctx.description,
      type: ctx.type,
      license: ctx.license,
      repository: ctx.repository,
      sideEffects: ctx.sideEffects
    }

    const addOptional = (key: keyof PackageJson) => {
      if (ctx[key]) {
        out[key as string] = ctx[key]
      }
    }

    addOptional("author")
    addOptional("homepage")
    addOptional("dependencies")
    addOptional("peerDependencies")
    addOptional("peerDependenciesMeta")
    addOptional("optionalDependencies")

    out.bin = { nozzl: "./dist/cli/main.js" }
    out.module = "./dist/index.js"
    out.types = "./dist/index.d.ts"
    out.exports = {
      "./package.json": "./package.json",
      ".": {
        types: "./dist/index.d.ts",
        default: "./dist/index.js"
      }
    }

    if (Array.length(modules) > 0) {
      out.exports = {
        ...out.exports,
        ...Record.fromEntries(modules.map((_) => {
          const conditions = {
            types: `./dist/${_}.d.ts`,
            default: `./dist/${_}.js`
          }

          return [`./${_}`, conditions]
        }))
      }
    }

    return out
  })

  const writePackageJson = buildPackageJson.pipe(
    Effect.map((_) => JSON.stringify(_, null, 2)),
    Effect.flatMap((_) => fs.writeFileString("dist/package.json", _)),
    Effect.withSpan("Pack-v2/buildPackageJson")
  )

  const mkDist = fsUtils.rmAndMkdir("dist")
  const copyReadme = fs.copy("README.md", "dist/README.md")
  const copyLicense = fs.copy("LICENSE", "dist/LICENSE")

  const copyDist = fsUtils.rmAndCopy("build", "dist/dist")
  const copySrc = fsUtils.rmAndCopy("src", "dist/src")

  const copySources = Effect.all([
    copyDist,
    copySrc
  ], { concurrency: "inherit", discard: true }).pipe(
    Effect.withSpan("Pack-v2/copySources")
  )

  yield* mkDist
  yield* Effect.all([
    writePackageJson,
    copyReadme,
    copyLicense,
    copySources
  ], { concurrency: "inherit", discard: true }).pipe(
    Effect.withConcurrency(10)
  )
})

NodeRuntime.runMain(program.pipe(Effect.provide(Layer.mergeAll(
  FsUtilsLive,
  PackageContextLive,
  NodeContext.layer
))))
