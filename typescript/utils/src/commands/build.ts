import { Command, Options } from "@effect/cli"
import { Command as Cmd } from "@effect/platform"
import { Effect, Schema, Struct } from "effect"
import * as Model from "../lib/Model.js"
import * as Utils from "../lib/Utils.js"

export const build = Command.make("build", {
  args: {
    project: Options.text("project").pipe(
      Options.withDefault("tsconfig.build.json"),
      Options.withDescription("The project file to use for building")
    )
  }
}, ({ args }) =>
  Effect.gen(function*() {
    const utils = yield* Utils.Utils
    const pkg = yield* utils.readJson("./package.json").pipe(
      Effect.flatMap(Schema.decodeUnknown(Model.PackageJson))
    )

    yield* utils.existsOrFail(args.project)
    yield* utils.rmAndMkdir("build")

    // TODO: Output errors instead of swallowing them here.
    yield* Effect.log("Building package")
    if ((yield* Cmd.make("tsc", "-b", args.project).pipe(Cmd.exitCode)) !== 0) {
      yield* Effect.dieMessage("Failed to build package")
    }

    yield* utils.rmAndMkdir("dist")
    yield* utils.copyIfExists("CHANGELOG.md", "dist/CHANGELOG.md")
    yield* utils.copyIfExists("README.md", "dist/README.md")
    yield* utils.copyIfExists("LICENSE", "dist/LICENSE")
    yield* utils.rmAndCopy("src", "dist/src")
    yield* utils.rmAndCopy("build/dist", "dist/dist")

    const json = Struct.evolve(pkg, {
      main: replaceJs,
      types: replaceDts,
      bin: replaceOptional((bin) => {
        const result: Record<string, string> = {}
        for (const [key, value] of Object.entries(bin)) {
          result[key] = replaceJs(value)!
        }

        return result
      }),
      exports: replaceOptional((exports) => {
        const result: Record<string, any> = {}
        for (const [key, value] of Object.entries(exports)) {
          if (typeof value === "string") {
            result[key] = {
              types: replaceDts(value),
              default: replaceJs(value)
            }
          } else {
            result[key] = Struct.evolve(value, {
              types: replaceDts,
              browser: replaceJs,
              default: replaceJs
            })
          }
        }

        return result
      })
    })

    yield* utils.writeJson("dist/package.json", json)
    yield* Effect.log("Build successful")
  })).pipe(
    Command.withDescription("Prepare a package for publishing"),
    Command.provide(Utils.Utils.Default)
  )

const replaceOptional = <T>(f: (value: T) => T) => (value: T | undefined): T | undefined =>
  value !== undefined ? f(value) as any : undefined as any
const replaceDts = replaceOptional((file: string) => file.replace(/^\.\/src\//, "./dist/").replace(/\.ts$/, ".d.ts"))
const replaceJs = replaceOptional((file: string) =>
  file.replace(/^\.\/src\//, "./dist/").replace(/\.ts$/, ".js").replace(/\.tsx$/, ".jsx")
)
