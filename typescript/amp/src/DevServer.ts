import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as ConfigLoader from "./ConfigLoader.ts"

/**
 * Error type for the dev server service.
 */
export class DevServerError extends Data.TaggedError("DevServerError")<{
  readonly cause?: unknown
  readonly message: string
}> {}

/**
 * Service definition for the development server.
 */
export class DevServer extends Context.Tag("Amp/DevServer")<DevServer, void>() {}

/**
 * Creates a dev server service instance.
 */
export const make = Effect.gen(function*() {
  const configLoader = yield* ConfigLoader.ConfigLoader

  // Find the amp.config.ts file in current directory.
  yield* configLoader.find().pipe(
    Effect.flatMap(Option.match({
      onNone: () => new DevServerError({ message: "Could not find amp.config.ts file in current directory" }),
      onSome: (_configFile) => Effect.succeed(_configFile),
    })),
  )

  // FIXME: DevServer needs to be updated to work with new manifest structure
  // For now, just log that dev server is not fully implemented
  yield* Effect.logWarning("DevServer is not yet updated for new manifest structure")
})

/**
 * Creates a dev server service layer.
 */
export const layer = () => make.pipe(Layer.scoped(DevServer))
