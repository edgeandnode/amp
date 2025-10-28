import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Prompt from "@effect/cli/Prompt"
import * as FileSystem from "@effect/platform/FileSystem"
import * as Path from "@effect/platform/Path"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Match from "effect/Match"
import * as Option from "effect/Option"
import { localEvmRpc } from "../templates/local-evm-rpc.ts"
import type { Template, TemplateAnswers } from "../templates/Template.ts"
import { resolveTemplateFile, TemplateError } from "../templates/Template.ts"

/**
 * Available templates
 */
const TEMPLATES: Record<string, Template> = {
  "local-evm-rpc": localEvmRpc,
}

/**
 * Interactive prompts for dataset configuration
 */
const datasetNamePrompt = Prompt.text({
  message: "Dataset name:",
  default: "my_dataset",
  validate: (input) =>
    /^[a-z_][a-z0-9_]*$/.test(input)
      ? Effect.succeed(input)
      : Effect.fail(
        "Dataset name must start with a letter or underscore and contain only lowercase letters, digits, and underscores",
      ),
})

const datasetVersionPrompt = Prompt.text({
  message: "Dataset version:",
  default: "0.1.0",
})

const projectNamePrompt = Prompt.text({
  message: "Project name (for README):",
  default: "amp_project",
  validate: (input) =>
    input.trim().length > 0
      ? Effect.succeed(input.trim())
      : Effect.fail("Project name cannot be empty"),
})

/**
 * Gets a template by name or returns an error
 */
const getTemplate = (name: string): Effect.Effect<Template, TemplateError> => {
  return Match.value(TEMPLATES[name]).pipe(
    Match.when(
      (t): t is Template => t !== undefined,
      (t) => Effect.succeed(t),
    ),
    Match.orElse(() =>
      Effect.fail(
        new TemplateError({
          message: `Template "${name}" not found. Available templates: ${Object.keys(TEMPLATES).join(", ")}`,
        }),
      )
    ),
  )
}

/**
 * Writes template files to the target directory
 */
const writeTemplateFiles = (
  template: Template,
  answers: TemplateAnswers,
  targetPath: string,
): Effect.Effect<void, TemplateError, FileSystem.FileSystem | Path.Path> =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path

    // Ensure target directory exists
    yield* fs.makeDirectory(targetPath, { recursive: true }).pipe(
      Effect.mapError(
        (cause) =>
          new TemplateError({
            message: `Failed to create directory: ${targetPath}`,
            cause,
          }),
      ),
    )

    // Write each file
    for (const [filePath, fileContent] of Object.entries(template.files)) {
      const fullPath = path.join(targetPath, filePath)
      const content = resolveTemplateFile(fileContent, answers)

      // Ensure parent directory exists
      const parentDir = path.dirname(fullPath)
      yield* fs.makeDirectory(parentDir, { recursive: true }).pipe(
        Effect.mapError(
          (cause) =>
            new TemplateError({
              message: `Failed to create directory: ${parentDir}`,
              cause,
            }),
        ),
      )

      // Write file
      yield* fs.writeFileString(fullPath, content).pipe(
        Effect.mapError(
          (cause) =>
            new TemplateError({
              message: `Failed to write file: ${fullPath}`,
              cause,
            }),
        ),
      )

      yield* Console.log(`  ✓ Created ${filePath}`)
    }
  })

/**
 * Validates dataset name format
 */
const validateDatasetName = (name: string): Effect.Effect<string, TemplateError> => {
  return /^[a-z_][a-z0-9_]*$/.test(name)
    ? Effect.succeed(name)
    : Effect.fail(
      new TemplateError({
        message:
          `Invalid dataset name: "${name}". Dataset name must start with a letter or underscore and contain only lowercase letters, digits, and underscores`,
      }),
    )
}

/**
 * Core initialization logic
 */
const initializeProject = (
  datasetName: string,
  datasetVersion: string,
  projectName: string,
): Effect.Effect<void, TemplateError, FileSystem.FileSystem | Path.Path> =>
  Effect.gen(function*() {
    const path = yield* Path.Path
    const fs = yield* FileSystem.FileSystem

    // Validate dataset name
    yield* validateDatasetName(datasetName)

    // Use current directory
    const targetPath = path.resolve(".")

    // Check if amp.config.ts already exists
    const configPath = path.join(targetPath, "amp.config.ts")
    const configExists = yield* fs.exists(configPath).pipe(
      Effect.mapError(
        (cause) =>
          new TemplateError({
            message: `Failed to check for existing amp.config.ts`,
            cause,
          }),
      ),
    )

    if (configExists) {
      yield* Effect.fail(
        new TemplateError({
          message:
            `amp.config.ts already exists in this directory. Remove it or run amp init in a different directory.`,
        }),
      )
    }

    // Get template (only local-evm-rpc for now)
    const template = yield* getTemplate("local-evm-rpc")

    // Prepare answers
    const answers: TemplateAnswers = {
      projectName,
      datasetName,
      datasetVersion,
      network: "anvil",
    }

    yield* Console.log(`\n🎨 Initializing Amp project with template: ${template.name}`)
    yield* Console.log(`📁 Target directory: ${targetPath}\n`)

    // Write files
    yield* writeTemplateFiles(template, answers, targetPath)

    // Run post-install hook if present
    if (template.postInstall) {
      yield* Console.log("\n⚙️  Running post-install...")
      yield* template.postInstall(targetPath)
    }

    yield* Console.log(`\n🎉 Project initialized successfully!\n`)
    yield* Console.log(`Next steps:`)
    yield* Console.log(`  1. anvil (in another terminal)`)
    yield* Console.log(`  2. amp dev`)
    yield* Console.log(`\nFor more information, see README.md\n`)
  })

/**
 * Initialize command with both interactive and non-interactive modes
 * - Interactive mode: Prompts user for values (default when no flags provided)
 * - Non-interactive mode: Uses flags or defaults (when --flags or -y provided)
 */
export const init = Command.make("init", {
  args: {
    datasetName: Options.text("dataset-name").pipe(
      Options.withDescription("Dataset identifier (lowercase, alphanumeric, underscore only). Example: my_dataset"),
      Options.optional,
    ),
    datasetVersion: Options.text("dataset-version").pipe(
      Options.withDescription("Semantic version for the dataset. Example: 0.1.0"),
      Options.optional,
    ),
    projectName: Options.text("project-name").pipe(
      Options.withDescription("Human-readable project name used in generated README. Example: \"My Project\""),
      Options.optional,
    ),
    yes: Options.boolean("yes").pipe(
      Options.withAlias("y"),
      Options.withDescription("Non-interactive mode: use default values without prompting"),
      Options.withDefault(false),
    ),
  },
}).pipe(
  Command.withDescription("Initialize a new Amp project from a template"),
  Command.withHandler(({ args }) =>
    Effect.gen(function*() {
      // Determine if we should use interactive mode or flag-based mode
      const hasAnyFlags = Option.isSome(args.datasetName) ||
        Option.isSome(args.datasetVersion) ||
        Option.isSome(args.projectName)

      // If -y/--yes flag is set, use all defaults
      if (args.yes && !hasAnyFlags) {
        yield* initializeProject("my_dataset", "0.1.0", "amp_project")
        return
      }

      // If any flags are provided, validate and use them (non-interactive mode)
      if (hasAnyFlags) {
        const datasetName = Option.getOrElse(args.datasetName, () => "my_dataset")
        const datasetVersion = Option.getOrElse(args.datasetVersion, () => "0.1.0")
        const projectName = Option.getOrElse(args.projectName, () => "amp_project")

        yield* initializeProject(datasetName, datasetVersion, projectName)
        return
      }

      // Interactive mode - prompt for all values
      const datasetNameAnswer = yield* datasetNamePrompt
      const datasetVersionAnswer = yield* datasetVersionPrompt
      const projectNameAnswer = yield* projectNamePrompt

      // Add spacing before initialization output
      yield* Console.log("")

      yield* initializeProject(datasetNameAnswer, datasetVersionAnswer, projectNameAnswer)
    })
  ),
)
