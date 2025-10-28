import type * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

/**
 * Answers to template prompts
 */
export interface TemplateAnswers {
  readonly projectName?: string | undefined
  readonly datasetName: string
  readonly datasetVersion?: string | undefined
  readonly network?: string | undefined
}

/**
 * A file in a template that can be either a static string or dynamically generated
 */
export type TemplateFile = string | ((answers: TemplateAnswers) => string)

/**
 * Template definition
 */
export interface Template {
  /**
   * Template identifier (e.g., "local-evm-rpc")
   */
  readonly name: string

  /**
   * Human-readable description
   */
  readonly description: string

  /**
   * Map of file paths to their content
   * File paths are relative to the project root
   */
  readonly files: Record<string, TemplateFile>

  /**
   * Optional post-installation hook
   * Runs after files are written
   */
  readonly postInstall?: (projectPath: string) => Effect.Effect<void, TemplateError>
}

/**
 * Error type for template operations
 */
export class TemplateError extends Schema.TaggedError<TemplateError>("TemplateError")("TemplateError", {
  cause: Schema.Unknown.pipe(Schema.optional),
  message: Schema.String,
}) {}

/**
 * Resolves a template file to its string content
 */
export const resolveTemplateFile = (file: TemplateFile, answers: TemplateAnswers): string => {
  return typeof file === "function" ? file(answers) : file
}
