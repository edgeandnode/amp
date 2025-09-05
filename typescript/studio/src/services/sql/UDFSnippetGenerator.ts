/**
 * UDF Snippet Generator
 *
 * This module provides specialized functionality for generating Monaco Editor
 * snippets for Nozzle User-Defined Functions (UDFs). It creates intelligent
 * parameter placeholders with proper tab navigation and type hints.
 *
 * Key Features:
 * - Snippet generation with tabstop navigation
 * - Parameter placeholders with descriptive names
 * - Special handling for dataset-prefixed functions
 * - Type-aware parameter suggestions
 * - Integration with Monaco's snippet system
 *
 * @file udfSnippets.ts
 * @author SQL Intellisense System
 */
import type {
  IMarkdownString,
  Position,
} from "monaco-editor/esm/vs/editor/editor.api"
import { languages } from "monaco-editor/esm/vs/editor/editor.api"

import type { UserDefinedFunction } from "./types"

/**
 * UDF Snippet Configuration
 *
 * Configuration options for customizing UDF snippet behavior.
 */
export interface UdfSnippetConfig {
  /** Whether to include parameter type hints in snippets */
  includeTypeHints: boolean

  /** Whether to include example values as placeholder text */
  includeExampleValues: boolean

  /** Default dataset name for dataset-prefixed UDFs */
  defaultDataset: string

  /** Whether to automatically trigger parameter hints after insertion */
  triggerParameterHints: boolean
}

/**
 * Default UDF snippet configuration
 */
export const DEFAULT_UDF_SNIPPET_CONFIG: UdfSnippetConfig = {
  includeTypeHints: true,
  includeExampleValues: false,
  defaultDataset: "anvil",
  triggerParameterHints: true,
}

/**
 * UDF Snippet Generator
 *
 * Main class for generating Monaco Editor snippets for UDF functions.
 * Provides intelligent parameter placeholders and tab navigation.
 */
export class UdfSnippetGenerator {
  constructor(private config: UdfSnippetConfig = DEFAULT_UDF_SNIPPET_CONFIG) {}

  /**
   * Create UDF Snippet
   *
   * Generates a Monaco Editor snippet string for the given UDF with proper
   * tab stops and parameter placeholders.
   *
   * @param udf - User-defined function definition
   * @returns Monaco snippet string with tabstops
   */
  createUdfSnippet(udf: UserDefinedFunction): string {
    // Handle special UDF cases with custom parameter logic
    switch (udf.name) {
      case "evm_decode_log":
        return this.createEvmDecodeLogSnippet()

      case "evm_topic":
        return this.createEvmTopicSnippet()

      case "${dataset}.eth_call":
        return this.createEthCallSnippet()

      case "evm_decode_params":
        return this.createEvmDecodeParamsSnippet()

      case "evm_encode_params":
        return this.createEvmEncodeParamsSnippet()

      case "evm_encode_type":
        return this.createEvmEncodeTypeSnippet()

      case "evm_decode_type":
        return this.createEvmDecodeTypeSnippet()

      case "attestation_hash":
        return this.createAttestationHashSnippet()

      default:
        return this.createGenericUdfSnippet(udf)
    }
  }

  /**
   * EVM Decode Log Snippet
   *
   * Creates snippet for evm_decode_log function with topic placeholders.
   *
   * @private
   */
  private createEvmDecodeLogSnippet(): string {
    const topics = this.config.includeExampleValues
      ? [
          "0x${1:ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef}",
          "${2:from_topic}",
          "${3:to_topic}",
        ]
      : ["${1:topic1}", "${2:topic2}", "${3:topic3}"]

    const data = this.config.includeExampleValues
      ? "${4:0x000...}"
      : "${4:data}"
    const signature = this.config.includeExampleValues
      ? "${5:Transfer(address indexed from, address indexed to, uint256 value)}"
      : "${5:event_signature}"

    return `evm_decode_log(${topics.join(", ")}, ${data}, '${signature}')$0`
  }

  /**
   * EVM Topic Snippet
   *
   * Creates snippet for evm_topic function with event signature placeholder.
   *
   * @private
   */
  private createEvmTopicSnippet(): string {
    const signature = this.config.includeExampleValues
      ? "${1:Transfer(address indexed from, address indexed to, uint256 value)}"
      : "${1:event_signature}"

    return `evm_topic('${signature}')$0`
  }

  /**
   * Eth Call Snippet
   *
   * Creates snippet for dataset-prefixed eth_call function.
   *
   * @private
   */
  private createEthCallSnippet(): string {
    const dataset = this.config.includeExampleValues
      ? `\${1:${this.config.defaultDataset}}`
      : "${1:dataset}"

    const params = this.config.includeExampleValues
      ? [
          "${2:0x0000000000000000000000000000000000000000}", // from_address
          "${3:0x1234567890123456789012345678901234567890}", // to_address
          "${4:0x70a08231}", // input_data (balanceOf signature)
          "${5:latest}", // block
        ]
      : [
          "${2:from_address}",
          "${3:to_address}",
          "${4:input_data}",
          "${5:block}",
        ]

    return `${dataset}.eth_call(${params.join(", ")}, '${params[3]}')$0`
  }

  /**
   * EVM Decode Params Snippet
   *
   * Creates snippet for evm_decode_params function.
   *
   * @private
   */
  private createEvmDecodeParamsSnippet(): string {
    const inputData = this.config.includeExampleValues
      ? "${1:0xa9059cbb...}"
      : "${1:input_data}"

    const signature = this.config.includeExampleValues
      ? "${2:transfer(address to, uint256 amount)}"
      : "${2:function_signature}"

    return `evm_decode_params(${inputData}, '${signature}')$0`
  }

  /**
   * EVM Encode Params Snippet
   *
   * Creates snippet for evm_encode_params with variable arguments.
   *
   * @private
   */
  private createEvmEncodeParamsSnippet(): string {
    const args = this.config.includeExampleValues
      ? ["${1:0x1234567890123456789012345678901234567890}", "${2:1000}"]
      : ["${1:arg1}", "${2:arg2}"]

    const signature = this.config.includeExampleValues
      ? "${3:transfer(address to, uint256 amount)}"
      : "${3:function_signature}"

    // Allow for additional arguments
    const additionalArgs = "${4:, ${5:arg3}}"

    return `evm_encode_params(${args.join(", ")}${additionalArgs}, '${signature}')$0`
  }

  /**
   * EVM Encode Type Snippet
   *
   * Creates snippet for evm_encode_type function.
   *
   * @private
   */
  private createEvmEncodeTypeSnippet(): string {
    const value = this.config.includeExampleValues ? "${1:1000}" : "${1:value}"
    const type = this.config.includeExampleValues ? "${2:uint256}" : "${2:type}"

    return `evm_encode_type(${value}, '${type}')$0`
  }

  /**
   * EVM Decode Type Snippet
   *
   * Creates snippet for evm_decode_type function.
   *
   * @private
   */
  private createEvmDecodeTypeSnippet(): string {
    const data = this.config.includeExampleValues
      ? "${1:0x000...}"
      : "${1:data}"
    const type = this.config.includeExampleValues ? "${2:uint256}" : "${2:type}"

    return `evm_decode_type(${data}, '${type}')$0`
  }

  /**
   * Attestation Hash Snippet
   *
   * Creates snippet for attestation_hash with variable arguments.
   *
   * @private
   */
  private createAttestationHashSnippet(): string {
    const columns = this.config.includeExampleValues
      ? ["${1:block_number}", "${2:transaction_hash}"]
      : ["${1:column1}", "${2:column2}"]

    // Allow for additional columns
    const additionalColumns = "${3:, ${4:column3}}"

    return `attestation_hash(${columns.join(", ")}${additionalColumns})$0`
  }

  /**
   * Generic UDF Snippet
   *
   * Creates a generic snippet for UDFs that don't have special handling.
   * Uses the parameters array if available.
   *
   * @private
   */
  private createGenericUdfSnippet(udf: UserDefinedFunction): string {
    if (udf.parameters && udf.parameters.length > 0) {
      const params = udf.parameters.map((param, i) => {
        const tabstop = i + 1
        if (this.config.includeTypeHints) {
          return `\${${tabstop}:${param}}`
        }
        return `\${${tabstop}}`
      })

      return `${udf.name}(${params.join(", ")})$0`
    }

    // Fallback for UDFs without parameter information
    return `${udf.name}(\${1})$0`
  }

  /**
   * Create Completion Item with Snippet
   *
   * Creates a complete Monaco completion item with snippet and documentation.
   *
   * @param udf - User-defined function definition
   * @param sortText - Sort order for the completion item
   * @returns Monaco completion item configured for snippet insertion
   */
  createCompletionItem(
    udf: UserDefinedFunction,
    sortText: string,
    position: Position,
  ): languages.CompletionItem {
    const snippet = this.createUdfSnippet(udf)
    const displayName = udf.name.replace("${dataset}", "{dataset}")

    // Create rich documentation
    const documentation: IMarkdownString = {
      value: [
        `**${displayName}** - Nozzle User-Defined Function`,
        "",
        udf.description,
        "",
        "**SQL Signature:**",
        "```sql",
        udf.sql.trim(),
        "```",
        "",
        udf.example ? `**Example:**\n\`\`\`sql\n${udf.example}\n\`\`\`` : "",
        "",
        "💡 **Tip:** Use Tab to navigate between parameters after insertion",
        "",
        this.generateParameterHints(udf),
      ]
        .filter((line) => line !== "")
        .join("\n"),
      isTrusted: true,
    }

    return {
      label: displayName,
      kind: languages.CompletionItemKind.Function,
      detail: this.createDetailText(udf),
      documentation,
      insertText: snippet,
      insertTextRules: languages.CompletionItemInsertTextRule.InsertAsSnippet,
      sortText,
      filterText: udf.name,
      // Trigger parameter hints after insertion if configured
      ...(this.config.triggerParameterHints
        ? {
            command: {
              id: "editor.action.triggerParameterHints",
              title: "Trigger Parameter Hints",
            },
          }
        : {}),
      range: {
        startColumn: position.column,
        startLineNumber: position.lineNumber,
        endColumn: position.column,
        endLineNumber: position.lineNumber,
      },
    }
  }

  /**
   * Create Detail Text
   *
   * Generates brief detail text for the completion item.
   *
   * @private
   */
  private createDetailText(udf: UserDefinedFunction): string {
    const paramCount = udf.parameters?.length || 0
    const paramText = paramCount === 1 ? "parameter" : "parameters"

    if (udf.returnType) {
      return `Nozzle UDF → ${udf.returnType} (${paramCount} ${paramText})`
    }

    return `Nozzle UDF (${paramCount} ${paramText})`
  }

  /**
   * Generate Parameter Hints
   *
   * Creates parameter hint documentation for UDF functions.
   *
   * @private
   */
  private generateParameterHints(udf: UserDefinedFunction): string {
    if (!udf.parameters || udf.parameters.length === 0) {
      return ""
    }

    const hints = udf.parameters.map((param, index) => {
      return `**${param}**: Parameter ${index + 1}`
    })

    return ["**Parameters:**", ...hints].join("\n")
  }

  /**
   * Create Hover Information
   *
   * Generates hover information for UDF functions that appear in queries.
   *
   * @param udf - User-defined function definition
   * @returns Monaco hover information
   */
  createHoverInfo(udf: UserDefinedFunction): any {
    // TODO: Define Monaco Hover type
    const displayName = udf.name.replace("${dataset}", "{dataset}")

    const contents: IMarkdownString = {
      value: [
        `### ${displayName}`,
        "",
        udf.description,
        "",
        "**SQL Signature:**",
        "```sql",
        udf.sql.trim(),
        "```",
        "",
        udf.example ? `**Example:**\n\`\`\`sql\n${udf.example}\n\`\`\`` : "",
        "",
        udf.returnType ? `**Returns:** ${udf.returnType}` : "",
      ]
        .filter((line) => line !== "")
        .join("\n"),
      isTrusted: true,
    }

    return {
      contents: [contents],
    }
  }

  /**
   * Update Configuration
   *
   * Updates the snippet generator configuration.
   *
   * @param config - New configuration options
   */
  updateConfig(config: Partial<UdfSnippetConfig>): void {
    this.config = { ...this.config, ...config }
  }
}

/**
 * Convenience function to create UDF snippet
 *
 * @param udf - User-defined function definition
 * @param config - Optional configuration
 * @returns Monaco snippet string
 */
export function createUdfSnippet(
  udf: UserDefinedFunction,
  config?: UdfSnippetConfig,
): string {
  const generator = new UdfSnippetGenerator(config)
  return generator.createUdfSnippet(udf)
}

/**
 * Convenience function to create UDF completion item
 *
 * @param udf - User-defined function definition
 * @param sortText - Sort order for completion
 * @param config - Optional configuration
 * @returns Monaco completion item
 */
export function createUdfCompletionItem(
  udf: UserDefinedFunction,
  sortText: string,
  position: Position,
  config?: UdfSnippetConfig,
): languages.CompletionItem {
  const generator = new UdfSnippetGenerator(config)
  return generator.createCompletionItem(udf, sortText, position)
}
