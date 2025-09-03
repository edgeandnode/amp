/**
 * UDF Snippets - Advanced snippet generation for Nozzle User-Defined Functions
 *
 * This service creates Monaco Editor snippets for UDF functions with:
 * - Tab navigation between parameters (tabstops)
 * - Parameter placeholders with types
 * - Dynamic dataset prefix handling for eth_call
 * - Proper SQL signature integration
 *
 * Monaco snippets use the TextMate snippet format:
 * - ${1:placeholder} for tabstops with placeholders
 * - $0 for final cursor position
 * - Tab key navigates between tabstops
 */

import type { UserDefinedFunction } from "./nozzleSQLProviders"

/**
 * Creates a Monaco Editor snippet for a UDF function with proper tabstop navigation
 * Each UDF gets custom snippet logic based on its signature and usage patterns
 *
 * @param udf The user-defined function to create snippet for
 * @returns Monaco snippet string with tabstops
 */
export function createUDFSnippet(udf: UserDefinedFunction): string {
  // Handle specific UDF functions with tailored snippets
  switch (udf.name) {
    case "evm_decode_log":
      return createEvmDecodeLogSnippet()

    case "evm_topic":
      return createEvmTopicSnippet()

    case "${dataset}.eth_call":
      return createEthCallSnippet()

    case "evm_decode_params":
      return createEvmDecodeParamsSnippet()

    case "evm_encode_params":
      return createEvmEncodeParamsSnippet()

    case "evm_encode_type":
      return createEvmEncodeTypeSnippet()

    case "evm_decode_type":
      return createEvmDecodeTypeSnippet()

    case "attestation_hash":
      return createAttestationHashSnippet()

    default:
      // Generic fallback for unknown UDFs
      return createGenericUDFSnippet(udf)
  }
}

/**
 * Creates snippet for evm_decode_log function
 * Signature: T evm_decode_log(FixedSizeBinary(20) topic1, FixedSizeBinary(20) topic2, FixedSizeBinary(20) topic3, Binary data, Utf8 signature)
 */
function createEvmDecodeLogSnippet(): string {
  return `evm_decode_log(
  \${1:topic1},
  \${2:topic2}, 
  \${3:topic3},
  \${4:data},
  '\${5:Transfer(address,address,uint256)}'
)$0`
}

/**
 * Creates snippet for evm_topic function
 * Signature: FixedSizeBinary(32) evm_topic(Utf8 signature)
 */
function createEvmTopicSnippet(): string {
  return `evm_topic('\${1:Transfer(address,address,uint256)}')$0`
}

/**
 * Creates snippet for {dataset}.eth_call function with dynamic dataset prefix
 * Signature: (Binary, Utf8) {dataset}.eth_call(FixedSizeBinary(20) from, FixedSizeBinary(20) to, Binary input_data, Utf8 block)
 */
function createEthCallSnippet(): string {
  return `\${1:dataset}.eth_call(
  \${2:from_address},
  \${3:to_address}, 
  \${4:input_data},
  '\${5:latest}'
)$0`
}

/**
 * Creates snippet for evm_decode_params function
 * Signature: T evm_decode_params(Binary input, Utf8 signature)
 */
function createEvmDecodeParamsSnippet(): string {
  return `evm_decode_params(
  \${1:input},
  '\${2:function_signature}'
)$0`
}

/**
 * Creates snippet for evm_encode_params function
 * Signature: T evm_encode_params(Any args..., Utf8 signature)
 */
function createEvmEncodeParamsSnippet(): string {
  return `evm_encode_params(
  \${1:arg1},
  \${2:arg2},
  '\${3:function_signature}'
)$0`
}

/**
 * Creates snippet for evm_encode_type function
 * Signature: Binary evm_encode_type(Any value, Utf8 type)
 */
function createEvmEncodeTypeSnippet(): string {
  return `evm_encode_type(
  \${1:value},
  '\${2:uint256}'
)$0`
}

/**
 * Creates snippet for evm_decode_type function
 * Signature: T evm_decode_type(Binary data, Utf8 type)
 */
function createEvmDecodeTypeSnippet(): string {
  return `evm_decode_type(
  \${1:data},
  '\${2:uint256}'
)$0`
}

/**
 * Creates snippet for attestation_hash function
 * Signature: Binary attestation_hash(...)
 */
function createAttestationHashSnippet(): string {
  return `attestation_hash(\${1:column1}, \${2:column2})$0`
}

/**
 * Creates a generic snippet for unknown UDF functions
 * Falls back to simple function call with single parameter placeholder
 */
function createGenericUDFSnippet(udf: UserDefinedFunction): string {
  const functionName = udf.name.replace(/\$\{[^}]+\}/g, "{dataset}")
  return `${functionName}(\${1})$0`
}

/**
 * Enhanced UDF completion item creation with snippet support
 * This updates the basic UDF suggestions to use advanced snippets
 */
export function createUDFSuggestions(
  monaco: typeof import("monaco-editor"),
  udfs: Array<UserDefinedFunction>,
): Array<monaco.languages.CompletionItem> {
  return udfs.map((udf, index) => {
    // Clean display name for Monaco completion list
    const displayName = udf.name.replace(/\$\{[^}]+\}/g, "{dataset}")

    return {
      label: displayName,
      kind: monaco.languages.CompletionItemKind.Function,
      detail: "Nozzle UDF",
      documentation: {
        value: `**${displayName}** - Nozzle User-Defined Function\n\n${udf.description}\n\n**SQL Signature:**\n\`\`\`sql\n${udf.sql.trim()}\n\`\`\`\n\n💡 **Tip:** Use Tab to navigate between parameters after insertion`,
        isTrusted: true,
      },
      // Insert as snippet with tabstop navigation
      insertText: createUDFSnippet(udf),
      insertTextRules:
        monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
      // Priority: UDFs get high priority for function context (3-xxx)
      sortText: `3-${index.toString().padStart(3, "0")}`,
      // Add additional metadata for better UX
      command: {
        id: "editor.action.triggerSuggest",
        title: "Trigger Parameter Hints",
      },
    }
  })
}

/**
 * Provides hover information for UDF functions with enhanced documentation
 * This improves upon the basic hover provider with more detailed information
 */
export function provideUDFHover(
  monaco: typeof import("monaco-editor"),
  model: monaco.editor.ITextModel,
  position: monaco.Position,
  udfs: Array<UserDefinedFunction>,
): monaco.languages.ProviderResult<monaco.languages.Hover> {
  try {
    // Get the word at the current position
    const word = model.getWordAtPosition(position)
    if (!word) return null

    // Find matching UDF (handle both exact matches and pattern matches)
    const udf = udfs.find((u) => {
      const cleanName = u.name.replace(/\$\{[^}]+\}/g, "")
      return (
        u.name === word.word ||
        cleanName === word.word ||
        word.word.includes(cleanName) ||
        u.name.includes(word.word)
      )
    })

    if (!udf) return null

    // Create enhanced hover content
    const displayName = udf.name.replace(/\$\{[^}]+\}/g, "{dataset}")

    return {
      range: new monaco.Range(
        position.lineNumber,
        word.startColumn,
        position.lineNumber,
        word.endColumn,
      ),
      contents: [
        {
          value: `**${displayName}** (Nozzle UDF)`,
          isTrusted: true,
        },
        {
          value: udf.description,
          isTrusted: true,
        },
        {
          value: `**SQL Signature:**\n\`\`\`sql\n${udf.sql.trim()}\n\`\`\``,
          isTrusted: true,
        },
        {
          value: `💡 **Usage Tip:** Use Ctrl+Space for parameter suggestions, Tab to navigate parameters`,
          isTrusted: true,
        },
      ],
    }
  } catch (error) {
    console.error("❌ Error in UDF hover provider:", error)
    return null
  }
}

/**
 * Validates UDF function signatures in the editor
 * Provides helpful error messages for incorrect UDF usage
 */
export function validateUDFSignature(
  model: monaco.editor.ITextModel,
  udfs: Array<UserDefinedFunction>,
): Array<monaco.editor.IMarkerData> {
  const markers: Array<monaco.editor.IMarkerData> = []
  const query = model.getValue()

  try {
    // Check each UDF function call for proper signature
    udfs.forEach((udf) => {
      const functionName = udf.name.replace(/\$\{[^}]+\}/g, "\\w+")
      const regex = new RegExp(`${functionName}\\s*\\([^)]*\\)`, "gi")

      let match
      while ((match = regex.exec(query)) !== null) {
        // Basic validation - could be enhanced with more sophisticated parsing
        const callText = match[0]
        const paramCount = (callText.match(/,/g) || []).length + 1

        // Example validation for evm_decode_log (5 parameters expected)
        if (udf.name === "evm_decode_log" && paramCount !== 5) {
          const position = model.getPositionAt(match.index)
          markers.push({
            severity: monaco.MarkerSeverity.Error,
            message: `evm_decode_log expects 5 parameters (topic1, topic2, topic3, data, signature), got ${paramCount}`,
            startLineNumber: position.lineNumber,
            startColumn: position.column,
            endLineNumber: position.lineNumber,
            endColumn: position.column + callText.length,
            code: "nozzle.udf.signature",
          })
        }
      }
    })
  } catch (error) {
    console.error("❌ Error validating UDF signatures:", error)
  }

  return markers
}

// Monaco types are imported at the top of this file
