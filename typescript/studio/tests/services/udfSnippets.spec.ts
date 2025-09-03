/**
 * Unit Tests for UDF Snippets
 *
 * Tests the advanced UDF snippet generation, completion items, hover providers,
 * and signature validation for Monaco Editor UDF integration.
 */

import { describe, expect, it, beforeEach, vi } from "vitest"
import * as monaco from "monaco-editor"
import {
  createUDFSnippet,
  createUDFSuggestions,
  provideUDFHover,
  validateUDFSignature,
} from "../../src/services/udfSnippets"
import type { UserDefinedFunction } from "../../src/services/nozzleSQLProviders"

describe("UDF Snippets", () => {
  let testUdfs: Array<UserDefinedFunction>

  beforeEach(() => {
    testUdfs = [
      {
        name: "evm_decode_log",
        description: "Decodes EVM event logs using provided signature",
        sql: "evm_decode_log(topic1 FixedSizeBinary(20), topic2 FixedSizeBinary(20), topic3 FixedSizeBinary(20), data Binary, signature Utf8) -> T",
      },
      {
        name: "evm_topic",
        description: "Generates keccak256 hash of event signature",
        sql: "evm_topic(signature Utf8) -> FixedSizeBinary(32)",
      },
      {
        name: "${dataset}.eth_call",
        description: "Makes eth_call RPC requests at specified block",
        sql: "${dataset}.eth_call(from FixedSizeBinary(20), to FixedSizeBinary(20), input_data Binary, block Utf8) -> (Binary, Utf8)",
      },
      {
        name: "evm_encode_params",
        description: "ABI-encodes function parameters",
        sql: "evm_encode_params(args..., signature Utf8) -> T",
      },
      {
        name: "attestation_hash",
        description: "Generates cryptographic hash for data attestation",
        sql: "attestation_hash(...) -> Binary",
      },
      {
        name: "unknown_udf",
        description: "Unknown UDF for testing generic fallback",
        sql: "unknown_udf(param Any) -> Any",
      },
    ]
  })

  describe("createUDFSnippet", () => {
    it("should create evm_decode_log snippet with 5 tabstops", () => {
      const udf = testUdfs.find((u) => u.name === "evm_decode_log")!
      const snippet = createUDFSnippet(udf)

      expect(snippet).toContain("evm_decode_log(")
      expect(snippet).toContain("${1:topic1}")
      expect(snippet).toContain("${2:topic2}")
      expect(snippet).toContain("${3:topic3}")
      expect(snippet).toContain("${4:data}")
      expect(snippet).toContain("'${5:Transfer(address,address,uint256)}'")
      expect(snippet).toContain(")$0")
    })

    it("should create evm_topic snippet with signature placeholder", () => {
      const udf = testUdfs.find((u) => u.name === "evm_topic")!
      const snippet = createUDFSnippet(udf)

      expect(snippet).toBe(
        "evm_topic('${1:Transfer(address,address,uint256)}')$0",
      )
    })

    it("should create eth_call snippet with dynamic dataset prefix", () => {
      const udf = testUdfs.find((u) => u.name === "${dataset}.eth_call")!
      const snippet = createUDFSnippet(udf)

      expect(snippet).toContain("${1:dataset}.eth_call(")
      expect(snippet).toContain("${2:from_address}")
      expect(snippet).toContain("${3:to_address}")
      expect(snippet).toContain("${4:input_data}")
      expect(snippet).toContain("'${5:latest}'")
      expect(snippet).toContain(")$0")
    })

    it("should create evm_encode_params snippet with variable arguments", () => {
      const udf = testUdfs.find((u) => u.name === "evm_encode_params")!
      const snippet = createUDFSnippet(udf)

      expect(snippet).toContain("evm_encode_params(")
      expect(snippet).toContain("${1:arg1}")
      expect(snippet).toContain("${2:arg2}")
      expect(snippet).toContain("'${3:function_signature}'")
      expect(snippet).toContain(")$0")
    })

    it("should create attestation_hash snippet with placeholder columns", () => {
      const udf = testUdfs.find((u) => u.name === "attestation_hash")!
      const snippet = createUDFSnippet(udf)

      expect(snippet).toBe("attestation_hash(${1:column1}, ${2:column2})$0")
    })

    it("should create generic snippet for unknown UDFs", () => {
      const udf = testUdfs.find((u) => u.name === "unknown_udf")!
      const snippet = createUDFSnippet(udf)

      expect(snippet).toBe("unknown_udf(${1})$0")
    })

    it("should handle dataset placeholders in generic UDFs", () => {
      const udf: UserDefinedFunction = {
        name: "${dataset}.custom_function",
        description: "Custom dataset function",
        sql: "custom_function(param) -> result",
      }

      const snippet = createUDFSnippet(udf)
      expect(snippet).toBe("{dataset}.custom_function(${1})$0")
    })
  })

  describe("createUDFSuggestions", () => {
    it("should create completion items with snippet insertion", () => {
      const suggestions = createUDFSuggestions(testUdfs)

      expect(suggestions).toHaveLength(testUdfs.length)

      const evmDecodeLog = suggestions.find((s) => s.label === "evm_decode_log")
      expect(evmDecodeLog).toEqual({
        label: "evm_decode_log",
        kind: monaco.languages.CompletionItemKind.Function,
        detail: "Nozzle UDF",
        documentation: {
          value: expect.stringContaining(
            "**evm_decode_log** - Nozzle User-Defined Function",
          ),
          isTrusted: true,
        },
        insertText: expect.stringContaining("evm_decode_log("),
        insertTextRules:
          monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
        sortText: expect.stringMatching(/^3-\d{3}$/),
        command: {
          id: "editor.action.triggerSuggest",
          title: "Trigger Parameter Hints",
        },
      })
    })

    it("should clean display names for dataset placeholders", () => {
      const suggestions = createUDFSuggestions(testUdfs)

      const ethCall = suggestions.find((s) => s.label === "{dataset}.eth_call")
      expect(ethCall).toBeDefined()
      expect(ethCall!.label).toBe("{dataset}.eth_call")
    })

    it("should include usage tips in documentation", () => {
      const suggestions = createUDFSuggestions(testUdfs)

      suggestions.forEach((suggestion) => {
        expect(suggestion.documentation?.value).toContain(
          "💡 **Tip:** Use Tab to navigate between parameters after insertion",
        )
      })
    })

    it("should have proper sort order with priority prefix", () => {
      const suggestions = createUDFSuggestions(testUdfs)

      suggestions.forEach((suggestion, index) => {
        expect(suggestion.sortText).toBe(
          `3-${index.toString().padStart(3, "0")}`,
        )
      })
    })
  })

  describe("provideUDFHover", () => {
    let mockModel: monaco.editor.ITextModel
    let mockPosition: monaco.Position

    beforeEach(() => {
      mockModel = {
        getWordAtPosition: vi.fn(),
      } as any
      mockPosition = new monaco.Position(1, 10)
    })

    it("should provide hover for exact UDF name match", () => {
      mockModel.getWordAtPosition = vi.fn(() => ({
        word: "evm_decode_log",
        startColumn: 5,
        endColumn: 19,
      }))

      const hover = provideUDFHover(mockModel, mockPosition, testUdfs)

      expect(hover).toEqual({
        range: expect.any(monaco.Range),
        contents: [
          {
            value: "**evm_decode_log** (Nozzle UDF)",
            isTrusted: true,
          },
          {
            value: "Decodes EVM event logs using provided signature",
            isTrusted: true,
          },
          {
            value: expect.stringContaining("**SQL Signature:**"),
            isTrusted: true,
          },
          {
            value:
              "💡 **Usage Tip:** Use Ctrl+Space for parameter suggestions, Tab to navigate parameters",
            isTrusted: true,
          },
        ],
      })
    })

    it("should handle dataset placeholder matching", () => {
      mockModel.getWordAtPosition = vi.fn(() => ({
        word: "eth_call",
        startColumn: 10,
        endColumn: 18,
      }))

      const hover = provideUDFHover(mockModel, mockPosition, testUdfs)

      expect(hover?.contents).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            value: "**{dataset}.eth_call** (Nozzle UDF)",
            isTrusted: true,
          }),
        ]),
      )
    })

    it("should return null for non-matching words", () => {
      mockModel.getWordAtPosition = vi.fn(() => ({
        word: "not_a_udf",
        startColumn: 5,
        endColumn: 14,
      }))

      const hover = provideUDFHover(mockModel, mockPosition, testUdfs)
      expect(hover).toBeNull()
    })

    it("should return null when no word at position", () => {
      mockModel.getWordAtPosition = vi.fn(() => null)

      const hover = provideUDFHover(mockModel, mockPosition, testUdfs)
      expect(hover).toBeNull()
    })

    it("should handle errors gracefully", () => {
      mockModel.getWordAtPosition = vi.fn(() => {
        throw new Error("Model error")
      })

      const hover = provideUDFHover(mockModel, mockPosition, testUdfs)
      expect(hover).toBeNull()
    })
  })

  describe("validateUDFSignature", () => {
    let mockModel: monaco.editor.ITextModel

    beforeEach(() => {
      mockModel = {
        getValue: vi.fn(),
        getPositionAt: vi.fn(),
      } as any
    })

    it("should validate evm_decode_log parameter count", () => {
      const query =
        "SELECT evm_decode_log(topic1, topic2, topic3, data) FROM logs"
      mockModel.getValue = vi.fn(() => query)
      mockModel.getPositionAt = vi.fn(() => new monaco.Position(1, 7))

      const markers = validateUDFSignature(mockModel, testUdfs)

      expect(markers).toEqual([
        expect.objectContaining({
          severity: monaco.MarkerSeverity.Error,
          message: expect.stringContaining(
            "evm_decode_log expects 5 parameters",
          ),
          code: "nozzle.udf.signature",
        }),
      ])
    })

    it("should accept correct evm_decode_log usage", () => {
      const query =
        "SELECT evm_decode_log(topic1, topic2, topic3, data, 'Transfer(address,address,uint256)') FROM logs"
      mockModel.getValue = vi.fn(() => query)

      const markers = validateUDFSignature(mockModel, testUdfs)

      // Should not have any error markers for correct usage
      const evmDecodeLogErrors = markers.filter((m) =>
        m.message.includes("evm_decode_log"),
      )
      expect(evmDecodeLogErrors).toHaveLength(0)
    })

    it("should handle multiple UDF calls in one query", () => {
      const query = `
        SELECT 
          evm_decode_log(topic1, topic2, topic3, data, 'signature'),
          evm_topic('Transfer(address,address,uint256)'),
          evm_decode_log(topic1, topic2) -- This should error
        FROM logs
      `
      mockModel.getValue = vi.fn(() => query)
      mockModel.getPositionAt = vi.fn(
        (index) => new monaco.Position(Math.floor(index / 50) + 1, index % 50),
      )

      const markers = validateUDFSignature(mockModel, testUdfs)

      // Should find error in the second evm_decode_log call
      const evmDecodeLogErrors = markers.filter((m) =>
        m.message.includes("evm_decode_log"),
      )
      expect(evmDecodeLogErrors).toHaveLength(1)
    })

    it("should handle regex errors gracefully", () => {
      mockModel.getValue = vi.fn(() => {
        throw new Error("Model error")
      })

      expect(() => {
        const markers = validateUDFSignature(mockModel, testUdfs)
        expect(markers).toEqual([])
      }).not.toThrow()
    })

    it("should handle complex UDF patterns", () => {
      const udfWithComplexName: UserDefinedFunction = {
        name: "complex.function.name",
        description: "Complex UDF name",
        sql: "complex.function.name(param) -> result",
      }

      const query = "SELECT complex.function.name(param1, param2) FROM table"
      mockModel.getValue = vi.fn(() => query)

      const markers = validateUDFSignature(mockModel, [udfWithComplexName])
      expect(markers).toEqual([])
    })
  })

  describe("Integration Tests", () => {
    it("should work together: snippet -> completion -> hover", () => {
      // Test the full workflow from snippet generation to hover
      const udf = testUdfs[0] // evm_decode_log

      // 1. Generate snippet
      const snippet = createUDFSnippet(udf)
      expect(snippet).toContain("${1:topic1}")

      // 2. Create completion suggestion
      const suggestions = createUDFSuggestions([udf])
      expect(suggestions[0].insertText).toBe(snippet)

      // 3. Test hover
      const mockModel: monaco.editor.ITextModel = {
        getWordAtPosition: vi.fn(() => ({
          word: "evm_decode_log",
          startColumn: 1,
          endColumn: 15,
        })),
      } as any

      const hover = provideUDFHover(mockModel, new monaco.Position(1, 10), [
        udf,
      ])
      expect(hover?.contents).toHaveLength(4)
      expect(hover?.contents[0].value).toContain("evm_decode_log")
    })

    it("should maintain consistency across all UDF functions", () => {
      testUdfs.forEach((udf) => {
        // Each UDF should have a snippet
        const snippet = createUDFSnippet(udf)
        expect(snippet).toContain("$0") // Final cursor position

        // Each UDF should create a valid completion
        const completions = createUDFSuggestions([udf])
        expect(completions).toHaveLength(1)
        expect(completions[0].kind).toBe(
          monaco.languages.CompletionItemKind.Function,
        )
      })
    })
  })
})
