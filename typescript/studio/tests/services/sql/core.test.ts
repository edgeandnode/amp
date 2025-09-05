/**
 * Core unit tests for SQL intellisense system without Monaco dependencies
 * Tests the basic functionality of types, UDF snippets, and configurations
 */

import { describe, expect,test } from 'vitest'

import { createUdfCompletionItem,createUdfSnippet, UdfSnippetGenerator } from '../../../src/services/sql/UDFSnippetGenerator'

import { mockUDFs } from './fixtures'

import { COMPLETION_PRIORITY,DEFAULT_COMPLETION_CONFIG } from '../../../src/services/sql/types'

describe('SQL Intellisense Core Functionality', () => {
  describe('Configuration', () => {
    test('should have default completion config', () => {
      expect(DEFAULT_COMPLETION_CONFIG).toBeDefined()
      expect(DEFAULT_COMPLETION_CONFIG.minPrefixLength).toBe(0)
      expect(DEFAULT_COMPLETION_CONFIG.maxSuggestions).toBe(200)
      expect(DEFAULT_COMPLETION_CONFIG.enableSnippets).toBe(true)
      expect(DEFAULT_COMPLETION_CONFIG.enableContextFiltering).toBe(true)
      expect(DEFAULT_COMPLETION_CONFIG.enableAliasResolution).toBe(true)
      expect(DEFAULT_COMPLETION_CONFIG.contextCacheTTL).toBe(30000)
      expect(DEFAULT_COMPLETION_CONFIG.enableDebugLogging).toBe(false)
    })

    test('should have completion priorities', () => {
      expect(COMPLETION_PRIORITY.TABLE).toBe('1')
      expect(COMPLETION_PRIORITY.COLUMN).toBe('2')
      expect(COMPLETION_PRIORITY.UDF).toBe('3')
      expect(COMPLETION_PRIORITY.KEYWORD).toBe('4')
      expect(COMPLETION_PRIORITY.OPERATOR).toBe('5')
    })
  })

  describe('UDF Snippet Generator', () => {
    let generator: UdfSnippetGenerator

    test('should create UDF snippet generator', () => {
      generator = new UdfSnippetGenerator()
      expect(generator).toBeInstanceOf(UdfSnippetGenerator)
    })

    test('should generate snippet for evm_decode_log', () => {
      generator = new UdfSnippetGenerator()
      const udf = mockUDFs.find(u => u.name === 'evm_decode_log')!
      
      const snippet = generator.createUdfSnippet(udf)
      
      expect(snippet).toContain('evm_decode_log(')
      expect(snippet).toContain('${1:')
      expect(snippet).toContain('${2:')
      expect(snippet).toContain('$0') // Final tab stop
    })

    test('should generate snippet for evm_topic', () => {
      generator = new UdfSnippetGenerator()
      const udf = mockUDFs.find(u => u.name === 'evm_topic')!
      
      const snippet = generator.createUdfSnippet(udf)
      
      expect(snippet).toContain('evm_topic(')
      expect(snippet).toContain('${1:')
      expect(snippet).toContain('$0')
    })

    test('should generate snippet for dataset-prefixed UDF', () => {
      generator = new UdfSnippetGenerator()
      const udf = mockUDFs.find(u => u.name === '${dataset}.eth_call')!
      
      const snippet = generator.createUdfSnippet(udf)
      
      expect(snippet).toContain('.eth_call(')
      expect(snippet).toContain('${1:')
      expect(snippet).toContain('$0')
    })

    test('should generate snippet for attestation_hash', () => {
      generator = new UdfSnippetGenerator()
      const udf = mockUDFs.find(u => u.name === 'attestation_hash')!
      
      const snippet = generator.createUdfSnippet(udf)
      
      expect(snippet).toContain('attestation_hash(')
      expect(snippet).toContain('${1:')
      expect(snippet).toContain('$0')
    })

    test('should handle generic UDF with parameters', () => {
      generator = new UdfSnippetGenerator()
      const udf = mockUDFs.find(u => u.name === 'evm_encode_params')!
      
      const snippet = generator.createUdfSnippet(udf)
      
      expect(snippet).toContain('evm_encode_params(')
      expect(snippet).toContain('${1:')
      expect(snippet).toContain('$0')
    })

    test('should update snippet generator config', () => {
      generator = new UdfSnippetGenerator()
      
      generator.updateConfig({
        includeTypeHints: false,
        includeExampleValues: true
      })
      
      // Generator should not throw when config is updated
      expect(() => {
        const udf = mockUDFs[0]
        generator.createUdfSnippet(udf)
      }).not.toThrow()
    })
  })

  describe('Convenience Functions', () => {
    test('should create UDF snippet via convenience function', () => {
      const udf = mockUDFs.find(u => u.name === 'evm_topic')!
      
      const snippet = createUdfSnippet(udf)
      
      expect(snippet).toContain('evm_topic(')
      expect(snippet).toContain('${1:')
      expect(snippet).toContain('$0')
    })

    test('should create UDF completion item via convenience function', () => {
      // Mock Monaco types for this test
      const mockMonaco = {
        languages: {
          CompletionItemKind: {
            Function: 1
          },
          CompletionItemInsertTextRule: {
            InsertAsSnippet: 4
          }
        }
      }
      
      // Temporarily replace global monaco reference
      const originalMonaco = globalThis.monaco
      globalThis.monaco = mockMonaco as any
      
      try {
        const udf = mockUDFs.find(u => u.name === 'evm_topic')!
        
        const completionItem = createUdfCompletionItem(udf, '001')
        
        expect(completionItem.label).toBe('evm_topic')
        expect(completionItem.kind).toBe(1) // Function
        expect(completionItem.sortText).toBe('001')
        expect(completionItem.insertText).toContain('evm_topic(')
      } finally {
        // Restore original monaco reference
        globalThis.monaco = originalMonaco
      }
    })
  })

  describe('Mock Data Validation', () => {
    test('should have valid mock UDF data', () => {
      expect(mockUDFs).toBeDefined()
      expect(mockUDFs.length).toBe(8)
      
      // Check each UDF has required properties
      mockUDFs.forEach(udf => {
        expect(udf.name).toBeDefined()
        expect(typeof udf.name).toBe('string')
        expect(udf.description).toBeDefined()
        expect(typeof udf.description).toBe('string')
        expect(udf.sql).toBeDefined()
        expect(typeof udf.sql).toBe('string')
        
        if (udf.parameters) {
          expect(Array.isArray(udf.parameters)).toBe(true)
        }
      })
    })

    test('should have expected UDF functions', () => {
      const expectedNames = [
        'evm_decode_log',
        'evm_topic', 
        '${dataset}.eth_call',
        'attestation_hash',
        'evm_decode_params',
        'evm_encode_params',
        'evm_encode_type',
        'evm_decode_type'
      ]

      expectedNames.forEach(name => {
        const udf = mockUDFs.find(u => u.name === name)
        expect(udf).toBeDefined()
        expect(udf!.name).toBe(name)
      })
    })
  })
})
