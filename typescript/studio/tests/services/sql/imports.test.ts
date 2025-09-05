/**
 * Simple test to verify all imports work correctly
 */

import { describe, test, expect } from 'vitest'
import { QueryContextAnalyzer } from '../../../src/services/sql/contextAnalyzer'
import { NozzleCompletionProvider } from '../../../src/services/sql/completionProvider'
import { UdfSnippetGenerator } from '../../../src/services/sql/udfSnippets'
import { 
  setupNozzleSQLProviders, 
  updateProviderData, 
  getProviderMetrics,
  areProvidersActive 
} from '../../../src/services/sql'
import { mockMetadata, mockUDFs } from './fixtures'

describe('SQL Intellisense Imports', () => {
  test('should import all core classes', () => {
    expect(QueryContextAnalyzer).toBeDefined()
    expect(NozzleCompletionProvider).toBeDefined()
    expect(UdfSnippetGenerator).toBeDefined()
  })

  test('should import main API functions', () => {
    expect(setupNozzleSQLProviders).toBeDefined()
    expect(updateProviderData).toBeDefined()
    expect(getProviderMetrics).toBeDefined()
    expect(areProvidersActive).toBeDefined()
  })

  test('should import test fixtures', () => {
    expect(mockMetadata).toBeDefined()
    expect(mockUDFs).toBeDefined()
    expect(mockMetadata.length).toBe(3)
    expect(mockUDFs.length).toBe(8)
  })

  test('should create analyzer instance', () => {
    const analyzer = new QueryContextAnalyzer()
    expect(analyzer).toBeInstanceOf(QueryContextAnalyzer)
  })

  test('should create completion provider instance', () => {
    const analyzer = new QueryContextAnalyzer()
    const provider = new NozzleCompletionProvider(mockMetadata, mockUDFs, analyzer)
    expect(provider).toBeInstanceOf(NozzleCompletionProvider)
  })

  test('should create UDF snippet generator instance', () => {
    const generator = new UdfSnippetGenerator()
    expect(generator).toBeInstanceOf(UdfSnippetGenerator)
  })

  test('should check provider status initially', () => {
    // Initially no providers should be active
    expect(areProvidersActive()).toBe(false)
  })
})