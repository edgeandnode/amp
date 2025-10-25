/**
 * Amp SQL Intellisense Services
 *
 * This module provides SQL intellisense for the Amp Studio query playground.
 * It exports the unified SQL provider and related utilities.
 *
 * @file index.ts
 */

// Export the unified provider (what's actually used in production)
export { type ISQLProvider, type SQLProviderConfig, UnifiedSQLProvider } from "./UnifiedSQLProvider"

// Export supporting classes and utilities
export { AmpCompletionProvider } from "./AmpCompletionProvider"
export { QueryContextAnalyzer } from "./QueryContextAnalyzer"
export { SqlValidation } from "./SqlValidation"
export { createUdfCompletionItem, createUdfSnippet, UdfSnippetGenerator } from "./UDFSnippetGenerator"

// Export types
export * from "./types"

// Export manifest utilities
export { convertManifestToMetadata, mergeMetadataSources } from "./manifestConverter"
