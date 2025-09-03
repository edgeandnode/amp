/**
 * Monaco Editor Mock for Testing
 *
 * Provides a complete mock of Monaco Editor interfaces for unit testing
 * SQL intellisense functionality without requiring the full Monaco bundle.
 */

import { vi } from "vitest"

// Mock Monaco Editor Position class
export class Position {
  constructor(
    public lineNumber: number,
    public column: number,
  ) {}
}

// Mock Monaco Editor Range class
export class Range {
  constructor(
    public startLineNumber: number,
    public startColumn: number,
    public endLineNumber: number,
    public endColumn: number,
  ) {}
}

// Mock CompletionItemKind enum
export const languages = {
  CompletionItemKind: {
    Class: 7,
    Field: 5,
    Function: 3,
    Keyword: 14,
    Method: 1,
    Property: 9,
    Value: 12,
    Variable: 6,
  },

  CompletionItemInsertTextRule: {
    InsertAsSnippet: 4,
    KeepWhitespace: 1,
  },

  // Mock provider registration functions
  registerCompletionItemProvider: vi.fn(() => ({
    dispose: vi.fn(),
  })),

  registerHoverProvider: vi.fn(() => ({
    dispose: vi.fn(),
  })),

  registerCodeActionProvider: vi.fn(() => ({
    dispose: vi.fn(),
  })),
}

// Mock MarkerSeverity enum
export const MarkerSeverity = {
  Hint: 1,
  Info: 2,
  Warning: 4,
  Error: 8,
}

// Mock editor interfaces for testing
export const editor = {
  ITextModel: class {
    getLineContent = vi.fn(() => "")
    getValue = vi.fn(() => "")
    getWordAtPosition = vi.fn(() => null)
    getPositionAt = vi.fn(() => new Position(1, 1))
  },

  IStandaloneCodeEditor: class {
    // Mock editor methods as needed
  },
}

// Export all mocked types and interfaces
export default {
  Position,
  Range,
  languages,
  MarkerSeverity,
  editor,
}
