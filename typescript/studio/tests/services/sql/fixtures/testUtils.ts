/**
 * Test utilities for SQL intellisense testing
 * Provides helpers for creating Monaco models and testing completion providers
 */
import * as monaco from "monaco-editor/esm/vs/editor/editor.api"

/**
 * Creates a Monaco editor model for testing
 */
export function createTestModel(
  content: string,
  language = "sql",
): monaco.editor.ITextModel {
  return monaco.editor.createModel(content, language)
}

/**
 * Disposes a Monaco editor model after testing
 */
export function disposeTestModel(model: monaco.editor.ITextModel): void {
  model.dispose()
}

/**
 * Creates a Monaco Position for testing
 */
export function createTestPosition(
  line: number,
  column: number,
): monaco.Position {
  return new monaco.Position(line, column)
}

/**
 * Creates a Monaco Range for testing
 */
export function createTestRange(
  startLine: number,
  startColumn: number,
  endLine: number,
  endColumn: number,
): monaco.Range {
  return new monaco.Range(startLine, startColumn, endLine, endColumn)
}

/**
 * Creates a mock completion context for testing
 */
export function createMockCompletionContext(
  triggerKind: monaco.languages.CompletionTriggerKind = monaco.languages.CompletionTriggerKind.Invoke,
  triggerCharacter?: string | undefined,
): monaco.languages.CompletionContext {
  return {
    triggerKind,
    ...(triggerCharacter ? { triggerCharacter } : {}),
  }
}

/**
 * Creates a mock cancellation token for testing
 */
export function createMockCancellationToken(
  isCancelled = false,
): monaco.CancellationToken {
  return {
    isCancellationRequested: isCancelled,
    onCancellationRequested: () => ({ dispose: () => {} }),
  }
}

/**
 * Creates a test setup with model and position
 */
export interface TestSetup {
  model: monaco.editor.ITextModel
  position: monaco.Position
  cleanup: () => void
}

export function createTestSetup(
  query: string,
  line: number,
  column: number,
): TestSetup {
  const model = createTestModel(query)
  const position = createTestPosition(line, column)

  return {
    model,
    position,
    cleanup: () => disposeTestModel(model),
  }
}

/**
 * Helper to extract completion item labels from completion results
 */
export function extractCompletionLabels(
  completionList: monaco.languages.CompletionList | null,
): Array<string> {
  if (!completionList?.suggestions) {
    return []
  }

  return completionList.suggestions.map((item) => typeof item.label === "string" ? item.label : item.label.label)
}

/**
 * Helper to find completion item by label
 */
export function findCompletionByLabel(
  completionList: monaco.languages.CompletionList | null,
  label: string,
): monaco.languages.CompletionItem | undefined {
  if (!completionList?.suggestions) {
    return undefined
  }

  return completionList.suggestions.find((item) => {
    const itemLabel = typeof item.label === "string" ? item.label : item.label.label
    return itemLabel === label
  })
}

/**
 * Helper to verify completion contains expected items
 */
export function assertCompletionContains(
  completionList: monaco.languages.CompletionList | null,
  expectedLabels: Array<string>,
): void {
  const actualLabels = extractCompletionLabels(completionList)

  for (const expectedLabel of expectedLabels) {
    if (!actualLabels.includes(expectedLabel)) {
      throw new Error(
        `Expected completion to contain "${expectedLabel}". ` +
          `Actual completions: [${actualLabels.join(", ")}]`,
      )
    }
  }
}

/**
 * Helper to verify completion does NOT contain certain items
 */
export function assertCompletionDoesNotContain(
  completionList: monaco.languages.CompletionList | null,
  unexpectedLabels: Array<string>,
): void {
  const actualLabels = extractCompletionLabels(completionList)

  for (const unexpectedLabel of unexpectedLabels) {
    if (actualLabels.includes(unexpectedLabel)) {
      throw new Error(
        `Expected completion to NOT contain "${unexpectedLabel}". ` +
          `Actual completions: [${actualLabels.join(", ")}]`,
      )
    }
  }
}

/**
 * Helper to count completion items by kind
 */
export function countCompletionsByKind(
  completionList: monaco.languages.CompletionList | null,
  kind: monaco.languages.CompletionItemKind,
): number {
  if (!completionList?.suggestions) {
    return 0
  }

  return completionList.suggestions.filter((item) => item.kind === kind).length
}

/**
 * Performance testing helper
 */
export async function measurePerformance<T>(
  operation: () => Promise<T>,
  description: string,
): Promise<{ result: T; duration: number }> {
  const start = performance.now()
  const result = await operation()
  const end = performance.now()
  const duration = end - start

  console.log(`${description}: ${duration.toFixed(2)}ms`)

  return { result, duration }
}

/**
 * Helper to create multiple test models for batch testing
 */
export function createTestBatch(
  queries: Array<{ query: string; position: monaco.Position }>,
): Array<{ model: monaco.editor.ITextModel; position: monaco.Position }> {
  return queries.map(({ position, query }) => ({
    model: createTestModel(query),
    position,
  }))
}

/**
 * Helper to dispose multiple test models
 */
export function disposeTestBatch(
  batch: Array<{ model: monaco.editor.ITextModel; position: monaco.Position }>,
): void {
  batch.forEach(({ model }) => model.dispose())
}
