/**
 * EditorPage Helper Class for SQL Intellisense E2E Tests
 *
 * Provides robust, reusable methods for interacting with Monaco Editor
 * in Playwright tests. Handles common patterns like typing, completions,
 * and assertions while abstracting away timing and selector complexity.
 */

import type { Page, Locator } from "@playwright/test"
import { expect } from "@playwright/test"

export interface CompletionAssertion {
  /** Expected text content of the completion item */
  text: string
  /** Expected position in the completion list (0-based) */
  position?: number
  /** Whether this item should be marked as the primary/selected item */
  isPrimary?: boolean
}

export interface EditorOptions {
  /** Timeout for editor operations in ms */
  timeout?: number
  /** Whether to clear editor before operations */
  clearFirst?: boolean
}

export class EditorPage {
  private page: Page
  private editorSelector = ".monaco-editor"
  private editorContentSelector = ".monaco-editor .view-lines"
  private suggestionWidgetSelector = ".suggest-widget"
  private suggestionItemSelector = ".monaco-list .monaco-list-row"
  private hoverWidgetSelector = ".monaco-hover"

  constructor(page: Page) {
    this.page = page
  }

  /**
   * Wait for Monaco Editor to be ready and initialized
   */
  async waitForEditorReady(timeout = 30000): Promise<void> {
    await this.page.waitForLoadState("networkidle")
    await this.page.waitForSelector(this.editorSelector, { timeout })

    // Ensure editor is interactive by clicking and checking focus
    const editor = this.page.locator(this.editorSelector)
    await editor.click()

    // Wait a moment for editor to fully initialize
    await this.page.waitForTimeout(200)
  }

  /**
   * Get the editor locator
   */
  getEditor(): Locator {
    return this.page.locator(this.editorSelector)
  }

  /**
   * Clear all content from the editor
   */
  async clearEditor(): Promise<void> {
    const editor = this.getEditor()
    await editor.click()
    await this.page.keyboard.press("Control+a")
    await this.page.keyboard.press("Delete")
    await this.page.waitForTimeout(100) // Brief wait for content to clear
  }

  /**
   * Get current editor text content
   */
  async getEditorContent(): Promise<string> {
    const content = await this.page
      .locator(this.editorContentSelector)
      .textContent()
    return content?.trim() || ""
  }

  /**
   * Type text into the editor with proper cleanup and positioning
   */
  async typeInEditor(text: string, options: EditorOptions = {}): Promise<void> {
    const { clearFirst = false } = options

    if (clearFirst) {
      await this.clearEditor()
    }

    const editor = this.getEditor()
    await editor.click()

    // Type character by character to better simulate real user behavior
    await this.page.keyboard.type(text, { delay: 10 })
  }

  /**
   * Trigger code completion at current cursor position
   */
  async triggerCompletion(): Promise<void> {
    await this.page.keyboard.press("Control+Space")

    // Wait for completion widget to appear
    await this.page.waitForSelector(this.suggestionWidgetSelector, {
      timeout: 5000,
      state: "visible",
    })
  }

  /**
   * Wait for completion suggestions to appear
   */
  async waitForCompletions(timeout = 5000): Promise<void> {
    await this.page.waitForSelector(this.suggestionWidgetSelector, {
      timeout,
      state: "visible",
    })

    // Ensure at least one suggestion is visible
    await this.page.waitForSelector(this.suggestionItemSelector, {
      timeout: 2000,
      state: "visible",
    })
  }

  /**
   * Get all completion suggestion items
   */
  async getCompletionItems(): Promise<Array<string>> {
    const items = this.page.locator(this.suggestionItemSelector)
    const count = await items.count()
    const suggestions: Array<string> = []

    for (let i = 0; i < count; i++) {
      const text = await items.nth(i).textContent()
      if (text) {
        suggestions.push(text.trim())
      }
    }

    return suggestions
  }

  /**
   * Assert that specific completions are present and in expected order
   */
  async assertCompletions(expected: Array<CompletionAssertion>): Promise<void> {
    await this.waitForCompletions()

    const actualItems = await this.getCompletionItems()

    // Check that we have completions
    expect(actualItems.length).toBeGreaterThan(0)

    for (const assertion of expected) {
      const { text, position, isPrimary } = assertion

      // Check that the expected text is present
      expect(actualItems).toContain(text)

      // If position specified, check exact position
      if (position !== undefined) {
        expect(actualItems[position]).toBe(text)
      }

      // If marked as primary, should be first item
      if (isPrimary) {
        expect(actualItems[0]).toBe(text)
      }
    }
  }

  /**
   * Assert that completion widget is visible with at least one suggestion
   */
  async assertCompletionsVisible(): Promise<void> {
    await expect(this.page.locator(this.suggestionWidgetSelector)).toBeVisible()
    await expect(
      this.page.locator(this.suggestionItemSelector).first(),
    ).toBeVisible()
  }

  /**
   * Assert that completion widget is not visible
   */
  async assertCompletionsHidden(): Promise<void> {
    await expect(
      this.page.locator(this.suggestionWidgetSelector),
    ).not.toBeVisible()
  }

  /**
   * Accept the first completion suggestion
   */
  async acceptFirstCompletion(): Promise<void> {
    await this.waitForCompletions()
    await this.page.keyboard.press("Enter")

    // Wait briefly for text to be inserted
    await this.page.waitForTimeout(200)
  }

  /**
   * Accept completion at specific index
   */
  async acceptCompletionAtIndex(index: number): Promise<void> {
    await this.waitForCompletions()

    // Navigate to the desired item
    for (let i = 0; i < index; i++) {
      await this.page.keyboard.press("ArrowDown")
    }

    await this.page.keyboard.press("Enter")
    await this.page.waitForTimeout(200)
  }

  /**
   * Navigate completion list with arrow keys
   */
  async navigateCompletions(
    direction: "up" | "down",
    steps = 1,
  ): Promise<void> {
    await this.waitForCompletions()

    const key = direction === "up" ? "ArrowUp" : "ArrowDown"
    for (let i = 0; i < steps; i++) {
      await this.page.keyboard.press(key)
    }
  }

  /**
   * Dismiss completion suggestions
   */
  async dismissCompletions(): Promise<void> {
    await this.page.keyboard.press("Escape")
    await this.assertCompletionsHidden()
  }

  /**
   * Filter completions by typing additional text
   */
  async filterCompletions(filterText: string): Promise<void> {
    await this.waitForCompletions()
    await this.page.keyboard.type(filterText)

    // Wait for filtering to take effect
    await this.page.waitForTimeout(300)
  }

  /**
   * Hover over text at specific position to trigger hover documentation
   */
  async hoverAtPosition(x: number, y: number): Promise<void> {
    const editorContent = this.page.locator(this.editorContentSelector)
    await editorContent.hover({ position: { x, y } })
  }

  /**
   * Hover over specific text content in the editor
   */
  async hoverOverText(searchText: string): Promise<void> {
    // Find the text in the editor and hover over it
    const editorContent = this.page.locator(this.editorContentSelector)

    // Get bounding box and hover in the middle
    const textElement = this.page.locator(`text=${searchText}`).first()
    if (await textElement.isVisible()) {
      await textElement.hover()
    } else {
      // Fallback: hover at estimated position
      await this.hoverAtPosition(100, 20)
    }
  }

  /**
   * Assert that hover documentation is visible
   */
  async assertHoverVisible(): Promise<void> {
    const hoverWidget = this.page
      .locator(this.hoverWidgetSelector)
      .filter({ hasNotClass: "hidden" })
    await expect(hoverWidget.first()).toBeVisible({ timeout: 3000 })
  }

  /**
   * Assert that hover documentation contains specific text
   */
  async assertHoverContains(expectedText: string): Promise<void> {
    await this.assertHoverVisible()
    const hoverWidget = this.page
      .locator(this.hoverWidgetSelector)
      .filter({ hasNotClass: "hidden" })
    await expect(hoverWidget.first()).toContainText(expectedText)
  }

  /**
   * Complete a full SQL typing and completion workflow
   */
  async completeSQL(
    sqlParts: Array<string>,
    completionAtParts: Array<number> = [],
  ): Promise<void> {
    await this.clearEditor()

    for (let i = 0; i < sqlParts.length; i++) {
      const part = sqlParts[i]
      await this.typeInEditor(part)

      // If this part should trigger completion
      if (completionAtParts.includes(i)) {
        await this.triggerCompletion()
        await this.acceptFirstCompletion()
      }
    }
  }

  /**
   * Assert that editor contains specific text
   */
  async assertEditorContains(expectedText: string): Promise<void> {
    const content = await this.getEditorContent()
    expect(content).toContain(expectedText)
  }

  /**
   * Assert that editor matches exact text
   */
  async assertEditorEquals(expectedText: string): Promise<void> {
    const content = await this.getEditorContent()
    expect(content.replace(/\s+/g, " ").trim()).toBe(
      expectedText.replace(/\s+/g, " ").trim(),
    )
  }

  /**
   * Move cursor to end of editor content
   */
  async moveCursorToEnd(): Promise<void> {
    await this.page.keyboard.press("Control+End")
  }

  /**
   * Move cursor to beginning of editor content
   */
  async moveCursorToBeginning(): Promise<void> {
    await this.page.keyboard.press("Control+Home")
  }

  /**
   * Insert text at cursor position without triggering completions
   */
  async insertText(text: string): Promise<void> {
    await this.page.keyboard.type(text)
    await this.page.waitForTimeout(100)
  }

  /**
   * Create a new line in the editor
   */
  async newLine(): Promise<void> {
    await this.page.keyboard.press("Enter")
  }

  /**
   * Take a screenshot for debugging/documentation
   */
  async takeScreenshot(filename: string): Promise<void> {
    await this.page.screenshot({
      path: `test-results/${filename}`,
      fullPage: false,
    })
  }

  /**
   * Measure completion performance
   */
  async measureCompletionTime(): Promise<number> {
    const startTime = Date.now()
    await this.triggerCompletion()
    await this.waitForCompletions()
    return Date.now() - startTime
  }

  /**
   * Test complex query building workflow
   */
  async buildComplexQuery(scenario: {
    select: Array<string>
    from: string
    joins?: Array<{ type: string; table: string; on: string }>
    where?: Array<string>
    groupBy?: Array<string>
    orderBy?: Array<string>
    limit?: number
  }): Promise<void> {
    await this.clearEditor()

    // Build SELECT clause
    await this.typeInEditor("SELECT ")
    await this.triggerCompletion()

    for (let i = 0; i < scenario.select.length; i++) {
      if (i > 0) await this.typeInEditor(", ")
      await this.typeInEditor(scenario.select[i])
    }

    // Build FROM clause
    await this.typeInEditor("\nFROM ")
    await this.triggerCompletion()
    await this.typeInEditor(scenario.from)

    // Build JOIN clauses
    if (scenario.joins) {
      for (const join of scenario.joins) {
        await this.typeInEditor(`\n${join.type} JOIN `)
        await this.triggerCompletion()
        await this.typeInEditor(`${join.table} ON ${join.on}`)
      }
    }

    // Build WHERE clause
    if (scenario.where) {
      await this.typeInEditor("\nWHERE ")
      for (let i = 0; i < scenario.where.length; i++) {
        if (i > 0) await this.typeInEditor(" AND ")
        await this.typeInEditor(scenario.where[i])
      }
    }

    // Build GROUP BY
    if (scenario.groupBy) {
      await this.typeInEditor("\nGROUP BY ")
      for (let i = 0; i < scenario.groupBy.length; i++) {
        if (i > 0) await this.typeInEditor(", ")
        await this.typeInEditor(scenario.groupBy[i])
      }
    }

    // Build ORDER BY
    if (scenario.orderBy) {
      await this.typeInEditor("\nORDER BY ")
      for (let i = 0; i < scenario.orderBy.length; i++) {
        if (i > 0) await this.typeInEditor(", ")
        await this.typeInEditor(scenario.orderBy[i])
      }
    }

    // Add LIMIT
    if (scenario.limit) {
      await this.typeInEditor(`\nLIMIT ${scenario.limit}`)
    }
  }

  /**
   * Helper methods for metadata browser interaction testing
   */

  /**
   * Check if metadata browser is present and visible on the page
   */
  async isMetadataBrowserVisible(): Promise<boolean> {
    try {
      const metadataBrowser = this.page
        .locator('[data-testid="dataset-item"]')
        .first()
      await expect(metadataBrowser).toBeVisible({ timeout: 5000 })
      return true
    } catch {
      return false
    }
  }

  /**
   * Get all dataset items from the metadata browser
   */
  async getDatasetItems(): Promise<Array<Locator>> {
    const items = this.page.locator('[data-testid="dataset-item"]')
    const count = await items.count()
    const result = []
    for (let i = 0; i < count; i++) {
      result.push(items.nth(i))
    }
    return result
  }

  /**
   * Expand a dataset item to show its columns (if collapsed)
   */
  async expandDataset(datasetIndex = 0): Promise<void> {
    const datasets = await this.getDatasetItems()
    if (datasets.length > datasetIndex) {
      // The Collapsible.Trigger is the clickable header element
      const trigger = datasets[datasetIndex]
        .locator("button, [data-trigger], .cursor-pointer")
        .first()
      await trigger.click()

      // Wait for expansion
      await this.page.waitForTimeout(500)
    } else {
      throw new Error(`Dataset at index ${datasetIndex} not found`)
    }
  }

  /**
   * Get all visible column buttons from all expanded datasets
   */
  async getColumnButtons(): Promise<Array<Locator>> {
    const buttons = this.page.locator('[data-testid="column-button"]:visible')
    const count = await buttons.count()
    const result = []
    for (let i = 0; i < count; i++) {
      result.push(buttons.nth(i))
    }
    return result
  }

  /**
   * Get column button text (column name)
   */
  async getColumnButtonText(button: Locator): Promise<string | null> {
    try {
      const columnNameSpan = button.locator("span").first()
      return await columnNameSpan.textContent()
    } catch {
      return null
    }
  }

  /**
   * Click a specific column button and verify it inserts text into the editor
   */
  async clickColumnButtonAndVerify(buttonIndex: number): Promise<{
    columnText: string | null
    editorContentBefore: string
    editorContentAfter: string
    wasInserted: boolean
  }> {
    const buttons = await this.getColumnButtons()

    if (buttons.length <= buttonIndex) {
      throw new Error(
        `Column button at index ${buttonIndex} not found. Available: ${buttons.length}`,
      )
    }

    const button = buttons[buttonIndex]
    const columnText = await this.getColumnButtonText(button)
    const editorContentBefore = await this.getEditorContent()

    // Click the column button
    await button.click()

    // Wait a moment for insertion to complete
    await this.page.waitForTimeout(300)

    const editorContentAfter = await this.getEditorContent()
    const wasInserted = editorContentAfter !== editorContentBefore

    return {
      columnText,
      editorContentBefore,
      editorContentAfter,
      wasInserted,
    }
  }

  /**
   * Test column insertion by clicking multiple column buttons
   */
  async testMultipleColumnInsertions(maxColumns = 3): Promise<
    Array<{
      index: number
      columnText: string | null
      wasInserted: boolean
      insertedText?: string
    }>
  > {
    const results = []
    const buttons = await this.getColumnButtons()
    const columnsToTest = Math.min(maxColumns, buttons.length)

    for (let i = 0; i < columnsToTest; i++) {
      const result = await this.clickColumnButtonAndVerify(i)

      let insertedText: string | undefined
      if (result.wasInserted) {
        // Try to determine what was inserted
        if (result.editorContentAfter.includes(result.columnText || "")) {
          insertedText = result.columnText || undefined
        } else {
          // Look for qualified names (table.column)
          const diff = result.editorContentAfter
            .replace(result.editorContentBefore, "")
            .trim()
          insertedText = diff || undefined
        }
      }

      results.push({
        index: i,
        columnText: result.columnText,
        wasInserted: result.wasInserted,
        insertedText,
      })

      // Add space for next insertion
      if (i < columnsToTest - 1 && result.wasInserted) {
        await this.typeInEditor(", ")
      }
    }

    return results
  }
}
