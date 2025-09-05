/**
 * Test setup file for vitest
 * Sets up global mocks and test utilities
 */

import { beforeEach, afterEach } from 'vitest'

// Global test setup
beforeEach(() => {
  // Clear any global state before each test
})

afterEach(() => {
  // Cleanup after each test
})

// Make monaco available globally for tests that expect it
import * as monaco from 'monaco-editor'
globalThis.monaco = monaco