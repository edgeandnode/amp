import { defineConfig } from "vitest/config"
import { resolve } from "node:path"

export default defineConfig({
  test: {
    include: [
      "tests/**/*.{test,spec}.{ts,tsx}",
      "!tests/e2e/**", // Exclude E2E tests from unit test runs
    ],
    environment: "jsdom",
    setupFiles: ["./tests/setup/vitest.setup.ts"],
    globals: true,
    testTimeout: 10000,
  },
  resolve: {
    alias: {
      "@": resolve(__dirname, "./src"),
      // Mock Monaco Editor for testing
      "monaco-editor": resolve(
        __dirname,
        "./tests/mocks/monaco-editor.mock.ts",
      ),
    },
  },
})
