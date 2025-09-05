import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    include: ["tests/**/*.{test,spec}.{ts,tsx}"],
    environment: "jsdom",
    setupFiles: ["./tests/setup.ts"],
  },
  resolve: {
    alias: {
      'monaco-editor': new URL('./tests/mocks/monaco-editor.ts', import.meta.url).pathname
    }
  }
})
