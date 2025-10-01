import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    projects: ["typescript/*/vitest.config.ts"],
  },
})
