import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    // projects: ["packages/*/vitest.config.ts"],
    // TODO: Re-enable all tests across all packages.
    projects: ["packages/nozzle/vitest.config.ts"],
  },
})
