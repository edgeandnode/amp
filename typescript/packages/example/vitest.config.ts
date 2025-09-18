import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    testTimeout: 30000, // 30 seconds for circuit compilation/proof generation
  },
})
