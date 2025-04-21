import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    env: {
      NOZZLE_JSONL_URL: "http://localhost:1603",
      NOZZLE_ARROW_FLIGHT_URL: "http://localhost:1602"
    }
  }
})
