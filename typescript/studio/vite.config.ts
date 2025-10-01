import { resolve } from "path"

import tailwindcss from "@tailwindcss/vite"
import { tanstackRouter } from "@tanstack/router-plugin/vite"
import viteReact from "@vitejs/plugin-react"
import { defineConfig } from "vite"

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    tanstackRouter({
      target: "react",
      quoteStyle: "double",
      autoCodeSplitting: true,
      semicolons: false,
    }),
    viteReact(),
    tailwindcss(),
  ],
  resolve: {
    alias: {
      "@": resolve(__dirname, "./src"),
    },
  },
  optimizeDeps: {
    exclude: ["@effect/platform-node", "@effect/platform-node-shared"],
  },
  define: {
    global: "globalThis",
  },
})
