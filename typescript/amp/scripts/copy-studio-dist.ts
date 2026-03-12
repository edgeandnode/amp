import { cpSync, mkdirSync } from "node:fs"
import { resolve } from "node:path"

function copyStudioDist() {
  const src = resolve("../", "studio", "dist")
  const dest = resolve("./", "dist", "studio", "dist")

  mkdirSync(dest, { recursive: true })
  cpSync(src, dest, { recursive: true, force: true })

  console.info("[Build] Copied studio/dist to dist/studio/dist")
}

copyStudioDist()
