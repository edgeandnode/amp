// Determine API origin based on environment
// In development (pnpm run dev): use full URL with localhost
// In production (built and served via nozzle CLI): use relative path
export const API_ORIGIN = import.meta.env.MODE === "production"
  ? "/api/v1"
  : (import.meta.env.VITE_API_URL || "http://localhost:1615/api/v1")

export const RESERVED_FIELDS = new Set(["from", "select", "limit", "order"])
