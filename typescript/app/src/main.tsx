import { ReactQueryDevtools } from "@tanstack/react-query-devtools"
import { StrictMode } from "react"
import { createRoot } from "react-dom/client"

import App from "./App.tsx"
import Providers from "./Providers.tsx"

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <Providers>
      <App />
      <ReactQueryDevtools />
    </Providers>
  </StrictMode>
)
