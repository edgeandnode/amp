import { RouterProvider } from "@tanstack/react-router"
import { StrictMode } from "react"
import { createRoot } from "react-dom/client"

import "./styles.css"

import type { DatasetWorksAppRouter } from "./clients/Router.tsx"
import { createDatasetWorksAppRouter } from "./clients/Router.tsx"
import reportWebVitals from "./reportWebVitals.ts"

const router = createDatasetWorksAppRouter()

declare module "@tanstack/react-router" {
  interface Register {
    router: DatasetWorksAppRouter
  }
}

// Render the app
const rootElement = document.getElementById("app")
if (rootElement && !rootElement.innerHTML) {
  const root = createRoot(rootElement)
  root.render(
    <StrictMode>
      <RouterProvider router={router} />
    </StrictMode>,
  )
}

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals()
