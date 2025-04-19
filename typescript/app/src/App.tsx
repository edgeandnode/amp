import { useERC20Transfers } from "./useERC20Transfers"

function App() {
  const { data: rows } = useERC20Transfers()

  return (
    <ul>
      {(rows ?? []).map((row, id) => <li key={id}>{JSON.stringify(row)}</li>)}
    </ul>
  )
}

export default App
