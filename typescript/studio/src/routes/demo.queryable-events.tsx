import { createFileRoute } from "@tanstack/react-router"

import { useQueryableEvents } from "../hooks/useQueryableEvents"

export const Route = createFileRoute("/demo/queryable-events")({
  component() {
    const { data } = useQueryableEvents({
      refetchInterval: 10_000,
      initialData: [],
    })

    return (
      <div className="p-4 flex flex-col gap-y-4">
        <h1 className="text-2xl">Queryable Events | Demo</h1>
        <ul>
          {(data ?? []).map((event) => (
            <li key={event.name}>{event.signature}</li>
          ))}
        </ul>
      </div>
    )
  },
})
