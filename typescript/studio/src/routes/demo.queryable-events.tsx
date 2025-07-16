import { createFileRoute } from "@tanstack/react-router"

import { useQueryableEventsQuery } from "../hooks/useQueryableEventsQuery"

export const Route = createFileRoute("/demo/queryable-events")({
  component() {
    const { data } = useQueryableEventsQuery({
      retryDelay: 10_000,
    })

    return (
      <div className="p-4 flex flex-col gap-y-4">
        <h1 className="text-2xl">Queryable Events | Demo</h1>
        <ul>
          {data.map((event) => (
            <li key={event.name}>{event.signature}</li>
          ))}
        </ul>
      </div>
    )
  },
})
