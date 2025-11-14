"use client"

import { Accordion } from "@base-ui-components/react/accordion"
import type { StudioModel } from "@edgeandnode/amp"
import { Button, Divider } from "@graphprotocol/gds-react"
import { FileSolIcon, PlusIcon } from "@graphprotocol/gds-react/icons"

import { useQueryableEventsQuery } from "@/hooks/useQueryableEventsQuery"

export type SchemaBrowserProps = {
  onEventSelected: (event: StudioModel.QueryableEvent) => void
}
export function SchemaBrowser({ onEventSelected }: Readonly<SchemaBrowserProps>) {
  const { data: queryableEvents } = useQueryableEventsQuery()

  return (
    <div className="flex flex-col gap-4 px-2 py-6">
      <div className="flex flex-col gap-1 px-4">
        <p className="text-14">Contract Events</p>
        <p className="text-12 text-fg-muted">Events parsed from your contract ABIs.</p>
      </div>
      {queryableEvents.length > 0 ? (
        <Accordion.Root className="flex flex-col gap-0.5">
          {queryableEvents.map((event) => (
            <Accordion.Item key={event.signature} className="flex flex-col gap-2 data-open:bg-bg-muted rounded-8">
              <Accordion.Header className="flex gap-1 px-4 py-2 w-full hover:bg-bg-default rounded-6 justify-between items-center">
                <Accordion.Trigger type="button" className="flex flex-1 gap-1">
                  <FileSolIcon className="text-brand-400" aria-hidden="true" size={5} alt="" />
                  <div className="flex flex-col gap-1">
                    <span className="text-14">{event.name}</span>
                    <span className="text-12 text-fg-muted">{event.source}</span>
                  </div>
                </Accordion.Trigger>
                <Button variant="naked" size="large" onClick={() => onEventSelected(event)}>
                  <PlusIcon alt={`Add ${event.name}`} size={4} aria-hidden="true" />
                </Button>
              </Accordion.Header>
              <Accordion.Panel className="mb-4 flex flex-col px-4">
                {event.params.map((param) => (
                  <div
                    key={`${event.signature}__${param.name}`}
                    className="border-border-muted ml-2 flex items-center border-l py-1 pb-2"
                  >
                    <Divider className="mr-2 w-2" />
                    <div className="flex w-full items-center justify-between">
                      <span className="text-14">{param.name}</span>
                      <code className="text-14 text-bg-brand-elevated">{param.datatype}</code>
                    </div>
                  </div>
                ))}
              </Accordion.Panel>
            </Accordion.Item>
          ))}
        </Accordion.Root>
      ) : (
        <div className="flex flex-col items-center bg-bg-canvas border border-border-muted justify-center rounded-8 gap-4 p-6">
          <div className="p-2 rounded-8 bg-brand-1100 inline-flex items-center justify-center">
            <FileSolIcon size={5} alt="" aria-hidden="true" className="text-brand-200" />
          </div>
          <div className="flex flex-col items-center gap-1">
            <p className="text-14">No Sources Available</p>
            <p className="text-12 text-fg-muted text-center">
              Compile your Smart Contracts, the resulting ABIs will be parsed and any events displayed here.
            </p>
          </div>
        </div>
      )}
    </div>
  )
}
