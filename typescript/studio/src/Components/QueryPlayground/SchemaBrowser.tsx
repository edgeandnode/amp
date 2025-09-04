"use client"

import { Accordion } from "@base-ui-components/react/accordion"
import { Button } from "@graphprotocol/gds-react"
import { FolderIcon, FolderOpenIcon } from "@graphprotocol/gds-react/icons"

import { useQueryableEventsQuery } from "@/hooks/useQueryableEventsQuery"

export function SchemaBrowser() {
  const { data: queryableEvents } = useQueryableEventsQuery()

  return (
    <div className="flex flex-col gap-y-4 p-6">
      <p className="text-14">Schema</p>
      <Accordion.Root className="w-full box-border flex flex-col justify-center gap-y-3">
        {queryableEvents.map((event) => (
          <Accordion.Item
            key={event.signature}
            className="flex flex-col gap-y-2"
          >
            <Accordion.Header className="m-0">
              <Accordion.Trigger className="group flex items-start relative w-full gap-x-1 px-0 py-2 cursor-pointer">
                <FolderIcon
                  className="text-inherit group-data-[panel-open]:hidden block"
                  aria-hidden="true"
                  size={5}
                  variant="regular"
                  alt=""
                />
                <FolderOpenIcon
                  className="text-inherit group-data-[panel-open]:block hidden"
                  aria-hidden="true"
                  size={5}
                  variant="regular"
                  alt=""
                />
                <div className="w-full flex flex-col gap-y-1 items-center justify-start">
                  <span className="self-start text-14">{event.name}</span>
                  <span className="text-12 text-space-700 self-start">
                    {event.source}
                  </span>
                </div>
              </Accordion.Trigger>
            </Accordion.Header>
            <Accordion.Panel className="box-border overflow-y-auto overflow-x-hidden border-l border-white/20 ml-4 pl-1">
              <div className="w-full flex flex-col gap-y-1">
                {event.params.map((param) => (
                  <Button
                    key={`${event.signature}__${param.name}`}
                    type="button"
                    className="w-full flex items-center justify-between text-sm cursor-pointer border-none outline-none hover:bg-space-1200 px-4 py-1.5 rounded-4"
                  >
                    <span className="text-14">{param.name}</span>
                    <span className="ml-auto text-purple-200">
                      {param.datatype}
                    </span>
                  </Button>
                ))}
              </div>
            </Accordion.Panel>
          </Accordion.Item>
        ))}
      </Accordion.Root>
    </div>
  )
}
