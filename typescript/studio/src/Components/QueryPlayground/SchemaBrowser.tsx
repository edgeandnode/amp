"use client"

import { Accordion } from "@base-ui-components/react/accordion"
import { FolderIcon, FolderOpenIcon } from "@phosphor-icons/react"

import { useQueryableEventsQuery } from "@/hooks/useQueryableEventsQuery"

export function SchemaBrowser() {
  const { data: queryableEvents } = useQueryableEventsQuery()

  return (
    <div className="flex flex-col gap-y-4 p-6">
      <p className="text-sm">Schema</p>
      <Accordion.Root className="w-full box-border flex flex-col justify-center gap-y-3">
        {queryableEvents.map((event) => (
          <Accordion.Item
            key={event.signature}
            className="flex flex-col gap-y-2"
          >
            <Accordion.Header className="m-0">
              <Accordion.Trigger className="group flex items-start relative w-full gap-x-1 px-0 py-2 text-sm cursor-pointer">
                <FolderIcon
                  className="size-4 text-inherit group-data-[panel-open]:hidden block"
                  aria-hidden="true"
                />
                <FolderOpenIcon
                  className="size-4 text-inherit group-data-[panel-open]:block hidden"
                  aria-hidden="true"
                />
                <div className="w-full flex flex-col gap-y-1 items-center justify-start">
                  <span className="self-start text-gray-950 dark:text-white">
                    {event.name}
                  </span>
                  <span className="text-xs text-gray-600 dark:text-white/55 self-start">
                    {event.source}
                  </span>
                </div>
              </Accordion.Trigger>
            </Accordion.Header>
            <Accordion.Panel className="box-border overflow-y-auto overflow-x-hidden border-l border-gray-300 dark:border-white/25 ml-4 pl-1">
              <div className="w-full flex flex-col gap-y-1">
                {event.params.map((param) => (
                  <button
                    key={`${event.signature}__${param.name}`}
                    type="button"
                    className="w-full flex items-center justify-between text-sm cursor-pointer border-none outline-none bg-gray-200 dark:hover:bg-slate-800 px-4 py-1.5 rounded-md"
                  >
                    <span>{param.name}</span>
                    <span className="ml-auto text-purple-500/70">
                      {param.datatype}
                    </span>
                  </button>
                ))}
              </div>
            </Accordion.Panel>
          </Accordion.Item>
        ))}
      </Accordion.Root>
    </div>
  )
}
