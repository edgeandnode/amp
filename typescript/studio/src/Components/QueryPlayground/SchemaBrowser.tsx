"use client"

import { Accordion } from "@base-ui-components/react/accordion"
import { Tooltip } from "@base-ui-components/react/tooltip"
import type { StudioModel } from "@edgeandnode/amp"
import { FileSqlIcon, PlusIcon } from "@graphprotocol/gds-react/icons"

import { useQueryableEventsQuery } from "@/hooks/useQueryableEventsQuery"

import { ArrowIcon } from "../ArrowIcon.tsx"

export type SchemaBrowserProps = {
  onEventSelected: (event: StudioModel.QueryableEvent) => void
}
export function SchemaBrowser({ onEventSelected }: Readonly<SchemaBrowserProps>) {
  const { data: queryableEvents } = useQueryableEventsQuery()

  return (
    <div className="flex flex-col gap-y-4 p-6">
      <div className="flex flex-col gap-y-1">
        <p className="text-14">Contract Events</p>
        <p className="text-12 text-space-700">Events parsed from your contract ABIs.</p>
      </div>
      {queryableEvents.length > 0 ?
        (
          <Accordion.Root className="w-full box-border flex flex-col justify-center gap-y-3">
            {queryableEvents.map((event) => (
              <Accordion.Item key={event.signature} className="flex flex-col gap-y-2">
                <Accordion.Header className="m-0 flex items-start gap-x-1 px-0 py-2">
                  <Accordion.Trigger
                    type="button"
                    className="group flex items-start relative w-full gap-x-1 px-0 py-2 cursor-pointer"
                  >
                    <FileSqlIcon className="text-purple-400" aria-hidden="true" size={5} variant="regular" alt="" />
                    <div className="w-full flex flex-col gap-y-1 items-center justify-start">
                      <span className="self-start text-14">{event.name}</span>
                      <span className="text-12 text-space-700 self-start">{event.source}</span>
                    </div>
                  </Accordion.Trigger>
                  <Tooltip.Provider>
                    <Tooltip.Root>
                      <Tooltip.Trigger
                        type="button"
                        className="rounded-full p-2 hover:bg-space-1500 cursor-pointer inline-flex items-center justify-center shadow"
                        onClick={() => onEventSelected(event)}
                      >
                        <PlusIcon alt={`Add ${event.name}`} size={4} className="text-space-500" aria-hidden="true" />
                      </Tooltip.Trigger>
                      <Tooltip.Portal>
                        <Tooltip.Positioner sideOffset={10} side="left">
                          <Tooltip.Popup className="flex origin-(--transform-origin) flex-col rounded-6 bg-[canvas] px-2 py-1 text-10 shadow shadow-space-1200 outline-1 outline-space-1500 transition-[transform,scale,opacity] data-ending-style:scale-90 data-ending-style:opacity-0 data-instant:duration-0 data-starting-style:scale-90 data-starting-style:opacity-0">
                            <Tooltip.Arrow className="data-[side=bottom]:-top-2 data-[side=left]:right-[-13px] data-[side=left]:rotate-90 data-[side=right]:left-[-13px] data-[side=right]:-rotate-90 data-[side=top]:-bottom-2 data-[side=top]:rotate-180">
                              <ArrowIcon />
                            </Tooltip.Arrow>
                            Add to Query
                          </Tooltip.Popup>
                        </Tooltip.Positioner>
                      </Tooltip.Portal>
                    </Tooltip.Root>
                  </Tooltip.Provider>
                </Accordion.Header>
                <Accordion.Panel className="box-border overflow-y-auto overflow-x-hidden border-l border-white/20 ml-4 pl-1">
                  <div className="w-full flex flex-col gap-y-1">
                    {event.params.map((param) => (
                      <div
                        key={`${event.signature}__${param.name}`}
                        className="w-full flex items-center justify-between text-sm border-none outline-none px-4 py-1.5 rounded-4"
                      >
                        <span className="text-14">{param.name}</span>
                        <span className="ml-auto text-purple-200">{param.datatype}</span>
                      </div>
                    ))}
                  </div>
                </Accordion.Panel>
              </Accordion.Item>
            ))}
          </Accordion.Root>
        ) :
        (
          <div className="w-full flex flex-col items-center justify-center gap-y-4 p-6">
            <div className="p-2 rounded-8 bg-purple-1100 inline-flex items-center justify-center">
              <div className="size-full">
                <FileSqlIcon size={5} alt="" aria-hidden="true" className="text-purple-200" />
              </div>
            </div>
            <p className="text-14 text-white">No Sources Available</p>
            <p className="text-12 text-space-700 whitespace-break-spaces text-center">
              Compile your Smart Contracts, the resulting ABIs will be parsed and any events displayed here.
            </p>
          </div>
        )}
    </div>
  )
}
