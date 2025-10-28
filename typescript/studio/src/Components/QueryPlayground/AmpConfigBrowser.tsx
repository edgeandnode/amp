"use client"

import { Accordion } from "@base-ui-components/react/accordion"
import { Tooltip } from "@base-ui-components/react/tooltip"
import type { DatasetManifest } from "@edgeandnode/amp/Model"
import { PlusIcon, TableIcon } from "@graphprotocol/gds-react/icons"
import { String } from "effect"

import { useAmpConfigStreamQuery } from "@/hooks/useAmpConfigStream"

import { ArrowIcon } from "../ArrowIcon.tsx"

export type AmpConfigBrowserProps = {
  onTableSelected: (dataset: DatasetManifest["name"], table: string) => void
}
export function AmpConfigBrowser({ onTableSelected }: Readonly<AmpConfigBrowserProps>) {
  const { data: config } = useAmpConfigStreamQuery()

  if (config == null) {
    return null
  }

  const tables = Object.entries(config.tables)

  return (
    <div className="flex flex-col gap-y-4 p-6">
      <div className="flex flex-col gap-y-1">
        <p className="text-14">Dataset Tables</p>
        <p className="text-12 text-space-700">Tables derived from your current config.</p>
      </div>
      {tables.length > 0 ?
        (
          <Accordion.Root className="w-full box-border flex flex-col justify-center gap-y-3">
            {tables.map(([table, def]) => (
              <Accordion.Item key={table} className="flex flex-col gap-y-2">
                <Accordion.Header className="m-0 flex items-start gap-x-1 px-0 py-2">
                  <Accordion.Trigger
                    type="button"
                    className="group flex items-start relative w-full gap-x-1 px-0 py-2 cursor-pointer"
                  >
                    <TableIcon className="text-solar-500" aria-hidden="true" size={5} variant="regular" alt="" />
                    <div className="w-full flex flex-col gap-y-1 items-center justify-start">
                      <span className="self-start text-14">{table}</span>
                    </div>
                  </Accordion.Trigger>
                  <Tooltip.Provider>
                    <Tooltip.Root>
                      <Tooltip.Trigger
                        type="button"
                        className="rounded-full p-2 bg-transparent hover:bg-space-1500 cursor-pointer inline-flex items-center justify-center shadow"
                        onClick={() => onTableSelected(config.name, table)}
                      >
                        <PlusIcon alt={`Add ${table}`} size={4} className="text-space-500" aria-hidden="true" />
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
                <Accordion.Panel className="box-border overflow-y-auto overflow-x-hidden ml-4">
                  <pre className="bg-black text-white p-3 rounded-4 text-12 overflow-x-auto">
                  <code className="language-sql">{"input" in def ? String.trim((def as any).input.sql) : "No SQL (raw dataset)"}</code>
                  </pre>
                </Accordion.Panel>
              </Accordion.Item>
            ))}
          </Accordion.Root>
        ) :
        (
          <div className="w-full flex flex-col items-center justify-center gap-y-4 p-6">
            <div className="p-2 rounded-8 bg-solar-1000 inline-flex items-center justify-center">
              <div className="size-full">
                <TableIcon size={5} alt="" aria-hidden="true" className="text-solar-200" />
              </div>
            </div>
            <p className="text-14 text-white">No Tables Available</p>
            <p className="text-12 text-space-700 whitespace-break-spaces text-center">
              Add tables to your amp dataset config and they will show up here.
            </p>
          </div>
        )}
    </div>
  )
}
