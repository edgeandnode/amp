"use client"

import { Accordion } from "@base-ui-components/react/accordion"
import { Tooltip } from "@base-ui-components/react/tooltip"
import { FolderIcon, FolderOpenIcon, PlusIcon } from "@graphprotocol/gds-react/icons"
import { String } from "effect"
import type { Table } from "nozzl/Model"

import { useNozzleConfigStreamQuery } from "@/hooks/useNozzleConfigStream"

import { ArrowIcon } from "../ArrowIcon.tsx"

export type NozzleConfigBrowserProps = {
  onTableSelected: (table: string, def: Table) => void
}
export function NozzleConfigBrowser({
  onTableSelected,
}: Readonly<NozzleConfigBrowserProps>) {
  const { data: config } = useNozzleConfigStreamQuery()

  if (config == null) {
    return null
  }

  return (
    <div className="flex flex-col gap-y-4 p-6">
      <div className="flex flex-col gap-y-1">
        <p className="text-14">Dataset Config</p>
        <p className="text-10 text-space-700">
          These tables are derived from your nozzle config
        </p>
      </div>
      <Accordion.Root className="w-full box-border flex flex-col justify-center gap-y-3">
        {Object.entries(config.tables).map(([table, def]) => (
          <Accordion.Item key={table} className="flex flex-col gap-y-2">
            <Accordion.Header className="m-0 flex items-start gap-x-1 px-0 py-2">
              <Accordion.Trigger
                type="button"
                className="group flex items-start relative w-full gap-x-1 px-0 py-2 cursor-pointer"
              >
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
                  <span className="self-start text-14">{table}</span>
                </div>
              </Accordion.Trigger>
              <Tooltip.Provider>
                <Tooltip.Root>
                  <Tooltip.Trigger
                    type="button"
                    className="rounded-full p-2 bg-space-1200 hover:bg-space-1500 cursor-pointer inline-flex items-center justify-center shadow"
                    onClick={() =>
                      onTableSelected(table, def)}
                  >
                    <PlusIcon
                      alt={`Add ${table}`}
                      size={4}
                      className="text-white"
                      aria-hidden="true"
                    />
                  </Tooltip.Trigger>
                  <Tooltip.Portal>
                    <Tooltip.Positioner sideOffset={10} side="left">
                      <Tooltip.Popup className="flex origin-[var(--transform-origin)] flex-col rounded-6 bg-[canvas] px-2 py-1 text-10 shadow shadow-space-1200 outline-1 outline-space-1500 transition-[transform,scale,opacity] data-[ending-style]:scale-90 data-[ending-style]:opacity-0 data-[instant]:duration-0 data-[starting-style]:scale-90 data-[starting-style]:opacity-0">
                        <Tooltip.Arrow className="data-[side=bottom]:top-[-8px] data-[side=left]:right-[-13px] data-[side=left]:rotate-90 data-[side=right]:left-[-13px] data-[side=right]:-rotate-90 data-[side=top]:bottom-[-8px] data-[side=top]:rotate-180">
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
                <code className="language-sql">
                  {String.trim(def.input.sql)}
                </code>
              </pre>
            </Accordion.Panel>
          </Accordion.Item>
        ))}
      </Accordion.Root>
    </div>
  )
}
