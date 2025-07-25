"use client"

import { Accordion } from "@base-ui-components/react/accordion"
import { FolderIcon, FolderOpenIcon } from "@phosphor-icons/react"

import { useUDFSuspenseQuery } from "./useUDFQuery"

/** @todo define way to add UDF to playground input */
export type UDFProps = {}
export function UDF() {
  const { data: userDefinedFunctions } = useUDFSuspenseQuery()

  return (
    <div className="flex flex-col gap-y-4 p-6">
      <p className="text-sm">User Defined Functions (UDFs)</p>
      <Accordion.Root className="w-full box-border flex flex-col justify-center gap-y-3">
        {userDefinedFunctions.map((udf) => (
          <Accordion.Item key={udf.name} className="flex flex-col gap-y-2">
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
                  <span className="self-start">{udf.name}</span>
                  <span className="text-xs text-white/55 self-start text-left whitespace-break-spaces">
                    {udf.description}
                  </span>
                </div>
              </Accordion.Trigger>
            </Accordion.Header>
            <Accordion.Panel className="box-border overflow-y-auto overflow-x-hidden border-l border-white/25 ml-4 pl-4">
              {/** Use actual code component here. this is garbage */}
              <code
                className="font-mono text-xs p-4 rounded-md bg-black"
                key={udf.name}
              >
                {udf.sql}
              </code>
            </Accordion.Panel>
          </Accordion.Item>
        ))}
      </Accordion.Root>
    </div>
  )
}
