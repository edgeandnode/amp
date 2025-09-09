"use client"

import { Accordion } from "@base-ui-components/react/accordion"
import { FolderIcon, FolderOpenIcon } from "@graphprotocol/gds-react/icons"
import { String } from "effect"

import { USER_DEFINED_FUNCTIONS } from "@/constants"

/** @todo define way to add UDF to playground input */
export type UDFBrowserProps = {}
export function UDFBrowser() {
  const userDefinedFunctions = USER_DEFINED_FUNCTIONS

  return (
    <div className="flex flex-col gap-y-4 p-6">
      <div className="flex flex-col gap-y-1">
        <p className="text-14">User Defined Functions (UDFs)</p>
        <p className="text-10 text-space-700">
          Nozzle provided "built-in" SQL functions that can be called to manipulate the query data.
        </p>
      </div>
      <Accordion.Root className="w-full box-border flex flex-col justify-center gap-y-3">
        {userDefinedFunctions.map((udf) => (
          <Accordion.Item key={udf.name} className="flex flex-col gap-y-2">
            <Accordion.Header className="m-0">
              <Accordion.Trigger className="group flex items-start relative w-full gap-x-1 px-0 py-2 text-sm cursor-pointer">
                <FolderIcon
                  className="text-inherit group-data-[panel-open]:hidden block"
                  aria-hidden="true"
                  variant="regular"
                  size={5}
                  alt=""
                />
                <FolderOpenIcon
                  className="text-inherit group-data-[panel-open]:block hidden"
                  aria-hidden="true"
                  variant="regular"
                  size={5}
                  alt=""
                />
                <div className="w-full flex flex-col gap-y-1 items-center justify-start">
                  <span className="self-start text-14">{udf.name}</span>
                  <span className="text-12 text-space-700 self-start text-left whitespace-break-spaces">
                    {udf.description}
                  </span>
                </div>
              </Accordion.Trigger>
            </Accordion.Header>
            <Accordion.Panel className="box-border overflow-y-auto overflow-x-hidden ml-4">
              {/** Use actual code component here. this is garbage */}
              <pre className="bg-black text-white p-3 rounded-4 text-12 overflow-x-auto">
                <code className="language-sql">{String.trim(udf.sql)}</code>
              </pre>
            </Accordion.Panel>
          </Accordion.Item>
        ))}
      </Accordion.Root>
    </div>
  )
}
