"use client"

import type { ColumnDef, ColumnFiltersState } from "@tanstack/react-table"
import {
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
} from "@tanstack/react-table"
import { useMemo, useState } from "react"

import { classNames } from "../../utils/classnames"

type DemoDatasetQueryResult = {
  block_num: bigint
  address: `0x${string}`
  timestamp: bigint
  count: bigint
}

export function DatasetQueryResultTable() {
  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([])
  const [globalFilter, setGlobalFilter] = useState("")

  const columns = useMemo<Array<ColumnDef<DemoDatasetQueryResult, any>>>(
    () => [
      {
        accessorKey: "block_num",
        accessorFn(row) {
          return row.block_num.toString()
        },
        cell(props) {
          return props.getValue()
        },
        header() {
          return <span>Block #</span>
        },
      },
      {
        accessorKey: "address",
        cell(props) {
          return props.getValue()
        },
        header() {
          return <span>Address</span>
        },
      },
      {
        accessorKey: "timestamp",
        accessorFn(row) {
          return row.timestamp.toString()
        },
        cell(props) {
          return props.getValue()
        },
        header() {
          return <span>Block Timestamp</span>
        },
      },
      {
        accessorKey: "count",
        accessorFn(row) {
          return row.count.toString()
        },
        cell(props) {
          return props.getValue()
        },
        header() {
          return <span>Event.Count</span>
        },
      },
    ],
    [],
  )
  const data: Array<DemoDatasetQueryResult> = [
    {
      block_num: 100000n,
      timestamp: 1753138227n,
      address: "0xc142bcf040AbF93703c03DaCf02c54B40dA0eDEb",
      count: 1n,
    },
  ]

  const table = useReactTable({
    data,
    columns,
    state: {
      columnFilters,
      globalFilter,
    },
    onColumnFiltersChange: setColumnFilters,
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(), // client side filtering
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    debugTable: true,
    debugHeaders: true,
    debugColumns: false,
  })

  return (
    <div className="h-[550px] w-full px-4 mt-6 flex flex-col gap-y-4">
      <div className="w-full flex flex-col gap-y-4">
        {/** @todo derive correct status after running query */}
        <span className="text-white/35 text-sm">Result - Success</span>
      </div>
      <div className="overflow-x-auto w-full">
        <div className="flow-root">
          <div className="-mx-4 -my-2 sm:-mx-6 lg:-mx-8">
            <div className="inline-block min-w-full py-2 align-middle">
              <table className="min-w-full border-separate border-spacing-0">
                <thead>
                  {table.getHeaderGroups().map((headerGroup) => (
                    <tr key={headerGroup.id}>
                      {headerGroup.headers.map((header) => (
                        <th
                          scope="col"
                          className="sticky top-0 z-10 border-b border-slate-800 py-3.5 pr-3 pl-4 text-left text-xs font-light text-white/65 backdrop-blur-sm backdrop-filter sm:pl-6 lg:pl-8 uppercase"
                        >
                          {flexRender(
                            header.column.columnDef.header,
                            header.getContext(),
                          )}
                        </th>
                      ))}
                    </tr>
                  ))}
                </thead>
                <tbody>
                  {table.getRowModel().rows.map((row) => (
                    <tr key={row.id} className="divide-x divide-white/10">
                      {row.getVisibleCells().map((cell, cellIdx) => (
                        <td
                          className={classNames(
                            cellIdx === 0
                              ? "text-white font-semibold"
                              : "text-gray-200 font-light",
                            "py-4 pr-3 pl-4 text-xs whitespace-nowrap sm:pl-6 lg:pl-8 border-b border-white/10",
                          )}
                        >
                          {flexRender(
                            cell.column.columnDef.cell,
                            cell.getContext(),
                          )}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
