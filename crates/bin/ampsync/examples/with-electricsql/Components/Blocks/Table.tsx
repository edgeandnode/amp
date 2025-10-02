"use client";

import { XCircleIcon } from "@graphprotocol/gds-react/icons";
import { useAnvilBlockStream } from "@/hooks/useAnvilBlocksStream";

export function AnvilBlocksTable() {
  const { data: blocks, isError, error } = useAnvilBlockStream();

  if (isError && error !== false) {
    return (
      <div className="rounded-md bg-sonja-500 p-4 outline outline-sonja-600">
        <div className="flex">
          <div className="shrink-0">
            <XCircleIcon
              size={5}
              alt=""
              aria-hidden="true"
              className="text-white"
            />
          </div>
          <div className="ml-3">
            <h3 className="text-sm font-medium text-white">
              Failure fetching anvil blocks shape stream
            </h3>
            <div className="mt-2 text-sm text-white">
              <ul role="list" className="list-disc space-y-1 pl-5">
                <li>{error.message}</li>
                {error.cause != null ? (
                  <li>{JSON.stringify(error.cause)}</li>
                ) : null}
              </ul>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="flow-root">
      <div className="-my-2 overflow-x-auto">
        <div className="inline-block min-w-full py-2 align-middle">
          <table className="relative min-w-full divide-y divide-space-1200">
            <thead>
              <tr>
                <th
                  scope="col"
                  className="py-3.5 pr-3 pl-4 text-left text-sm font-semibold text-white sm:pl-0"
                >
                  Block #
                </th>
                <th
                  scope="col"
                  className="px-3 py-3.5 text-left text-sm font-semibold text-white"
                >
                  Timestamp
                </th>
                <th
                  scope="col"
                  className="px-3 py-3.5 text-left text-sm font-semibold text-white"
                >
                  Hash
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-white/10">
              {blocks.map((block) => (
                <tr key={block.hash}>
                  <td className="py-4 pr-3 pl-4 text-sm font-medium whitespace-nowrap sm:pl-0 text-white">
                    {block.block_num}
                  </td>
                  <td className="px-3 py-4 text-sm whitespace-nowrap text-space-500">
                    {block.timestamp}
                  </td>
                  <td className="px-3 py-4 text-sm whitespace-nowrap text-space-500">
                    {block.hash}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
