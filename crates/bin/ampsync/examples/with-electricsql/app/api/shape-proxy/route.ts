import { ELECTRIC_PROTOCOL_QUERY_PARAMS } from "@electric-sql/client";

import { env } from "@/env/server";

const electricSqlUrl = new URL(`${env.ELECTRIC_URL}v1/shape`);

export async function GET(request: Request) {
  const url = new URL(request.url);
  // Only pass through Electric protocol parameters, not table name
  url.searchParams.forEach((value, key) => {
    if (ELECTRIC_PROTOCOL_QUERY_PARAMS.includes(key)) {
      electricSqlUrl.searchParams.set(key, value);
    }
  });

  // Set the table server-side - not from client params
  // the table name: blocks comes from the nozzle.config.ts.
  // when ampsync starts, it will create the table: blocks in the database;
  // which electric-sql then reads from
  electricSqlUrl.searchParams.set(`table`, `blocks`);

  const electricSqlApiRequest = new Request(electricSqlUrl.toString(), {
    method: `GET`,
    headers: new Headers(),
  });
  // When proxying long-polling requests, content-encoding & content-length are added
  // erroneously (saying the body is gzipped when it's not) so we'll just remove
  // them to avoid content decoding errors in the browser.
  //
  // Similar-ish problem to https://github.com/wintercg/fetch/issues/23
  let resp = await fetch(electricSqlApiRequest);
  if (resp.headers.get(`content-encoding`)) {
    const headers = new Headers(resp.headers);
    headers.delete(`content-encoding`);
    headers.delete(`content-length`);
    resp = new Response(resp.body, {
      status: resp.status,
      statusText: resp.statusText,
      headers,
    });
  }
  return resp;
}
