"use client";

import { useShape } from "@electric-sql/react";
import { Schema } from "effect";

export const AnvilBlock = Schema.Struct({
  block_num: Schema.String,
  timestamp: Schema.NonNegativeBigInt,
  hash: Schema.String,
});
export type AnvilBlock = typeof AnvilBlock.Type;
const AnvilBlockDecoder = Schema.decodeUnknownSync(AnvilBlock);

export function useAnvilBlockStream() {
  return useShape<AnvilBlock>({
    url: `/api/shape-proxy`,
    transformer(message) {
      return AnvilBlockDecoder(message);
    },
  });
}
