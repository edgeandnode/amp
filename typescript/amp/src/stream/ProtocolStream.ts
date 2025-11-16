import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import type * as ArrowFlight from "../api/ArrowFlight.ts"
import * as Model from "../Model.ts"

/**
 * Protocol message: Data event with batch and ranges.
 */
export class ProtocolMessageData extends Schema.TaggedClass<ProtocolMessageData>()("ProtocolMessageData", {
  batch: Model.RecordBatchFromUint8Array,
  ranges: Model.BlockRanges,
}) {}

/**
 * Protocol message: Reorg event with previous, incoming, and invalidation ranges.
 */
export class ProtocolMessageReorg extends Schema.TaggedClass<ProtocolMessageReorg>()("ProtocolMessageReorg", {
  previous: Model.BlockRanges,
  incoming: Model.BlockRanges,
  invalidation: Model.InvalidationRanges,
}) {}

/**
 * Protocol message: Watermark event with ranges.
 */
export class ProtocolMessageWatermark
  extends Schema.TaggedClass<ProtocolMessageWatermark>()("ProtocolMessageWatermark", {
    ranges: Model.BlockRanges,
  })
{}

/**
 * Protocol messages emitted by the protocol stream.
 */
export type ProtocolMessage = ProtocolMessageData | ProtocolMessageReorg | ProtocolMessageWatermark

/**
 * Detect reorg by comparing incoming ranges against previous ranges.
 *
 * A reorg is detected when ranges differ (in any field - numbers, hash, or prevHash)
 * AND they overlap in block numbers. This indicates the chain state changed.
 *
 * # Protocol Invariant
 * If a reorg is detected, consecutiveness validation is skipped, as reorgs
 * intentionally break consecutiveness.
 */
const detectReorg = (
  previous: Model.BlockRanges,
  incoming: Model.BlockRanges,
): Model.InvalidationRanges => {
  const invalidation: Array<Model.InvalidationRange> = []
  for (const inc of incoming) {
    const prev = previous.find((p) => p.network === inc.network)
    if (prev === undefined) {
      continue
    }

    // Check if ranges differ in any field (numbers, hash, prevHash)
    const rangesDiffer = inc.numbers.start !== prev.numbers.start ||
      inc.numbers.end !== prev.numbers.end ||
      inc.hash !== prev.hash ||
      inc.prevHash !== prev.prevHash

    // Check if ranges overlap in block numbers
    const rangesOverlap = inc.numbers.start <= prev.numbers.end

    // Reorg detected: ranges differ AND overlap
    if (rangesDiffer && rangesOverlap) {
      invalidation.push(
        new Model.InvalidationRange({
          network: inc.network,
          numbers: {
            start: Model.BlockNumber.make(inc.numbers.start),
            end: Model.BlockNumber.make(Math.max(inc.numbers.end, prev.numbers.end)),
          },
        }),
      )
    }
  }

  return invalidation
}

export const createProtocolStream = (
  responses: Stream.Stream<Model.ResponseBatch, ArrowFlight.ArrowFlightError>,
  resume?: Model.BlockRanges,
): Stream.Stream<ProtocolMessage, ArrowFlight.ArrowFlightError> => {
  let previous: Model.BlockRanges | undefined = resume

  return responses.pipe(Stream.map((response) => {
    const ranges = response.metadata.ranges

    if (previous !== undefined) {
      const invalidation = detectReorg(previous, ranges)
      if (invalidation.length > 0) {
        const msg = new ProtocolMessageReorg({
          previous: [...previous],
          incoming: [...ranges],
          invalidation,
        })

        return msg
      }
    }

    // Emit Data or Watermark based on the `ranges completed` flag
    const msg = response.metadata.rangesComplete
      ? new ProtocolMessageWatermark({ ranges: [...ranges] })
      : new ProtocolMessageData({
        batch: response.data,
        ranges: [...ranges],
      })

    previous = ranges
    return msg
  }))
}
