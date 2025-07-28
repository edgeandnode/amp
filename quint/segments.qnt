module segment {
    import util.* from "util"

    type BlockNum = int
    type BlockHash = int

    // [start, end] block numbers
    type Range = (BlockNum, BlockNum)
    pure def rangeIsValid(r: Range): bool = and {
        0 <= r.rangeStart(),
        r.rangeStart() <= r.rangeEnd(),
    }
    pure def rangeStart(r: Range): BlockNum = {
        r._1
    }
    pure def rangeEnd(r: Range): BlockNum = {
        r._2
    }
    pure def rangeAdjacent(a: Range, b: Range): bool = {
        a.rangeEnd() + 1 == b.rangeStart()
    }

    // A file in object store containing data associated with a contiguous range of blocks.
    // It is assumed that the blocks associated with the data form a contiguous chain.
    type Segment = {
        // [start, end] block numbers
        range: Range,
        // hash of block `end`
        hash: BlockHash,
        // hash of block `start - 1`
        parentHash: BlockHash,
    }
    pure def segmentIsValid(s: Segment): bool = and {
        s.range.rangeIsValid(),
        s.hash != s.parentHash,
    }
    pure def segmentStart(s: Segment): BlockNum = rangeStart(s.range)
    pure def segmentEnd(s: Segment): BlockNum = rangeEnd(s.range)
    pure def newSegment(r: Range, fork: int): Segment = {
        // create a unique hash per (number, fork), that is also easy to identify when inspecting
        // traces
        pure def h(number: BlockNum, fork: int): BlockHash = {
            if (number < 0) 0 else (1000 + (fork * 100) + number)
        }
        { range: r, hash: h(r.rangeEnd(), fork), parentHash: h(r.rangeStart() - 1, fork) }
    }
    pure def segmentAdjacent(a: Segment, b: Segment): bool = and {
        rangeAdjacent(a.range, b.range),
        a.hash == b.parentHash,
    }

    // A chain of adjacent segments.
    type Chain = List[Segment]
    pure def chainIsValid(c: Chain): bool = and {
        c.length() > 0,
        c.listValues().forall(segmentIsValid),
        c.chainSegmentsAdjacent(),
    }
    pure def chainSegmentsAdjacent(c: List[Segment]): bool = {
        c.indices().exclude(Set(0)).forall(i => segmentAdjacent(c[i-1], c[i]))
    }
    pure def chainStart(c: Chain): int = {
        c.listValues().map(segmentStart).setMin().unwrapOr(segmentStart(c[0]))
    }
    pure def chainEnd(c: Chain): int = {
        c.listValues().map(segmentEnd).setMax().unwrapOr(segmentEnd(c[0]))
    }
    pure def chainFirst(c: Chain): Segment = {
        c[0]
    }
    pure def chainLast(c: Chain): Segment = {
        c[c.length() - 1]
    }
    pure def chainInsert(c: Chain, s: Segment): Chain = {
        if (segmentAdjacent(s, chainFirst(c))) concat(List(s), c)
        else if (segmentAdjacent(chainLast(c), s)) append(c, s)
        else c
    }

    // A table of blockchain data organized as segments. Segments may be added to the table in any
    // order. Some subset of segments form a canonical chain, which is the reader's view of the
    // table.
    type Table = Set[Chain]
    pure def tableIsValid(t: Table): bool = and {
        t.forall(c => chainIsValid(c)),
        t.canonical().size() <= 1,
        t.canonical().size() == 0 implies t.nonCanonical().size() == 0,
        t.size() > 0 implies t.canonical().size() == 1,
    }
    // There is at most 1 canonical chain. Any table constructed from the same set of segments, in
    // arbitrary order, will have the same canonical chain.
    pure def canonical(t: Table): Set[Chain] = {
        pure val earliestStart = t.map(chainStart).setMin().unwrapOr(0)
        pure val chainsFromStart = t.filter(c => chainStart(c) == earliestStart)

        pure val latestEnd = chainsFromStart.map(chainEnd).setMax().unwrapOr(0)
        pure val longestChains = chainsFromStart.filter(c => chainEnd(c) == latestEnd)

        // Filter by minimum segments to favor compaction
        pure val minLength = longestChains.map(c => c.length()).setMin().unwrapOr(0)
        pure val compactChains = longestChains.filter(c => c.length() == minLength)

        // Use max hash sum as tiebreaker to ensure at most 1 result
        pure val maxHashSum = compactChains
            .map(c => listValues(c).map(s => s.hash).setSum()).setMax().unwrapOr(0)
        compactChains.filter(c => listValues(c).map(s => s.hash).setSum() == maxHashSum)
    }
    pure def nonCanonical(t: Table): Set[Chain] = {
        t.exclude(canonical(t))
    }
    pure def insert(t: Table, s: Segment): Table = {
        pure val updateChains = t.map(c => chainInsert(c, s))
        if (updateChains.exists(c => listValues(c).contains(s))) updateChains
        else t.union(Set(List(s)))
    }
    pure def missingRanges(t: Table, r: Range): Set[Range] = {
        Set(r) // TODO
        // if (t.canonical().size() == 0) Set(range)
        // else t.canonical().map(c => {
        //     pure val canonicalEnd = c.chainEnd()
        //     if (canonicalEnd < range) Set((canonicalEnd + 1, range))
        //     else Set()
        // })
        // .flatten()
    }
    // The set of non-canonical chains that extend past the canonical chain without a gap in
    // between.
    pure def forks(t: Table): Set[Chain] = {
        t.canonical().map(canonicalChain => {
            t.nonCanonical().filter(c => and {
                chainEnd(c) > chainEnd(canonicalChain), // extends past canonical chain
                chainStart(c) + 1 <= chainStart(canonicalChain), // no gap between ranges
            })
        })
        .flatten()
    }
}

module model {
    import segment.*
    import util.* from "util"

    // state space limits
    pure val BLOCK_NUMBERS = 0.to(16)
    pure val FORKS = 0.to(3)

    var segments: Set[Segment]
    var table: Table

    action init = all {
        segments' = Set(),
        table' = Set(),
    }
    action step = {
        nondet start = BLOCK_NUMBERS.oneOf()
        nondet end = BLOCK_NUMBERS.filter(n => n >= start).oneOf()
        nondet fork = FORKS.oneOf()
        val ranges = table.missingRanges((start, end))
        val newSegments = ranges.map(r => newSegment(r, fork))
        val nextSegments = segments.union(newSegments)
        val nextTable = newSegments.fold(table, (t, s) => insert(t, s))
        all {
            assert(newSegments.forall(segmentIsValid)),
            // chain head monotonically increases
            assert(chainHead(segments) <= chainHead(nextSegments)),

            segments' = nextSegments,
            table' = nextTable,
        }
    }

    val safety = all {
        // checked by assert in `step`, and by `tableIsValid(table) and tableHasInsertedSegments`
        // segments.forall(segmentIsValid),
        table.tableIsValid(),
        tableHasInsertedSegments,
    }
    temporal liveness = always(or {
        canonicalStart(next(table)) < table.canonicalStart(),
        // table.next().canonicalEnd() == Some(segments.next().chainHead()),
    })
    val tableHasInsertedSegments = {
        segments == table.map(listValues).flatten()
    }
    pure def chainHead(s: Set[Segment]): BlockNum = {
        s.map(segmentEnd).setMax().unwrapOr(0)
    }
    pure def canonicalStart(t: Table): BlockNum = {
        t.canonical().map(chainStart).setMin().unwrapOr(0)
    }
    pure def canonicalEnd(t: Table): Option[BlockNum] = {
        t.canonical().map(chainEnd).setMax()
    }
}
