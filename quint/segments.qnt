module model {
    // state space limits
    pure val BLOCK_NUMBERS = 0.to(16)
    pure val FORKS = 0.to(3)
}

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

    pure def rangeStart(r: Range): BlockNum = r._1
    pure def rangeEnd(r: Range): BlockNum = r._2
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
        rangeIsValid(s.range),
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
        { range: r, hash: h(rangeEnd(r), fork), parentHash: h(rangeStart(r) - 1, fork) }
    }
    pure def segmentAdjacent(a: Segment, b: Segment): bool = and {
        rangeAdjacent(a.range, b.range),
        a.hash == b.parentHash,
    }

    // Chain types and functions
    // A chain of adjacent segments.
    type Chain = List[Segment]

    pure def chainIsValid(c: Chain): bool = and {
        c.length() > 0,
        listValues(c).forall(segmentIsValid),
        chainSegmentsAdjacent(c),
    }
    pure def chainSegmentsAdjacent(c: List[Segment]): bool = {
        c.indices().exclude(Set(0)).forall(i => segmentAdjacent(c[i-1], c[i]))
    }

    pure def chainStart(c: Chain): int = {
        listValues(c).map(segmentStart).fold(segmentStart(c[0]), (acc, start) => min(acc, start))
    }
    pure def chainEnd(c: Chain): int = {
        listValues(c).map(segmentEnd).fold(segmentEnd(c[0]), (acc, end) => max(acc, end))
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

    // Table types and functions
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
        pure val earliestStart = t.map(chainStart).fold(99, (acc, start) => min(acc, start))
        pure val chainsFromStart = t.filter(c => chainStart(c) == earliestStart)

        pure val latestEnd = chainsFromStart.map(chainEnd).fold(0, (acc, end) => max(acc, end))
        pure val longestChains = chainsFromStart.filter(c => chainEnd(c) == latestEnd)

        // Filter by minimum segments to favor compaction
        pure val minLength = longestChains.map(c => c.length()).fold(99, (acc, len) => min(acc, len))
        pure val compactChains = longestChains.filter(c => c.length() == minLength)

        // Use max hash sum as tiebreaker to ensure at most 1 result
        pure val maxHashSum = compactChains.map(c => listValues(c).map(s => s.hash).setSum()).fold(0, (acc, s) => max(acc, s))
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
    pure def missingRanges(t: Table, chainHead: BlockNum): Set[Range] = {
        if (t.canonical().size() == 0) Set((0, chainHead))
        else t.canonical().map(c => {
            pure val canonicalEnd = c.chainEnd()
            if (canonicalEnd < chainHead) Set((canonicalEnd + 1, chainHead))
            else Set()
        })
        .flatten()
    }
    // The set of non-canonical chains that extend past the canonical chain without a gap in
    // between.
    pure def tableForks(t: Table): Set[Chain] = {
        t.canonical().map(canonicalChain => {
            t.nonCanonical().filter(c => and {
                chainEnd(c) > chainEnd(canonicalChain), // extends past canonical chain
                chainStart(c) + 1 <= chainStart(canonicalChain), // no gap between ranges
            })
        })
        .flatten()
    }
}

module tableInsertModel {
    import model.*
    import segment.*
    import util.* from "util"

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
        val s = newSegment((start, end), fork)
        all {
            assert(segmentIsValid(s)),
            segments' = segments.union(Set(s)),
            table' = insert(table, s),
        }
    }

    val inv = all {
        // segments.forall(s => segmentIsValid(s)), // checked by assert in `step`, and by `tableIsValid(table) and tableHasInsertedSegments`
        tableIsValid(table),
        tableHasInsertedSegments,
    }
    val tableHasInsertedSegments = {
        segments == table.map(listValues).flatten()
    }
}

module tableReorgModel {
    import model.*
    import segment.*
    import util.* from "util"

    var segments: Set[Segment]
    var table: Table

    action init = all {
        segments' = Set(),
        table' = Set(),
    }
    action step = {
        nondet chainExtension = 0.to(5).oneOf()
        nondet fork = FORKS.oneOf()
        val end = chainHead(segments) + chainExtension
        val newSegments = missingRanges(table, end).map(r => newSegment(r, fork))
        val nextTable = newSegments.fold(table, (t, s) => insert(t, s))
        val nextSegments = segments.union(newSegments)
        all {
            // check chain head monotonically increases
            assert(chainHead(nextSegments) >= chainHead(segments)),
            // check canonical chain head monotonically increases
            assert(canonicalHead(nextTable) >= canonicalHead(table)),

            segments' = nextSegments,
            table' = nextTable,
        }
    }

    val inv = all {
        tableIsValid(table),
    }
    temporal reorgsResolve = noStuttering implies always(eventually(canonicalHead(table) == chainHead(segments)))
    temporal noStuttering = always(step.mustChange((segments, table)))

    pure def chainHead(s: Set[Segment]): BlockNum = {
        s.map(segmentEnd).fold(0, (acc, n) => max(acc, n))
    }
    pure def canonicalHead(t: Table): BlockNum = {
        t.canonical().map(chainEnd).fold(0, (acc, n) => max(acc, n))
    }
}
