module model {
    // state space limits
    pure val BLOCK_NUMBERS = 0.to(16)
    pure val FORKS = 0.to(3)
}

module range {
    type BlockNum = int
    // [start, end] block numbers
    type Range = (BlockNum, BlockNum)

    pure def wellFormed(r: Range): bool = and {
        0 <= r.start(),
        r.start() <= r.end(),
    }

    pure def start(r: Range): BlockNum = {
        r._1
    }
    pure def end(r: Range): BlockNum = {
        r._2
    }
    pure def adjacent(a: Range, b: Range): bool = {
        a.end() + 1 == b.start()
    }
}

module segment {
    import range
    import range.BlockNum
    import range.Range

    type BlockHash = int

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

    pure def wellFormed(s: Segment): bool = and {
        range::wellFormed(s.range),
        s.hash != s.parentHash,
    }

    pure def start(s: Segment): BlockNum = {
        range::start(s.range)
    }
    pure def end(s: Segment): BlockNum = {
        range::end(s.range)
    }
    pure def new(r: Range, fork: int): Segment = {
        // create a unique hash per (number, fork), that is also easy to identify when inspecting
        // traces
        pure def h(number: BlockNum, fork: int): BlockHash = {
            if (number < 0) 0 else (1000 + (fork * 100) + number)
        }
        { range: r, hash: h(range::end(r), fork), parentHash: h(range::start(r) - 1, fork) }
    }
    pure def adjacent(a: Segment, b: Segment): bool = and {
        range::adjacent(a.range, b.range),
        a.hash == b.parentHash,
    }
}

module chain {
    import segment
    import segment.Segment
    import util.* from "util"

    // A chain of adjacent segments.
    type Chain = List[Segment]

    pure def wellFormed(c: Chain): bool = and {
        c.length() > 0,
        listValues(c).forall(segment::wellFormed),
        segmentsAdjacent(c),
    }
    pure def segmentsAdjacent(c: List[Segment]): bool = {
        c.indices().exclude(Set(0)).forall(i => segment::adjacent(c[i-1], c[i]))
    }

    pure def start(c: Chain): int = {
        listValues(c).map(segment::start).fold(segment::start(c[0]), min)
    }
    pure def end(c: Chain): int = {
        listValues(c).map(segment::end).fold(segment::end(c[0]), max)
    }
    pure def first(c: Chain): Segment = {
        c[0]
    }
    pure def last(c: Chain): Segment = {
        c[c.length() - 1]
    }
    pure def insert(c: Chain, s: Segment): Chain = {
        if (segment::adjacent(s, c.first())) concat(List(s), c)
        else if (segment::adjacent(c.last(), s)) append(c, s)
        else c
    }
}

module table {
    import range
    import range.BlockNum
    import range.Range
    import segment
    import segment.Segment
    import chain
    import chain.Chain
    import util.* from "util"

    // A table of blockchain data organized as segments. Segments may be added to the table in any
    // order. Some subset of segments form a canonical chain, which is the reader's view of the
    // table.
    type Table = Set[Chain]

    pure def wellFormed(t: Table): bool = and {
        t.forall(c => chain::wellFormed(c)),
        t.canonical().size() <= 1,
        t.canonical().size() == 0 implies t.nonCanonical().size() == 0,
        t.size() > 0 implies t.canonical().size() == 1,
    }

    // There is at most 1 canonical chain. Any table constructed from the same set of segments, in
    // arbitrary order, will have the same canonical chain.
    pure def canonical(t: Table): Set[Chain] = {
        pure val earliestStart = t.map(chain::start).fold(99, min)
        pure val chainsFromStart = t.filter(c => chain::start(c) == earliestStart)

        pure val latestEnd = chainsFromStart.map(chain::end).fold(0, max)
        pure val longestChains = chainsFromStart.filter(c => chain::end(c) == latestEnd)

        // Filter by minimum segments to favor compaction
        pure val minLength = longestChains.map(c => c.length()).fold(99, min)
        pure val compactChains = longestChains.filter(c => c.length() == minLength)

        // Use max hash sum as tiebreaker to ensure at most 1 result
        pure val maxHashSum = compactChains.map(c => listValues(c).map(s => s.hash).setSum()).fold(0, max)
        compactChains.filter(c => listValues(c).map(s => s.hash).setSum() == maxHashSum)
    }
    pure def nonCanonical(t: Table): Set[Chain] = {
        t.exclude(canonical(t))
    }
    pure def insert(t: Table, s: Segment): Table = {
        pure val updateChains = t.map(c => chain::insert(c, s))
        if (updateChains.exists(c => listValues(c).contains(s))) updateChains
        else t.union(Set(List(s)))
    }
    pure def missingRanges(t: Table, chainHead: BlockNum): Set[Range] = {
        Set() // TODO
    }
    // The set of non-canonical chains that extend past the canonical chain without a gap in
    // between.
    pure def forks(t: Table): Set[Chain] = {
        t.canonical().map(canonicalChain => {
            t.nonCanonical().filter(c => and {
                chain::end(c) > chain::end(canonicalChain), // extends past canonical chain
                chain::start(c) + 1 <= chain::start(canonicalChain), // no gap between ranges
            })
        })
        .flatten()
    }
}

module tableInsertModel {
    import model.*
    import segment
    import segment.Segment
    import table
    import table.Table
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
        val s = segment::new((start, end), fork)
        all {
            assert(segment::wellFormed(s)),
            segments' = segments.union(Set(s)),
            table' = table::insert(table, s),
        }
    }

    val inv = all {
        // segments.forall(s => segment::wellFormed(s)), // checked by assert in `step`, and by `table::wellFormed(table) and tableHasInsertedSegments`
        table::wellFormed(table),
        tableHasInsertedSegments,
    }
    val tableHasInsertedSegments = {
        segments == table.map(listValues).flatten()
    }
}

module tableReorgModel {
    import model.*
    import range.BlockNum
    import segment
    import segment.Segment
    import table
    import table.Table
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
        val end = chainHead + chainExtension
        val nextTable = table::missingRanges(table, end)
            .map(r => segment::new(r, fork))
            .fold(table, table::insert)
        all {
            segments' = segments,
            table' = nextTable,
        }
    }

    val inv = all {
        table::wellFormed(table),
    }

    val chainHead: BlockNum = {
        segments.map(segment::end).fold(0, max)
    }
}
