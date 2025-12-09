# Compaction Algorithm

## Configuring the Compaction Algorithm

The compaction algorithm

- **_Target Partition Size_** *:
The limit to the size of data permitted in a single partition file. Any number of the sub parameters/limits may be set in addition to bytes. A candidate partition is considered to exceed this limit if any of the set limits are met or exceeded. This configuration defines when any kind of compaction is complete for a partition regardless of the scheme. The sub parameters are:
  - bytes *‡
  - blocks †
  - rows †

- **_Cooldown_** †:

  A duration to determine a cooldown period ($c$) for a segment. This period is considered expired for a segment if the interval between its `created_on` timestamp and now is greater than or equal to $c$.

- **_Eager Compaction Limit_** †:

  The upper generational limit for eager compaction. This is set in the `max_eager_generation` config value. Default of `0`, meaning only newly written files are eagerly compacted.

\* Required
† Optional
‡ Corresponds with and may be overridden by the `NOZZLE_PARTITION_SIZE_MB` environment variable or the optional `partition_size_mb` dump command argument.

## Compaction Schemes

With 1 required and 2 optional configurable top-level parameters, users can define 5 distinct compaction schemes:

1. **Strict Eager Compaction**:
  To simply compact as eagerly as possible, set a target partition size, disable `max_eager_generation` by setting it to `-1` and disable cooldown by setting a `cooldown_duration` of zero. All candidate segments and compaction groups are considered _Cold_ in this scheme.
  It is configured by setting:
    - _Target Partition Size_
    - `max_eager_generation = -1`
    - Cooldown to zero
2. **Cooldown Compaction**:
Compaction is controlled based on a set cooldown value. Candidate segments and compaction groups are conditionally considered _Hot_ or _Cold_ depending on the expiration of their cooldown period. When compacting, segments will be scanned in chain-time order. When a cold segment is found, a compaction group is initiated and following segments will be added to it up to the partition size.
It is configured by setting:
    - _Target Partition Size_
    - _Base Cooldown Duration_
3. **_Hybrid Compaction_**:
While cooldown is good to prevent frequently re-compacting the same data, if chain head files are immediately set on cooldown that might mean a lot of tiny files waiting to be compacted. To eagerly compact recent files while still respecting a cooldown for older ones, combine the cooldown with `max_eager_generation`. Candidate segments and compaction groups are conditionally considered _Live_, _Hot_, or _Cold_ depending on their generation and age at the time of consideration. Settings:
    - _Target Partition Size_
    - _Base Cooldown Duration_
    - `max_eager_generation`

### Why not Generationally-Tiered Compaction?

Generationally tiered compaction is typically employed by LSMs, and could be described as:

>Where membership is predicated on all candidate segment files to be contiguous and share the same Generation value.

The file writing and compaction facilities track the _Generation_ of each new segment. A segment's _Generation_ represents the current compaction depth of the file at the time of writing. For example a segment whose _Generation_ value is 0 is considered a raw or uncompacted file.

The base logic of the compaction algorithm will only compact candidate segment files if they are both contiguous and share the same _Generation_ value. This organizes compaction operations into a binary tree structure when the compaction ratio $k = 2$.

The binary tree structure of generationally tiered compaction provides excellent scaling but at the cost of leftover, not compacted segments. To illustrate how the number of files scales at chain head we can represent a segment and its generation like `[ g ]` for example we visualize the state of table files after 81,935 compaction cycles (where $k=2$ and the target size is $2^{15}$ blocks) like: `[ 15 ] [ 15 ] [ 14 ] [ 3 ] [ 2 ] [ 0 ]`. In this example the overall un-compacted file length is 4 with a generational height of 14. We can calculate the number of un-compacted files at any compaction cycle $n$ and target generation $g$ as $popcount\left(n\mod2^{g+1}\right)$ which gives us a worse case scenario every $2^g-1$ cycles where the remaining un-compacted file count is equal to $g$. This is not ideal if the goal is to minimize the number of small files.

The cooldown-based compaction algorithm avoids this worst-case behaviour by introducing a period for all segments after which the file is promoted as an "accumulator" that may compact all segments between it and the chain head. It also enables the compaction of segments that are ingested to bridge gaps between non-contiguous segment chains which is useful for restarting historical data dumps for example.

## Addtional Terms

### Segment Tiers

By combining the cooldown and eager compaction tier concepts, useful categories emerge to describe the state of a given segment:

- **_Live_**: a recently ingested file or collection thereof (i.e. smaller than the maximum eager generation).
- **_Hot_**: a segment that has been recently compacted and needs to stabilize before compacting again.
- **_Cold_**: a historical or previously compacted segment that may act as an accumulator.

Each category or tier corresponds to a different compaction strategy, hopefully providing the necessary flexibility to handle all kinds of use cases.
