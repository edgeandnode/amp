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

  The upper limit for eager compaction. To activate, at least one of the sub parameters/limits must be set. A candidate partition is considered to exceed this limit if any of the set limits are met or exceeded. This configuration only defines when eager compaction is complete for a partition. The sub parameters are:
  - bytes †
  - blocks †
  - rows †
  - file count †
  - generation †

\* Required
† Optional
‡ Corresponds with and may be overridden by the `NOZZLE_PARTITION_SIZE_MB` environment variable or the optional `partition_size_mb` dump command argument.

## Compaction Schemes

With 1 required and 2 optional configurable top-level parameters, users can define 5 distinct compaction schemes:

1. **_[Strict Generationally-Tiered Compaction](#generationally-tiered-compaction)_**:
  The default compaction configuration when the compactor is enabled. All candidate segments and compaction groups are considered _Hot_ in this scheme.
  It is configured by setting:
    - _Target Partition Size_
2. **_[Relaxed Generationally-Tiered Compaction (Cooldown)](#scaled-cooldown-tiered-compaction)_**:
Introduces _Scaled, Cooldown-Tiered Compaction_ as described above. Candidate segments and compaction groups are conditionally considered _Hot_ or _Cold_ depending on the expiration of their cooldown period.
It is configured by setting:
    - _Target Partition Size_
    - _Base Cooldown Duration_
3. **_[Relaxed Generationally-Tiered Compaction (Eager)](#tiered-eager-compaction)_**:
Introduces _Eager Compaction_ for compaction groups smaller than the eager compaction limit. Candidate segments and compaction groups are conditionally considered _Hot_ or _Live_ depending on their size at the time of consideration. Effectively raises the average size of the smallest file at the end of any given compaction cycle.
It is configured by setting:
    - _Target Partition Size_
    - _Eager Compaction Limit_
4. **_Hybrid Compaction_**:
A combination of both _Relaxed Generationally-Tiered Compaction_ schemes. Candidate segments and compaction groups are conditionally considered _Live_, _Hot_, or _Cold_ depending on their size and age at the time of consideration. Setting
    - _Target Partition Size_
    - _Base Cooldown Duration_
    - _Eager Compaction Limit_
5. **_Strict Eager Compaction_**:
A special case of either of the previous two schemes where the _Eager Compaction Limit_ is set greater than or equal to _Target Partition Size_. All candidate segments are considered _Live_ in this scheme.
It is configured by setting:
    - _Target Partition Size_
    - _Eager Compaction Limit_

### Generationally-Tiered Compaction

>Where membership is predicated on all candidate segment files to be contiguous and share the same Generation value.

The file writing and compaction facilities track the _Generation_ of each new segment. A segment's _Generation_ represents the current compaction depth of the file at the time of writing. For example a segment whose _Generation_ value is 0 is considered a raw or uncompacted file.

The base logic of the compaction algorithm will only compact candidate segment files if they are both contiguous and share the same _Generation_ value. This organizes compaction operations into a binary tree structure when the compaction ratio $k = 2$.

### Scaled, Cooldown-Tiered Compaction

> Where membership is predicated on a segment being promoted to accumulator after some _cooldown_ period has elapsed since it was written;  _cooldown_ is an interval multiple of the segment's Generation value.

The binary tree structure of generationally tiered compaction provides excellent scaling but at the cost of leftover, not compacted segments. To illustrate how the number of files scales at chain head we can represent a segment and its generation like `[ g ]` for example we visualize the state of table files after 81,935 compaction cycles (where $k=2$ and the target size is $2^{15}$ blocks) like: `[ 15 ] [ 15 ] [ 14 ] [ 3 ] [ 2 ] [ 0 ]`. In this example the overall un-compacted file length is 4 with a generational height of 14. We can calculate the number of un-compacted files at any compaction cycle $n$ and target generation $g$ as $popcount\left(n\mod2^{g+1}\right)$ which gives us a worse case scenario every $2^g-1$ cycles where the remaining un-compacted file count is equal to $g$. This is not ideal if the goal is to minimize the number of small files.

The new compaction algorithm introduces a cooldown period for all segments after which the file is promoted as an "accumulator" that may compact all segments between it and the chain head. The cooldown for a segment is calculated by multiplying a configured `base_duration` by the generation of the segment.

This relaxation of Generationally-Tiered Compaction mitigates its worst case scenario while maintaining overall $\log_2(N)$ scaling when configured properly. It also enables the compaction of segments that are ingested to bridge gaps between non-contiguous segment chains which is useful for restarting historical data dumps for example.

### Tiered Eager Compaction

> Where segments eagerly compact into an accumulator segment until a size threshold is met after which the compaction scheme described above takes precedence.

Eager compaction is not necessarily pathological and must be available as a compaction strategy. The new compaction algorithm introduces an optional, configurable, secondary size threshold for eager compaction. When set, all segments eagerly compact into an accumulator until it exceeds the threshold.

This extends the compaction algorithm with the following scenarios based on the value of the secondary threshold:

- if set greater than or equal to the target partition file size, the algorithm is effectively Eager Compaction
- else if set to a non-unbounded value, increases the minimum segment size while performing the above described compaction strategy.
- else if not set, just perform the above described compaction strategy.

## Addtional Terms

### Segment Tiers

By combining the cooldown and eager compaction tier concepts, useful categories emerge to describe the state of a given segment:

- **_Live_**: a recently ingested file or collection thereof (i.e. smaller than the secondary threshold)
- **_Hot_**: a segment that has been recently compacted and needs to stabilize before compacting again
- **_Cold_**: a historical or previously compacted segment that may act as an accumulator.

Each category or tier corresponds to a different compaction strategy, providing (I think) the necessary flexibility to handle all kinds of use cases.
