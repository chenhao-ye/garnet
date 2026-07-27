# C5 (snapshot) replication results

Results from the three `replication_dist_c5_*` sweeps (real 3-node cluster: node0 primary,
node1 replica, node2 client). This is the first end-to-end C5 (snapshot read protocol) replication
run; it was enabled by the n>1 replica-replay fix (transaction/checkpoint records are now dispatched
to participating replay tasks -- see the "Fix" note at the end).

## Setup

- Write distribution swept across three configs: **Uniform / Zipf / ZipfRev** (SETs to the primary).
  Reader is always **Zipf** (GETs to the replica, C5 snapshot reads).
- Fixed: m=1 physical sublog, 10M keys (8B/8B), 16 reader threads, 1g AOF memory, per-side lag budget
  256MB (`aof_ship_max_lag` + `replica_offset_max_lag`), diskless null-device AOF, `performance`
  governor, 30s x repeat 5.
- Swept (per config, 60 combos): replay tasks **n in {2,4,8,16,32}** x snapshot freq **{5,500} ms**
  x writer threads **{1,2,4,8,16,32}**.
- All 180 combos completed with **0 replica-replay crashes**, validating the n>1 C5-replication path.

All tables below are at **writers=16** (a balanced 16-writer / 16-reader load) unless noted. Columns
are `<write-dist>/<snapshot-freq-ms>`: U=Uniform, Z=Zipf, Zr=ZipfRev.

## Table 1 -- Reader throughput (Mops/s), writers=16

| n | U/5 | U/500 | Z/5 | Z/500 | Zr/5 | Zr/500 |
|---|---|---|---|---|---|---|
| 2  | 10.82 | 10.29 | 10.17 | 10.95 | 11.13 | 11.30 |
| 4  | 9.87  | 9.53  | 9.28  | 10.09 | 10.71 | 10.65 |
| 8  | 9.47  | 8.39  | 8.62  | 9.19  | 9.60  | 10.15 |
| 16 | 7.16  | 7.68  | 7.75  | 8.93  | 7.98  | 9.21  |
| 32 | 6.25  | 6.51  | 6.93  | 7.90  | 7.06  | 9.07  |

At light load (writers=1) the reader holds ~11 Mops/s for every n (no write pressure on the replica).

## Table 2 -- Writer throughput (Mops/s), writers=16

| n | U/5 | U/500 | Z/5 | Z/500 | Zr/5 | Zr/500 |
|---|---|---|---|---|---|---|
| 2  | 1.70 | 1.41 | 1.60 | 1.94 | 1.77 | 2.42 |
| 4  | 1.81 | 1.86 | 2.62 | 2.96 | 2.40 | 3.26 |
| 8  | 2.65 | 2.45 | 2.93 | 4.25 | 3.32 | 4.08 |
| 16 | 2.63 | 3.18 | 3.75 | 4.57 | 3.62 | 4.91 |
| 32 | 3.42 | 3.51 | 4.14 | 4.68 | 3.71 | 4.04 |

## Table 3 -- Read freshness p50 (staleness, seconds), writers=16

| n | U/5 | U/500 | Z/5 | Z/500 | Zr/5 | Zr/500 |
|---|---|---|---|---|---|---|
| 2  | 4.26 | 5.50 | 4.53 | 3.98 | 4.09 | 3.42 |
| 4  | 4.01 | 3.98 | 2.72 | 2.89 | 3.04 | 2.50 |
| 8  | 2.72 | 3.00 | 2.50 | 2.00 | 2.20 | 2.00 |
| 16 | 2.73 | 2.50 | 1.95 | 2.00 | 2.00 | 1.50 |
| 32 | 2.13 | 2.50 | 1.75 | 2.00 | 1.95 | **0.50** |

Replication lag pins at the **~496 MB budget** for essentially every combo at writers=16 (the replica
cannot keep up and backpressure holds it at the budget), with one exception: ZipfRev / n=32 / 500ms,
where the replica fully caught up (lag 4 MB, freshness 0.5 s).

## Table 4 -- Freshness/lag regime vs write load (Zipf, n=8, freq=5)

| writers | reader Mops/s | repl lag | freshness p50 |
|---|---|---|---|
| 1  | 11.26 | 0 MB   | 3.9 ms |
| 2  | 10.60 | 0 MB   | 4.0 ms |
| 4  | 8.69  | 75 MB  | 9.1 ms |
| 8  | 8.53  | 496 MB | 2.43 s |
| 16 | 8.62  | 496 MB | 2.50 s |
| 32 | 8.25  | 500 MB | 2.48 s |

## Discussion

**The replay-task count `n` is a three-way trade, not a free scaling knob.** Increasing n on the
replica (Tables 1-3, writers=16):
- **hurts reader throughput** (n=2 -> n=32: ~10-11 -> ~6-9 Mops/s, a 20-40% drop), because more
  replay tasks churn the log tail that the C5 snapshot read lands in -- the same reader-vs-replay
  cache contention measured in the single-node C5 study, now confirmed under real replication;
- **helps writer throughput** (n=2 -> n=32: ~1.5-2.4 -> ~3.5-4.7 Mops/s, roughly 2x), because more
  parallel replay lets the replica drain the AOF faster, relieving backpressure on the primary;
- **improves freshness** (n=2 -> n=32: ~4-5 s -> ~0.5-2.5 s), for the same reason -- faster replay
  means the replica sits less far behind.

So n must be chosen for the deployment's priority: low n favors read-serving throughput; high n favors
write throughput and freshness. There is no single n that maximizes all three.

**Under sustained write load the replica saturates the lag budget, and freshness is dominated by that
lag, not the snapshot interval.** Table 4 shows a sharp regime change: below the replay ceiling
(writers <= 2 here) the replica keeps up (lag ~0, freshness a few ms, i.e. ~snapshot interval); above
it (writers >= 8) the replica pins at the 256MB/side budget and reads are stale by ~2.5 s -- the
time-equivalent of the buffered lag. The snapshot frequency (5 vs 500 ms) is therefore a second-order
effect on freshness in the loaded regime: its ~0.5 s worst-case contribution is small next to a
multi-second replication lag. Freshness at scale is a *keep-up* problem (raise n, or the replay rate),
not a snapshot-interval problem.

**Less-frequent snapshots modestly help the reader.** Across Table 1, freq=500 ms generally gives
equal-or-higher reader throughput than freq=5 ms (clearest at high n, e.g. ZipfRev n=32: 7.06 -> 9.07
Mops/s), consistent with the single-node finding that frequent snapshotting steers reads into the
hot append region and adds flush/coherence overhead. Because freshness is lag-bound under load,
freq=500 ms buys reader throughput at little freshness cost -- a favorable trade in this regime.

**Write distribution: ZipfRev >= Zipf > Uniform for both reader throughput and keep-up.** At n=32,
freq=500 the reader runs 9.07 (ZipfRev) / 7.90 (Zipf) / 6.51 (Uniform) Mops/s, and only ZipfRev
(n=32/500ms) fully drained the lag. Uniform writes spread across the whole key space, maximizing the
working set the replay tasks touch and the cache pressure on the co-located reader; skewed writes
concentrate churn on a hot set, leaving more of the store cold and cheap for both replay and reads.

**Caveats.** (1) m is fixed at 1; the interaction with physical sharding (m>1) is not measured here.
(2) The multi-second freshness is a property of the 256MB/side lag budget saturating under load, not
of the C5 protocol per se; a smaller budget would cap staleness lower at the cost of more aggressive
backpressure. (3) The no-C5 (MultiLog timestamp) `replication_dist_*` baseline was not run in this
batch, so there is no C5-vs-timestamp head-to-head yet -- these numbers characterize C5 in isolation.

## Fix that made this run possible

C5 replication with n>1 previously crashed the replica replay immediately: the channel-mode driver
routed every record to a single owning task via `GetReplayTaskIdx`, which returns -1 for records with
no single owner (checkpoint commit markers, flushdb, multi-exec -- all carried under `TransactionHeader`),
causing `replayTasks[-1]`. A SET always carries a per-key `ShardedHeader`; the offending records are
the **checkpoint commit markers** replication emits during attach/sync. Fix (in
`ReplicaReplayDriver.ConsumeAndScheduleReplay`): dispatch such records to each *participating* replay
task via the header access vector (`CanReplay`), matching the scan-filter path. The config side also
requires `aof_replay_task_count` to be set on both primary and replica (via the `server_params` shared
sweep layer) so the primary emits properly tagged sharded records.

## Reproduce / data

Configs `replication_dist_c5_{wuniform,wzipf,wzipfrev}_rzipf` in `experiment/configs/`; per-run data
under `result/replication_dist_c5_*/` (summary.txt + result.yaml).
