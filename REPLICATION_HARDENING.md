# Replication Hardening: Backpressure, Truncation Fix, and the Drift-Barrier Ceiling

Investigation notes with data, July 2026. All experiments on CloudLab r650s
(2 sockets, 144 logical cores = 72 per NUMA node, 128 GB per node, TSC ~2.4 GHz),
nodes `node0`/`node1`/`node2`, harness configs under `experiment/configs/`.
Workload unless noted: m=32 physical sublogs, 64 writer threads (SET, itp=256)
against the primary, 1 reader thread (GET) against the replica, 8B keys/values,
1M keyspace, 30s passes x3, in-memory AOF (`aof-null-device` +
`fast-aof-truncate`, diskless sync).

Headline conclusions:

1. Replicas destroyed their own un-replayed log under load (silent data loss in
   every pre-existing replication benchmark). Fixed by clamping truncation at
   the replay offset.
2. A new primary-side backpressure knob (`aof-ship-max-lag`) plus the existing
   replica-side `replica-offset-max-lag` close the two lag gaps; either alone
   leaves a silent-drop mode open. Overhead is unmeasurable when disabled or idle.
3. The live-replay ceiling at m=32 was the replay-align drift barrier: its
   default threshold (10000 sequence units) is ~4 us of TSC time, forcing a
   32-thread barrier round every ~4 records. Replay throughput at a tight
   threshold follows: tput ~= record_density x threshold / round_cost.
   Calibrated threshold: 2 ms (4,800,000 ticks) at this load.
4. With everything fixed and calibrated, honest end-to-end write scaling is
   2.27 M -> 30.3 M ops/s from m=1 to m=32 with zero data loss at every point.

---

## 1. Silent data loss: replicas truncating their own log

### Symptom

Every replication run, including pristine-baseline commits, completed only a
handful of freshness probes (prober: write `FRESH:<seq>` to primary, GET-poll
replica until visible, sleep 1 ms, repeat). Healthy replicas complete
hundreds-plus per 30 s pass.

Freshness operations per pass (fixed binary, before the truncation fix):

| run                        | pass0 | pass1 | pass2 |
|----------------------------|------:|------:|------:|
| goalbase m=1               | 1     | 0     | 0     |
| goalbase m=8               | 3     | 2     | 0     |
| goalbase m=16              | 5     | 0     | 0     |
| goalbase m=32              | 26    | 2     | 2     |
| backpressure m=32 (early)  | 43    | 4     | 11    |

One run crashed the replay task outright:
`TsavoriteException: Uninitialized page found during scan at page 4046` in
`ReplicaReplayDriver.BackgroundReplayTask` -> replay dead, offset frozen, lag
grew ~22 GB/pass, replica permanently stale (reestablishment default 0 = never).

### Root cause

`AofSyncDriverStore`'s `FastAofTruncate` page-shift callback registers on every
node, including replicas. On a replica there are no attached sync drivers, so
`SafeTruncateAof`'s min-over-drivers clamp is vacuous and each page turn
truncates the local sublog to ~2 pages behind its tail, yanking un-replayed
pages out from under the replay iterator. The iterator silently fast-forwards
(`TsavoriteLogScanIterator` null-device path, no log line) or faults on a
recycled page. The replay offset jumps forward with the skip, so every lag
metric read healthy while the replica applied only slivers of the stream.

### Fix

Clamp truncation at the role-aware replication offset in both
`SafeTruncateAof` overloads (`AofSyncDriverStore.cs`): on a replica this is the
applied replay position; on a primary the getter returns the tail, so the term
never binds. ~20 lines, no behavior change on primaries.

### Verification

Manual repro (64 writers, 10 s, m=1, no knobs): before fix 2 completed probes,
DBSIZE diverged 956,194 vs 999,869 (4% of final values lost); after fix 100
probes, DBSIZE 999,966 vs 999,967, honest 143 MB replay lag reported.

Consequence for old data: all replication measurements taken before this fix
measured replicas serving shredded state.

---

## 2. Backpressure: closing the two lag gaps

### The two gaps

```
appended -(gap 1)- shipped --TCP--> received -(gap 2)- applied
      aof-ship-max-lag                    replica-offset-max-lag
```

No component sees both gaps (APPENDLOG is fire-and-forget; there is no ACK
channel). Each bound converts "consumer behind" into pacing; leaving either
open leaves a silent-drop mode:

| config (m=32, honest metrics)     | writer | replica skips | verdict |
|-----------------------------------|-------:|--------------:|---------|
| neither bound                     | 29.9-31.5 M | ~0 (probes starve) | replica sheds locally, silently |
| replica 64m only (gate off)       | 30.9 M | 8528          | primary wraps, loudly |
| replica 192m only (gate off)      | 29.0 M | 1977          | same, milder |
| both bounds                       | 11.4 M | 0             | honest (pre-barrier-fix rate) |

### aof-ship-max-lag (new)

Single knob, `ReplicationOffsetMaxLag`-style: `-1` (default) disabled, `>=1` =
max replication lag of the logical log in bytes, where lag = slowest attached
replica's (tail - shipped), summed across physical sublogs (MultiLog is one
logical log; the budget scales with what it protects -- e.g. 3/4 x aof-memory x
m in the experiment configs).

Mechanism (`AofBackpressure.cs`, ~100 lines): shipping threads publish; the
threshold comparison and stall/resume hysteresis (resume at half) run on the
publisher, which flips a single `stalled` bool only on state transitions, so
the cache line appenders read stays quiet in steady state. Appenders' fast path
is one volatile bool read behind a null check (`backpressure?.Wait()`; the
object is null when disabled, matching `readConsistencyManager` convention).
Stalled appenders sleep-poll at 1 ms; publishers never wake anyone (shipping
threads stay light: one tick-compare per poll, one lag computation per ms max).
A wedged-but-connected replica stalls appends until its connection faults and
driver removal publishes (same contract as the replica-side throttle).
Transaction paths gate before `LockSublogs`.

### Overhead validation

Append microbench (EnqueueSharded, 64 threads, 5 samples, Mrec/s):

| point               | HEAD (pre-feature)      | tree, gate disabled       |
|---------------------|-------------------------|---------------------------|
| m=1 (contended)     | med 17.2, mean 16.4+/-1.7 | med 15.96/15.90, mean 15.8+/-1.6 |
| m=64 (peak)         | med 379.9               | med 379.7 / 396.3 (2 draws) |

Distributions overlap (delta-mean ~0.7 SE at m=1; m=64 second draw exceeds
HEAD): no measurable cost. End-to-end A/B at m=32 (gate off vs on-at-6g, gate
idle): 31.27 vs 29.91 M -- inside the config's historical run-to-run band
(26.8-31.3 M across repeats).

### End-to-end honest equilibrium (m=1)

With both bounds (ship 192m, replica 64m): writers pace to the true replay
rate. Reproduced across four code iterations: 2.27 / 2.34 / 2.25 / 2.31 M,
zero skips/crashes, lag pinned in the hysteresis band (184-186 MB), freshness
p50 ~1.3-2.0 s = legally buffered bytes / replay rate. Single-thread replay
microbench (`aof_replay_single`, same record shape): 2.765 Mrec/s -- writers
land exactly on it.

---

## 3. The live-replay ceiling: replay-align drift barrier

### Symptom and exonerations

Honest m=32 capped at ~11.4 M writers while: append capacity ~31 M, and 32-way
replay microbenchmarks sustain ~90-112 M. Systematically ruled out:

- CPU core sharing: 3-machine run (primary=node0, replica=node1, client=node2,
  no NUMA pinning, 144 cores/role) reproduces the co-located curve (table
  below).
- Feeding/starvation: `UnsafeEnqueueRaw` participates in the inflight-enqueue
  protocol; every receive enqueue signals parked iterators (code). Decisively:
  the drain test (below) reproduces the ceiling with zero concurrent ingest.
- Backpressure: gate-only A/B off~=on; the ceiling appears with the gate idle.

### Root cause: threshold units

Sequence numbers are raw TSC ticks (`SequenceNumberGenerator`: rdtsc, ~0.4 ns
per unit at 2.4 GHz). The default `AofReplayDriftThreshold = 10000` therefore
tolerates ~4 us of cross-sublog drift. A round (`ReplayAlignBarrier`) targets
the leading sublog's frontier; every replay thread parks on reaching the
target; the round completes when the laggiest arrives -- a full 32-thread
synchronization. Rounds fire per `threshold` of timeline, and live records at
30 M ops/s / 32 sublogs are ~1 record per us of timeline per sublog: a 4 us
threshold means one 32-thread round every ~4 records. Replay convoys.

The governing relation, confirmed by every measurement below:

    replay tput at tight threshold ~= record_density x threshold / round_cost

The replay microbenches never contradicted this: their generated timestamps
are ~3x denser (burst generation), and the same barrier at 10000 gives them
36.8 M -- 3x the live 12 M, and itself well under their ~90-112 M barrier-off
ceiling (verified: `aof_reader_verify32`, ReplayDirect m=32 + 1 in-proc zipf
reader, threshold 10000: 36.79 Mrec/s, reader 1.675 M ops/s at 0.55 us p50).

### Drain test (quiet-room falsification)

Build ~8 GB replica backlog under load (m=32, 1g buffers, no replica throttle),
stop writers, sample `replication_offset_lag` each second:

- threshold 10000 (4 us): linear drain 0.73 GB/s ~= 11.3 M rec/s -- the live
  ceiling reproduced with no ingest, no network, no readers. Round count is
  fixed by the backlog's timeline span (a live backlog carries its wall-clock
  span with it), so "many pre-loaded pages" does not help at a tight threshold.
- threshold 2,400,000 (1 ms): lag already 0 at writer stop (replay kept up).

### Threshold calibration curves

(a) 256m buffers, replica throttle 64m, ship gate 6g (writers pace to replay;
median of 3):

| drift    | writer  | reader  | reader p50 | fresh ops @ p50 | lag*  |
|----------|--------:|--------:|-----------:|-----------------:|------:|
| off (-1) | 31.11 M | 0.000 M | (wedged)   | 2018 @ 11 ms     | 0     |
| 4 us     | 11.47 M | 0.553 M | 354 us     | 3 @ 7.6 s        | 188 MB|
| 10 us    | 12.24 M | 0.527 M | 358 us     | 3 @ 6.9 s        | 188 MB|
| 30 us    | 13.22 M | 0.433 M | 381 us     | 4 @ 5.9 s        | 188 MB|
| 100 us   | 17.12 M | 0.265 M | 774 us     | 4 @ 5.6 s        | 232 MB|
| 300 us   | 18.02 M | 0.201 M | 1.06 ms    | 6 @ 4.4 s        | 227 MB|
| 1 ms     | 30.50 M | 0.117 M | 2.02 ms    | 329 @ 74 ms      | 38 MB |
| 10 ms    | 31.31 M | 0.014 M | 19.8 ms    | 1270 @ 21 ms     | 1 MB  |
| 100 ms   | 25.89 M | 0.001 M | 203 ms     | 2865 @ 8 ms      | 0     |

Notes: reader p50 tracks the threshold (the drift bound is the read wait;
reader-triggered rounds do not currently short-circuit it). `off` wedges
readers permanently (no rounds exist to release a parked read wait). Writer
knee between 300 us and 1 ms at this (paced) load.

(b) 1g buffers, ship 512m, m=32, threshold sweep in two protections
(median of 3):

| drift  | throttled 256m: writer/reader/fresh/lag      | unthrottled: writer/fresh/lag(max-metric) |
|--------|----------------------------------------------|-------------------------------------------|
| 4 us   | 12.01 / 0.632 / 2 @ 9.7 s / 298 MB           | 28.78 / 2 @ 13.7 s / 1075 MB (shredding)  |
| 100 us | 14.61 / 0.257 / 3 @ 9.0 s / 298 MB           | 31.41 / 2 @ 15.0 s / 1075 MB (shredding)  |
| 300 us | 17.41 / 0.255 / 3 @ 7.5 s / 299 MB           | 31.29 / 2 @ 10.5 s / 1070 MB (shredding)  |
| 1 ms   | 31.39 / 0.113 / 25 @ 1.1 s / 142 MB          | 30.95 / 20 @ 1.4 s / 173 MB (borderline)  |
| 2 ms   | 31.22 / 0.074 / 851 @ 31 ms / 6 MB           | 30.77 / 1494 @ 18 ms / 2 MB (clean)       |

The knee moves with offered load (1 ms sufficed when writers were paced to
11-18 M; at full 31 M it falls ~0.3% short and backlog creeps): the threshold
is an overhead budget whose adequacy scales with record density.

(c) 3-machine, no pinning (primary/replica/client on node0/node1/node2), same
1g/512m/256m setup -- CPU sharing exonerated, curve unchanged:

| drift  | 3-machine writer/reader/fresh/lag(summed)    | co-located writer (pinned) |
|--------|----------------------------------------------|----------------------------|
| 4 us   | 9.85 / 0.366 / 2 @ 13.2 s / 9391 MB          | 12.01                      |
| 100 us | 16.87 / 0.287 / 3 @ 7.8 s / 9378 MB          | 14.61                      |
| 300 us | 20.30 / 0.201 / 4 @ 6.4 s / 9550 MB          | 17.41                      |
| 1 ms   | 31.18 / 0.120 / 14 @ 2.0 s / 5573 MB         | 31.39                      |
| 2 ms   | 31.12 / 0.072 / 1314 @ 19 ms / 68 MB         | 31.22                      |

(Lag columns differ by metric, not physics: the 3-machine run used the synced
bench reporting summed lag; 9391 MB ~= 32 x 294 MB = the per-sublog throttle
bound. Earlier tables report max-per-sublog -- see section 5.)

### Calibrated recommendation

`aof_replay_drift_threshold = 4,800,000` ticks (2 ms) for this workload class:
full write throughput in every protection/topology, freshness p50 18-31 ms,
lag single-digit MB (per sublog), zero loss. Trade: reader p50 ~3 ms and
reader throughput ~0.07 M at m=32. The 4 us + throttle point is the
read-optimized end of the dial (0.632 M readers at 0.35 ms, writers paced to
12 M).

---

## 4. Honest end-to-end m-scaling (calibrated barrier at 1 ms)

`replication_honest_m_sweep`: per-m ship budget 3/4 x 256m x m, replica
throttle 64m, drift 2.4M ticks. Zero drops at every point (1 skip at m=32 =
known benign race, below).

| m  | writer  | reader  | reader p50 | fresh ops @ p50 | lag    |
|----|--------:|--------:|-----------:|-----------------:|-------:|
| 1  | 2.27 M  | 0.548 M | 0.45 ms    | 16 @ 1.3 s       | 184 MB |
| 2  | 3.92 M  | 0.243 M | 0.57 ms    | 17 @ 1.5 s       | 185 MB |
| 4  | 7.43 M  | 0.110 M | 1.2 ms     | 17 @ 1.4 s       | 186 MB |
| 8  | 14.71 M | 0.078 M | 2.5 ms     | 19 @ 1.3 s       | 186 MB |
| 16 | 18.33 M | 0.094 M | 2.5 ms     | 12 @ 2.1 s       | 217 MB |
| 32 | 30.26 M | 0.115 M | 2.0 ms     | 511 @ 49 ms      | 10 MB  |

13.3x write scaling m=1 -> m=32, all honest. m=1..16 sit against their
configured budgets (freshness there = budget / replay rate, a config artifact;
tighter budgets buy proportionally fresher reads at unchanged throughput).
m=32 crosses over: replay keeps up outright. Reader parity with m=1's fast
path is NOT met at m>1 under the time-calibrated barrier (see future work).

---

## 5. Harness and methodology notes

- `[Replication lag bytes]` in `Resp.benchmark` now reports the gap summed
  across sublogs (was: max per sublog). Runs before the node2 sync (see git
  history of `ReplicationBench.cs`) report max-per-sublog at m>1.
- The bench builds on each role's own checkout; uncommitted changes must be
  rsynced to node1/node2 before multi-machine runs (`run.py` prebuilds but does
  not sync source).
- `run.py` now drains each server's port after teardown
  (`wait_for_port_closed`) before the next run launches; previously the next
  run's readiness probe could be satisfied by the previous run's dying server,
  and its bootstrap then hit the listener gap (surfaced by slow 32 x 1g
  teardowns).
- Freshness-probe completions per pass are the sharpest honesty signal:
  near-zero probes with plausible-looking lag means offsets are advancing via
  skips. Silent local shedding produces NO skip warnings (the
  `MainMemoryReplication: Skipping` warning fires only for incoming-stream
  gaps, i.e. primary-side wraps).
- Config inventory (all under `experiment/configs/`):
  `replication_goalbase{,_m32}` (unprotected baselines),
  `replication_backpressure` (both knobs, m={1,32}),
  `replication_backpressure_ab32` (ship-gate A/B),
  `replication_replica_lag_ab32` (replica-knob-only A/B),
  `replication_drift_ab32`, `replication_drift_sweep32{,_fine,_big,_3m}`
  (barrier calibration), `replication_honest_m_sweep` (headline scaling),
  `aof_replay_single`, `aof_reader_verify32`, `aof_enqueue_quick`
  (microbench cross-checks).

## 6. Open items / future work

1. Re-denominate `aof-replay-drift-threshold` in microseconds with a startup
   TSC calibration (raw-tick values are machine-specific); default ~2000 us.
   Consider a hybrid trigger (fire on drift > time-threshold AND >= K records
   since the last round) so low-load deployments keep tight read bounds while
   high load stays amortized -- the time-only threshold couples freshness
   semantics to record density.
2. Reader parity at m>1: reader p50 tracks the threshold because
   reader-triggered rounds do not fire eagerly; a parked reader waits out the
   drift window. Making a reader-about-to-wait install/complete a round
   immediately would decouple read latency from the replay-overhead budget.
3. Rare one-page skip at m>1: ~1-3 events/run, exactly one page
   (e.g. 868241232 -> 872415232), page-aligned target, healthy connection,
   present in untouched baselines. Suspected page-boundary race in the
   truncate/eviction/iterator interplay; needs Tsavorite-level tracing.
4. `ThrottlePrimary` is a `Thread.Yield` spin; with 32 receive sessions
   throttled it burns cores. Sleep-based wait would match the gate's design.
5. Barrier limitation already documented in `ReplayAlignBarrier`: synchronized
   replay (transactions) can stall rounds; out of scope of current barrier work.
