# C5 baseline: periodic-snapshot stale reads (`AofReadWithTimestamp = false`)

Record of a code review validating that the C5 baseline (take a snapshot periodically,
serve reads from it) is implemented in code-replication as the **Snapshot read protocol**,
selected by `AofReadWithTimestamp = false`. The alternative, `AofReadWithTimestamp = true`
(the default), is the MultiLog **Timestamp** prefix-consistent read protocol.

## Idea

C5 exploits the log-structured KV (TsavoriteKV): freeze everything below a chosen log
address as immutable, and serve each key's read from the newest version at or below that
address. That returns an internally consistent but stale snapshot. A background task
periodically advances the frozen boundary to the current read-only tail, so staleness is
bounded by the snapshot interval plus replay lag.

## Selection knob

- `GarnetServerOptions.AofReadWithTimestamp` (default `true`). Doc string:
  "When true, use the Timestamp (prefix-consistent) read protocol on replicas. When false,
  use the Snapshot read protocol." (`libs/server/Servers/GarnetServerOptions.cs:144`)
- Set from the user option `AofReadProtocol`:
  `AofReadWithTimestamp = string.IsNullOrEmpty(AofReadProtocol) || AofReadProtocol.Equals("timestamp", OrdinalIgnoreCase)`.
  So `--aof-read-protocol snapshot` (any non-"timestamp" value) selects C5.
  (`libs/host/Configuration/Options.cs:903`)
- `AofSnapshotFreq` (default 5 ms): snapshot interval, "Only applies when
  AofReadWithTimestamp is false." (`GarnetServerOptions.cs:150`)
- The MultiLog timestamp machinery is disabled in snapshot mode:
  `timePulseEnabled = MultiLogEnabled && AofReadWithTimestamp` (`AofSyncTask.cs:106`).

## Periodic snapshot = advancing an immutable address

On the replica, a background task runs only in snapshot mode:

- `ReplicaReplayDriver` starts `BackgroundSnapshotTask` when
  `physicalSublogIdx == 0 && !serverOptions.AofReadWithTimestamp`
  (`ReplicaReplayDriver.cs:116`). Loop: `await Task.Delay(AofSnapshotFreq);
  storeWrapper.TryAdvanceSnapshotAfterReplay();`.
- `TryAdvanceSnapshotAfterReplay` (`StoreWrapper.cs`): early-returns in timestamp mode
  (`if (AofReadWithTimestamp) return;`); otherwise time-gated to `AofSnapshotFreq` and
  calls `TakeSnapshot()`.
- `TakeSnapshot()` (`StoreWrapper.cs:972`) is the crux:
  ```csharp
  store.Log.Flush(wait: true);
  Interlocked.Exchange(ref snapshotAddress, store.Log.SafeReadOnlyAddress);
  ```
  The snapshot boundary is set to `SafeReadOnlyAddress`, the boundary of the log-structured
  store's read-only (immutable) region. "Take a snapshot" = advance the immutable boundary
  to the current flushed tail, at most every `AofSnapshotFreq` ms.
- `snapshotAddress` starts at `long.MaxValue` ("read latest / no snapshot yet"); the first
  consistent read lazily takes a snapshot under `snapshotMutex` (`StoreWrapper.cs:922`).
- `ResetSnapshotState()` restores `long.MaxValue` (e.g. on reattach).

## Reads served from below the immutable address

- A consistent-read session gets a snapshot-address callback only in this mode:
  `getSnapshotAddress = IsConsistentReadSession && !AofReadWithTimestamp ?
  storeWrapper.GetSnapshotAddress : null` (`StorageSession.cs:118`). Consistent read is
  enforced only on replicas (`StoreWrapper.EnforceConsistentRead => enforceConsistentRead
  && clusterProvider.IsReplica()`).
- `ConsistentReadContext.Read` dispatches to `SnapshotRead` when the callback is set
  (`ConsistentReadContext.cs:85`):
  ```csharp
  var snapshotAddr = getSnapshotAddress();
  var scanFn = new SnapshotVersionScanFunctions(snapshotAddr);
  Session.store.Log.IterateKeyVersions(ref scanFn, key);
  if (scanFn.foundAddress != kInvalidAddress)
      return BasicContext.ReadAtAddress(scanFn.foundAddress, key, ...);
  return new Status(StatusCode.NotFound);
  ```
- `SnapshotVersionScanFunctions.Reader` walks the key's version chain newest-to-oldest and
  picks the newest version strictly below the boundary (tombstone below the boundary =>
  NotFound):
  ```csharp
  if (recordMetadata.Address >= snapshotMaxAddress) return true;  // skip newer, keep scanning
  if (!logRecord.Info.Tombstone) foundAddress = recordMetadata.Address;
  return false; // stop: found the snapshot version
  ```

That is exactly "data below a certain address is immutable; reading from there returns a
stale snapshot," implemented on the log-structured version chain.

## Summary of the two protocols

| | Timestamp (`AofReadWithTimestamp=true`, default) | Snapshot / C5 (`false`) |
|---|---|---|
| Read | direct read + per-key sequence-number/frontier check (PrefixLoom) | walk key version chain, read newest version below the frozen boundary |
| Freshness | near-live (blocks until the key's frontier is replayed) | stale up to `AofSnapshotFreq` (default 5 ms) + replay lag |
| Background work | in-band time pulses on idle sublogs | `BackgroundSnapshotTask` advancing `snapshotAddress` every `AofSnapshotFreq` |
| Per-read cost | direct read | `IterateKeyVersions` walk + `ReadAtAddress` |

## Nuances to keep in mind

1. **Object/unified stores share the main store's snapshot address.** `getSnapshotAddress`
   returns `StoreWrapper.snapshotAddress`, derived from the *main* `store.Log`, but the same
   callback is handed to the object and unified store sessions (`StorageSession.cs`). For a
   string GET/SET workload (main store only) this is fine; for object-store reads the
   boundary is a main-store address applied to a different log, so confirm intent before
   using C5 for object workloads.
2. **Read cost is dominated by co-location with the writers, not version-chain depth.** In
   principle each snapshot read iterates the key's versions above the boundary before reading,
   but with a tail-tracking snapshot the walk is almost always 1 step. The measured cost of C5
   reads under parallel replay comes instead from cache-coherence contention: the tail-tracking
   snapshot steers reads into the log's active append region, where the parallel replay tasks are
   writing. See "Measured C5 replay scaling" below.

## C5 does not need the replay barrier (fix for replay_task > 1)

The `n > 1` replay path (`ReplicaReplayDriver.ConsumeAndScheduleReplay`) fans each batch out
to the `n` replay tasks and then blocks on `LeaderFollowerBarrier.WaitCompleted(ReplicaSyncTimeout)`
until all tasks drain the batch. The drift gate (`aof_replay_drift_threshold`) throttles a task
that races ahead so the virtual sublogs stay within `drift` of each other. That gate exists only
to keep the **timestamp** read frontier well-defined; it is what stalls the batch drain at high
task counts.

Observed on the aof reader-during-replay bench, m=1 x n=32, 1 reader:
- Any finite `aof_replay_drift_threshold` (10k or 2.4M) => `GarnetException: Timed out draining
  replay batch` (SIGABRT), for **both** read protocols (the gate is independent of the read path).
- `aof_replay_drift_threshold = -1` (gate off) => runs fine.

C5 reads from a periodic snapshot, not from per-sublog frontiers, so it never needs the drift
gate. Fix: gate drift bounding on the timestamp protocol
(`ReadConsistencyManager.cs`):

```csharp
readonly bool driftBoundingEnabled =
    serverOptions.AofReadWithTimestamp
    && serverOptions.AofReplayDriftThreshold >= 0
    && serverOptions.AofVirtualSublogCount > 1;
```

After the fix (verified): C5 (snapshot) at m=1/n=32 runs with *any* drift setting, while the
timestamp protocol at m=1/n=32 still times out (it requires the barrier). So the barrier is only
engaged where it is actually needed.

### Measured C5 replay scaling (m=1, snapshot, drift off, 10M keys, 1 Zipf reader)

The single reader slows sharply as replay parallelism `n` grows (aof reader-during-replay bench,
InProc, uniform replay, Zipf read, `--aof-snapshot-freq 5`):

| replay tasks n | reader throughput (Mops/s) | reader IPC | reader cyc/op | LLC-miss/op |
|---|---|---|---|---|
| 1  | 1.95 | 1.56 | 1,687  | 1.9 |
| 8  | 0.75 | 1.15 | 4,042  | 3.7 |
| 32 | 0.23 | 0.43 | 11,744 | 4.5 |

**The cause is cache-coherence contention with the parallel replay writers, not the C5 read
protocol's own work.** The read protocol is cheap and stays cheap: the version-chain walk is 1
step (`avg_walk_steps` ~= 1.00 at every `n`), no read goes pending (0%), epoch drain scans are
0.2% of protections, and GC is ~4% of the pass. Yet per-read cost triples. The reader stays 95%
on-CPU (schedstat: `rq_wait` 0%, `sleep` ~= the GC share), so it is memory-stalled, not blocked
or descheduled: instruction count per op is roughly flat while cycles/op rise 7x, i.e. IPC
collapses from 1.56 to 0.43. LLC-misses stay ~2-4/op throughout, so the stall is coherence /
cache-hit latency on cache-resident lines, not DRAM misses (spreading the threads across two
sockets did not help and slightly hurt, confirming specific-line bouncing rather than aggregate
memory bandwidth).

The contended lines are dataset-size-independent: the slowdown is identical at 100K, 1M, and 10M
keys. That rules out per-key record coherence (a small Zipf hot set over 100K keys would collide
with the uniform writers far more often than over 10M) and cache-capacity eviction (100K keys fit
in LLC). What is invariant is the log's active append region. The background snapshot task
advances the read-only boundary toward the tail every `AofSnapshotFreq` (5ms), so each snapshot
read resolves to a *recently written* version that physically lives in the region the `n` replay
tasks are concurrently appending to, flushing, and advancing tail/head addresses in. The reader's
per-op memory accesses (the record it reads via `ReadAtAddress`, the mutable-region record header
the walk starts from, and the shared allocator address fields consulted per walk step) land on
cache lines the writers are dirtying, so every access pays a cross-core snoop whose cost grows
with the number of concurrent writers.

Freezing the snapshot confirms this directly. With `--aof-snapshot-freq 100000` (only the initial
lazy snapshot is ever taken, so the boundary stays far behind the tail near the bulk-loaded data),
reads resolve to old, stable versions in the cold read-only region that no writer touches:

| snapshot freq | reader throughput (Mops/s) | reader IPC | readAtAddr us/op |
|---|---|---|---|
| 5 ms (tracks tail)   | 0.24 | 0.47 | 1.15  |
| 100000 ms (frozen)   | 0.60 | 1.64 | 0.058 |

`ReadAtAddress` drops ~20x (1.15 -> 0.058 us/op) and IPC recovers to 1.64 once reads leave the hot
append zone. This also settles the copy-up intuition: frequent snapshots do not lengthen version
chains (the walk stays 1 step), but they do steer every read into the actively-mutated tail
region, and co-location with the parallel writers there is the contention. The residual gap from
the n=1 baseline (0.60 vs 1.95 Mops/s) is the generic cost of 32 busy replay threads sharing the
socket. The timestamp protocol cannot run this config at all (barrier incompatible with n>1).

**The degradation is coherence on the read's target region, not the chain walk (proof).** Because
the walk is only ~1 record deep, "chain-walk contention" is not a real explanation. A clean control
isolates the true cause: C5 inline read (walk only, 1 epoch acquire, identical machinery),
uniform/uniform, n=32, tracking (freq=5ms) vs frozen (freq=100000):

| | reader Mops/s | IPC | walk_steps | LLC-miss/op |
|---|---|---|---|---|
| tracking (recent versions) | 0.34 | 0.48 | 1.002 | 7.68  |
| frozen (cold originals)    | 0.71 | 1.16 | 1.003 | 10.57 |
| (both at n=1)              | ~1.5 | ~1.2 | -      | ~5.5  |

Same walk, same chain depth (1), same scan-iterator machinery, same 32 writers running -- only the
resolved *address* differs (recent-near-boundary vs cold-original). Frozen is 2.1x faster with IPC
recovered. The clincher is the LLC-miss inversion: frozen incurs *more* raw LLC-misses (10.57 vs
7.68) yet runs 2x faster, because tracking's misses are cross-core coherence misses on writer-owned
lines (expensive, serialized behind writer traffic) while frozen's are clean DRAM misses to cold,
uncontended lines (cheap, pipelined). And it scales with writer count as coherence must: at n=1
tracking and frozen are equal (no writers -> target hotness irrelevant); the 2x gap appears only at
n=32. So the drop with more sublogs is coherence on the recently-written log region the tail-tracking
snapshot reads land in -- physically resident in writer cores' caches and adjacent (same line/page)
to in-flight appends -- not the act of walking a chain.

**Snapshot-freq sweep confirms the decomposition (C5 inline, uniform, n=32).** As the snapshot is
taken more rarely, reads move from the hot recent region to the cold region, and C5 recovers in two
regimes:

| snapshot freq | reader Mops/s | IPC |
|---|---|---|
| 5, 20, 1000 ms (tracks tail) | ~0.33 | ~0.5 |
| 10000, 100000, 1000000 ms (frozen) | ~0.65-0.83 | ~1.1-1.4 |
| n=1 reference (no writers) | ~1.5 | ~1.2 |
| timestamp n=32 reference | ~0.64 | ~0.9 |

Barely-snapshotting recovers C5 to its coherence-free ceiling (~0.75 Mops/s, IPC ~1.2) and it then
*exceeds* the timestamp reader in this uniform case (cold stale reads beat fresh hot reads). But it
does NOT reach the n=1 value (~1.5 Mops/s): a residual gap remains that snapshot frequency cannot
close, and it splits into two parts that must not be conflated.

- **C5-frozen vs C5-at-n=1 (~2x): generic parallel-replay memory-subsystem load.** The cost of 32
  replay threads saturating the shared socket (bandwidth + snoop traffic). This is not
  protocol-specific -- the timestamp reader also drops from its n=1 value by a similar factor.
- **C5-frozen vs timestamp at the same contention: read-path work, NOT coherence.** This is the
  part that answers "is C5's read slower than MultiLog's". `perf c2c` on the reader settles it:
  frozen-C5 total Load-HITM (6507) is essentially equal to the timestamp reader's (5892), while
  tracking-C5 is 2.4x higher (15804). So freezing removes C5's *excess* coherence and leaves it at
  the timestamp reader's coherence level -- yet C5 is still measurably slower. The residual is the
  walk's per-read *work*, not sharing: C5 reaches the record through log-structural accesses
  (`RecordInfo.PreviousAddress`, per-step `HeadAddress`, page-array address resolution) inside the
  scan-iterator plus a redundant `ReadAtAddress` re-fetch, versus the timestamp read's single O(1)
  hash-index lookup. Per-thread PMU counters on the reader (next subsection) show this concretely:
  C5 retires ~1.8x more instructions/op than timestamp and touches ~1.5x more L1/L2-missing lines/op,
  and that gap is roughly constant across writer count. (Earlier drafts attributed this residual to
  "coherence on global metadata the writers churn"; both the c2c HITM parity and the per-op HITM
  parity below show that is wrong -- frozen-C5 is not paying extra coherence over timestamp.)

The transition sits between 1 s and 10 s because the run is ~2 s -- the snapshot must be rarer than
the run length to stay frozen at its initial boundary.

### The C5-vs-timestamp reader gap grows with parallelism (Zipf/Zipf)

A single "~14%" figure is misleading: the gap is small at low parallelism and large at high
parallelism. Reader throughput (Mops/s, 1 reader, 10M keys, Zipf read over Zipf replay; C5 is
m=1 x n with the snapshot frozen at `freq=100000`, timestamp is m x n=1;
`../data/aof_reader_c5_dist_freq` and `../data/aof_reader_zipf`):

| k | C5 tracking (freq 5) | C5 frozen (freq 100000) | timestamp | frozen gap vs TS |
|---|---|---|---|---|
| 1  | 2.15 | 2.01 | 1.88 | C5 wins |
| 2  | 1.46 | 1.62 | 1.72 | -6%  |
| 4  | 1.38 | 1.47 | 1.65 | -11% |
| 8  | 1.08 | 1.28 | 1.58 | -19% |
| 16 | 0.78 | 1.15 | 1.54 | -25% |
| 32 | 0.42 | 0.86 | 1.33 | -55% |

At k=1 C5 slightly *beats* timestamp (both read paths cost the same with no contention). The gap
opens with k: at k=32, aggressive tracking is 3.1x below timestamp (coherence), and even the frozen
snapshot -- coherence removed -- is 55% below (best observed frozen-regime point was `freq=10000` at
1.01 Mops/s, still -24%). The tracking-to-frozen recovery at k=32 (0.42 -> 0.86-1.01) is the
removable coherence; the frozen-to-timestamp residual is the read-path work above, which no snapshot
frequency can close. Note this comparison also mixes in *structure*: C5 here is one physical log
(m=1) that all n writers churn, while timestamp is m physical logs (writes spread, reads hit settled
hash-indexed records), a ~1.3x advantage on top of the read-protocol difference.

### Why the reader degrades with writers for C5 but not timestamp (per-op PMU decomposition)

The definitive measurement. A per-thread `perf_event_open` split on the reader (cycles,
instructions, and the Icelake memory-source events: L3-hit `0x4d1`, L3-miss `0x20d1`, local HITM
`0x4d2`, remote HITM `0x4d3`) swept over writer count k, single Zipf reader / Zipf replay / 10M
keys. C5 is m=1 x n=k; timestamp (the real MultiLog, drift barrier on) is m=k x n=1; both readers
face k concurrent replay writers. Two reps, `performance` governor, `perf_event_paranoid=1`.

Throughput (Mops/s) vs k -- C5 collapses, timestamp barely moves:

| k | c5 tracking | c5 frozen | timestamp |
|---|---|---|---|
| 1  | 2.00 | 1.90 | 1.97 |
| 4  | 0.91 | 1.15 | 1.69 |
| 8  | 0.69 | 0.80 | 1.58 |
| 16 | 0.55 | 0.46 | 1.48 |
| 32 | 0.28 | 0.45 | 1.23 |

The master metric is `cyc/op` (inverse of throughput), which factors exactly as
`cyc/op = instr/op / IPC`. At k=32:

| cfg | cyc/op | instr/op | IPC | hitm_local/op | L1/L2-miss loads/op (hitm+l3miss+l3hit) |
|---|---|---|---|---|---|
| c5 tracking | 8470 | 5792 | 0.68 | 2.23 | 5.6 |
| c5 frozen   | 4320 | 7280 | 1.69 | 0.97 | 2.9 |
| timestamp   | 2335 | 3162 | 1.35 | 2.08 | 3.7 |

C5-tracking is 3.6x slower than timestamp at k=32, splitting multiplicatively into **~1.8x more
instructions** (5792 vs 3162: the version-chain walk + `ReadAtAddress` re-fetch vs one O(1) hash
lookup; a roughly constant gap across k) and **~2.0x lower IPC** (0.68 vs 1.35: memory stalls, and
*this* is the term that scales with writers -- C5's IPC falls 1.61 -> 0.68 as k:1->32 while
timestamp's only sags 1.69 -> 1.35).

Three counter facts refute the coherence explanation directly:

1. **Per-op HITM is equal at k=32** -- timestamp 2.08 vs C5-tracking 2.23. Timestamp incurs the same
   cross-core coherence per op and stays 4.4x faster. Coherence per op is not what separates them.
2. **The walk does not lengthen with writers.** `avg_walk_steps` (C5_DIAG) = 1.45 (tracking) / 1.00
   (frozen), identical at k=8 and k=32. Yet `walk_us/op` doubles (0.70 -> 1.59) and `readAtAddr_us/op`
   doubles (0.34 -> 0.82). Same number of accesses, each ~2x slower under load -- a per-access
   latency effect, not a longer walk.
3. **Fewest misses is not fastest.** Frozen-C5 has the *lowest* per-op misses (2.9) and lowest HITM
   (0.97) of the three, yet is slower than timestamp (0.45 vs 1.23 Mops) -- because it retires the
   *most* instructions (7280). Frozen's slowness vs timestamp is pure work (instructions at high IPC,
   cold data); tracking's extra slowness on top is the per-access latency inflation below.

**Why the *same* walk takes longer (per-miss latency, not walk length or serialization).** cyc/op
= instr/op / IPC, and the IPC-collapse term resolves to per-miss cost. Cycles per L1/L2-missing load
(`cyc/op` / (l3hit+l3miss+hitm)/op): C5-tracking 473 (k=8) -> 995 (k=32), a 2x rise; timestamp 462 ->
451, flat. The *same* memory accesses cost 2x more cycles for C5 as writers scale, and not at all for
timestamp. The distinguishing counter is `l3hit/op` -- clean loads that miss private L1/L2 and
resolve in the shared L3: C5 2.11/op vs timestamp 0.45/op (~5x). C5's walk re-reads several
writer-produced records (`RecordInfo`, `PreviousAddress`, value) plus a redundant `ReadAtAddress`, a
footprint that spills out of the reader's private cache into the shared L3; timestamp's single
hash-indexed read stays L1/L2-resident. As 8 -> 32 writers stream appends through the shared L3 and
mesh, each L3 access queues longer; C5, hitting the shared L3 ~5x more per op, absorbs that inflation
~5x over, while timestamp barely touches the contended shared cache. So the mechanism is the
**volume of shared-L3 / coherence traffic per op meeting a shared cache whose latency rises with
writer count** -- C5 does more of that traffic (bigger per-op read footprint from the walk + refetch),
timestamp does little of it.

This is *not* a memory-level-parallelism (serialization) effect: the reader's average outstanding
L1D misses (`l1d_pend_miss.pending` / cycles) is *higher* for C5 than timestamp at k=32 (1.04 vs
0.54), so C5 overlaps its misses more, not less. The bottleneck is traffic volume into a contended
shared cache, not an inability to overlap. Freezing removes most of it (cold records live in
uncontended parts of L3/DRAM -> IPC stays ~1.69) but leaves the instruction-count term, so frozen-C5
stays above timestamp in cyc/op. Instrumentation is throwaway (per-thread PMU counters incl. the
Icelake memory-source and `l1d_pend_miss.pending` events in `AofBench.RunReader`, c5wt worktree, not
for commit; requires `perf_event_paranoid<=1`); reproducible via the `aof_reader_c5_dist_freq` /
`aof_reader_zipf` configs plus the memory-source counter build.

### Why the timestamp (MultiLog) reader does not collapse the same way

At m=1/n=1 with no contention the two read paths cost the same (both ~1.5 Mops/s, ~5.5
LLC-misses/op, IPC ~1.2). The divergence is entirely under parallel replay, and it is a
read-path difference. A/B at identical m=32/n=1 uniform/uniform contention (timestamp run with
its frontier wait bypassed via a diagnostic `TS_SKIP_WAIT`, so it reads stale like C5 and only the
read path differs):

| read path | reader Mops/s | instr/op | LLC-miss/op | cyc/op | IPC |
|---|---|---|---|---|---|
| C5 snapshot            | 0.23 | 4,182 | 10.0 | 13,272 | 0.32 |
| Timestamp (wait bypassed) | 0.49 | 2,759 | 5.76 |  6,048 | 0.46 |

Measured with a thread-local epoch-acquire counter on the reader (m=1, uniform, n=32):

| read path | reader Mops/s | acquires/op | IPC | instr/op | LLC-miss/op |
|---|---|---|---|---|---|
| timestamp (direct)          | 0.64 | 1.00 | 0.90 | 4,022 | 6.09 |
| C5 inline (walk only)       | 0.34 | 1.00 | 0.51 | 4,463 | 7.42 |
| C5 base (walk + ReadAtAddr) | 0.27 | 2.00 | 0.46 | 4,994 | 7.05 |

The C5 read path is more expensive under contention for two *separable* reasons:

1. **The redundant `ReadAtAddress` is a whole second epoch-protected operation.** C5 base does
   2.00 epoch acquires/op; C5 inline and timestamp both do 1.00. Each acquire is a locked
   `Interlocked.CompareExchange` + fence (`LightEpoch.Acquire -> TryAcquireEntry`). Removing the
   second read (inline) drops 2.00 -> 1.00 acquires and recovers +24% (0.27 -> 0.34 Mops/s).
2. **The version-chain walk itself stalls more than the direct read at equal epoch/instruction
   cost.** C5 inline and timestamp both do 1.00 acquire/op and nearly the same instr/op (4,463 vs
   4,022), yet the walk runs at half the IPC (0.51 vs 0.90). So the per-record epoch atomic is NOT
   the reason (equal count); it is pure memory-stall. The difference is *which* memory each touches
   to locate the record: the timestamp read reaches it through the fixed **hash index** (`FindTag`
   -> bucket -> record), while the C5 walk follows the **in-log version chain** in
   `BeginGetPrevInMemory` (reading each record's `RecordInfo.PreviousAddress`, `HeadAddress` per
   step, and resolving physical addresses through the page array). Those log-structural accesses
   overlap the region the replay writers are appending to and advancing, so they become cross-core
   coherence stalls (LLC-miss 7.42 vs 6.09, plus coherence hits not counted as misses). At n=1 the
   two paths are near-identical (1.45 vs 1.58 Mops/s, IPC 1.10 vs 1.23, LLC-miss 5.56 vs 5.60);
   only under contention does the walk's IPC collapse. (Which single access dominates the residual
   was not isolated; it would need per-access instrumentation inside the iterator.)

The `ReadAtAddress` is redundant: `IterateHashChain` already hands the full `LogRecord` to the
scan-iterator `Reader` (`AllocatorScan.cs`), so C5 can produce the read output from the walk
instead of re-reading it.

**Optimization measured (inline read).** Producing the output inside
`SnapshotVersionScanFunctions.Reader` (calling `functions.Reader` on the found record, which is
below the boundary / read-only, with the epoch held by the walk) and skipping `ReadAtAddress`:

| | n=1 (no contention) | n=32 uniform/uniform |
|---|---|---|
| baseline (ReadAtAddress)  | 1.517 Mops/s, instr/op 2509 | 0.253 Mops/s, cyc/op 11,506, readAtAddr 1.097 us |
| inline read               | 1.509 Mops/s, instr/op 2511 | 0.339 Mops/s, cyc/op  8,613, readAtAddr 0.069 us |

At n=1 the two are byte-identical (same instr/op and LLC-misses), so the inline path does the same
value copy; it just drops the second fetch. Under contention it gives +34% reader throughput
(0.253 -> 0.339 Mops/s) and cuts cyc/op by 25%. The redundant fetch is nearly free uncontended but
costs ~1.1 us under contention, because its epoch-resume and address-resolution accesses land on
the writer-churned lines. It closes ~40% of the gap to the timestamp read path (~0.49 Mops/s under
the same contention). The residual is the version-chain walk (`IterateKeyVersions`, still ~2.43
us/op under contention), which is more intrinsic to the snapshot approach since a walk is required
to locate the version below the boundary. The inline read is now the default on this branch:
`ConsistentReadContext.SnapshotRead` produces the output from the `LogRecord` the version walk
already holds (via `functions.Reader`) and skips `ReadAtAddress`. It is always-on for the
(non-transactional) consistent-read path; the transactional path still fetches via `ReadAtAddress`
(`inlineRead: false`). The diagnostic scaffolding (`SnapshotReadStats`, `C5_DIAG`, the
`C5_INLINE_READ` env gate) that surrounded it in the `snapshot-bench-c5wt` worktree was dropped.

Caveat: this A/B isolates the read path only. It does not fully explain why the real timestamp
reader (barrier-optim branch, frontier wait active) degrades even less (~20-40% across m=1..32 in
`../data/aof_reader`) than the wait-bypassed timestamp path here (which still drops ~3x from n=1).
That residual likely comes from the frontier wait coordinating reads with replay progress and/or
other barrier-optim improvements, not measured in this worktree.

Instrumentation used to establish this (throwaway, in the `snapshot-bench-c5wt` worktree, not for
commit): per-read walk-depth / pending counters and walk-vs-read timing in
`ConsistentReadContext.SnapshotRead`; the reader thread's `schedstat` (on-CPU vs runqueue-wait vs
sleep) and per-thread `perf_event_open` counters (cycles / instructions / LLC-misses) in
`AofBench.RunReader`; `LightEpoch` drain-scan / epoch-bump counters; and a `perf record` cycles
profile of the named `C5READER` thread.

## C5 also caps replay throughput (independent of any reader)

Beyond the reader, the C5 snapshot protocol limits the replica's *replay* throughput, even with no
reader attached. Sweep of replay-task count n at m=1, no reader, null device (configs
`aof_replay_virtual_c5` vs `aof_replay_virtual`, repeat 7):

| n | C5 replay (M rec/s) | timestamp (M rec/s) | C5/TS |
|---|---|---|---|
| 1  | 2.44 | 2.31 | 1.05 |
| 2  | 3.07 | 3.90 | 0.79 |
| 4  | 4.54 | 8.46 | 0.54 |
| 8  | 6.50 | 15.6 | 0.42 |
| 16 | 7.06 | 20.8 | 0.34 |
| 32 | 6.83 | 24.7 | 0.28 |
| 64 | 5.65 | 23.4 | 0.24 |

At n=1 the two are equal (~2.4 M rec/s). The timestamp protocol then scales replay roughly 10x
(to ~24 M rec/s at n=32), whereas C5 plateaus near 7 M rec/s by n=16 and regresses beyond it. So
C5 caps replay about 3.5x below the timestamp protocol at high parallelism.

**The cause is the periodic snapshot flush, confirmed directly.** Each snapshot is
`TakeSnapshot() -> store.Log.Flush(wait: true)`, a synchronous global stall on the store that
serializes all n replay tasks and grows more costly as n rises. During active replay the trigger is
the **replay driver itself**, not the background task: `ReplicaReplayDriver.Consume` calls
`storeWrapper.TryAdvanceSnapshotAfterReplay()` after every batch, which is time-gated to
`AofSnapshotFreq` (5 ms). Verified by stack trace: with the background task disabled, `TakeSnapshot`
still fires ~2072 times at freq=5 (once at freq=100000), all from `Consume ->
TryAdvanceSnapshotAfterReplay`. The `BackgroundSnapshotTask` is redundant during active replay (its
ticks hit the same shared time-gate the driver just reset) and only covers idle catch-up; disabling
it has no measurable effect on replay throughput (m=1/n=32: 7.58 vs 7.34 at freq=5, 24.50 vs 23.91
at freq=100000). Freezing the snapshot removes the periodic flush and C5 replay recovers *exactly*
to the timestamp rate (m=1, n=32, no reader):

| config | replay throughput (M rec/s) |
|---|---|
| C5 tracking (freq 5 ms)   | 7.4  |
| C5 frozen (freq 100000 ms) | 23.9 |
| timestamp                 | 23.9 |

So the snapshot frequency is a double-edged knob for C5: frequent snapshots keep reads fresh but
(a) steer reads into the writers' hot region (reader coherence, above) and (b) stall replay via the
synchronous flush; infrequent snapshots give fast replay and fast (cold) reads but staler data. The
timestamp protocol has no such flush and pays neither cost.

## Key files

- `libs/server/Servers/GarnetServerOptions.cs` -- `AofReadWithTimestamp`, `AofSnapshotFreq`
- `libs/host/Configuration/Options.cs` -- `AofReadProtocol` -> `AofReadWithTimestamp`
- `libs/server/StoreWrapper.cs` -- `snapshotAddress`, `GetSnapshotAddress`,
  `TryAdvanceSnapshotAfterReplay`, `TakeSnapshot`, `ResetSnapshotState`
- `libs/server/Storage/Session/StorageSession.cs` -- wires `getSnapshotAddress` into sessions
- `libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs` --
  `SnapshotRead`, `SnapshotVersionScanFunctions`
- `libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs` --
  `BackgroundSnapshotTask`
