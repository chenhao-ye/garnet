# In-band quiescence time pulses for multi-log replication

This change replaces the replica time-advance mechanism (`CLUSTER
ADVANCE_TIME`) with an in-band, per-sublog pulse that rides each sublog's
existing replication connection and is applied by the thread that owns that
sublog's replay. It removes the dedicated primary-side connection and task,
the replica-side background worker and its work queue, and the interlocked
updates on the per-record replay path; it extends time advance to the
single-physical-log configuration; and it integrates pulses with the
replay-align drift barrier so an idle sublog cannot stall a barrier round.

## 1. Motivation

### Why time advance exists

With `AofPhysicalSublogCount = m > 1`, each physical sublog ships records to
the replica on its own connection, and each replica virtual sublog tracks the
maximum replayed sequence number (`VirtualSublogReplayState.sketchMax`). A
prefix-consistent read blocks while the reading session's timestamp has
reached the target sublog's max. Session timestamps advance by reading keys on
other sublogs, so a sublog that stops receiving records freezes its max while
session timestamps keep growing; reads routed to the idle sublog then block
until `ReplicaSyncTimeout`. Something must tell the replica that logical time
still flows on a sublog that produces no records.

### Problems with the implementation this replaces

- **A second writer on the hot path.** The replica applied time advance from a
  dedicated background task, concurrently with the replay threads. Every
  per-record metadata update therefore used an interlocked compare-exchange
  loop -- two lock-prefixed read-modify-writes per replayed record to defend
  against a writer that fired at most once per `AofTailWitnessFreq`.
- **Out-of-band delivery needed its own ordering machinery.** The pulse
  traveled on a separate connection, so nothing ordered it against the record
  streams. The message carried a full tail-address vector, and a replica-side
  worker (queue, wakeup events, `TaskType.AdvanceTimeReplicaTask`) polled
  replay progress against the witnessed tails before applying.
- **Lifecycle sprawl.** One extra connection and background task per attached
  replica on the primary; on the replica, a registered task with start sites
  in both sync paths and cancel sites in both sync paths and failover.
- **The drift barrier could strand.** Replay-align barrier arrivals happened
  only on the per-record replay path. A sublog whose max was advanced by time
  pulses never arrived, so an active round could wait forever for it.
- **No coverage for m = 1.** The primary started the mechanism only for
  `AofPhysicalSublogCount > 1`, so the single-physical-log,
  multiple-replay-task configuration (`MultiLog(1, n)`) had no time advance at
  all.
- **A timestamp-ordering window.** The primary observed the tails first and
  acquired the pulse timestamp second. A record released between the two steps
  landed beyond the witnessed tail with a stamp below the pulse's, and an
  operation depending on it could also stamp below the pulse; a session that
  had observed the dependent could then read the un-replayed record's key and
  pass the freshness check.

## 2. How it works

### Primary: pulse from inside the per-sublog sync task

Each `AofSyncTask`'s consume loop already invokes `Throttle()` on every poll,
including empty ones; the pulse hooks there (`MaybeSendTimePulse`). Enabled
when `MultiLogEnabled && AofReadWithTimestamp`.

1. **Idle gating.** `Consume` refreshes `lastSendTicks` whenever records ship
   (shipped records carry time themselves). A pulse is considered only when
   the sublog has shipped nothing for `AofTailWitnessFreq` milliseconds.
2. **Converged check.** The task snapshots all sublogs' allocation tails and
   skips the pulse if none moved since its last pulse: a pulse is useful only
   if some sublog's records can advance a session timestamp past this idle
   sublog's max. Under full quiescence every task sends one trailing pulse and
   goes silent; any later append moves a tail and re-arms the next poll. The
   snapshot is taken before the timestamp below, so the timestamp provably
   exceeds the stamp of every record inside it.
3. **Timestamp acquisition.** `T = seqNumGen.GetSequenceNumber() + 1`. The +1
   makes T strictly larger than every stamp already acquired (an earlier
   counter read returns at most this read's value), which the convergence
   claim needs: session timestamps are always drawn from record stamps, so
   after the trailing pulse every session sits strictly below every sublog's
   max and no reader stays blocked once pulses cease.
4. **Allocation-tail gate.** After acquiring T, the task aborts the pulse
   unless `iter.NextAddress == physicalSublog.TailAddress` -- the iterator has
   shipped everything that holds log space. Records are stamped before they
   reserve log space, so every record allocated before T's acquisition is
   covered by the gate. A record not yet allocated ships after the pulse on
   this connection, and anything that depends on it acquires its stamp only
   after its bucket latch releases, hence after T -- so no session can ever
   hold a timestamp that requires seeing that record while this pulse is what
   unblocked it. The gate must use the allocation tail, not the safe tail: a
   completed record sitting above a still-copying straggler is excluded from
   the safe tail yet already visible to dependents through the store.
5. **Send.** `CLUSTER ADVANCE_TIME <physicalSublogIdx> <T>` on the sublog's
   own sync connection, fire-and-forget (no response, matching the APPENDLOG
   discipline on that connection), then snapshot the tails and reset the idle
   clock.

One assumption is introduced by the strict bound in step 3 and documented at
the call site: a record stamped after T's acquisition but within the same
counter tick carries a stamp one below T; its observers must not be able to
stamp within that same tick. Equivalently, one counter tick must be shorter
than a completed append followed by a dependent's read-to-stamp path. The
native invariant-TSC path satisfies this by orders of magnitude
(sub-nanosecond ticks against a chain of hundreds of nanoseconds); the
`Stopwatch` fallback satisfies it on Linux (nanosecond ticks) with a thinner
margin on Windows QPC (typically 100 ns ticks). Record-only replay needs no
such assumption -- its freshness checks treat timestamp ties conservatively.

### Wire

`GarnetClientSession.ExecuteClusterAdvanceTime(int physicalSublogIdx, long
sequenceNumber)` writes `*4 CLUSTER ADVANCE_TIME :<idx> :<seq>` and returns
nothing. Because the pulse shares the sublog's replication connection, the
replica receives it strictly after every record shipped before it -- the
ordering that lets the replica replace the old tail-vector comparison with a
purely local check.

### Replica: pulses applied by the replay-owning thread

`NetworkClusterAdvanceTime` parses the two arguments, bounds-checks the index,
drops the pulse while AOF streaming is disallowed, and routes to the sublog's
`ReplicaReplayDriver.SignalTimeAdvance(seq)`. No response is written. A pulse
that arrives before the session's replay driver exists is dropped; pulses are
re-sent while the condition that produced them persists, and a fresh sync
re-arms the primary's initial pulse.

Inside the driver, two single-writer counters hand the value across threads:

- `pendingPulseSequenceNumber` -- written only by the session thread
  (`SignalTimeAdvance`), monotonic latest-wins. Pulses are cumulative, so a
  depth-one slot is sufficient: overwriting an unapplied older pulse loses
  nothing.
- `appliedPulseSequenceNumber` -- written only by the replay-owning thread,
  advanced after a pulse is applied. The steady-state fast path is
  `pending <= applied -> return`, two reads and no atomics, checked on every
  consume-loop poll via the driver's `Throttle()`.

The applier is the thread that owns replay for the sublog: the background
consume loop in asynchronous replay, or the session thread itself in
synchronous replay and before the background iterator exists (applied inline
under the driver's worker monitor). Application is gated on the sublog being
fully caught up (`replicationOffset == local TailAddress`): the session
enqueued every pre-pulse chunk into the local sublog before recording the
pulse, and the volatile write/read pair on `pendingPulseSequenceNumber` orders
those enqueues before the applier's tail read -- so a caught-up sublog has
provably replayed everything the pulse was ordered after. A pulse that arrives
during traffic simply stays pending until an idle poll; the in-flight records
carry time themselves.

Applying the pulse (`ApplyPulse`):

- `AofReplayTaskCount == 1`: the applier owns the only virtual sublog and
  advances it directly.
- `AofReplayTaskCount > 1`: the value is staged
  (`stagedPulseSequenceNumber`) and broadcast through the same batch
  lifecycle as records -- a null-pointer control slot in each replay task's
  ring (channel mode) or a zero-length batch through the shared batch context
  (scan-and-filter mode). Each task applies the staged value to its own
  virtual sublog on its own thread, preserving the single-writer discipline.
  The staged slot is stable while the batch is in flight because the driver
  thread is the only batch producer and runs the pulse batch to completion.

The application itself is
`ReadConsistencyManager.AdvanceVirtualSublogTime(virtualSublogIdx, seq)`:
advance the sublog max (monotonic, so a late record with a smaller raw stamp
can never drag time backward -- it is implicitly reordered after the pulse),
wake any reader whose target the new max crossed, and make a non-blocking
arrival at the drift barrier.

### Read-consistency metadata: single writer, plain stores

With the background worker gone, every write to a virtual sublog's
`sketch[]`/`sketchMax` happens on the thread that owns its replay, or on a
thread that is provably the sole writer at that moment (a transaction
coordinator while the owners are parked at a barrier; the AOF benchmark
raising frontiers after its replay pass, via
`UpdatePhysicalSublogMaxSequenceNumber`, which carries that sole-writer
contract). `UpdateKeySequenceNumber` and `UpdateMaxSequenceNumber` are
therefore plain compare-plus-`Volatile.Write` -- the two interlocked
read-modify-writes per replayed record are gone.

### Drift barrier integration

`ReplayAlignBarrier` gains a second arrival source and per-participant
idempotence:

- `CheckAndWait(virtualSublogIdx, frontier)` -- the per-record arrival;
  blocking, as before (a leader pauses so laggards catch up).
- `CheckAndArrive(virtualSublogIdx, frontier)` -- the pulse arrival;
  **non-blocking**. An idle sublog has no replay work to pause, and the
  threads that deliver pulses (a session thread in synchronous replay, or the
  consume loop a session waits on through the ring batch) must not park on
  round completion. The participant is counted toward the round and the round
  released if it was last.
- `lastArrivedRound[]`, one slot per participant written by its owner thread,
  makes arrivals idempotent within a round -- necessary because a
  non-blocking arrival keeps processing and would otherwise decrement the
  countdown again on its next record.

A round can therefore complete while some sublogs are idle: the next pulse
always reaches the round target, because the target is some sublog's
previously published max, which is below every subsequently issued timestamp.
Multi-log recovery additionally disables the barrier for its duration
(`AofRecover.MultiLogRecover`): there are no reader sessions to protect, and
the finite per-sublog logs end at different times, so a round fired near the
end would wait for drivers that already finished.

### Removed

- Primary: the per-replica `AdvancePhysicalSublogTime` task and its dedicated
  `GarnetClientSession`; the unused `endPoint` field on `AofSyncDriver`;
  `GarnetAppendOnlyFile.GetLargerThanMaximumSequenceNumber` (the pulse inlines
  the explicit `+ 1` with its reasoning at the call site).
- Replica: `AdvanceTimeWorker`, its `ConcurrentStack` work queue and wakeup
  events, `StartAdvanceTimeBackgroundTask`, `TaskType.AdvanceTimeReplicaTask`
  and its placement mapping, and all start/cancel call sites in the diskless
  sync, disk-based sync, and failover paths. No replica-side global lifecycle
  remains: pending-pulse state lives in the replay driver and dies with it on
  re-sync.

## 3. Behavioral changes

- `MultiLog(1, n)` receives time advance; previously `m == 1` emitted nothing.
- The per-record replay path drops two interlocked atomics.
- An idle sublog no longer strands a drift-barrier round.
- Under full quiescence the primary sends one trailing pulse per sublog and
  then goes silent; under partial load it pulses only idle sublogs, every
  `AofTailWitnessFreq`; under full load it sends nothing.
- `AofTailWitnessFreq` keeps its name and default; its meaning is now the
  idle time before a sublog's sync task emits a pulse (option help text
  updated).
- The wire format of `CLUSTER ADVANCE_TIME` changed (per-sublog, two integer
  arguments, no reply), so primary and replica must run the same build for
  this internode command, consistent with how the other internode `CLUSTER`
  replication commands evolve.

## 4. Files changed

| File | Change |
|------|--------|
| `AofSyncTask.cs` | + idle-gated pulse emission (`MaybeSendTimePulse`): converged check, `GetSequenceNumber() + 1`, allocation-tail gate |
| `AofSyncDriver.cs` | - `AdvancePhysicalSublogTime` task and its connection; - unused `endPoint` |
| `GarnetClientSessionReplicationExtensions.cs` | `ExecuteClusterAdvanceTime(sublogIdx, seq)`, fire-and-forget |
| `RespClusterReplicationCommands.cs` | `NetworkClusterAdvanceTime` routes to the sublog's replay driver, writes no reply |
| `ReplicaReplayDriver.cs` | + pending/applied/staged pulse state, `SignalTimeAdvance`, caught-up gate, `ApplyPulse` broadcast; `Throttle` applies pending pulses |
| `ReplicaReplayTask.cs` | + `AddPulseMarker`; both replay loops apply a staged pulse to their own virtual sublog |
| `ReplicationManager.cs` | - advance-time worker, queue, and signals |
| `TaskType.cs` | - `AdvanceTimeReplicaTask` |
| `ReplicaDisklessSync.cs`, `ReplicaDiskbasedSync.cs`, `ReplicaFailoverSession.cs` | - start/cancel call sites |
| `VirtualSublogReplayState.cs` | plain-store updates under the single-writer ownership discipline |
| `ReadConsistencyManager.cs` | + `AdvanceVirtualSublogTime`; arrival calls carry the participant index; sole-writer contract on `UpdatePhysicalSublogMaxSequenceNumber` |
| `ReplayAlignBarrier.cs` | + `CheckAndArrive`, `lastArrivedRound[]` dedup, `ReleaseRound`; known-limitation note for synchronized replay |
| `AofRecover.cs` | barrier disabled across multi-log recovery |
| `GarnetAppendOnlyFile.cs` | - `GetLargerThanMaximumSequenceNumber` |
| `GarnetServerOptions.cs`, `Options.cs` | `AofTailWitnessFreq` help text |
| `GarnetTestLoggingEventType.cs` | + `LogRunAofSyncTask` (restores the test project's compile) |
| `ClusterMultiLogQuiescenceTests.cs` (new) | quiescent-sublog read test, m=2/n=1 and m=1/n=2 |

## 5. Validation

- New test `ClusterMultiLogQuiescentSublogReadTest`: write one key per virtual
  sublog, read the freshest key on the replica (the session timestamp advances
  to the system max), then read the key on the now-quiescent sublog. Both
  variants (m=2/n=1 and m=1/n=2) complete in about a second; without the
  pulse the second read blocks for the replica sync timeout, and the m=1
  variant previously had no mechanism at all.
- MultiLog cluster suites: the diskless-sync sharded fixture, which hangs and
  aborts the test run without this change (quiescent-sublog round strands),
  passes in full. The suites' remaining failures are identical with and
  without this change when run in isolation (7 tests, all in
  transaction-with-parallel-replay and checkpoint scenarios listed under
  TODOs).
- Standalone `RespAofTests`: 26/26 (single-log paths unaffected).
- AofBench smoke (ReplayDirect, m=4, 2 in-proc readers, reader-skip):
  4.7-5.6 Mrec/s replay with reader p99.9 under 9 us -- the plain-store hot
  path behaves under concurrent replay and consistent reads.
- Full solution builds clean (`TreatWarningsAsErrors`); `dotnet format`
  clean on all changed files.

## 6. TODOs

- [ ] Drift barrier vs transaction/custom-procedure replay. A thread inside a
      synchronized replay scope (transaction group, custom procedure) can
      park at a drift round while its peers are parked at the transaction
      `LeaderBarrier`, and a custom-procedure coordinator updating other
      sublogs' keys can arrive at the barrier for participants it does not
      own. Both resolve only by the synchronization timeout. Intended fix:
      thread-scoped arrival suppression around `ProcessTransactionGroup` and
      `ProcessSynchronizedOperation` (scopes must remain synchronous).
- [ ] Channel mode (`AofReplayTaskCount > 1`) routes `TransactionHeader`
      records through `GetReplayTaskIdx == -1`, an unguarded index;
      transaction replay does not work in channel mode (pre-existing).
- [ ] Drift-round vs batch-barrier timeout in channel mode: a round whose
      target lies beyond a task's current ring batch can strand the batch
      until timeout (pre-existing; the round lifecycle needs a
      batch-boundary-aware abort).
- [ ] Batch-read validation (`AfterConsistentReadKeyBatch`) uses per-key
      sketch slots as change detectors; a concurrent record whose raw stamp
      is small due to stamp/append inversion could evade the redo check.
      Needs a dedicated correctness pass (pre-existing).
- [ ] The pulse's strict timestamp bound assumes one counter tick is shorter
      than an append-then-observe dependency chain. The margin is wide on the
      native TSC path and on Linux, thin on Windows QPC; if multi-log on
      Windows without the native library becomes a target, revisit.
