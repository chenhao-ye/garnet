# Redundant Key Hash Computation Analysis

## Background

Two hash functions are used on keys across the codebase. They share the same underlying computation:

| Hash API | Definition | Returns |
|----------|-----------|---------|
| `GarnetLog.HASH(key)` | `GarnetKeyComparer.Instance.GetHashCode64(key) & long.MaxValue` | Non-negative `long` |
| `storeFunctions.GetKeyHashCode64(key)` | `GarnetKeyComparer.Instance.GetHashCode64(key)` | Signed `long` (may be negative) |

Both call `GarnetKeyComparer.StaticGetHashCode64(key)` which delegates to `SpanByteComparer.StaticGetHashCode64(key.KeyBytes)` (-> `Utility.HashBytes`). The only difference is the `& long.MaxValue` sign-bit mask applied by `GarnetLog.HASH`.

The Tsavorite hash (`GetKeyHashCode64`) is used for hash table bucket lookup in all store operations (Read/Upsert/RMW/Delete). The AOF hash (`GarnetLog.HASH`) is used for physical sublog selection, virtual sublog mapping, and read consistency tracking.

## All Hash Call Sites

### `GarnetLog.HASH` (AOF layer)

| File | Line | Method / Context | Purpose |
|------|------|-----------------|---------|
| `libs/server/AOF/GarnetLog.cs` | 587, 652, 715 | `Enqueue` (3 overloads) | Physical sublog selection on write path |
| `libs/server/AOF/AofProcessor.cs` | 568 | `CanReplay` | Check if record belongs to this replay task |
| `libs/server/AOF/AofProcessor.cs` | 601 | `GetReplayTaskIdx` | Get assigned replay task index |
| `libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs` | 36 | `GetKeySequenceNumber(key)` | Read consistency: get seq# for key |
| `libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs` | 109 | `UpdateVirtualSublogKeySequenceNumber(int, key, seqNum)` | Replay: update key tracking |
| `libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs` | 215 | `BeforeConsistentReadKeyBatch` | Batch read consistency check |
| `libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs` | 234 | `ConsistentUnifiedStoreGetDBKeys.Reader` | SCAN with read consistency |
| `libs/server/Transaction/TransactionManager.cs` | 517 | `ComputeCustomProcShardedLogAccess` | Custom txn procedure: build access vectors |
| `libs/server/Transaction/TransactionManager.cs` | 561 | `ComputeSublogAccessVector` | MULTI-EXEC: build access vectors |
| `benchmark/Resp.benchmark/OfflineBench/AOFBench/AofGen.cs` | 97 | AOF benchmark generator | Test/benchmark only |

### `storeFunctions.GetKeyHashCode64` (Tsavorite layer)

Every store operation computes the hash for hash table bucket lookup. The hash is computed once at the context entry point and then threaded through Tsavorite internals via `OperationStackContext.hei.hash`.

| Context | Methods | File |
|---------|---------|------|
| `BasicContext` | `Upsert`, `RMW`, `Delete`, `Read` | `BasicContext.cs:201-431` |
| `UnsafeContext` | `Upsert`, `RMW`, `Delete`, `Read` | `UnsafeContext.cs:176-366` |
| `TransactionalContext` | `Upsert`, `RMW`, `Delete`, `Read` | `TransactionalContext.cs:525-766` |
| `TransactionalUnsafeContext` | `Upsert`, `RMW`, `Delete`, `Read` | `TransactionalUnsafeContext.cs:284-474` |
| `ConsistentReadContext` | `Read` (+ separate hash for consistency) | `ConsistentReadContext.cs:109,166` |
| `TransactionalConsistentReadContext` | `Read` (+ separate hash for consistency) | `TransactionalConsistentReadContext.cs:149,206` |

All contexts support an `Options.KeyHash` nullable field (`UpsertOptions.KeyHash`, `RMWOptions.KeyHash`, `ReadOptions.KeyHash`, `DeleteOptions.KeyHash`) to pass a pre-computed hash and skip recomputation. Defined in `libs/storage/Tsavorite/cs/src/core/Index/Common/OperationOptions.cs`.

### Tsavorite callback info structs — hash is available in write callbacks

The hash computed by Tsavorite flows into callback info structs, making it accessible to session function implementations:

| Struct | Field | Populated in | File |
|--------|-------|-------------|------|
| `UpsertInfo.KeyHash` | `stackCtx.hei.hash` | `InternalUpsert.cs:122,325` | `CallbackInfos.cs:40` |
| `RMWInfo.KeyHash` | `stackCtx.hei.hash` | `InternalRMW.cs:124,385` | `CallbackInfos.cs:120` |
| `DeleteInfo.KeyHash` | `stackCtx.hei.hash` | `InternalDelete.cs:84` | `CallbackInfos.cs:181` |

These structs are passed to all session function callbacks (`InPlaceWriter`, `PostInitialWriter`, `InPlaceUpdater`, `PostCopyUpdater`, `InPlaceDeleter`, `PostInitialDeleter`, and the `PostXxxOperation` methods). The hash is already used in some callbacks for `watchVersionMap.IncrementVersion(info.KeyHash)` but is **not** currently passed to the AOF `WriteLog*` methods.

### Other hash consumers that reuse pre-computed hashes (no redundancy)

| Consumer | Hash Source | File |
|----------|-----------|------|
| Lock table (`GetBucketIndex`) | `key.KeyHash` from Tsavorite | `OverflowBucketLockTable.cs:25-30` |
| Watch version map (`IncrementVersion`/`ReadVersion`) | `info.KeyHash` from callbacks | `WatchVersionMap.cs:33-41` |
| Virtual sublog replay sketch | `GarnetLog.HASH` result | `VirtualSublogReplayState.cs:37,45,66` |

These all consume a previously computed hash — no redundant computation.

## Other Hash Functions on Key Data (different algorithms, not shareable)

### Cluster Hash Slot (CRC16)

- **Function:** `HashSlotUtils.HashSlot(key)` — CRC16 (polynomial 0x1021) mod 16384
- **File:** `libs/common/HashSlotUtils.cs:60-128`
- **Purpose:** Redis cluster slot assignment for routing
- **Note:** Completely different algorithm from the store/AOF hash. Cannot share results. The `slot` parameter in `SingleKeySlotVerify` supports passing a pre-computed slot to avoid recomputation for multi-key commands.

### Migration Sketch (MurmurHash2)

- **Function:** `HashUtils.MurmurHash2x64A(key)` 
- **File:** `libs/common/HashUtils.cs:193-266`, used in `libs/cluster/Server/Migration/Sketch.cs:40,55,69,84`
- **Purpose:** Bloom filter for tracking key migration status
- **Note:** Different algorithm. Only used during cluster migration, not in the hot path.

### HyperLogLog (MurmurHash2)

- **Function:** `HashUtils.MurmurHash2x64A(element)` — hashes element values, not keys
- **File:** `libs/server/Resp/HyperLogLog/HyperLogLog.cs:654`
- **Purpose:** HyperLogLog cardinality estimation

### PubSub / Object internals (custom hash, magic 40343)

- **Function:** `ByteArrayComparer.GetHashCode` / `ByteArrayWrapper.GetHashCode` — same algorithm as `Utility.HashBytes`
- **Files:** `libs/server/Resp/ByteArrayComparer.cs:31-56`, `libs/server/ByteArrayWrapper.cs:41-67`
- **Purpose:** Dictionary lookup for Hash fields, Set elements, PubSub channels
- **Note:** Same hash algorithm as Tsavorite's `Utility.HashBytes`, but applied to field/element data within objects, not to Redis keys.

## Identified Redundant Paths

### Path A: Replay (HIGH IMPACT)

**Problem:** Hash computed in `CanReplay`/`GetReplayTaskIdx`, discarded, then recomputed in `UpdateKeySequenceNumber` for the same record.

**Call chain (ContinuousBackgroundReplay):**
1. `ReplicaReplayTask.ContinuousBackgroundReplay` -> `AofProcessor.CanReplay(entryPtr, ...)` -> `GarnetLog.HASH(key)` at `AofProcessor.cs:568`
2. If CanReplay returns true -> `ProcessAofRecordInternal` -> `ReplayOp` -> `UpdateKeySequenceNumber(sublogIdx, entryPtr)` -> `ReadConsistencyManager.UpdateVirtualSublogKeySequenceNumber(sublogIdx, key, seqNum)` -> `GarnetLog.HASH(key)` **AGAIN** at `ReadConsistencyManager.cs:109`

**Call chain (ConsumeAndScheduleReplay):**
1. `ReplicaReplayDriver.ConsumeAndScheduleReplay` -> `AofProcessor.GetReplayTaskIdx(entryPtr)` -> `GarnetLog.HASH(key)` at `AofProcessor.cs:601`
2. Record dispatched to worker -> `ReplicaReplayTask.ProcessRecord` -> `ProcessAofRecordInternal` -> `ReplayOp` -> `UpdateKeySequenceNumber` -> `GarnetLog.HASH(key)` **AGAIN**

**Note:** An overload `UpdateVirtualSublogKeySequenceNumber(long keyHash, long sequenceNumber)` already existed at `ReadConsistencyManager.cs:118` but was not used in the replay path.

### Path B: Consistent Reads (MEDIUM IMPACT)

**Problem:** `ConsistentReadContext.Read` computes hash for consistency protocol, then inner `BasicContext.Read` recomputes it for hash table lookup.

**Call chain:**
1. `ConsistentReadContext.Read(key)` -> `GetKeyHash(key)` -> `storeFunctions.GetKeyHashCode64(key)` at `ConsistentReadContext.cs:109`
2. Passes hash to `BeforeConsistentReadCallback(hash)` for consistency protocol
3. Calls `BasicContext.Read(key)` -> `store.ContextRead(key)` -> `storeFunctions.GetKeyHashCode64(key)` **AGAIN** at `Tsavorite.cs:518`

Same pattern in `TransactionalConsistentReadContext.Read()`.

**Note:** `ReadOptions.KeyHash` mechanism already existed to pass pre-computed hash, but was not used by the consistent read contexts.

### Path C: Write Path — Tsavorite store hash + AOF Enqueue hash (FEASIBLE)

**Problem:** Store `Upsert/RMW/Delete` computes `storeFunctions.GetKeyHashCode64(key)` for hash table lookup, then the AOF callback recomputes it via `GarnetLog.HASH(key)` for sublog selection.

**Call chain:**
1. `BasicContext.Upsert(key)` -> `storeFunctions.GetKeyHashCode64(key)` for hash table lookup
2. Hash flows into `UpsertInfo.KeyHash` (populated in `InternalUpsert.cs:122,325`)
3. Tsavorite invokes `PostUpsertOperation(key, ref input, valueSpan, ref upsertInfo, epochAccessor)` at `UpsertMethods.cs:99`
4. `PostUpsertOperation` calls `WriteLogUpsert(key.KeyBytes, ref input, valueSpan, upsertInfo.Version, upsertInfo.SessionID, epochAccessor)` — **does NOT pass `upsertInfo.KeyHash`**
5. `WriteLogUpsert` calls `GarnetLog.Enqueue()` -> `HASH(key)` at `GarnetLog.cs:587` — **second hash**

Same pattern for RMW (`RMWInfo.KeyHash` available at `PostRMWOperation`) and Delete (`DeleteInfo.KeyHash` available at `PostDeleteOperation`).

**Note on sign-bit:** `info.KeyHash` = `GetKeyHashCode64(key)` = `GetHashCode64(key)` (may be negative). `GarnetLog.HASH(key)` = `GetHashCode64(key) & long.MaxValue` (always non-negative). The conversion is trivial: `keyHash & long.MaxValue`.

## Changes Made (branch `chenhaoy/hash-opt`)

### Path A Fix: Replay path hash deduplication

1. **`ReadConsistencyManager.cs`**: Added new overload `UpdateVirtualSublogKeySequenceNumber(int virtualSublogIdx, long keyHash, long sequenceNumber)` that accepts both virtualSublogIdx and pre-computed keyHash.

2. **`AofProcessor.cs`**:
   - `UpdateKeySequenceNumber`: Added optional `long keyHash = -1` parameter. When >= 0, skips recomputation.
   - `CanReplay`: Added `out long keyHash` parameter to return the computed hash.
   - `GetReplayTaskIdx`: Added `out long keyHash` parameter to return the computed hash.
   - `ProcessAofRecordInternal`: Added `long keyHash = -1` parameter, threaded through `ReplayOp` and all `UpdateKeySequenceNumber` calls.

3. **`ReplicaReplayTask.cs`**:
   - Added `long keyHash` field to `ReplayRecord` struct.
   - `ContinuousBackgroundReplay`: Passes `keyHash` from `CanReplay` to `ProcessAofRecordInternal`.
   - `ProcessRecord` / `ProcessRecordWithPrefetch`: Pass `record.keyHash` to `ProcessAofRecordInternal`.

4. **`ReplicaReplayDriver.cs`**: `ConsumeAndScheduleReplay` captures `keyHash` from `GetReplayTaskIdx` and stores it in `ReplayRecord`.

5. **`RecoverLogDriver.cs`**: Multi-task recovery path passes `keyHash` from `CanReplay` to `ProcessAofRecordInternal`.

### Path B Fix: Consistent read hash deduplication

1. **`ConsistentReadContext.cs`**: Both `Read` overloads now pass the pre-computed hash via `ReadOptions.KeyHash` to the inner `BasicContext.Read`, avoiding a second `GetKeyHashCode64` call.

2. **`TransactionalConsistentReadContext.cs`**: Same change — both `Read` overloads pass hash via `ReadOptions.KeyHash` to the inner `TransactionalContext.Read`.

### Path C Fix: Write path hash deduplication

1. **`GarnetLog.cs`**: All 3 `Enqueue` overloads now accept `long keyHash = -1`. When `keyHash >= 0`, uses it directly instead of calling `HASH(key)`.

2. **MainStore `PrivateMethods.cs`**: `WriteLogUpsert`, `WriteLogRMW`, `WriteLogDelete` accept `long keyHash = -1` and forward to `GarnetLog.Enqueue`.

3. **ObjectStore `PrivateMethods.cs`**: Same — all `WriteLog*` methods accept and forward `keyHash`.

4. **UnifiedStore `PrivateMethods.cs`**: Same — all `WriteLog*` methods accept and forward `keyHash`.

5. **All `PostXxxOperation` callbacks** across MainStore, ObjectStore, UnifiedStore now pass `info.KeyHash & long.MaxValue` (sign-bit conversion from Tsavorite's signed hash to AOF's non-negative hash).

## Benchmark Results: `aof_replay_physical`

| Physical Sublogs | Baseline (Mrec/s) | Path A+B (Mrec/s) | Path A+B+C (Mrec/s) | Change (A+B+C vs Baseline) |
|:---:|---:|---:|---:|---:|
| 1 | 1,716.6 | 1,734.8 | 1,702.0 | -0.9% |
| 2 | 2,614.8 | 2,562.1 | 2,756.0 | +5.4% |
| 4 | 4,559.7 | 4,451.0 | 4,848.0 | +6.3% |
| 8 | 8,933.0 | 8,902.6 | 8,595.0 | -3.8% |
| 16 | 15,895.5 | 16,445.9 | 16,624.0 | +4.6% |
| 32 | 24,281.2 | 26,834.9 | 29,439.0 | **+21.2%** |

The `aof_replay_physical` benchmark tests the full write+replay pipeline (enqueue on primary + replay on replica). Path C saves one hash per enqueue on the write side; Paths A+B save hashes on the replay/read side. At 32 sublogs, throughput improved from 24.3M to 29.4M records/s (+21%). Lower sublog counts are within single-sample noise.
