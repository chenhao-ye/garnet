# Read Transaction Protocol for MultiLog Replicas

## 0. Terminology

We use the terminology as defined in the paper.

| Name     | Full name                             | Code alias                               |
|----------|---------------------------------------|------------------------------------------|
| `KRT(K)` | key replayed timestamp for key `K`    | key sequence number                      |
| `LRT(L)` | log replayed timestamp for sublog `L` | sublog max sequence number               |
| `SCT`    | session current timestamp             | maximum session sequence number (`mSSN`) |

### validity window

Recall in the single-key read protocol, we define **validity window**:
For each key `K` on virtual sublog `L`, the validity window of `K`'s current value is `(KRT(K), LRT(L))`. The bounds are exclusive, i.e., `KRT(K) < t < LRT(L)`, because timestamps are not unique, and we cannot differentiate the order of mutations that happen at the same timestamp.

```text
   logical time -->
            KRT(K)              LRT(L)
              |                    |
   -----------x------------------->|
              ^                    ^
            current             applied up to here
            value set           (no other new version of this key up to here)
              (======= valid ======)
```

## 1. Goal

A read transaction reads a batch of keys `{K_1, ..., K_n}` and must return values consistent with the primary's state at **a single logical timestamp** `T_txn` (i.e., atomicity).

In validity-window terms, the snapshot invariant is: there is a single logical timestamp `T_txn` that lies strictly inside the validity window of every key in the batch. That is, for each key `K_i`, `KRT(K_i) < T_txn < LRT(L_i)`.

Equivalently, `T_txn` must strictly exceed the largest `KRT` in the batch and be strictly below the smallest `LRT` of the participating sublogs:

```text
max_i KRT(K_i)   <   T_txn   <   min_i LRT(L_i)
```

Operationally we use `SCT := max_i KRT(K_i)` as the lower bound; once the protocol has ensured that every `LRT(L_i) > SCT`, the batch read is atomic.

```text
   K_1 validity:    (==================)
   K_2 validity:             (===============)
   K_3 validity:                  (==============)
                                  |^^^^|
                                   overlap: any time in this window is valid to read
                                   set SCT as the lower bound
```

If the validity windows do not share a common point, no snapshot exists at any `t`. The reader either waits for replay to advance and create the overlap or reports failure:

```text
   K_1 validity:    (=======)
   K_2 validity:                  (==============)
                               X
                  no overlap, no snapshot exists
```

## 2. Existing Protocol

In short, the existing read transaction protocol on replica does not have a single T_txn to execute reads.
It is only correct if the involved keys are only modified by write transactions, but any point-writes can corrupt the atomicity of read transactions.

### 2.1 Pseudocode of Existing Read Transaction Protocol

The current batch read protocol (`BeforeConsistentReadKeyBatch`, `BasicContext.ReadWithPrefetch`, `AfterConsistentReadKeyBatch`):

```text
ReadBatch(session, K_1, ..., K_n):
retry:
    S := copy(session)                  # make a copy just in case of retry

    # -- PREPARE --
    for i := 1..n:
        L_i := get_sublog(K_i)
        if S.lastL != L_i:
            while S.SCT  >=  LRT(L_i):
                wait_for_advance(L_i)
        S.lastL := L_i
        S.SCT := max(S.SCT, KRT(K_i))   # advance session timestamp as we walk

    # -- READ --
    for i := 1..n:
        V_i := store_read(K_i)          # acquires per-key latch internally

    # -- VALIDATE --
    if exists i such that KRT(K_i) > S.SCT:
        goto retry                      # must retry from the beginning

    ### SIDE NOTE:
    ### may be a bug: this copy of S is not applied back to session

    return (v_1, ..., v_n)
```

### 2.2 Why This Does Not Read at a Single `T_txn`

`S.SCT` is the running maximum of `KRT(K_i)`, whose value is incremented when iterating over keys.
*After `S.SCT` is finalized, it is not validated against every key's `LRT` to confirm it falls within an overlap of all keys' validity windows (i.e., `S.SCT` is still smaller than every key's `LRT`).*
The current validation phase is only to validate against KRT again to ensure no concurrent replay updating the key.

```text
                                    K_A := new_a         K_B := new_b
                                        |                    |
   logical time:    o-------------------x--------------------x------->
```

To read K_A and K_B at the single timestamp, it can return `{old_a, old_b}` (for t_txn < T_a), `{new_a, old_b}` (for T_a < t_txn < T_b), `{new_a, new_b}` (for t > T_b).

On the primary, two sublogs (each holds the records that hash to it):

```text
   L_A on primary:  o-------------------x---------------------------->
                                        ^T_a (K_A := new_a)

   L_B on primary:  o----------------------------------------x------->
                                                             ^T_b (K_B := new_b)
```

Replica state: `L_A` is slow and has not yet applied the update to `K_A`; `L_B` has applied the update to `K_B`.

```text
   L_A on replica:  o-------->|   // LRT(L_A) < T_a; K_A is still old_a
                              ^LRT(L_A)

   L_B on replica:  o----------------------------------------x------>|  // LRT(L_B) > T_b
                                                             ^T_b (K_B := new_b applied)
```

The validity windows of the current values `{old_a, new_b}` have no overlap, so if a transactional read on `K_A` and `K_B` sees these two values, it should detect non-atomicity and reject the values:

```text
   K_A validity:    (=========)
                    |         |
                    ^T_old    ^LRT(L_A) < T_a

   K_B validity:                                              (======)
                                                              |      |
                                                              ^T_b   ^LRT(L_B)
                        no overlap -- no snapshot exists
```

The existing read transactional protocol allows such a read:

```text
PREPARE:
  K_A:  lastL nil => skip check
        S.SCT := KRT(K_A) = T_old;   lastL := L_A
                                                    # now S.SCT is T_old
  K_B:  lastL = L_A != L_B => check
            S.SCT < LRT(L_B) => check passes w/o waiting
                                                    # advance S.SCT to T_b
                                                    # however, this S.SCT = T_b is outside
                                                    # old_a's validity window (T_old, LRT(L_A))
READ:    K_A = old_a,   K_B = new_b
RETURN:  {K_A = old_a, K_B = new_b}
```

The returned values `{old_a, new_b}` is not a valid state on the primary.
Recall the only valid results are: `{old_a, old_b}`, `{new_a, old_b}`, `{new_a, new_b}`.

### 2.3 Locks on Txn Replay Are Only a Partial Fix

The existing transaction replay protocol holds locks until every participating sublog finishes replay.
Such lock holding can help align the validity windows of the transaction's keys on a single timestamp `T_X`, but it does not ensure the read protocol sees the system state atomically.
Note that a point-write is equivalent to a single-key transaction, so the example above of two point writes can be replaced with two write transactions.
Even though each write transaction is itself committed atomically on the replica, the reader still sees an inconsistent state.

More fundamentally, we argue that replaying a write transaction should not do anything special beyond replaying all records as if they were independent writes. No cross-sublog waiting, no extra lock holding.

Consider the following two cases:

- **Case 1**: a multi-key transaction `X` writes `K_A := new_a` and `K_B := new_b`. The commit record is stamped with `T_X`.
- **Case 2**: two independent clients issue point writes `K_A := new_a` and `K_B := new_b`, and by coincidence both records land at the same logical timestamp `T_X`.

The two cases above are equivalent as if two writes happen exactly at `T_X`. Therefore, the replay protocol should not treat them differently. Replaying txn log records (case 1) should the same as replaying two regular log records stamped at `T_X` (case 2).

The transactional read protocol requires a different fix instead of relying on txn-replay threads to hold locks.

## 3. Proposed Read Transaction Protocol

We now propose a different read protocol that ensures the atomicity of a read transaction.
The goal is the same as before: read every key at a single timestamp `T_txn` (atomicity), with `T_txn > SCT` (prefix-consistency against previous session reads).

The protocol is still based on the validity window.
It tracks a time window for `T_txn`, denoted as `(T_min, T_max)`. The transaction fails and must retry if such a window collapses (i.e., `T_min >= T_max`, so no `T_txn` possible).

```text
ReadBatch(session, K_1, ..., K_n):
    # -- PREPARE --
    # wait until every participating sublog has replayed past SCT for prefix-consistency
    for i := 1..n:
        L_i := get_sublog(K_i)
        wait until LRT(L_i) > SCT
    # now we only need to care about read atomicity

  retry:
    T_min := max(SCT, max( KRT(K_i) for i := 1..n ))   # lower bound
    T_max := +inf                                      # upper bound

    # -- READ READY KEYS --
    # remember keys whose sublogs are not yet past T_min; read them later
    pending_sublogs := {}    # maps L to list of (i, K)
    for i := 1..n:
        L_i := get_sublog(K_i)
        T_L := LRT(L_i)
        if T_L > T_min:                                 # ready to read
            val_buffer[i] := store_read(K_i)
            T_max := min(T_max, T_L)                    # tighten upper bound
            T_min := max(T_min, KRT(K_i))               # tighten lower bound
            if T_min >= T_max: goto retry               # range collapsed
        else:
            pending_sublogs[L_i].add(i, K_i)

    # -- READ PENDING KEYS --
    while pending_sublogs is not empty:
        wait on L_j in pending_sublogs: wake up if ( T_L := LRT(L_j) ) > T_min:
            for i, K_i in pending_sublogs[L_j]:
                val_buffer[i] := store_read(K_i)
                T_max := min(T_max, T_L)
                T_min := max(T_min, KRT(K_i))
                if T_min >= T_max: goto retry
            pending_sublogs.remove(L_j)

    # -- UPDATE SESSION STATE --
    # any t in (T_min, T_max) is a valid T_txn; use T_min as the new SCT lower bound
    S.SCT := T_min
    S.lastL := any participating L_i  # we can actually make lastL a bitmask

    return val_buffer
```

The tightening is straightforward: for each `store_read` returning `V_i`, the value's validity window is `(KRT(K_i), LRT(L_i))` at read time. Acquiring `LRT` before the read gives a conservative `T_max` bound (LRT only grows, so `T_L` is at most the actual read-time LRT). Acquiring `KRT` after the read catches concurrent replay updates: if a replay applied a new record on `K_i` concurrently with store_read, `KRT(K_i)` rises, and `T_min` rises with it.
If `T_min >= T_max`, no `T_txn` is consistent with every read so far; we retry with fresh samples.

The retry restarts the whole batch. As a future optimization, the abort can be a partial redo instead of redoing everything, but that requires more metadata bookkeeping (basically remembering every key's read window) to determine which values are still usable. *(Probably not worth the complexity, given the gain is only at a concurrent race.)*

Also note the wait on `pending_sublogs`: for a lower abort rate under contention, it should not be a sequential scan (wait `L_1`, then wait `L_2`, ...). It should wake up for *any* sublog whose `LRT` goes past `T_min` and read the values immediately, before those values are overwritten by replay threads.

### 3.1 Discussion and Analysis

In addition to the correct read atomicity, the proposed new read protocol has some very nice properties:

**1. No special txn-replay protocol with blocking/lock holding**: A write transaction is replayed just like a batch of log records stamped at the commit timestamp. One sublog does not need to wait on other participating sublogs, nor does it need to hold locks until all participating sublogs are done. Replay threads are never blocked by readers.

**2. The atomic reads do not need a wall-clock-consistent replica image**: We read a value whenever it is ready; that value can be overwritten by replay threads after our read. We don't need a wall-clock moment when all keys hold their snapshot values simultaneously. Everything on the replica remains barrier-free.

**3. No deadlock issues at all**: We read keys one by one, with only a bucket latch protecting each individual read. We never hold a batch of keys' locks together. Atomicity is protected by timestamps, not by locks.
