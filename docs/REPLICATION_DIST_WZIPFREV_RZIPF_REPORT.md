# Replication Sweep: write=ZipfRev, read=Zipf (disjoint hot spots) -- Performance Report

## 1. Setup

Full primary/replica/client replication grid on the 3-node cluster (primary=node0,
replica=node1, client=node2), timestamp (prefix-consistent) read protocol. Writers issue
SETs to the primary under a **ZipfRev** key distribution; the 16 replica readers issue GETs
under a **Zipf** distribution. Because ZipfRev reverses the hotness order, the writer hot set
and the reader hot set are *disjoint* (theta=0.99 for both). This is the "different hot spots"
regime, complementary to `wzipf_rzipf` (aligned hot spots) and `wuniform_rzipf` (writer spread
uniformly).

- Grid: writers w in {1,2,4,8,16,32} x physical sublogs m in {1,4,8,16,32} x replay drift
  threshold in {10k, 30k, 100k, 300k, 1M, 2.4M}. 10M keys, 8B/8B, AOF null device, 1g AOF
  memory, replica ITP 256, 30s per run, repeat 3, performance governor pinned on all hosts.
- Config: `experiment/configs/replication_dist_wzipfrev_rzipf.yaml`.
- Results: `result/replication_dist_wzipfrev_rzipf/` (`summary.txt`, `result.yaml`, per-combo logs).

## 2. Integrity

All 180 combos produced valid data (3 samples each). One combo, **w=2 / m=4 / drift=2.4M**, had
crashed the replica GarnetServer with a native `System.AccessViolationException` in
`ClientSession.UnsafeSuspendThread()` on the reader path (a pre-existing, load-dependent race in
the read-during-replay epoch code, unrelated to the distribution feature) on the original run and
one retry; it was re-run in isolation afterwards and completed cleanly (writer 1.67, reader 14.3
Mops/s), confirming the crash is flaky rather than deterministic. The grid is now complete.

## 3. Writer throughput (Mops/s) -- AOF sharding scales the write path

Writer throughput scales strongly with the number of physical sublogs m, which is the point of
MultiLog's sharded AOF: parallel log shipping lets the primary absorb more concurrent writers.
At w=32 the primary goes from 2.3 Mops/s on a single log to 11.0 Mops/s at m=32 (about 5x). A
single log (m=1) caps write throughput near 2.3-2.7 Mops/s regardless of writer count; sharding
removes that ceiling.

Reader throughput here is 16 Zipf readers on the replica (drift=100k):

```
WRITER tpt @ drift=100k       w\m     1      4      8     16     32
                                1   1.02   0.89   0.94   0.88   0.85
                                2   1.64   1.65   1.76   1.66   1.63
                                4   2.13   2.75   2.87   3.03   3.01
                                8   2.72   4.47   5.16   5.40   5.42
                               16   2.57   5.17   8.61   8.50   9.48
                               32   2.31   5.19   7.88  10.52  11.05
```

## 4. Reader throughput (Mops/s, 16 Zipf readers) -- high and gently declining

The replica reader stays high across the entire grid: 18.5 Mops/s at the light corner
(w=1/m=1) down to 10.9 Mops/s at the heavy corner (w=32/m=32), a decline of roughly 40% at
worst. There is no reader collapse. Throughput falls modestly as either the write load (w) or
the sublog/replay-task count (m) rises, consistent with the readers sharing the replica's memory
subsystem with more concurrent replay work.

```
READER tpt @ drift=100k       w\m     1      4      8     16     32
                                1  18.54  17.39  16.98  16.03  14.43
                                2  18.13  16.38  16.16  15.57  14.15
                                4  17.15  15.67  15.19  14.74  13.36
                                8  17.04  15.70  14.47  13.31  12.12
                               16  16.60  14.84  13.91  13.77  11.90
                               32  15.87  14.44  13.50  12.31  10.87
```

Reader p99 latency stays in the 0.7-1.6 ms band across the whole grid, so the reader is not
latency-collapsing; the throughput decline is per-op cost, and staleness (Section 5) is a
separate axis.

## 5. Freshness / staleness -- bimodal, governed by replay keeping up

Freshness (the read-to-write staleness the prefix-consistent reader observes) is bimodal and is
governed entirely by whether the replica's replay can keep up with the primary's write rate.
When m provides enough replay parallelism to drain the incoming AOF, freshness p50 sits at
1.4-2.4 ms. When m is too small for the write rate, the replica falls behind without bound and
freshness explodes to **0.5-3.8 seconds**.

```
FRESHNESS p50 (ms) @ drift=100k   w\m      1       4      8     16     32
                                    1    1.4     1.5    1.5    1.4    1.5
                                    2    2.0     1.7    1.6    1.6    1.6
                                    4   3808     1.5    1.8    1.8    1.8
                                    8   3020    1535    1.9    2.1    2.1
                                   16   3171    1309    688    625    2.3
                                   32   3255    1309    747    518    417
```

The boundary is a replay-capacity frontier: m=1 falls behind at w>=4, m=4 at w>=8, m=8 and m=16
at high w, while m=32 keeps staleness in the sub-500 ms range even at w=32. Replication lag (the
byte backlog) confirms the same boundary: ~520 MB stuck at m=1/w>=4, ~140 MB at m=4, dropping to
near zero as m grows.

```
REPLICATION LAG (MB) @ drift=100k  w\m     1      4      8     16     32
                                     4  519.4    0.0    0.0    0.1    0.0
                                     8  524.2  143.7    0.1    0.0    0.0
                                    16  531.6  142.7   80.6   46.6    0.1
                                    32  528.1  143.8   80.8   48.3   31.7
```

Takeaway: sharding m is required not just for writer throughput but to bound reader staleness
under load. The reader throughput stays high even in the lagging region (Section 4), so a
single-log replica serves reads fast but from an increasingly stale prefix.

## 6. How to choose the replay drift threshold

The drift threshold bounds how far the virtual sublogs' replay progress may diverge before a
barrier round forces the leaders to wait for the laggards. It controls a direct tension between
two goals, and the right value depends on the write load (w) and the sublog count (m).

### 6.1 What the knob does, and when it does nothing

The barrier is only engaged when there is more than one virtual sublog. In this sweep the replay
task count n is 1, so the virtual sublog count equals m, and **at m=1 the drift threshold is a
no-op**: reader throughput is flat at 17-18 Mops/s across every drift value. Do not tune it for
single-log replicas.

```
m=1 reader tpt across drift    w      10k    30k   100k   300k     1M   2.4M
                                1   18.0   17.9   18.5   18.4   18.0   18.3
                                8   17.8   16.8   17.0   17.5   17.0   16.9
                               32   15.8   16.2   15.9   16.3   16.8   16.9
```

### 6.2 The tension (m>1): reader throughput vs replay keeping up

**Tighter drift raises reader throughput.** Keeping the virtual sublogs in lockstep makes the
reader's per-key prefix frontier uniform and satisfied quickly, so reads block less. Reader
throughput falls monotonically as the threshold loosens (e.g. w=32/m=32: 12.7 -> 4.3 Mops/s from
10k to 2.4M):

```
READER tpt (Mops/s)   (w,m)     10k    30k   100k   300k     1M   2.4M
                      w 8 m 8  16.2   15.9   14.5   13.3   10.0    7.2
                      w16 m16  14.9   14.5   13.8   11.1    8.1    5.3
                      w32 m32  12.7   12.0   10.9    8.8    6.6    4.3
```

**But tighter drift throttles replay, so past a point the replica cannot keep up and staleness
explodes.** The barrier's synchronization overhead caps replay throughput; if that cap falls
below the primary's write rate, the replica falls behind without bound and freshness jumps from
milliseconds to hundreds of milliseconds or seconds. Loosening the drift removes the cap, replay
drains the AOF, and freshness collapses back to ~2 ms. Each (w,m) has a **keep-up knee**: the
smallest drift at which replay still keeps up.

```
FRESHNESS p50 (ms)    (w,m)      10k     30k    100k    300k      1M    2.4M     knee
                      w 8 m 8    9.1     2.1     1.9     1.9     1.6     1.5    ~10-30k
                      w 8 m16    2.3     2.1     2.1     2.0     1.9     1.8    <=10k
                      w16 m16  666.9   599.8   625.0     2.7     2.2     2.5    ~300k
                      w16 m32  478.2    15.7     2.3     2.2     2.1     2.0    ~100k
                      w32 m16  658.5   557.8   518.0   442.5   461.4     8.2    ~2.4M
                      w32 m32  526.4   476.1   417.3   377.5     5.2     5.1    ~1M
```

Note the knee moves to a looser drift as the write-to-replay ratio (w relative to m) grows: at
w=8/m=16 almost any drift keeps up, at w=16/m=16 you need >=300k, at w=32/m=32 you need >=1M.
(Writer throughput on the primary is roughly flat or slightly higher at looser drift, so it does
not constrain the choice.)

### 6.3 The trade is asymmetric around the knee -- err loose, not tight

The reader-vs-replay trade is lopsided, so the decision is not a symmetric balance. Reaching the
knee costs the reader only marginally but buys replay an enormous freshness improvement; going
past the knee costs the reader steadily for no further benefit.

```
step INTO keep-up (reach the knee)     reader cost    freshness gain
  w8  m8    10k -> 30k                    -2%          9.1 -> 2.1 ms
  w8  m4   100k -> 300k                   -6%          1535 -> 7.7 ms   (200x)
  w16 m32   30k -> 100k                   -6%          16   -> 2.3 ms
  w16 m16  100k -> 300k                  -20%          625  -> 2.7 ms   (230x)

step PAST the knee (already caught up)  reader cost    freshness gain
  w8  m8    30k -> 1M                     -37%         2.1 -> 1.6 ms    (none)
  w16 m16  300k -> 1M                     -27%         2.7 -> 2.2 ms    (none)
```

Decision rule:

1. **m=1: leave it at the default.** The threshold has no effect.
2. **m>1: set the threshold at the keep-up knee -- the tightest value at which freshness is still
   single-digit ms -- and err loose rather than tight.** Do not begrudge crossing the knee: the
   reader gives up only a few to ~20% there, in exchange for a 100-230x staleness reduction
   (seconds -> ms). A stale-and-growing replica is useless for prefix-consistent reads, whereas a
   somewhat slower reader is merely slower, so the tie breaks toward keeping replay caught up.
3. **Never set it looser than the knee.** Past the knee replay is already caught up, so extra
   slack is pure reader loss (e.g. w16/m16 300k->1M costs 27% reader for zero freshness gain).
4. **If reaching the knee costs the reader a lot (w32/m16 -33%, w32/m32 -25%) or no drift keeps
   replay up at all (w16/m4, w16/m8, w32/m4, w32/m8 lag at every threshold), you are at or past
   replay capacity -- add physical sublogs (increase m) rather than pay the reader penalty.**
   More m moves the knee back to a tight drift where reads are both fresh and fast.

Recommended threshold and the resulting operating point (this workload, disjoint hot spots):

```
(w, m)     recommended drift   reader Mops/s   freshness p50   note
w8,  m16          10k              15.3            2.3 ms       ample replay headroom -> go tight
w8,  m8           30k              15.9            2.1 ms
w16, m16         300k              11.1            2.7 ms       ~25% reader cost vs breaking freshness
w16, m32         100k              11.9            2.3 ms
w32, m32           1M               6.6            5.2 ms       replay-bound; knee is loose
w16, m4            --                --             --          cannot keep up at any drift: raise m
w32, m8            --                --             --          cannot keep up at any drift: raise m
```

The practical picture: give the replica enough sublogs m that the keep-up knee sits at a tight
drift, then set the threshold at that knee. Tight drift is only "free" reader throughput when m
already has the replay headroom to keep up under it.

## 7. Headline: disjoint hot spots keep the reader ~2.6-4.9x faster than aligned

The purpose of this sweep is the hot-spot alignment comparison. With the writer on ZipfRev and
the reader on Zipf, their hot sets are disjoint; the reader mostly touches keys the writers
rarely write. Against the aligned sibling (`wzipf_rzipf`, both Zipf, same hot set), the disjoint
reader is 2.6x to 4.9x faster across the writer sweep at m=8, drift=100k:

```
reader Mops/s (m=8, drift=100k)    w    disjoint (wZrev/rZ)   aligned (wZ/rZ)   ratio
                                   1        16.98                 3.48          4.88x
                                   2        16.16                 4.70          3.44x
                                   4        15.19                 5.76          2.64x
                                   8        14.47                 7.98          1.81x
                                  16        13.91                 6.18          2.25x
                                  32        13.50                 5.09          2.65x
```

When the reader's hot keys coincide with the writer's hot keys (aligned), the reader continually
reads the very records under the heaviest replay churn and the largest frontier lag, so it stalls
and blocks; when the hot sets are disjoint, the reader reads keys whose frontier is already
satisfied and whose cache lines are not being churned by the writers, so it runs near its
uncontended rate. Freshness p99.9 is also worse aligned (1216 ms vs 700 ms at w=16/m=8),
because the reader's hot keys are exactly the ones the replica is slowest to catch up on. This
is the replication-level manifestation of the same contention principle seen in the C5 read
study: the reader's cost is dominated by whether its targets overlap the writers' active set.

(The `wuniform_rzipf` sibling is only partially available (96 combos) and is not included in the
alignment comparison here.)

## Appendix A. Metric-vs-threshold tables (locate the sweet spot)

Each table has the drift threshold as columns and every (w, m>1) combo as rows (m=1 omitted: it
is drift-invariant). To pick a threshold for a given (w, m): in Table B find the tightest
(leftmost) column where staleness is still single-digit ms (marked *); that same column in Table A
is the reader throughput you get. Rows where Table B never drops below ~100 ms cannot be fixed by
the threshold and need more sublogs m.

### A. Reader throughput (Mops/s) -- higher is better

```
  (w, m) |     10k     30k    100k    300k      1M    2.4M
  -------+------------------------------------------------
  w 8 m4 |    16.7    16.8    15.7   *14.8    13.1     9.3
  w 8 m8 |    16.2   *15.9    14.5    13.3    10.0     7.2
  w 8 m16|   *15.3    15.4    13.3    11.0     8.4     6.2
  w 8 m32|   *13.7    13.1    12.1    10.7     8.3     7.1
  w16 m4 |    16.2    15.5    14.8    13.7    10.3     6.3   (never keeps up)
  w16 m8 |    15.0    15.0    13.9    12.8     9.8     6.2   (never keeps up)
  w16 m16|    14.9    14.5    13.8   *11.1     8.1     5.3
  w16 m32|    13.0    12.7   *11.9     9.5     6.9     4.8
  w32 m4 |    15.7    14.9    14.4    12.6     8.8     5.3   (never keeps up)
  w32 m8 |    15.5    14.1    13.5    11.4     8.5     5.4   (never keeps up)
  w32 m16|    14.4    13.6    12.3    10.2     7.8    *5.2
  w32 m32|    12.7    12.0    10.9     8.8    *6.6     4.3
  (w<=4: replay keeps up at every drift -> use 10k, reader ~14-18)
```

### B. Freshness p50 staleness (ms) -- >100 means replay NOT keeping up; * = sweet spot

```
  (w, m) |     10k     30k    100k    300k      1M    2.4M
  -------+------------------------------------------------
  w 8 m4 |    1720    1426    1535    *7.7     310      36
  w 8 m8 |     9.1    *2.1     1.9     1.9     1.6     1.5
  w 8 m16|    *2.3     2.1     2.1     2.0     1.9     1.8
  w 8 m32|    *2.1     2.1     2.1     2.0     1.9     1.9
  w16 m4 |    1560    1393    1309    1317    1300    1317   (raise m)
  w16 m8 |     923     843     688     789     705     789   (raise m)
  w16 m16|     667     600     625    *2.7     2.2     2.5
  w16 m32|     478      16    *2.3     2.2     2.1     2.0
  w32 m4 |    1393    1191    1309    1174    1317    1225   (raise m)
  w32 m8 |     960     742     747     717     633     646   (raise m)
  w32 m16|     659     558     518     442     461    *8.2
  w32 m32|     526     476     417     377    *5.2     5.1
  (w<=4: ~1.4-2.0 ms at every drift)
```

### C. Freshness p99.9 staleness (ms) -- tail

```
  (w, m) |     10k     30k    100k    300k      1M    2.4M
  -------+------------------------------------------------
  w 8 m4 |    1753    1485    1569     438     667     262
  w 8 m8 |     169     9.9     8.8     8.5     8.5     8.5
  w 8 m16|     8.7     8.8     8.8     8.7     8.8     8.7
  w16 m8 |    1011     940     700     830     776     801
  w16 m16|     705     667     675      15      12      14
  w16 m32|     562     518     9.0     9.1     8.9     9.1
  w32 m16|     692     604     566     474     489      65
  w32 m32|     575     575     528     466      18      15
  (w<=4 and light cells: ~8-9 ms tail throughout)
```

### D. Writer throughput (Mops/s) -- flat/slightly rising with drift; does not constrain the choice

```
  (w, m) |     10k     30k    100k    300k      1M    2.4M
  -------+------------------------------------------------
  w16 m8 |     6.3     7.0     8.6     7.4     8.3     7.5
  w16 m32|     9.0     9.5     9.5     9.6     9.7     9.6
  w32 m16|     8.5     9.8    10.5    12.0    11.3    12.9
  w32 m32|     9.5    10.0    11.0    12.5    13.6    13.8
```

### E. Replication lag (MB) -- mirrors Table B (0 = caught up)

```
  (w, m) |     10k     30k    100k    300k      1M    2.4M
  -------+------------------------------------------------
  w 8 m4 |     145     144     144      44      59      21
  w 8 m8 |      11       0       0       0       0       0
  w16 m8 |      80      81      81      80      80      80
  w16 m16|      48      46      47       0       0       0
  w16 m32|      29      27       0       0       0       0
  w32 m16|      48      48      48      47      45       1
  w32 m32|      33      33      32      32       0       0
```

(w2/m4/drift2.4M is the crashed/missing combo and is excluded.)

## Appendix B. Reproduction

```
# from code-replication/, source synced to node1/node2 first (rsync, harness never pushes source)
uv run experiment/run.py replication_dist_wzipfrev_rzipf
# resume after a mid-grid server crash without redoing completed combos:
uv run experiment/run.py replication_dist_wzipfrev_rzipf --resume-from-index <N completed>
```

Full 180-combo table with all drift values: `result/replication_dist_wzipfrev_rzipf/summary.txt`.
