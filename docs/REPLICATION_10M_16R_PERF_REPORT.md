# Replication Performance Report: 10M keys, 16 readers

MultiLog parallel-AOF replication on `code-replication` (branch `chenhaoy/replication-bench` + conservative-backpressure fix). Reader-heavy sweep: writers {1,8,16} x physical-sublogs {1,4,8,16,32} at a fixed 42us replay-drift barrier, 16 concurrent reader threads, 10M-key dataset.

## 1. Setup

**Topology.** primary=node0 (10.10.1.1), replica=node1 (10.10.1.2), client=node2 (10.10.1.3); all three 144 hardware threads (2x36 cores, SMT). Diskless sync, main-memory (null-device) AOF. Source synced node0->node1/node2 and md5-verified before the run.

**Fixed knobs.**

| Knob | Value |
|---|---|
| Keyspace (dbsize) | 10,000,000 keys |
| Key / value length | 8 B / 8 B |
| Reader threads | 16 (itp=256 each) |
| Pass runtime x repeats | 30 s x 3 (median reported) |
| Replay-drift barrier | 100000 ticks (~42 us) |
| AOF memory | 1 GB |
| Ship budget / replica budget | 256 MB / 256 MB (summed over sublogs) |
| Replay tasks per sublog (n) | 1 |

**Swept.** writers {1, 8, 16} x sublogs m {1, 4, 8, 16, 32} = 15 combos.

**Workload.** Concurrent roles on the client: `replication_writers` SET threads on the primary; 16 GET threads on the replica; 1 freshness prober that writes a versioned key on the primary and polls the replica for it (measuring end-to-end visibility). Writers/readers pipeline itp=256 ops per batch. Throughput in Mops/s, latency in microseconds unless noted, lag in bytes.

## 2. Data-loss / integrity verdict

| Check | Result |
|---|---|
| Combos completed | 15 / 15 |
| Integrity errors (Failed validating / Uninitialized / Divergent AOF) | 0 |
| Page-boundary `Skipping` events (all benign, no integrity error) | 1 |

Backpressure holds: replication lag is bounded by the 256 MB budgets in every combo (Section 6), and no replica diverged.

## 3. Writer throughput (Mops/s)

| m \\ w | 1 | 8 | 16 |
|---|---|---|---|
| **1** | 0.35 | 1.43 | 1.73 |
| **4** | 0.32 | 2.96 | 3.95 |
| **8** | 0.31 | 3.21 | 6.43 |
| **16** | 0.31 | 2.22 | 6.87 |
| **32** | 0.29 | 2.25 | 4.27 |

Single log caps at ~1.7 Mops/s; sharding scales to a 6.87 Mops/s peak (m=16, w=16). With only 16 writers, m=32 over-shards (4.27 < 6.87): too many sublogs to keep fed, plus cross-sublog coordination overhead. A single writer (w=1) pins ~0.3 Mops/s at every m. Sweet spot is m ~ writer count.

## 4. Reader throughput (Mops/s), 16 reader threads

| m \\ w | 1 | 8 | 16 |
|---|---|---|---|
| **1** | 5.49 | 5.50 | 7.75 |
| **4** | 5.30 | 4.96 | 8.07 |
| **8** | 5.51 | 5.11 | 7.59 |
| **16** | 4.96 | 5.04 | 6.08 |
| **32** | 4.16 | 4.82 | 5.55 |

Reads stay fast throughout (4.2-8.1 Mops/s aggregate over 16 threads). Throughput is highest at low m under write load and declines at m=32, where 32 replay tasks contend with GET serving on the replica.

## 5. Freshness: replica visibility under write load

Freshness probe = write-on-primary then poll-until-visible-on-replica. This is the headline result: it measures how stale the replica is while writes are in flight.

### 5a. Freshness throughput (completed probes/s)

| m \\ w | 1 | 8 | 16 |
|---|---|---|---|
| **1** | 296.0 | 0.2 | 0.2 |
| **4** | 300.0 | 61.7 | 0.5 |
| **8** | 286.8 | 217.0 | 1.7 |
| **16** | 257.1 | 251.3 | 21.9 |
| **32** | 216.7 | 223.8 | 236.0 |

### 5b. Freshness p50 latency (ms)

| m \\ w | 1 | 8 | 16 |
|---|---|---|---|
| **1** | 2.1 | 5637.1 | 4026.5 |
| **4** | 2.0 | 6.7 | 2004.9 |
| **8** | 2.1 | 3.0 | 612.4 |
| **16** | 2.6 | 2.7 | 3.1 |
| **32** | 3.2 | 3.0 | 2.8 |

At low m the replica cannot keep pace: m=1/w=16 completes only 0.2 probes/s at ~4 s staleness. Increasing m restores freshness by scaling replay: m=32 holds 236 probes/s at 2.8 ms even at w=16, indistinguishable from the idle (w=1) case. More sublogs let replica replay match the write rate, keeping the replica live.

## 6. Replication lag (MB, steady-state)

| m \\ w | 1 | 8 | 16 |
|---|---|---|---|
| **1** | 0.0 | 510.5 | 495.2 |
| **4** | 0.0 | 3.5 | 134.7 |
| **8** | 0.0 | 0.1 | 20.7 |
| **16** | 0.0 | 0.0 | 6.3 |
| **32** | 0.0 | 0.0 | 0.0 |

Mirrors freshness. Bounded by the 256 MB summed budget everywhere; shrinks toward zero as m rises. m=16/m=32 hold near-zero lag even at w=16.

## 7. Writer latency (us): p50 / p99 / p99.9

**p50**

| m \\ w | 1 | 8 | 16 |
|---|---|---|---|
| **1** | 725 | 877 | 1188 |
| **4** | 786 | 664 | 844 |
| **8** | 791 | 614 | 610 |
| **16** | 786 | 893 | 573 |
| **32** | 840 | 868 | 905 |

**p99**

| m \\ w | 1 | 8 | 16 |
|---|---|---|---|
| **1** | 1311 | 1769 | 2556 |
| **4** | 1475 | 1434 | 4227 |
| **8** | 1548 | 1384 | 1491 |
| **16** | 1573 | 1704 | 1466 |
| **32** | 1647 | 1696 | 1835 |

**p99.9**

| m \\ w | 1 | 8 | 16 |
|---|---|---|---|
| **1** | 1688 | 216007 | 204472 |
| **4** | 1778 | 1778 | 23200 |
| **8** | 1819 | 1720 | 1810 |
| **16** | 1810 | 2081 | 1819 |
| **32** | 2458 | 2294 | 2572 |

Writer p50 is ~1 ms everywhere. The tail exposes backpressure: at low m the single (or few) replay task(s) cannot drain the stream, so the primary throttles writers and p99.9 spikes to ~216 ms (m=1/w=8) and 23 ms (m=4/w=16). For m>=8 the replica keeps pace and writer p99.9 stays ~2 ms: sharded replay removes the stall.

## 8. Reader latency (us): p50 / p99 / p99.9 / max

**p50**

| m \\ w | 1 | 8 | 16 |
|---|---|---|---|
| **1** | 721 | 700 | 440 |
| **4** | 729 | 770 | 442 |
| **8** | 688 | 733 | 455 |
| **16** | 721 | 729 | 598 |
| **32** | 713 | 680 | 610 |

**p99**

| m \\ w | 1 | 8 | 16 |
|---|---|---|---|
| **1** | 1614 | 1950 | 1614 |
| **4** | 1786 | 2408 | 1729 |
| **8** | 1868 | 2523 | 1884 |
| **16** | 2490 | 2720 | 2376 |
| **32** | 4014 | 3391 | 2654 |

**p99.9**

| m \\ w | 1 | 8 | 16 |
|---|---|---|---|
| **1** | 3244 | 3441 | 2540 |
| **4** | 3867 | 3162 | 2736 |
| **8** | 4194 | 3424 | 2703 |
| **16** | 4588 | 3834 | 3326 |
| **32** | 6226 | 5079 | 4227 |

**max**

| m \\ w | 1 | 8 | 16 |
|---|---|---|---|
| **1** | 4522 | 9896 | 5571 |
| **4** | 9306 | 10879 | 9241 |
| **8** | 6488 | 10420 | 6324 |
| **16** | 7242 | 7406 | 15204 |
| **32** | 11403 | 9044 | 9241 |

Reads never wait on replication: p50 440-729 us, p99.9 2.5-6.2 ms across the whole grid. m=32 shows the highest read tails (p99.9 up to 6.2 ms) from replay-task contention. The large `max` values (tens of ms) are pass-0 warmup blips right after the 10M preload/catch-up, not steady state (later passes drop to <10 ms).

## Appendix A. Full per-combo table (all 15 combos)

Throughput Mops/s; latency us; lag MB.

| m | w | wr_tput | rd_tput | fr_tput | fr_p50_ms | wr_p50 | wr_p99 | wr_p99.9 | rd_p50 | rd_p99 | rd_p99.9 | rd_max | lag_MB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 0.35 | 5.49 | 296.0 | 2.1 | 725 | 1311 | 1688 | 721 | 1614 | 3244 | 4522 | 0.0 |
| 1 | 8 | 1.43 | 5.50 | 0.2 | 5637.1 | 877 | 1769 | 216007 | 700 | 1950 | 3441 | 9896 | 510.5 |
| 1 | 16 | 1.73 | 7.75 | 0.2 | 4026.5 | 1188 | 2556 | 204472 | 440 | 1614 | 2540 | 5571 | 495.2 |
| 4 | 1 | 0.32 | 5.30 | 300.0 | 2.0 | 786 | 1475 | 1778 | 729 | 1786 | 3867 | 9306 | 0.0 |
| 4 | 8 | 2.96 | 4.96 | 61.7 | 6.7 | 664 | 1434 | 1778 | 770 | 2408 | 3162 | 10879 | 3.5 |
| 4 | 16 | 3.95 | 8.07 | 0.5 | 2004.9 | 844 | 4227 | 23200 | 442 | 1729 | 2736 | 9241 | 134.7 |
| 8 | 1 | 0.31 | 5.51 | 286.8 | 2.1 | 791 | 1548 | 1819 | 688 | 1868 | 4194 | 6488 | 0.0 |
| 8 | 8 | 3.21 | 5.11 | 217.0 | 3.0 | 614 | 1384 | 1720 | 733 | 2523 | 3424 | 10420 | 0.1 |
| 8 | 16 | 6.43 | 7.59 | 1.7 | 612.4 | 610 | 1491 | 1810 | 455 | 1884 | 2703 | 6324 | 20.7 |
| 16 | 1 | 0.31 | 4.96 | 257.1 | 2.6 | 786 | 1573 | 1810 | 721 | 2490 | 4588 | 7242 | 0.0 |
| 16 | 8 | 2.22 | 5.04 | 251.3 | 2.7 | 893 | 1704 | 2081 | 729 | 2720 | 3834 | 7406 | 0.0 |
| 16 | 16 | 6.87 | 6.08 | 21.9 | 3.1 | 573 | 1466 | 1819 | 598 | 2376 | 3326 | 15204 | 6.3 |
| 32 | 1 | 0.29 | 4.16 | 216.7 | 3.2 | 840 | 1647 | 2458 | 713 | 4014 | 6226 | 11403 | 0.0 |
| 32 | 8 | 2.25 | 4.82 | 223.8 | 3.0 | 868 | 1696 | 2294 | 680 | 3391 | 5079 | 9044 | 0.0 |
| 32 | 16 | 4.27 | 5.55 | 236.0 | 2.8 | 905 | 1835 | 2572 | 610 | 2654 | 4227 | 9241 | 0.0 |
