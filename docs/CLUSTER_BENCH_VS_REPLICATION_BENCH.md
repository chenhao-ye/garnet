# cluster-bench (vazois) reproduction vs our ReplicationBench

Reproduction of the colleague's `cluster-bench` runs (1 primary + 1 replica, concurrent 100%
replica-read and 100% primary-write) and comparison against our multi-node ReplicationBench. Goal:
explain why the numbers "differ a lot".

## Setups

| | colleague | this reproduction | our ReplicationBench |
|---|---|---|---|
| branch | vazois/cluster-bench | vazois/cluster-bench (same) | chenhaoy/replication-bench |
| bench tool | cluster-bench | cluster-bench (same) | ReplicationBench |
| topology | 1 small machine, all co-located | 1 big machine (node0, 144c), all co-located | 3 dedicated nodes (primary/replica/client) |
| client concurrency | 4 threads x batch 128 | 4 threads x batch 128 (same) | 16 readers x ITP 256 + writers |
| m (physical sublogs) | 4 | 4 | 4 |
| drift / lag | AofReplayMaxDrift=262144, MaxLag=262144 | same | drift {100k,300k,1M}, lag=0 (huge) |
| runtime .NET | net8.0 | net10.0 (net8 runtime absent here) | net10.0 |

Deviations in the reproduction, all noted: net10.0 instead of net8.0 (only the net10 runtime is
installed); `--logger-level Information` instead of Trace (Trace + FileLogger only lowers the
colleague's numbers, so this makes the repro an upper bound); `global.json` SDK pin relaxed
10.0.301 -> 10.0.201 to build. Servers, replica, and both bench instances all run on node0
(co-located, mirroring the colleague's single-machine topology). Performance governor pinned.

## Results (all m=4)

```
                          WRITE (primary)     READ (replica)
colleague (small box)        ~1.2 Mops/s      ~1.7 -> collapse to ~0.28 Mops/s
this repro (node0, alone)     4.6 Mops/s       9.1 Mops/s
this repro (node0, concur.)   4.5 Mops/s       0.25 Mops/s   <-- reader collapses
our ReplicationBench          2.7 Mops/s      13.7 Mops/s    <-- reader sustained (concurrent)
   (w=4, m=4, drift=100k)     (concurrent)     (concurrent, no collapse)
```

(cluster-bench read-alone was verified as real hits, not misses: primary and replica both report
DBSIZE=10,000,000 with `replication_offset_lag:0` after populate, and the read collapses under
write load -- misses would be unaffected by replay load. The tool's `garnet_hit_rate: 0.00` line is
a red herring: it only queries the primary (7000), while reads route to the replica (7001).)

## Why they differ -- it is topology and client concurrency, not a missing optimization

1. **Absolute write throughput tracks machine size, because everything is co-located.** On the
   colleague's small box, primary + replica + both bench instances share a few cores, so writes cap
   at ~1.2 Mops/s. On node0 (144 cores) the same cluster-bench write hits ~4.6 Mops/s. Nothing about
   the algorithm changed; there are just more cores to go around.

2. **The reader collapse under concurrent write is real and reproduces -- it is CPU contention
   between the replica's replay and the co-located reads.** When the primary streams writes fast,
   the replica spends its CPU replaying that firehose across its m sublogs, starving the reads that
   share the same machine. Our node0 repro collapses to 0.25 Mops/s (matching the colleague's
   collapsed ~0.28); the only reason the colleague sees a steady ~1.7 Mops/s *before* collapsing is
   that their slower ~1.2 Mops/s write takes longer to saturate the replica's replay, whereas
   node0's 4.5 Mops/s write saturates it immediately.

3. **Our ReplicationBench does not collapse because the replica has a dedicated machine.** With the
   replica alone on node1, replay and reads each have ample CPU, so the reader sustains ~13-18
   Mops/s under concurrent write. Same protocol, same m=4 -- the difference is that reads and replay
   are not fighting the writer/primary for the same cores. Client concurrency also matters: 16
   readers x ITP 256 pipelines far more than cluster-bench's 4 threads x batch 128 (read-alone on
   node0 was 9.1 Mops/s with the cluster-bench client vs ~15-18 with our reader client).

## Answer to "is this expected / am I missing config / missing optimization?"

Expected for a single co-located machine. No missing optimization and no config error: the reader
collapse is the replica's replay starving co-located reads of CPU, and it recovers when the writer
backs off (less write -> less replay CPU -> reads get the cores back), exactly as observed. To get
high *sustained* concurrent read throughput, give the replica dedicated cores (separate machine);
our multi-node ReplicationBench then sustains 13-18 Mops/s reads at m=4 with no collapse.

## NUMA pinning: separating primary/replica recovers the reader ~5.8x

node0 is a 2-socket box (NUMA node0 = even CPUs, node1 = odd CPUs, 72 logical each). Pinning the
primary and replica to different NUMA nodes (concurrent read+write, m=4):

| config | concurrent READ | concurrent WRITE |
|---|---|---|
| baseline (primary + replica both on node0) | 0.21 Mops/s | 4.59 Mops/s |
| NUMA-split (primary->node0, replica->node1; readbench->node1, writebench->node0) | 1.24 Mops/s | 4.82 Mops/s |

So a large part of the co-located collapse is the *primary's* processing contending with the
*replica's* on the same socket. Giving the replica its own socket recovers concurrent read ~5.8x
(0.21 -> 1.24 Mops/s) with write unchanged. But it does not fully recover: 1.24 Mops/s is still far
below read-alone (9.1) and our dedicated-node ReplicationBench (13.7), because the reader still
shares the replica's socket with the replica's own replay of the ~4.8 Mops/s write firehose (the
reader-vs-replay contention studied for C5), and cluster-bench's client concurrency is limited
(4 threads x batch 128). 1.24 Mops/s coincides with the colleague's pre-collapse ~1.7 Mops/s
ballpark, i.e. NUMA-splitting on node0 reproduces their "steady" phase.

## Core-count sweep: more cores HURT the reader (co-located, unthrottled writer)

Pinning the whole co-located system (primary + replica + both bench clients) to a shared pool of N
node0 cores and sweeping N (concurrent read+write, m=4):

| N cores | READ Mops/s | WRITE Mops/s |
|---|---|---|
| 4  | 1.08 | 1.06 |
| 8  | 0.92 | 2.14 |
| 16 | 0.56 | 3.34 |
| 32 | 0.39 | 4.64 |
| 64 | 0.24 | 4.70 |

The reader is NOT core-starved for lack of cores -- the opposite. Adding cores lets the unthrottled
writer scale up (1.06 -> 4.7 Mops/s), so the write firehose gets faster, so the replica spends more
of its cores replaying it, which starves the co-located reader (1.08 -> 0.24 Mops/s). At N=4 the
reader is healthiest precisely because the writer is throttled to ~1 Mops/s and the replica keeps
up. So:

- **Write path saturates at ~32 cores** (4.64; 64 only 4.70).
- **The reader is throttled by the write/replay rate, not by cores** -- no shared-core budget fixes
  it, because more cores make the writer faster.
- **Running reads AND writes fast requires isolating the replica**, not adding shared cores:
  NUMA-splitting primary/replica took the reader 0.24 -> 1.24 Mops/s; a dedicated replica machine
  (our ReplicationBench) sustains 13.7, because the replica's ~72 cores fit replay (~4 threads at
  m=4) + read-serving (16 readers) without competing with the primary/writer.

Bottom line on "cores to fully run this": ~32 cores saturate the primary's write path; the replica
reader instead needs its own cores (separate socket/machine) sized for replay + read-serving
(~20+ for this m=4 / 16-reader workload) -- on a shared machine the reader is bottlenecked by
co-location with replay regardless of total core count.

## Reproduce

Orchestration scripts (not committed): `vaz_repro.sh` (concurrent) and `vaz_phases.sh`
(populate -> read-alone -> write-alone -> concurrent) in the job scratchpad. Server flags mirror
the colleague's config (`--aof-physical-sublog-count 4 --aof-replay-max-drift 262144
--replica-offset-max-lag 262144 --aof-tail-witness-freq 0 --aof-null-device --fast-aof-truncate
--aof-commit-freq -1 --index 1g`), 1 primary (7000) + 1 replica (7001), cluster bootstrapped over
RESP (epochs, ADDSLOTSRANGE, MEET, REPLICAOF).
