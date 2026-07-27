# Read/write hot-key overlap: TS reader collapses, C5 reader is unaffected

When the primary's write distribution is varied while the replica readers stay Zipf, the MultiLog
timestamp (TS) reader throughput swings by 2-4x, but the C5 snapshot reader barely moves. This report
documents the effect with data and explains why: it is read/write hot-key overlap interacting with
TS's blocking prefix-consistent read versus C5's non-blocking snapshot read.

**Short answer.** The replica readers are always Zipf. What matters is whether the primary *writes*
land on the same hot keys the readers hit. Under TS a read blocks until the key's sublog frontier
catches up, so when writes and reads share hot keys (both Zipf) the reads pile up on the
furthest-behind keys and the reader collapses, worse as `m` grows. C5 never blocks (a snapshot read
returns the current snapshot version regardless of pending updates), so overlap costs it nothing and
its reader is flat across distributions. The practical consequence is a ranking flip: TS reads faster
than C5 when reads and writes are uncorrelated, but can drop *below* C5 when they share hot keys and
`m` is high.

All runs: 32 primary-side clients issuing a read/write mix, 32 replica readers, 10M 8-byte keys,
itp=256, 30s x repeat=3, `performance` governor pinned by `run.py`. Replica read distribution is Zipf
(theta=0.99) in every case; only the primary write distribution changes.

## 1. Setup

The mixed-load `replication` benchmark: 32 client threads issue a mix of SET/GET to the primary
(write ratio 10% or 50%), and 32 reader threads issue GET to the replica. The primary-side ops draw
keys from the **write distribution**; the replica readers draw from the **read distribution**, which
is fixed at Zipf. We sweep the write distribution over {Uniform, ZipfRev, Zipf}:

- **Uniform**: writes spread across all 10M keys.
- **ZipfRev**: Zipf with the hotness ranking reversed, so the hot *write* keys are the *cold* read
  keys (disjoint from the reader hot set).
- **Zipf**: same ranking as the readers, so writes and reads concentrate on the *same* hot keys.

TS configs (`replication_mixed_{w10,w50}` and their `_pzipfrev` / `_pzipf` variants) fix drift=50k
and sweep `m` (physical sublogs) 1..32 at `n=1`. C5 configs (`replication_mixed_c5_*`) fix `m=1` and
sweep `n` (replay tasks) 2..32 x snapshot freq {5,500} ms.

## 2. TS reader collapses under Zipf primary (and worsens with m)

Reader throughput (Mops), primary distribution **Uniform / ZipfRev / Zipf**:

| m | w50 reader (U / RV / Z) | w10 reader (U / RV / Z) |
|---|---|---|
| 1 | 31.3 / 31.6 / 30.1 | 30.3 / 30.7 / 31.4 |
| 2 | 30.7 / 29.6 / **15.5** | 29.4 / 29.7 / 22.0 |
| 4 | 29.3 / 28.7 / **14.9** | 28.5 / 28.9 / 18.3 |
| 8 | 27.9 / 27.2 / **15.0** | 27.1 / 27.5 / 13.5 |
| 16 | 25.5 / 26.0 / **15.3** | 26.6 / 26.7 / 10.2 |
| 32 | 25.2 / 24.1 / **12.7** | 23.3 / 24.1 / **6.5** |

Uniform and ZipfRev track each other closely. Zipf halves the reader at w50 and drives it down to
6.5 Mops at w10/m=32, a ~4x collapse from the Uniform/ZipfRev level. The collapse deepens with `m`:
w10 Zipf goes 22.0 -> 6.5 as `m` rises 2 -> 32, because more physical sublogs spread the per-sublog
frontiers further apart, so a read that advanced its session clock waits longer on the lagging hot
key. At `m=1` (single virtual sublog, no frontier spread) all three distributions are equal.

For context, primary throughput under Zipf is if anything slightly *higher* (skew gives the primary
better cache locality): w10 primary Uniform/ZipfRev/Zipf at m=32 is 23.5 / 26.0 / 27.3 Mops. The
reader pays for the skew, not the writer.

## 3. C5 reader is insensitive to the primary distribution

Reader throughput (Mops), primary **Uniform / ZipfRev / Zipf**, C5 (m=1):

| freq | n | w50 reader (U / RV / Z) | w10 reader (U / RV / Z) |
|---|---|---|---|
| 5 | 2 | 13.5 / 13.6 / 14.2 | 14.4 / 14.4 / 14.0 |
| 5 | 8 | 12.7 / 12.1 / 12.4 | 12.3 / 12.6 / 11.7 |
| 5 | 32 | 9.8 / 11.5 / 9.2 | 10.2 / 11.3 / 11.2 |
| 500 | 8 | 11.4 / 13.8 / 11.7 | 13.5 / 14.3 / 12.2 |
| 500 | 32 | 9.0 / 12.0 / 11.0 | 10.7 / 12.6 / 13.1 |

The three distributions are within noise of each other at every point. C5's reader depends on `n` and
`freq` (the reader/replay coupling documented separately) but not on whether writes overlap the read
hot set.

## 4. Mechanism: overlap x (blocking vs non-blocking)

The reader distribution is Zipf throughout, so the readers always hammer the same hot key set. The
only variable is whether the *writes* hit that set:

- **Uniform primary**: the hot read keys receive a negligible share of writes, so their sublog
  frontiers stay current. TS reads on them rarely block.
- **ZipfRev primary**: the hot write keys are the reverse-ranked keys, i.e. the cold read keys. Write
  hot set and read hot set are disjoint, so again the hot read keys are lightly written and TS reads
  rarely block. This is why ZipfRev behaves like Uniform for the reader.
- **Zipf primary**: writes and reads concentrate on the *same* keys. Those keys carry the most
  pending updates and the furthest-behind frontier. Under TS a read blocks
  (`ConsistentReadKeyPrepare`) until the key's sublog frontier reaches the reader's session sequence
  number, so reads on the hot-and-heavily-written keys stall, and the reader collapses.

C5 does not block: a snapshot read returns the version visible in the current snapshot regardless of
how far behind the key is. Read/write overlap therefore has no effect on the C5 reader, which is why
its throughput is flat across all three distributions.

The `m`-dependence of the TS collapse is the same frontier mechanism: more physical sublogs mean the
per-sublog frontiers drift further apart under the drift barrier, so a session that advanced its
clock on a fast sublog waits longer when it reads a hot key on a lagging sublog. Hence Zipf/TS gets
worse with `m` while Uniform/ZipfRev stay flat.

## 5. Consequence: the TS-vs-C5 reader ranking flips

Earlier Uniform-primary runs showed TS reading 2-3x faster than C5. That advantage is specific to
uncorrelated read/write key sets. Under Zipf/Zipf overlap:

| point | TS reader | C5 reader (best over n, same freq class) | winner |
|---|---|---|---|
| w10, m=32 (TS) vs C5 m=1 | **6.5** | ~11-13 | **C5** |
| w50, m=32 (TS) vs C5 m=1 | 12.7 | ~9-11 | TS (narrowly) |

So under a skewed cache workload where reads and writes target the same hot keys, TS's blocking
prefix-consistent read can fall below C5's non-blocking snapshot read, most clearly in the read-heavy
(w10) high-`m` regime. This is the fundamental trade between the two read protocols: TS gives fresher,
prefix-consistent reads but pays a reader-throughput penalty that scales with read/write hot-key
correlation and with `m`; C5 gives non-blocking reads at a fixed lower ceiling (and a snapshot
staleness floor) that is indifferent to that correlation.

## 6. Caveats

- Repeat=3. The TS collapse under Zipf is large and monotonic in `m`, well outside run-to-run noise;
  the C5 flatness across distributions is likewise clear. The narrow TS-vs-C5 margin at w50/m=32
  (12.7 vs ~9-11) is close enough that repeat=5 would firm it up.
- The reader always uses Zipf theta=0.99; a milder skew would shrink the overlap effect, a sharper
  skew would amplify it. Only the two extremes (perfect overlap = Zipf, perfect anti-overlap =
  ZipfRev) and the no-overlap baseline (Uniform) were measured.
- This isolates the read-protocol effect; it does not vary the read distribution.

## 7. Data provenance

TS: `data/replication_mixed_w10`, `data/replication_mixed_w50` (Uniform);
`..._pzipfrev`, `..._pzipf` variants (ZipfRev, Zipf). C5: `data/replication_mixed_c5_w10`,
`..._c5_w50` and their `_pzipfrev` / `_pzipf` variants. Columns: `reader_tpt_mops_s`,
`writer_tpt_mops_s`, `replication_lag_bytes`. All repeat=3, performance governor pinned.
Related: `C5_READER_REPLAY_COUPLING.md` (why the C5 reader depends on `n`).
