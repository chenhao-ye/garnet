// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers.Binary;
using System.Diagnostics;
using System.Text;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Resp.benchmark
{
    public class Page(int size)
    {
        public int Length => payload.Length;
        public byte[] payload = GC.AllocateArray<byte>(size, pinned: true);
        public int payloadLength = 0;
        public int recordCount = 0;
    }

    public sealed class KVPairBuffer
    {
        public byte[] Keys;
        public byte[] Value;
        public int KeyLen;
        public int Count;
    }

    public sealed class AofGen
    {
        readonly GarnetLog garnetLog;

        public readonly GarnetAppendOnlyFile appendOnlyFile;

        readonly Options options;
        readonly GarnetServerOptions aofServerOptions;

        /// <summary>
        /// threads x pageNum
        /// </summary>
        Page[][] pageBuffers;

        /// <summary>
        /// Warmup pages: threads x pageNum. One StoreUpsert per key (shuffled), replayed untimed
        /// before the measured pass on every replay run. Built by <see cref="GenerateWarmupData"/>.
        /// </summary>
        Page[][] warmupPageBuffers;

        /// <summary>
        /// Per-thread flat key buffer + shared value (one entry per thread).
        /// Rebuilt by <see cref="BuildKVPairBuffersForRun"/> at the start of every Run() so the partition matches the current threadCount.
        /// </summary>
        KVPairBuffer[] kvPairBuffers;

        // Per-sublog flat key buffers, indexed by physical sublog idx. Built once in the constructor;
        // BuildKVPairBuffersForRun shares these by reference (1-sublog threads) or concatenates them (multi-sublog threads).
        byte[][] perSublogKeysets;
        int[] bucketCounts;
        byte[] globalKeys;
        byte[] sharedValue;

        public const double ZipfTheta = 0.99;

        // Sublog each global key hashes to, indexed by global key index. Built with the
        // per-sublog keysets; the zipf generators dispatch sampled keys through it.
        int[] sublogAssign;

        // Pseudo-clock ticks between consecutive records of the emulated global stream. Generated
        // records model ONE totally ordered log dealt across the sublogs: the stream advances one
        // tick per record, so consecutive records WITHIN a sublog average PseudoTimestampPace
        // apart while the cross-sublog interleaving stays on a single timeline.
        readonly long globalStreamTick;

        long total_number_of_aof_records = 0L;
        long total_number_of_aof_bytes = 0L;

        public Page[] GetPageBuffers(int threadIdx) => pageBuffers[threadIdx];
        public Page[] GetWarmupPageBuffers(int threadIdx) => warmupPageBuffers[threadIdx];
        public KVPairBuffer GetKVPairBuffer(int threadIdx) => kvPairBuffers[threadIdx];

        readonly int keyLen;

        public byte[] GlobalKeys => globalKeys;
        public int KeyLen => keyLen;

        /// <summary>
        /// Effective key length for the given options: --keylength widened to fit dbsize digits.
        /// Static so a Client-role process derives the identical value without an AofGen.
        /// </summary>
        public static int DeriveKeyLen(Options options)
            => Math.Max(options.KeyLength, NumUtils.NumDigits(options.DbSize));

        /// <summary>
        /// Builds the global keyset: key i = hex MurmurHash3 of i, zero-padded to keyLen.
        /// Deterministic from (dbsize, keyLen) alone, so a Client-role process regenerates the
        /// exact keys the replica generated, with no AOF page generation.
        /// </summary>
        public static byte[] BuildGlobalKeys(int dbsize, int keyLen)
        {
            var keys = new byte[dbsize * keyLen];
            for (int i = 0; i < dbsize; i++)
                FormatHexKey(keys, i * keyLen, keyLen, i);
            return keys;
        }

        public AofGen(Options options)
        {
            this.options = options;
            this.keyLen = DeriveKeyLen(options);
            this.globalStreamTick = Math.Max(1, options.PseudoTimestampPace / Math.Max(1, options.AofPhysicalSublogCount));
            if (options.KeyLength > 0 && this.keyLen != options.KeyLength)
                Console.WriteLine($"[Warning] --keylength {options.KeyLength} is too small for --dbsize {options.DbSize}; expanding to {this.keyLen}.");
            this.aofServerOptions = new GarnetServerOptions()
            {
                EnableAOF = true,
                AofMemorySize = options.AofMemorySize,
                AofPageSize = options.AofPageSize,
                UseAofNullDevice = true,
                EnableFastCommit = true,
                CommitFrequencyMs = -1,
                FastAofTruncate = true,
                EnableCluster = true,
                ReplicationOffsetMaxLag = 0,
                AofPhysicalSublogCount = options.AofPhysicalSublogCount,
                AofReplayTaskCount = options.AofReplayTaskCount
            };
            aofServerOptions.GetAofSettings(0, out var logSettings);
            appendOnlyFile = new GarnetAppendOnlyFile(aofServerOptions, logSettings, Program.loggerFactory.CreateLogger("AofGen - AOF instance"));
            garnetLog = appendOnlyFile.Log;

            if (options.IsReplayEnabled)
                pageBuffers = new Page[options.AofPhysicalSublogCount][];

            BuildSublogKeysets();

            // Replay drives GeneratePages() (via bench.GenerateData()) before Run(), so kvPairBuffers must
            // be populated up front for the sublog-indexed access in GeneratePages. Run(AofPhysicalSublogCount)
            // later re-invokes this with the same threadCount, which is idempotent.
            if (options.IsReplayEnabled)
                BuildKVPairBuffersForRun(options.AofPhysicalSublogCount);
        }

        public NetworkBufferSettings GetAofSyncNetworkBufferSettings()
        {
            var aofSyncSendBufferSize = 2 << aofServerOptions.AofPageSizeBits();
            var aofSyncInitialReceiveBufferSize = 1 << 17;
            return new(aofSyncSendBufferSize, aofSyncInitialReceiveBufferSize);
        }

        // Phase A: build the global hex keyset, plus (Replay only) per-sublog buckets.
        unsafe void BuildSublogKeysets()
        {
            var dbsize = options.DbSize;
            var sublogCount = options.AofPhysicalSublogCount;

            globalKeys = BuildGlobalKeys(dbsize, keyLen);

            var valueLen = options.ValueLength;
            sharedValue = GC.AllocateArray<byte>(valueLen, pinned: true);
            Array.Fill(sharedValue, (byte)'V');

            if (!options.IsReplayEnabled)
            {
                Console.WriteLine($"Pre-built global keyset (dbsize={dbsize}, keyLen={keyLen}).");
                return;
            }

            // Replay binds generator/worker thread t to sublog t. A bucket with zero keys would
            // leave its sublog with no warmup upserts and no replayable records, hence the throw.
            sublogAssign = new int[dbsize];
            bucketCounts = new int[sublogCount];
            for (int i = 0; i < dbsize; i++)
            {
                var keySpan = globalKeys.AsSpan(i * keyLen, keyLen);
                int sub = garnetLog.GetPhysicalSublogIdx(keySpan);
                sublogAssign[i] = sub;
                bucketCounts[sub]++;
            }
            for (int s = 0; s < sublogCount; s++)
                if (bucketCounts[s] == 0)
                    throw new Exception(
                        $"Hash distribution leaves sublog {s} of {sublogCount} with zero of the {dbsize} generated keys. " +
                        $"Increase --dbsize to populate every bucket.");

            perSublogKeysets = new byte[sublogCount][];
            for (int s = 0; s < sublogCount; s++)
                perSublogKeysets[s] = GC.AllocateArray<byte>(bucketCounts[s] * keyLen, pinned: true);
            var offsets = new int[sublogCount];
            for (int i = 0; i < dbsize; i++)
            {
                int s = sublogAssign[i];
                Buffer.BlockCopy(globalKeys, i * keyLen, perSublogKeysets[s], offsets[s] * keyLen, keyLen);
                offsets[s]++;
            }

        }

        // Phase B: (re)build kvPairBuffers for the current Run's threadCount.
        // Replay: identity mapping thread t -> sublog t (caller passes threadCount == AofPhysicalSublogCount).
        // EnqueueSharded: thread t owns the keys whose GarnetLog.HASH(key) % threadCount == t;
        //   sublog routing inside Enqueue is independent and happens via the same hash mod sublogCount.
        // EnqueueRandom: every thread gets a private copy of all dbsize keys.
        public void BuildKVPairBuffersForRun(int threadCount)
        {
            var sublogCount = options.AofPhysicalSublogCount;
            kvPairBuffers = new KVPairBuffer[threadCount];

            if (options.IsReplayEnabled)
            {
                if (threadCount != sublogCount)
                    throw new Exception($"Replay requires threadCount ({threadCount}) == AofPhysicalSublogCount ({sublogCount}).");
                for (int t = 0; t < threadCount; t++)
                {
                    kvPairBuffers[t] = new KVPairBuffer
                    {
                        Keys = perSublogKeysets[t],
                        Value = sharedValue,
                        KeyLen = keyLen,
                        Count = bucketCounts[t],
                    };
                }
            }
            else if (options.AofBenchType == AofBenchType.EnqueueSharded)
            {
                var dbsize = options.DbSize;
                var threadAssign = new int[dbsize];
                var perThreadCounts = new int[threadCount];
                for (int i = 0; i < dbsize; i++)
                {
                    var hash = GarnetLog.HASH(globalKeys.AsSpan(i * keyLen, keyLen));
                    int t = (int)((ulong)hash % (ulong)threadCount);
                    threadAssign[i] = t;
                    perThreadCounts[t]++;
                }
                for (int t = 0; t < threadCount; t++)
                {
                    kvPairBuffers[t] = new KVPairBuffer
                    {
                        Keys = GC.AllocateArray<byte>(perThreadCounts[t] * keyLen, pinned: true),
                        Value = sharedValue,
                        KeyLen = keyLen,
                        Count = perThreadCounts[t],
                    };
                }
                var offsets = new int[threadCount];
                for (int i = 0; i < dbsize; i++)
                {
                    int t = threadAssign[i];
                    Buffer.BlockCopy(globalKeys, i * keyLen, kvPairBuffers[t].Keys, offsets[t] * keyLen, keyLen);
                    offsets[t]++;
                }
                Console.WriteLine($"threads={threadCount} sublogs={sublogCount} (hash%T): per-thread key counts=[{string.Join(", ", perThreadCounts)}]");
            }
            else
            {
                var dbsize = options.DbSize;
                for (int t = 0; t < threadCount; t++)
                {
                    var keys = GC.AllocateArray<byte>(dbsize * keyLen, pinned: true);
                    Buffer.BlockCopy(globalKeys, 0, keys, 0, dbsize * keyLen);
                    kvPairBuffers[t] = new KVPairBuffer
                    {
                        Keys = keys,
                        Value = sharedValue,
                        KeyLen = keyLen,
                        Count = dbsize,
                    };
                }
            }
        }

        // Hex-encode keyLen characters derived from MurmurHash2x64A(i). High-entropy bytes
        // ensure Utility.HashBytes(key) % k is near-uniform for k up to 64.
        // If keyLen > 16 (very unusual), the tail is padded with '0'.
        static unsafe void FormatHexKey(byte[] dest, int offset, int keyLen, int i)
        {
            Span<byte> mix = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(mix, i);
            ulong h;
            fixed (byte* p = mix)
                h = Garnet.common.HashUtils.MurmurHash2x64A(p, 8);
            int hexLen = Math.Min(keyLen, 16);
            Encoding.ASCII.GetBytes(h.ToString("x16").AsSpan(0, hexLen), dest.AsSpan(offset, hexLen));
            for (int j = hexLen; j < keyLen; j++)
                dest[offset + j] = (byte)'0';
        }

        public void GenerateData()
        {
            if (!options.IsReplayEnabled) return;

            Console.WriteLine($"Generating AoFBench Data!");
            var threads = options.AofPhysicalSublogCount;
            // startClock must be larger than the max warmup clock
            var startClock = GenerateWarmupData(threads) + options.PseudoTimestampPace;

            // Zipf generation emulates one global stream of (threads x per-generator budget)
            // records: generator g owns the contiguous pseudo-clock range starting at
            // startClock + g x budget x globalStreamTick and dispatches each sampled record to
            // the sublog its key hashes to (see GenerateZipfStream). The budget matches the
            // volume the uniform path generates per sublog.
            var zipf = options.AofReplayDist == KeyDistribution.Zipf;
            var perThreadRecords = zipf ? (long)options.AofGenPages * RecordsPerPage() : 0L;
            var segments = zipf ? new Page[threads][][] : null;
            var endClocks = zipf ? new long[threads] : null;
            long BaseClock(int g) => startClock + g * perThreadRecords * globalStreamTick;

            var workers = new Thread[threads];
            for (var idx = 0; idx < threads; ++idx)
            {
                var x = idx;
                workers[idx] = zipf
                    ? new Thread(() => segments[x] = GenerateZipfStream(x, BaseClock(x), perThreadRecords, out endClocks[x]))
                    : new Thread(() => GeneratePages(x, startClock));
            }

            Stopwatch swatch = new();
            swatch.Start();
            foreach (var worker in workers)
                worker.Start();
            foreach (var worker in workers)
                worker.Join();
            swatch.Stop();

            if (zipf)
            {
                // Each sublog's stream is the generators' segments concatenated in generator
                // order: sequence numbers stay increasing within every sublog while per-sublog
                // record volumes follow each sublog's share of the zipf mass. That holds only
                // if the clock ranges the generators actually produced are disjoint and
                // ascending, so validate them before merging.
                for (var g = 0; g < threads; g++)
                {
                    if (endClocks[g] > (g + 1 < threads ? BaseClock(g + 1) : long.MaxValue))
                        throw new Exception(
                            $"Zipf generator {g} produced clock range [{BaseClock(g)}, {endClocks[g]}), " +
                            $"overlapping generator {g + 1}'s base clock {BaseClock(g + 1)}.");
                }
                Console.WriteLine($"  zipf generator clock ranges=[{string.Join(", ", Enumerable.Range(0, threads).Select(g => $"[{BaseClock(g)}, {endClocks[g]})"))}]");

                for (var s = 0; s < threads; s++)
                {
                    var pages = new List<Page>();
                    for (var g = 0; g < threads; g++)
                        pages.AddRange(segments[g][s]);
                    pageBuffers[s] = pages.ToArray();
                }
            }

            // Per-sublog load report: keys from the hash partition, records/pages/bytes from
            // the generated page set. Uniform record shares track the key shares; under Zipf
            // the gap between the two percentage columns is the dispatched load imbalance.
            // Plain (non-bracketed) lines so parse.py does not pick them up as samples.
            long totalPages = 0;
            Console.WriteLine($"AofGen per-sublog load (sublogs={threads}, dbsize={options.DbSize:N0}):");
            Console.WriteLine($"{"sublog",6} {"keys",12} {"keys%",7} {"records",14} {"records%",9} {"pages",6} {"bytes",16}");
            for (var s = 0; s < threads; s++)
            {
                long records = 0, bytes = 0;
                foreach (var p in pageBuffers[s])
                {
                    records += p.recordCount;
                    bytes += p.payloadLength;
                }
                totalPages += pageBuffers[s].Length;
                Console.WriteLine(
                    $"{s,6} {bucketCounts[s],12:N0} {100.0 * bucketCounts[s] / options.DbSize,6:F1}% " +
                    $"{records,14:N0} {100.0 * records / total_number_of_aof_records,8:F1}% " +
                    $"{pageBuffers[s].Length,6} {bytes,16:N0}");
            }

            var seconds = swatch.ElapsedMilliseconds / 1000.0;
            Console.WriteLine($"Generated {totalPages:N0} pages of size {aofServerOptions.AofPageSize} in {seconds:N2} secs");
            Console.WriteLine($"Generated number of AOF records: {total_number_of_aof_records:N0}");
            Console.WriteLine($"Generated number of AOF bytes: {total_number_of_aof_bytes:N0}");
        }

        // Build the warmup page-set in parallel (one thread per sublog) and log a plain summary.
        // Returns the highest final warmup clock across sublogs (0 in single-log mode, which carries
        // no sequence numbers).
        long GenerateWarmupData(int threads)
        {
            warmupPageBuffers = new Page[threads][];
            var finalClocks = new long[threads];
            var workers = new Thread[threads];
            for (var idx = 0; idx < threads; ++idx)
            {
                var x = idx;
                workers[idx] = new Thread(() => finalClocks[x] = GenerateWarmupPages(x));
            }

            Stopwatch swatch = new();
            swatch.Start();
            foreach (var worker in workers)
                worker.Start();
            foreach (var worker in workers)
                worker.Join();
            swatch.Stop();

            long warmupPages = 0, warmupRecords = 0, warmupBytes = 0, maxClock = 0;
            for (var t = 0; t < threads; t++)
            {
                warmupPages += warmupPageBuffers[t].Length;
                foreach (var p in warmupPageBuffers[t])
                {
                    warmupRecords += p.recordCount;
                    warmupBytes += p.payloadLength;
                }
                if (finalClocks[t] > maxClock) maxClock = finalClocks[t];
            }
            var seconds = swatch.ElapsedMilliseconds / 1000.0;
            // Plain (non-bracketed) line: parse.py keys on "[name]: value" blocks, so warmup
            // logging must avoid that format to not create a spurious sample.
            Console.WriteLine($"Generated warmup {warmupRecords:N0} records ({warmupBytes:N0} bytes) across {threads} sublogs in {warmupPages:N0} pages ({seconds:N2} secs)");
            return maxClock;
        }

        // Build one StoreUpsert per key for this sublog, in shuffled order.
        // Each sharded record's sequence number is the running pseudoClock.
        // Returns the final clock so the measured pass can start strictly above it.
        unsafe long GenerateWarmupPages(int threadId)
        {
            long pseudoClock = threadId;
            long pseudoTimestampPace = options.PseudoTimestampPace;

            var buf = kvPairBuffers[threadId];
            var keys = buf.Keys;
            var value = buf.Value;
            var myCount = buf.Count;
            var useShardedHeader = options.AofPhysicalSublogCount > 1 || options.AofReplayTaskCount > 1;
            var pageSize = 1 << aofServerOptions.AofPageSizeBits();

            // Shuffle key indices so first-touch allocations happen in random order.
            var order = new int[myCount];
            for (var i = 0; i < myCount; i++) order[i] = i;
            new Random(0x5EED + threadId).Shuffle(order);

            var pages = new List<Page>();
            fixed (byte* keysPtr = keys)
            fixed (byte* valuePtr = value)
            {
                var valueLen = value.Length;
                var keyPos = 0;
                while (keyPos < myCount)
                {
                    var page = new Page(pageSize);
                    var isFirstPage = pages.Count == 0;
                    fixed (byte* pagePtr = page.payload)
                    {
                        var pageOffset = pagePtr;
                        var pageEnd = pageOffset + page.Length - (isFirstPage ? 64 : 0);
                        // Fill the page; a key that does not fit leaves keyPos unadvanced so it is
                        // retried on the next page (every key is emitted exactly once).
                        while (keyPos < myCount)
                        {
                            var idx = order[keyPos];
                            var keyPtr = keysPtr + idx * keyLen;
                            var key = SpanByte.FromPinnedPointer(keyPtr, keyLen);
                            var v = SpanByte.FromPinnedPointer(valuePtr, valueLen);
                            StringInput input = default;
                            var aofHeader = new AofHeader { opType = AofEntryType.StoreUpsert, storeVersion = 1, sessionID = 0 };
                            if (!useShardedHeader)
                            {
                                if (!garnetLog.GetSubLog(threadId).DummyEnqueue(
                                    ref pageOffset, pageEnd, aofHeader, key, v, ref input))
                                    break;
                            }
                            else
                            {
                                var replayTag = garnetLog.GetReplayTag(new ReadOnlySpan<byte>(keyPtr, keyLen));
                                var extendedAofHeader = new AofShardedHeader
                                {
                                    basicHeader = new AofHeader
                                    {
                                        padding = AofHeader.MakePadding(AofHeaderType.ShardedHeader, replayTag),
                                        opType = aofHeader.opType,
                                        storeVersion = aofHeader.storeVersion,
                                        sessionID = aofHeader.sessionID
                                    },
                                    sequenceNumber = pseudoClock
                                };
                                if (!garnetLog.GetSubLog(threadId).DummyEnqueue(
                                    ref pageOffset, pageEnd, extendedAofHeader, key, v, ref input))
                                    break;
                                pseudoClock += pseudoTimestampPace;
                            }
                            page.recordCount++;
                            keyPos++;
                        }
                        page.payloadLength = (int)(pageOffset - pagePtr);
                    }
                    if (page.recordCount == 0)
                        throw new Exception($"Warmup record for sublog {threadId} does not fit in a {pageSize}-byte page.");
                    pages.Add(page);
                }
            }
            warmupPageBuffers[threadId] = pages.ToArray();
            return pseudoClock;
        }

        unsafe void GeneratePages(int threadId, long startPseudoClock)
        {
            // Round-robin deal of the emulated global stream: sublogs start globalStreamTick
            // apart and each advances PseudoTimestampPace per record, so round k of the deal
            // (one record per sublog) occupies its own clock window within [k x pace, (k+1) x pace).
            long pseudoClock = startPseudoClock + threadId * globalStreamTick;
            long pseudoTimestampPace = options.PseudoTimestampPace;

            var rng = new Random(789110123 + threadId);
            var buf = kvPairBuffers[threadId];
            var keys = buf.Keys;
            var value = buf.Value;
            var myCount = buf.Count;
            long number_of_aof_records = 0L;
            long number_of_aof_bytes = 0L;
            var pages = options.AofGenPages;
            pageBuffers[threadId] = new Page[pages];
            var useShardedHeader = options.AofPhysicalSublogCount > 1 || options.AofReplayTaskCount > 1;
            var pageSize = 1 << aofServerOptions.AofPageSizeBits();

            fixed (byte* keysPtr = keys)
            fixed (byte* valuePtr = value)
            {
                var valueLen = value.Length;
                for (var pageIdx = 0; pageIdx < pages; pageIdx++)
                {
                    var page = new Page(pageSize);
                    pageBuffers[threadId][pageIdx] = page;
                    fixed (byte* pagePtr = page.payload)
                    {
                        var pageOffset = pagePtr;
                        var pageEnd = pageOffset + page.Length;
                        while (true)
                        {
                            int idx = rng.Next(myCount);
                            var keyPtr = keysPtr + idx * keyLen;
                            var key = SpanByte.FromPinnedPointer(keyPtr, keyLen);
                            var v = SpanByte.FromPinnedPointer(valuePtr, valueLen);
                            StringInput input = default;
                            var aofHeader = new AofHeader { opType = AofEntryType.StoreUpsert, storeVersion = 1, sessionID = 0 };
                            if (!useShardedHeader)
                            {
                                if (!garnetLog.GetSubLog(threadId).DummyEnqueue(
                                    ref pageOffset, pageEnd, aofHeader, key, v, ref input))
                                    break;
                            }
                            else
                            {
                                var replayTag = garnetLog.GetReplayTag(new ReadOnlySpan<byte>(keyPtr, keyLen));
                                var extendedAofHeader = new AofShardedHeader
                                {
                                    basicHeader = new AofHeader
                                    {
                                        padding = AofHeader.MakePadding(AofHeaderType.ShardedHeader, replayTag),
                                        opType = aofHeader.opType,
                                        storeVersion = aofHeader.storeVersion,
                                        sessionID = aofHeader.sessionID
                                    },
                                    sequenceNumber = pseudoClock
                                };
                                if (!garnetLog.GetSubLog(threadId).DummyEnqueue(
                                    ref pageOffset, pageEnd, extendedAofHeader, key, v, ref input))
                                    break;
                                pseudoClock += pseudoTimestampPace;
                            }
                            page.recordCount++;
                        }
                        var payloadLength = (int)(pageOffset - pagePtr);
                        page.payloadLength = payloadLength;
                        number_of_aof_records += page.recordCount;
                        number_of_aof_bytes += payloadLength;
                    }
                }
            }
            _ = Interlocked.Add(ref total_number_of_aof_records, number_of_aof_records);
            _ = Interlocked.Add(ref total_number_of_aof_bytes, number_of_aof_bytes);
        }

        // One zipf generator: samples recordCount keys from the global zipf, appends each record
        // to the page list of the sublog the key hashes to, and advances the global stream clock
        // one tick per record. Returns the per-sublog page lists for this generator's clock
        // range; endClock reports one tick past the last assigned sequence number, which the
        // caller validates against the next generator's base clock before merging.
        unsafe Page[][] GenerateZipfStream(int genIdx, long baseClock, long recordCount, out long endClock)
        {
            var sublogCount = options.AofPhysicalSublogCount;
            var zipfg = new ZipfGenerator(new RandomGenerator((uint)(789110123 + genIdx)), options.DbSize, ZipfTheta);
            var useShardedHeader = sublogCount > 1 || options.AofReplayTaskCount > 1;
            var pageSize = 1 << aofServerOptions.AofPageSizeBits();
            long pseudoClock = baseClock;
            long number_of_aof_records = 0L;
            long number_of_aof_bytes = 0L;

            var pages = new List<Page>[sublogCount];
            var current = new Page[sublogCount];
            var pageOffsets = new int[sublogCount];
            for (var s = 0; s < sublogCount; s++)
                pages[s] = new List<Page>();

            // Seal the sublog's open page: record its payload length and retire it to the list.
            void SealPage(int s)
            {
                var page = current[s];
                if (page == null) return;
                page.payloadLength = pageOffsets[s];
                number_of_aof_bytes += page.payloadLength;
                pages[s].Add(page);
                current[s] = null;
            }

            fixed (byte* keysPtr = globalKeys)
            fixed (byte* valuePtr = sharedValue)
            {
                var valueLen = sharedValue.Length;
                for (long k = 0; k < recordCount; k++)
                {
                    var keyIdx = zipfg.Next();
                    var s = sublogAssign[keyIdx];
                    var keyPtr = keysPtr + keyIdx * keyLen;
                    var key = SpanByte.FromPinnedPointer(keyPtr, keyLen);
                    var v = SpanByte.FromPinnedPointer(valuePtr, valueLen);

                    // A record that does not fit seals the open page and retries on a fresh one.
                    while (true)
                    {
                        if (current[s] == null)
                        {
                            current[s] = new Page(pageSize);
                            pageOffsets[s] = 0;
                        }
                        var page = current[s];
                        bool enqueued;
                        fixed (byte* pagePtr = page.payload)
                        {
                            var pageOffset = pagePtr + pageOffsets[s];
                            var pageEnd = pagePtr + page.Length;
                            StringInput input = default;
                            var aofHeader = new AofHeader { opType = AofEntryType.StoreUpsert, storeVersion = 1, sessionID = 0 };
                            if (!useShardedHeader)
                            {
                                enqueued = garnetLog.GetSubLog(s).DummyEnqueue(
                                    ref pageOffset, pageEnd, aofHeader, key, v, ref input);
                            }
                            else
                            {
                                var replayTag = garnetLog.GetReplayTag(new ReadOnlySpan<byte>(keyPtr, keyLen));
                                var extendedAofHeader = new AofShardedHeader
                                {
                                    basicHeader = new AofHeader
                                    {
                                        padding = AofHeader.MakePadding(AofHeaderType.ShardedHeader, replayTag),
                                        opType = aofHeader.opType,
                                        storeVersion = aofHeader.storeVersion,
                                        sessionID = aofHeader.sessionID
                                    },
                                    sequenceNumber = pseudoClock
                                };
                                enqueued = garnetLog.GetSubLog(s).DummyEnqueue(
                                    ref pageOffset, pageEnd, extendedAofHeader, key, v, ref input);
                            }
                            if (enqueued)
                            {
                                pageOffsets[s] = (int)(pageOffset - pagePtr);
                                page.recordCount++;
                            }
                        }
                        if (enqueued) break;
                        if (page.recordCount == 0)
                            throw new Exception($"Zipf record for sublog {s} does not fit in a {pageSize}-byte page.");
                        SealPage(s);
                    }
                    if (useShardedHeader)
                        pseudoClock += globalStreamTick;
                    number_of_aof_records++;
                }
            }
            for (var s = 0; s < sublogCount; s++)
                SealPage(s);

            endClock = pseudoClock;
            _ = Interlocked.Add(ref total_number_of_aof_records, number_of_aof_records);
            _ = Interlocked.Add(ref total_number_of_aof_bytes, number_of_aof_bytes);
            var result = new Page[sublogCount][];
            for (var s = 0; s < sublogCount; s++)
                result[s] = pages[s].ToArray();
            return result;
        }

        // Records that fit in one generated page. Every generated record has the same header
        // type and key/value lengths, so it occupies the same number of page bytes
        // (DummyEnqueueLength) and page capacity is a plain division. Sets the per-generator
        // record budget of the zipf path to the same volume the uniform path generates per
        // sublog (aof_gen_pages full pages).
        int RecordsPerPage()
        {
            var key = (ReadOnlySpan<byte>)globalKeys.AsSpan(0, keyLen);
            var value = (ReadOnlySpan<byte>)sharedValue;
            StringInput input = default;
            var recordSize = options.AofPhysicalSublogCount > 1 || options.AofReplayTaskCount > 1
                ? garnetLog.GetSubLog(0).DummyEnqueueLength<AofShardedHeader, StringInput>(key, value, ref input)
                : garnetLog.GetSubLog(0).DummyEnqueueLength<AofHeader, StringInput>(key, value, ref input);
            return (1 << aofServerOptions.AofPageSizeBits()) / recordSize;
        }
    }
}