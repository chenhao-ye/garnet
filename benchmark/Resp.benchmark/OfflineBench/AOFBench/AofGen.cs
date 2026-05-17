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

        long total_number_of_aof_records = 0L;
        long total_number_of_aof_bytes = 0L;

        public Page[] GetPageBuffers(int threadIdx) => pageBuffers[threadIdx];
        public KVPairBuffer GetKVPairBuffer(int threadIdx) => kvPairBuffers[threadIdx];

        readonly int keyLen;

        public AofGen(Options options)
        {
            this.options = options;
            this.keyLen = Math.Max(options.KeyLength, NumUtils.NumDigits(options.DbSize));
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
                AofReplayTaskCount = options.AofReplayTaskCount,
                DisablePrefixConsistency = options.DisablePrefixConsistency,
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

        // Phase A: build the fixed-size global hex keyset and bucket it by physical sublog.
        // Throws if the hash leaves any sublog bucket empty, instead of letting a worker spin in
        // a rejection-sampling loop forever (the failure mode we hit at k>=16 with structured numeric keys).
        unsafe void BuildSublogKeysets()
        {
            var dbsize = options.DbSize;
            var sublogCount = options.AofPhysicalSublogCount;

            globalKeys = new byte[dbsize * keyLen];
            var sublogAssign = new int[dbsize];
            bucketCounts = new int[sublogCount];
            for (int i = 0; i < dbsize; i++)
            {
                FormatHexKey(globalKeys, i * keyLen, keyLen, i);
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

            // One shared, fixed-content value buffer for every key and every thread.
            // Read-only sharing — no false-sharing risk, and benchmark metrics only care
            // about the byte count, not the bytes themselves.
            var valueLen = options.ValueLength;
            sharedValue = GC.AllocateArray<byte>(valueLen, pinned: true);
            Array.Fill(sharedValue, (byte)'V');

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

            int min = int.MaxValue, max = 0;
            for (int s = 0; s < sublogCount; s++)
            {
                if (bucketCounts[s] < min) min = bucketCounts[s];
                if (bucketCounts[s] > max) max = bucketCounts[s];
            }
            Console.WriteLine($"Per-sublog key distribution (dbsize={dbsize}, sublogs={sublogCount}): min={min} max={max}");
            Console.WriteLine($"  counts=[{string.Join(", ", bucketCounts)}]");
        }

        // Phase B: (re)build kvPairBuffers for the current Run's threadCount.
        // EnqueueSharded / Replay: round-robin partition — thread t owns sublogs {s : s % threadCount == t}.
        //   * 1-sublog threads share the perSublogKeysets entry by reference (no copy).
        //   * Multi-sublog threads get a concatenated pinned buffer.
        // EnqueueRandom: every thread gets a private pinned copy of all dbsize keys (unchanged).
        public void BuildKVPairBuffersForRun(int threadCount)
        {
            var sublogCount = options.AofPhysicalSublogCount;
            kvPairBuffers = new KVPairBuffer[threadCount];

            bool sharded = options.IsReplayEnabled || options.AofBenchType == AofBenchType.EnqueueSharded;

            if (sharded)
            {
                // Round-robin:
                //   T >= S: thread t owns exactly sublog (t % S); multiple threads share a sublog.
                //   T <  S: thread t owns sublogs {s : s % T == t}; multiple sublogs per thread.
                var ownedSublogs = new int[threadCount][];
                if (threadCount >= sublogCount)
                {
                    for (int t = 0; t < threadCount; t++)
                        ownedSublogs[t] = [t % sublogCount];
                }
                else
                {
                    var ownedCount = new int[threadCount];
                    for (int s = 0; s < sublogCount; s++)
                        ownedCount[s % threadCount]++;
                    for (int t = 0; t < threadCount; t++)
                        ownedSublogs[t] = new int[ownedCount[t]];
                    var fillOffset = new int[threadCount];
                    for (int s = 0; s < sublogCount; s++)
                    {
                        int t = s % threadCount;
                        ownedSublogs[t][fillOffset[t]++] = s;
                    }
                }

                var perThreadCounts = new int[threadCount];
                for (int t = 0; t < threadCount; t++)
                {
                    var owned = ownedSublogs[t];
                    int count = 0;
                    foreach (var s in owned) count += bucketCounts[s];

                    byte[] keys;
                    if (owned.Length == 1)
                    {
                        keys = perSublogKeysets[owned[0]];
                    }
                    else
                    {
                        keys = GC.AllocateArray<byte>(count * keyLen, pinned: true);
                        int dstOffset = 0;
                        foreach (var s in owned)
                        {
                            int byteLen = bucketCounts[s] * keyLen;
                            Buffer.BlockCopy(perSublogKeysets[s], 0, keys, dstOffset, byteLen);
                            dstOffset += byteLen;
                        }
                    }
                    kvPairBuffers[t] = new KVPairBuffer
                    {
                        Keys = keys,
                        Value = sharedValue,
                        KeyLen = keyLen,
                        Count = count,
                    };
                    perThreadCounts[t] = count;
                }

                var partition = new StringBuilder();
                for (int t = 0; t < threadCount; t++)
                {
                    if (t > 0) partition.Append("; ");
                    partition.Append($"t{t}->{{");
                    for (int i = 0; i < ownedSublogs[t].Length; i++)
                    {
                        if (i > 0) partition.Append(',');
                        partition.Append('s').Append(ownedSublogs[t][i]);
                    }
                    partition.Append('}');
                }
                Console.WriteLine($"threads={threadCount} sublogs={sublogCount} (round-robin): {partition}");
                Console.WriteLine($"  per-thread key counts=[{string.Join(", ", perThreadCounts)}]");
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

        // Hex-encode keyLen characters derived from MurmurHash3(i). High-entropy bytes
        // ensure Utility.HashBytes(key) % k is near-uniform for k up to 64 — unlike the
        // previous "i.ToString().PadLeft(keyLen, 'X')" which clustered low bits and left
        // 6/12/24 of the 16/32/64 buckets empty.
        // If keyLen > 16 (very unusual), the tail is padded with '0'.
        static unsafe void FormatHexKey(byte[] dest, int offset, int keyLen, int i)
        {
            Span<byte> mix = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(mix, i);
            ulong h;
            fixed (byte* p = mix)
                h = Garnet.common.HashUtils.MurmurHash3x64A(p, 8);
            int hexLen = Math.Min(keyLen, 16);
            Encoding.ASCII.GetBytes(h.ToString("x16").AsSpan(0, hexLen), dest.AsSpan(offset, hexLen));
            for (int j = hexLen; j < keyLen; j++)
                dest[offset + j] = (byte)'0';
        }

        public void GenerateData()
        {
            if (!options.IsReplayEnabled)
            {
                // Per-thread buffers are (re)built at the start of each Run() now, so nothing to do here.
                Console.WriteLine($"Pre-built per-sublog keysets from {options.DbSize} hex keys (keyLen={keyLen}, sublogs={options.AofPhysicalSublogCount}).");
                return;
            }

            Console.WriteLine($"Generating AoFBench Data!");
            var threads = options.AofPhysicalSublogCount;
            var workers = new Thread[threads];
            for (var idx = 0; idx < threads; ++idx)
            {
                var x = idx;
                workers[idx] = new Thread(() => GeneratePages(x));
            }

            Stopwatch swatch = new();
            swatch.Start();
            foreach (var worker in workers)
                worker.Start();
            foreach (var worker in workers)
                worker.Join();
            swatch.Stop();

            var seconds = swatch.ElapsedMilliseconds / 1000.0;
            Console.WriteLine($"Generated {threads}x{options.AofGenPages} pages of size {aofServerOptions.AofPageSize} in {seconds:N2} secs");
            Console.WriteLine($"Generated number of AOF records: {total_number_of_aof_records:N0}");
            Console.WriteLine($"Generated number of AOF bytes: {total_number_of_aof_bytes:N0}");
        }

        unsafe void GeneratePages(int threadId)
        {
            var seqNumGen = new SequenceNumberGenerator(0);
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
                        var pageEnd = pageOffset + page.Length - (pageIdx == 0 ? 64 : 0);
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
                                    sequenceNumber = seqNumGen.GetSequenceNumber()
                                };
                                if (!garnetLog.GetSubLog(threadId).DummyEnqueue(
                                    ref pageOffset, pageEnd, extendedAofHeader, key, v, ref input))
                                    break;
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
    }
}