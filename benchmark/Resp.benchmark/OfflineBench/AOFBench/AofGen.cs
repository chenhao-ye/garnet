// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
        /// </summary>
        KVPairBuffer[] kvPairBuffers;

        long total_number_of_aof_records = 0L;
        long total_number_of_aof_bytes = 0L;

        public Page[] GetPageBuffers(int threadIdx) => pageBuffers[threadIdx];
        public KVPairBuffer GetKVPairBuffer(int threadIdx) => kvPairBuffers[threadIdx];

        readonly LightEpoch aofEpoch;

        readonly int keyLen;

        public AofGen(Options options)
        {
            this.options = options;
            this.aofEpoch = new LightEpoch();
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
                AofReplicationRefreshFrequencyMs = options.AofReplicationRefreshFrequencyMs,
                EnableCluster = true,
                ReplicationOffsetMaxLag = 0,
                AofPhysicalSublogCount = options.AofPhysicalSublogCount,
                AofReplayTaskCount = options.AofReplayTaskCount
            };
            aofServerOptions.GetAofSettings(0, aofEpoch, out var logSettings);
            appendOnlyFile = new GarnetAppendOnlyFile(aofServerOptions, logSettings, Program.loggerFactory.CreateLogger("AofGen - AOF instance"));
            garnetLog = appendOnlyFile.Log;

            if (options.IsReplayEnabled)
            {
                pageBuffers = new Page[options.AofPhysicalSublogCount][];
            }
            else
            {
                kvPairBuffers = new KVPairBuffer[options.NumThreads.Max()];
            }

            if (options.AofPhysicalSublogCount != options.NumThreads.Max() && options.AofBenchType == AofBenchType.EnqueueSharded)
                throw new Exception("Use --threads(MAX)== --aof-sublog-count to generated perfectly sharded data!");
        }

        public NetworkBufferSettings GetAofSyncNetworkBufferSettings()
        {
            var aofSyncSendBufferSize = 2 << aofServerOptions.AofPageSizeBits();
            var aofSyncInitialReceiveBufferSize = 1 << 17;
            return new(aofSyncSendBufferSize, aofSyncInitialReceiveBufferSize);
        }

        byte[] GetKey(Random rng, ZipfGenerator zipf)
        {
            int key = zipf != null ? zipf.Next() : rng.Next(options.DbSize);
            return Encoding.ASCII.GetBytes(key.ToString().PadLeft(keyLen, 'X'));
        }

        byte[] GetKey(int threadId, Random rng, ZipfGenerator zipf)
        {
            while (true)
            {
                var keyData = GetKey(rng, zipf);
                var physicalSublogIdx = garnetLog.GetPhysicalSublogIdx(keyData.AsSpan());
                if (physicalSublogIdx == threadId) return keyData;
            }
        }

        byte[] GetValue() => Encoding.ASCII.GetBytes(Generator.CreateHexId(size: Math.Max(options.ValueLength, 8)));

        List<(byte[], byte[])> GenerateKVPairs(int threadId, bool random, int count)
        {
            var rng = new Random(789110123 + threadId);
            var zipf = options.Zipf
                ? new ZipfGenerator(new RandomGenerator((uint)(789110123 + threadId)), options.DbSize, 0.99)
                : null;

            var kvPairs = new List<(byte[], byte[])>(count);
            for (var i = 0; i < count; i++)
            {
                var key = random ? GetKey(rng, zipf) : GetKey(threadId, rng, zipf);
                var value = GetValue();
                kvPairs.Add((key, value));
            }
            return kvPairs;
        }

        public void GenerateData()
        {
            Console.WriteLine($"Generating AoFBench Data!");
            var threads = options.IsReplayEnabled ? options.AofPhysicalSublogCount : options.NumThreads.Max();
            var workers = new Thread[threads];

            // Run the experiment.
            for (var idx = 0; idx < threads; ++idx)
            {
                var x = idx;
                workers[idx] = options.AofBenchType switch
                {
                    AofBenchType.Replay or AofBenchType.ReplayNoResp or AofBenchType.ReplayDirect => new Thread(() => GeneratePages(x)),
                    AofBenchType.EnqueueSharded or AofBenchType.EnqueueRandom => new Thread(() => GenerateKeys(x)),
                    _ => throw new Exception($"AofBenchType {options.AofBenchType} not supported"),
                };
            }

            Stopwatch swatch = new();
            swatch.Start();

            // Start threads.
            foreach (var worker in workers)
                worker.Start();

            // Wait for workers to complete
            foreach (var worker in workers)
                worker.Join();

            swatch.Stop();

            var seconds = swatch.ElapsedMilliseconds / 1000.0;
            if (options.IsReplayEnabled)
            {
                Console.WriteLine($"Generated {threads}x{options.AofGenPages} pages of size {aofServerOptions.AofPageSize} in {seconds:N2} secs");
                Console.WriteLine($"Generated number of AOF records: {total_number_of_aof_records:N0}");
                Console.WriteLine($"Generated number of AOF bytes: {total_number_of_aof_bytes:N0}");
            }
            else
            {
                var bufferLen = options.AofGenRecords > 0 ? options.AofGenRecords : 2 * options.DbSize;
                Console.WriteLine($"Generated {threads}x{bufferLen} KV pairs in {seconds:N2} secs");
            }
        }

        unsafe void GeneratePages(int threadId)
        {
            var seqNumGen = new SequenceNumberGenerator(0);
            var number_of_aof_records = 0L;
            var number_of_aof_bytes = 0L;
            var kvPairs = GenerateKVPairs(threadId, options.AofPhysicalSublogCount == 1, options.DbSize);
            // Console.WriteLine($"[{threadId}] {string.Join(',', kvPairs.Select(x => Encoding.ASCII.GetString(x.Item1) + "=" + Encoding.ASCII.GetString(x.Item2)))}");
            var pages = options.AofGenPages;
            pageBuffers[threadId] = new Page[pages];
            for (var i = 0; i < pages; i++)
            {
                pageBuffers[threadId][i] = new Page(1 << aofServerOptions.AofPageSizeBits());
                FillPage(threadId, kvPairs, i, pageBuffers[threadId][i]);
            }

            // Console.WriteLine($"[{threadId}] - Generated {number_of_aof_records:N0} AOF records, {number_of_aof_bytes:N0} AOF bytes");
            _ = Interlocked.Add(ref total_number_of_aof_records, number_of_aof_records);
            _ = Interlocked.Add(ref total_number_of_aof_bytes, number_of_aof_bytes);

            void FillPage(int threadId, List<(byte[], byte[])> kvPairs, int pageCount, Page page)
            {
                fixed (byte* pagePtr = page.payload)
                {
                    var pageOffset = pagePtr;
                    // First page starts from 64 address, so the payload space must be smaller
                    var pageEnd = pageOffset + page.Length - (pageCount == 0 ? 64 : 0);
                    var kvOffset = 0;
                    while (true)
                    {
                        var kvPair = kvPairs[kvOffset++ % kvPairs.Count];
                        var keyData = kvPair.Item1;
                        var valueData = kvPair.Item2;
                        StringInput input = default;
                        fixed (byte* keyPtr = keyData)
                        fixed (byte* valuePtr = valueData)
                        {
                            var key = SpanByte.FromPinnedPointer(keyPtr, keyData.Length);
                            var value = SpanByte.FromPinnedPointer(valuePtr, valueData.Length);
                            var aofHeader = new AofHeader { opType = AofEntryType.StoreUpsert, storeVersion = 1, sessionID = 0 };
                            var useShardedHeader = options.AofPhysicalSublogCount > 1 || options.AofReplayTaskCount > 1;
                            if (!useShardedHeader)
                            {
                                if (!garnetLog.GetSubLog(threadId).DummyEnqueue(
                                    ref pageOffset,
                                    pageEnd,
                                    aofHeader,
                                    key,
                                    value,
                                    ref input))
                                    break;
                            }
                            else
                            {
                                var replayTag = garnetLog.GetReplayTag(new ReadOnlySpan<byte>(keyPtr, keyData.Length));
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
                                    ref pageOffset,
                                    pageEnd,
                                    extendedAofHeader,
                                    key,
                                    value,
                                    ref input))
                                    break;
                            }
                            page.recordCount++;
                        }
                    }

                    var payloadLength = (int)(pageOffset - pagePtr);
                    page.payloadLength = payloadLength;
                    number_of_aof_records += page.recordCount;
                    number_of_aof_bytes += payloadLength;
                }
            }
        }

        void GenerateKeys(int threadId)
        {
            var count = options.AofGenRecords > 0 ? options.AofGenRecords : 2 * options.DbSize;
            var rng = new Random(789110123 + threadId);
            var zipf = options.Zipf
                ? new ZipfGenerator(new RandomGenerator((uint)(789110123 + threadId)), options.DbSize, 0.99)
                : null;
            var isRandom = options.AofBenchType == AofBenchType.EnqueueRandom;

            var keys = GC.AllocateArray<byte>(count * keyLen, pinned: true);
            var keysSpan = keys.AsSpan();
            for (var i = 0; i < count; i++)
            {
                var slot = keysSpan.Slice(i * keyLen, keyLen);
                while (true)
                {
                    int k = zipf != null ? zipf.Next() : rng.Next(options.DbSize);
                    WriteKeyBytes(slot, k);
                    if (isRandom) break;
                    if (garnetLog.GetPhysicalSublogIdx(slot) == threadId) break;
                }
            }

            var valueLen = Math.Max(options.ValueLength, 8);
            var value = GC.AllocateArray<byte>(valueLen, pinned: true);
            Encoding.ASCII.GetBytes(Generator.CreateHexId(size: valueLen), value);

            kvPairBuffers[threadId] = new KVPairBuffer
            {
                Keys = keys,
                Value = value,
                KeyLen = keyLen,
                Count = count,
            };
        }

        void WriteKeyBytes(Span<byte> dest, int key)
        {
            int pos = dest.Length;
            int n = key;
            do
            {
                dest[--pos] = (byte)('0' + (n % 10));
                n /= 10;
            } while (n > 0);
            while (pos > 0) dest[--pos] = (byte)'X';
        }
    }
}