// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Text;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Resp.benchmark
{
    /// <summary>
    /// A pre-built RESP message containing AOF page data.
    /// The buffer layout is: [reserved prefix space][RESP message bytes]
    /// messageOffset points to where the actual RESP message starts within the buffer.
    /// </summary>
    public class RespPage
    {
        /// <summary>Pinned buffer holding the complete RESP message</summary>
        public byte[] buffer;
        /// <summary>Offset within buffer where the RESP message starts</summary>
        public int messageOffset;
        /// <summary>Length of the RESP message</summary>
        public int messageLength;
        /// <summary>AOF payload bytes (for stats)</summary>
        public int payloadLength;
        /// <summary>Number of AOF records in the payload</summary>
        public int recordCount;

        public RespPage(int totalSize)
        {
            buffer = GC.AllocateArray<byte>(totalSize, pinned: true);
        }
    }

    public sealed class AofGen
    {
        /// <summary>
        /// Maximum reserved space for the RESP prefix (array header, CLUSTER, APPENDLOG, nodeId, addresses, bulk string header).
        /// This is generous — actual prefix is typically ~100-150 bytes.
        /// </summary>
        const int MaxRespPrefixSize = 256;

        /// <summary>
        /// Suffix is just "\r\n" after the bulk string payload.
        /// </summary>
        const int RespSuffixSize = 2;

        readonly GarnetLog garnetLog;

        public readonly GarnetAppendOnlyFile appendOnlyFile;

        readonly Options options;
        readonly GarnetServerOptions aofServerOptions;

        /// <summary>
        /// threads x pageNum — pre-built RESP messages for Replay benchmark
        /// </summary>
        RespPage[][] respPageBuffers;

        /// <summary>
        /// DBSize kv pairs
        /// </summary>
        List<(byte[], byte[])>[] kvPairBuffers;

        long total_number_of_aof_records = 0L;
        long total_number_of_aof_bytes = 0L;

        public RespPage[] GetRespPageBuffers(int threadIdx) => respPageBuffers[threadIdx];
        public List<(byte[], byte[])> GetKVPairBuffer(int threadIdx) => kvPairBuffers[threadIdx];

        readonly LightEpoch aofEpoch;

        /// <summary>
        /// Primary node ID, needed for building RESP messages.
        /// Set externally by AofBench before calling GenerateData().
        /// </summary>
        public string PrimaryId { get; set; }

        public AofGen(Options options)
        {
            this.options = options;
            this.aofEpoch = new LightEpoch();
            this.aofServerOptions = new GarnetServerOptions()
            {
                EnableAOF = true,
                AofMemorySize = options.AofMemorySize,
                AofPageSize = options.AofPageSize,
                UseAofNullDevice = true,
                EnableFastCommit = true,
                CommitFrequencyMs = -1,
                FastAofTruncate = true,
                AofReplicationRefreshFrequencyMs = 10,
                EnableCluster = true,
                ReplicationOffsetMaxLag = 0,
                AofPhysicalSublogCount = options.AofPhysicalSublogCount
            };
            aofServerOptions.GetAofSettings(0, aofEpoch, out var logSettings);
            appendOnlyFile = new GarnetAppendOnlyFile(aofServerOptions, logSettings, Program.loggerFactory.CreateLogger("AofGen - AOF instance"));
            garnetLog = new GarnetLog(appendOnlyFile, aofServerOptions, logSettings);

            if (options.AofBenchType == AofBenchType.Replay)
            {
                respPageBuffers = new RespPage[options.AofPhysicalSublogCount][];
            }
            else
            {
                kvPairBuffers = new List<(byte[], byte[])>[options.NumThreads.Max()];
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

        byte[] GetKey() => Encoding.ASCII.GetBytes(Generator.CreateHexId(size: Math.Max(options.KeyLength, 8)));

        byte[] GetKey(int threadId)
        {
            while (true)
            {
                var keyData = Encoding.ASCII.GetBytes(Generator.CreateHexId(size: Math.Max(options.KeyLength, 8)));
                var hash = GarnetLog.HASH(keyData.AsSpan());
                var physicalSublogIdx = (int)(hash % garnetLog.Size);
                if (physicalSublogIdx == threadId) return keyData;
            }
        }

        byte[] GetValue() => Encoding.ASCII.GetBytes(Generator.CreateHexId(size: Math.Max(options.ValueLength, 8)));

        List<(byte[], byte[])> GenerateKVPairs(int threadId, bool random)
        {
            var kvPairs = new List<(byte[], byte[])>();

            for (var i = 0; i < options.DbSize; i++)
            {
                var key = random ? GetKey() : GetKey(threadId);
                var value = GetValue();
                kvPairs.Add((key, value));
            }
            return kvPairs;
        }

        public unsafe void GenerateData()
        {
            var seqNumGen = new SequenceNumberGenerator(0);
            Console.WriteLine($"Generating AoFBench Data!");
            var threads = options.AofBenchType == AofBenchType.Replay ? options.AofPhysicalSublogCount : options.NumThreads.Max();
            var workers = new Thread[threads];

            // Run the experiment.
            for (var idx = 0; idx < threads; ++idx)
            {
                var x = idx;
                workers[idx] = options.AofBenchType switch
                {
                    AofBenchType.Replay => new Thread(() => GenerateRespPages(x)),
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
            if (options.AofBenchType == AofBenchType.Replay)
            {
                Console.WriteLine($"Generated {threads}x{options.AofGenPages} RESP pages of size {aofServerOptions.AofPageSize} in {seconds:N2} secs");
                Console.WriteLine($"Generated number of AOF records: {total_number_of_aof_records:N0}");
                Console.WriteLine($"Generated number of AOF bytes: {total_number_of_aof_bytes:N0}");
            }
            else
            {
                Console.WriteLine($"Generated {threads}x{options.DbSize} KV pairs in {seconds:N2} secs");
            }

            void GenerateRespPages(int threadId)
            {
                var number_of_aof_records = 0L;
                var number_of_aof_bytes = 0L;
                var kvPairs = GenerateKVPairs(threadId, options.AofPhysicalSublogCount == 1);
                var pages = options.AofGenPages;
                var pageSize = 1 << aofServerOptions.AofPageSizeBits();

                respPageBuffers[threadId] = new RespPage[pages];

                // Compute addresses deterministically (matching RunAofReplayBench logic)
                var previousAddress = 64L; // Initial previousAddress for first message (will be set by AofSync)
                var currentAddress = 64L;

                for (var i = 0; i < pages; i++)
                {
                    // Allocate buffer: prefix reserved space + page size + suffix
                    var respPage = new RespPage(MaxRespPrefixSize + pageSize + RespSuffixSize);
                    respPageBuffers[threadId][i] = respPage;

                    // First, fill AOF records into the payload region (after reserved prefix space)
                    // We'll write the RESP framing around it afterward
                    var payloadLength = FillPayload(threadId, kvPairs, i, respPage, pageSize);

                    // Compute addresses for this page
                    var nextAddress = currentAddress + payloadLength;

                    // Build complete RESP message in-place
                    BuildRespMessage(respPage, threadId, previousAddress, currentAddress, nextAddress, payloadLength);

                    // Advance addresses for next page
                    previousAddress = currentAddress;
                    currentAddress = currentAddress == 64 ? pageSize : currentAddress + pageSize;
                }

                _ = Interlocked.Add(ref total_number_of_aof_records, number_of_aof_records);
                _ = Interlocked.Add(ref total_number_of_aof_bytes, number_of_aof_bytes);

                int FillPayload(int threadId, List<(byte[], byte[])> kvPairs, int pageCount, RespPage respPage, int pageSize)
                {
                    fixed (byte* bufferPtr = respPage.buffer)
                    {
                        // AOF payload starts after the reserved prefix space
                        var payloadStart = bufferPtr + MaxRespPrefixSize;
                        var pageOffset = payloadStart;
                        // First page starts from 64 address, so the payload space must be smaller
                        var pageEnd = payloadStart + pageSize - (pageCount == 0 ? 64 : 0);
                        var kvOffset = 0;
                        var recordCount = 0;

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
                                    var extendedAofHeader = new AofShardedHeader
                                    {
                                        basicHeader = new AofHeader
                                        {
                                            padding = (byte)AofHeaderType.ShardedHeader,
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
                                recordCount++;
                            }
                        }

                        var payloadLength = (int)(pageOffset - payloadStart);
                        respPage.payloadLength = payloadLength;
                        respPage.recordCount = recordCount;
                        number_of_aof_records += recordCount;
                        number_of_aof_bytes += payloadLength;
                        return payloadLength;
                    }
                }
            }

            void GenerateKeys(int threadId)
            {
                kvPairBuffers[threadId] = GenerateKVPairs(threadId, options.AofBenchType == AofBenchType.EnqueueRandom);
                //Console.WriteLine($"[{threadId}] - Generated {kvPairBuffers[threadId].Count} KV pairs for {options.AofBenchType}");
            }
        }

        /// <summary>
        /// Build a complete RESP CLUSTER APPENDLOG message in the RespPage buffer.
        /// The AOF payload is already at buffer[MaxRespPrefixSize .. MaxRespPrefixSize + payloadLength].
        /// We write the RESP prefix backwards from the payload, and the suffix (\r\n) after the payload.
        /// </summary>
        unsafe void BuildRespMessage(RespPage respPage, int physicalSublogIdx, long previousAddress, long currentAddress, long nextAddress, int payloadLength)
        {
            fixed (byte* bufferPtr = respPage.buffer)
            {
                // Step 1: Write the bulk string header for the payload just before the payload data
                // Format: $<payloadLength>\r\n
                // We write this right-to-left into the reserved prefix space

                // Step 2: Write the suffix (\r\n) after payload
                var suffixPtr = bufferPtr + MaxRespPrefixSize + payloadLength;
                suffixPtr[0] = (byte)'\r';
                suffixPtr[1] = (byte)'\n';

                // Step 3: Build the full RESP prefix into a temporary area, then copy it
                // We use the beginning of the buffer as scratch space to write the prefix
                var prefixBuf = stackalloc byte[MaxRespPrefixSize];
                var curr = prefixBuf;
                var end = prefixBuf + MaxRespPrefixSize;

                var CLUSTER = "$7\r\nCLUSTER\r\n"u8;
                var appendLog = "APPENDLOG"u8;

                // *8\r\n
                if (!RespWriteUtils.TryWriteArrayLength(8, ref curr, end))
                    throw new GarnetException("RESP prefix buffer too small");
                // $7\r\nCLUSTER\r\n
                if (!RespWriteUtils.TryWriteDirect(CLUSTER, ref curr, end))
                    throw new GarnetException("RESP prefix buffer too small");
                // $9\r\nAPPENDLOG\r\n
                if (!RespWriteUtils.TryWriteBulkString(appendLog, ref curr, end))
                    throw new GarnetException("RESP prefix buffer too small");
                // nodeId
                if (!RespWriteUtils.TryWriteAsciiBulkString(PrimaryId, ref curr, end))
                    throw new GarnetException("RESP prefix buffer too small");
                // physicalSublogIdx
                if (!RespWriteUtils.TryWriteArrayItem(physicalSublogIdx, ref curr, end))
                    throw new GarnetException("RESP prefix buffer too small");
                // previousAddress
                if (!RespWriteUtils.TryWriteArrayItem(previousAddress, ref curr, end))
                    throw new GarnetException("RESP prefix buffer too small");
                // currentAddress
                if (!RespWriteUtils.TryWriteArrayItem(currentAddress, ref curr, end))
                    throw new GarnetException("RESP prefix buffer too small");
                // nextAddress
                if (!RespWriteUtils.TryWriteArrayItem(nextAddress, ref curr, end))
                    throw new GarnetException("RESP prefix buffer too small");
                // $<payloadLength>\r\n  (bulk string header for payload — the payload data itself is already in place)
                var payloadLenDigits = Garnet.common.NumUtils.CountDigits(payloadLength);
                var bulkStringHeaderLen = 1 + payloadLenDigits + 2; // $<digits>\r\n
                if (curr + bulkStringHeaderLen > end)
                    throw new GarnetException("RESP prefix buffer too small");
                *curr++ = (byte)'$';
                Garnet.common.NumUtils.WriteInt32(payloadLength, payloadLenDigits, ref curr);
                *curr++ = (byte)'\r';
                *curr++ = (byte)'\n';

                var prefixLength = (int)(curr - prefixBuf);

                // The RESP message starts at (MaxRespPrefixSize - prefixLength) in the buffer
                var messageOffset = MaxRespPrefixSize - prefixLength;
                Buffer.MemoryCopy(prefixBuf, bufferPtr + messageOffset, prefixLength, prefixLength);

                respPage.messageOffset = messageOffset;
                respPage.messageLength = prefixLength + payloadLength + RespSuffixSize;
            }
        }
    }
}
