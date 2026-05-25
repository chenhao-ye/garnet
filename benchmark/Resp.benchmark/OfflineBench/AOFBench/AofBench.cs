// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using System.Text;
using Garnet.server;
using HdrHistogram;
using Tsavorite.core;

namespace Resp.benchmark
{
    public class AofBench
    {
        public static GarnetServerOptions GetServerOptions(Options options)
        {
            var serverOptions = new GarnetServerOptions
            {
                ClusterAnnounceEndpoint = new IPEndPoint(IPAddress.Loopback, 6379),
                QuietMode = true,
                IndexMemorySize = options.IndexMemorySize,
                EnableAOF = options.EnableAOF || options.AofBench,
                EnableCluster = options.EnableCluster,
                ClusterConfigFlushFrequencyMs = -1,
                FastAofTruncate = options.EnableCluster && options.UseAofNullDevice,
                UseAofNullDevice = options.UseAofNullDevice,
                AofMemorySize = options.AofMemorySize,
                AofPageSize = options.AofPageSize,
                CommitFrequencyMs = options.CommitFrequencyMs,
                AofPhysicalSublogCount = options.AofPhysicalSublogCount,
                AofReplayTaskCount = options.AofReplayTaskCount,
                ReplicationOffsetMaxLag = 0,
                CheckpointDir = OperatingSystem.IsLinux() ? "/tmp" : null,
            };
            return serverOptions;
        }

        readonly ManualResetEventSlim waiter = new();
        readonly Options options;
        readonly AofGen aofGen;
        readonly AofReplayStream[] aofReplayStream;
        readonly GarnetServerInstance instance;
        StringBuilder stats = new();
        long total_bytes_processed = 0;
        long total_pages_processed = 0;
        long total_records_replayed = 0;
        long total_records_enqueued = 0;

        volatile bool done = false;

        AofAddress aofTailAddress;
        readonly LightEpoch epoch;

        long readerOperationsCompleted;
        static readonly long HistogramLowerBound = 1;
        static readonly long HistogramUpperBound = TimeStamp.Seconds(100);
        const int HistogramSigFigs = 2;

        public AofBench(Options options)
        {
            this.options = options;

            var replayEnabled = options.AofBenchType is AofBenchType.Replay or AofBenchType.ReplayNoResp;
            if (!options.EnableCluster && options.AofBenchType == AofBenchType.Replay)
                throw new Exception("InProc/AofBench with AofBenchType.Replay requires --cluster!");

            var serverOptions = GetServerOptions(options);
            aofGen = new AofGen(options);

            if (options.IsReplayEnabled)
            {
                options.EnableCluster = true;
                instance = new GarnetServerInstance(options);
                aofReplayStream = [.. Enumerable.Range(0, options.AofPhysicalSublogCount).Select(
                    x => new AofReplayStream(instance, threadId: x, startAddress: 64, options))];
            }
            else
            {
                epoch = new LightEpoch();
            }
        }

        public void GenerateData() => aofGen.GenerateData();

        public void Run(int threads)
        {
            aofGen.BuildKVPairBuffersForRun(threads);
            var workers = new Thread[threads];

            var useReaders = options.IsReplayEnabled && options.AofReplayReader > 0;
            RespServerSession[] readerSessions = null;
            LongHistogram[] readerHistograms = null;

            Console.WriteLine($"Epoch instance count:{LightEpoch.ActiveInstanceCount()}");

            try
            {
                var msg = options.AofBenchType switch
                {
                    AofBenchType.Replay or AofBenchType.ReplayNoResp or AofBenchType.ReplayDirect => $">>> Running {options.AofBenchType} using {threads}x{options.AofReplayTaskCount} worker(s) >>>",
                    AofBenchType.EnqueueSharded or AofBenchType.EnqueueRandom => $">>> Running {options.AofBenchType} using {threads} worker(s) >>>",
                    _ => throw new Exception($"AofBenchType {options.AofBenchType} not supported"),
                };
                Console.WriteLine(msg);

                if (options.IsReplayEnabled)
                    aofTailAddress = aofGen.appendOnlyFile.Log.TailAddress;

                if (useReaders)
                {
                    instance.server.StoreWrapper.appendOnlyFile.CreateOrUpdateKeySequenceManager();
                    if (options.AofReaderSkip)
                        for (var i = 0; i < options.AofPhysicalSublogCount; i++)
                            RaiseSublogFrontierToMax(i);
                    readerSessions = instance.server.GetRespSessions(options.AofReplayReader);
                    readerHistograms = new LongHistogram[options.AofReplayReader];
                    for (var i = 0; i < options.AofReplayReader; i++)
                        readerHistograms[i] = new LongHistogram(HistogramLowerBound, HistogramUpperBound, HistogramSigFigs);
                }

                // Run the experiment.
                for (var idx = 0; idx < threads; ++idx)
                {
                    var x = idx;
                    workers[idx] = options.AofBenchType switch
                    {
                        AofBenchType.Replay => new Thread(() => RunAofReplayBench(x)),
                        AofBenchType.ReplayNoResp => new Thread(() => RunAofReplayBenchNoResp(x)),
                        AofBenchType.ReplayDirect => new Thread(() => RunAofReplayBenchDirect(x)),
                        AofBenchType.EnqueueSharded or AofBenchType.EnqueueRandom => new Thread(() => RunAofEnqueBench(x)),
                        _ => throw new Exception($"AofBenchType {options.AofBenchType} not supported"),
                    };
                }

                Thread[] readers = null;
                if (useReaders)
                {
                    readers = new Thread[options.AofReplayReader];
                    for (var idx = 0; idx < options.AofReplayReader; idx++)
                    {
                        var x = idx;
                        var session = readerSessions[idx];
                        var hist = readerHistograms[idx];
                        readers[idx] = new Thread(() => RunReader(x, session, hist));
                    }
                }

                // Start threads.
                foreach (var worker in workers)
                    worker.Start();
                if (readers != null)
                {
                    foreach (var r in readers)
                        r.Start();
                }

                waiter.Set();

                Stopwatch swatch = new();
                swatch.Start();
                if (useReaders)  // replay timestamp must be monotonic => run a single-pass over AOF pages
                {
                    // Single-pass: the first replay worker to exhaust pages sets `done`
                    foreach (var worker in workers)
                        worker.Join();
                    foreach (var reader in readers)
                        reader.Join();
                }
                else  // cyclic over AOF pages until RunTime
                {
                    Thread.Sleep(TimeSpan.FromSeconds(options.RunTime));
                    done = true;
                    foreach (var worker in workers)
                        worker.Join();
                }

                swatch.Stop();

                var seconds = swatch.ElapsedMilliseconds / 1000.0;
                if (options.IsReplayEnabled)
                {
                    var bytesPerSecond = (total_bytes_processed / seconds) / (double)1_000_000_000;
                    var recordsReplayedPerSecond = total_records_replayed / seconds;
                    Console.WriteLine($"[Total time]: {swatch.ElapsedMilliseconds:N2} ms for {total_bytes_processed:N0} AOF bytes");
                    Console.WriteLine($"[Bandwidth]: {bytesPerSecond:N2} GiB/sec");
                    Console.WriteLine($"[Total pages send]: {total_pages_processed:N0}");
                    Console.WriteLine($"[Total records replayed]: {total_records_replayed:N0}");
                    Console.WriteLine($"[Throughput]: {recordsReplayedPerSecond:N2} records/sec");
                }
                else
                {
                    var bytesPerSecond = (total_bytes_processed / seconds) / (double)1_000_000_000;
                    var recordsEnqueuedPerSecond = total_records_enqueued / seconds;
                    Console.WriteLine($"[Total time]: {swatch.ElapsedMilliseconds:N2} ms for {total_bytes_processed:N0} AOF bytes");
                    Console.WriteLine($"[Bandwidth]: {bytesPerSecond:N2} GiB/sec");
                    Console.WriteLine($"[Total records enqueued]: {total_records_enqueued:N0}");
                    Console.WriteLine($"[Throughput]: {recordsEnqueuedPerSecond:N2} records/sec");
                }

                if (useReaders)
                {
                    var readerThroughput = readerOperationsCompleted / seconds;
                    Console.WriteLine($"[Reader operations]: {readerOperationsCompleted:N0}");
                    Console.WriteLine($"[Reader throughput]: {readerThroughput:N2} ops/sec");
                    var merged = new LongHistogram(HistogramLowerBound, HistogramUpperBound, HistogramSigFigs);
                    foreach (var h in readerHistograms)
                        merged.Add(h);
                    if (merged.TotalCount > 0)
                    {
                        var s = OutputScalingFactor.TimeStampToMicroseconds;
                        Console.WriteLine(
                            $"[Reader latency us] " +
                            $"p50={Math.Round(merged.GetValueAtPercentile(50) / s, 2)} " +
                            $"p90={Math.Round(merged.GetValueAtPercentile(90) / s, 2)} " +
                            $"p99={Math.Round(merged.GetValueAtPercentile(99) / s, 2)} " +
                            $"p99.9={Math.Round(merged.GetValueAtPercentile(99.9) / s, 2)} " +
                            $"max={Math.Round(merged.GetMaxValue() / s, 2)}");
                    }
                }
            }
            finally
            {
                done = false;
                total_records_replayed = 0;
                total_records_enqueued = 0;
                total_bytes_processed = 0;
                readerOperationsCompleted = 0;
                if (readerSessions != null)
                    foreach (var s in readerSessions)
                        s?.Dispose();
                waiter.Reset();
                GC.Collect();
                GC.WaitForPendingFinalizers();
                Console.WriteLine("------------------------------");
            }
        }

        unsafe void RunAofEnqueBench(int threadId)
        {
            var buf = aofGen.GetKVPairBuffer(threadId);
            var keys = buf.Keys;
            var valueBytes = buf.Value;
            var keyLen = buf.KeyLen;
            var count = buf.Count;
            var valueLen = valueBytes.Length;
            var rng = new Random(789110123 + threadId);
            var recordsEnqueued = 0L;
            var bytesEnqueued = 0L;

            waiter.Wait();

            fixed (byte* keysPtr = keys)
            fixed (byte* valPtr = valueBytes)
            {
                var value = SpanByte.FromPinnedPointer(valPtr, valueLen);
                while (!done)
                {
                    int i = rng.Next(count);
                    var key = SpanByte.FromPinnedPointer(keysPtr + i * keyLen, keyLen);
                    var keyHash = GarnetLog.HASH(key);
                    StringInput input = default;
                    aofGen.appendOnlyFile.Log.Enqueue(
                        AofEntryType.StoreUpsert,
                        1,
                        threadId,
                        key,
                        value,
                        ref input,
                        epoch,
                        keyHash,
                        out _);
                    bytesEnqueued += sizeof(AofShardedHeader) + key.TotalSize() + value.TotalSize() + input.SerializedLength;
                    recordsEnqueued++;
                }
            }
            //Console.WriteLine($"[{threadId}] - Enqueued: {recordsEnqueued:N0} records");
            _ = Interlocked.Add(ref total_records_enqueued, recordsEnqueued);
            _ = Interlocked.Add(ref total_bytes_processed, bytesEnqueued);
        }

        // Set the physical sublog's frontier to long.MaxValue.
        // Any reader currently parked inside BeforeConsistentReadKey on this sublog returns immediately.
        // Subsequent reads on this sublog skip the wait path.
        void RaiseSublogFrontierToMax(int physicalSublogIdx)
            => instance.server.StoreWrapper.appendOnlyFile.readConsistencyManager
                ?.UpdatePhysicalSublogMaxSequenceNumber(physicalSublogIdx, long.MaxValue);

        unsafe void RunAofReplayBench(int threadId)
        {
            var buffers = aofGen.GetPageBuffers(threadId);
            var offset = 0;
            var currentAddress = 64L;
            var nextAddress = 64L;
            var pagesSend = 0L;
            var totalBytes = 0L;
            var recordsReplayedCount = 0L;
            var singlePass = options.AofReplayReader > 0;

            waiter.Wait();

            // Initialize stream for replay
            aofReplayStream[threadId].InitializeReplayStream();

            while (!done)
            {
                if (singlePass && offset >= buffers.Length)
                {
                    done = true;
                    break;
                }
                var pos = offset++ % buffers.Length;
                var currPage = buffers[pos];
                fixed (byte* payloadPtr = currPage.payload)
                {
                    nextAddress = currentAddress + currPage.payloadLength;
                    aofReplayStream[threadId].Consume(payloadPtr, currPage.payloadLength, currentAddress, nextAddress, isProtected: false);

                    // First page has a valid address from 64.
                    // After that currentAddress starts from beginning of bage (i.e. multiple of page size)
                    currentAddress = currentAddress == 64 ? currPage.Length : currentAddress + currPage.Length;
                    pagesSend++;
                    totalBytes += currPage.payloadLength;
                    recordsReplayedCount += currPage.recordCount;
                }
            }

            if (singlePass)
                RaiseSublogFrontierToMax(threadId);
            _ = Interlocked.Add(ref total_pages_processed, pagesSend);
            _ = Interlocked.Add(ref total_bytes_processed, totalBytes);
            _ = Interlocked.Add(ref total_records_replayed, recordsReplayedCount);
        }

        unsafe void RunAofReplayBenchNoResp(int threadId)
        {
            var buffers = aofGen.GetPageBuffers(threadId);
            var offset = 0;
            var currentAddress = 64L;
            var nextAddress = 64L;
            var pagesSend = 0L;
            var totalBytes = 0L;
            var recordsReplayedCount = 0L;
            var singlePass = options.AofReplayReader > 0;

            waiter.Wait();

            // Initialize stream for replay
            aofReplayStream[threadId].InitializeReplayStream();

            while (!done)
            {
                if (singlePass && offset >= buffers.Length)
                {
                    done = true;
                    break;
                }
                var pos = offset++ % buffers.Length;
                var currPage = buffers[pos];
                fixed (byte* payloadPtr = currPage.payload)
                {
                    nextAddress = currentAddress + currPage.payloadLength;
                    aofReplayStream[threadId].ConsumeNoResp(payloadPtr, currPage.payloadLength, currentAddress, nextAddress, isProtected: false);

                    // First page has a valid address from 64.
                    // After that currentAddress starts from beginning of bage (i.e. multiple of page size)
                    currentAddress = currentAddress == 64 ? currPage.Length : currentAddress + currPage.Length;
                    pagesSend++;
                    totalBytes += currPage.payloadLength;
                    recordsReplayedCount += currPage.recordCount;
                }
            }

            if (singlePass)
                RaiseSublogFrontierToMax(threadId);
            _ = Interlocked.Add(ref total_pages_processed, pagesSend);
            _ = Interlocked.Add(ref total_bytes_processed, totalBytes);
            _ = Interlocked.Add(ref total_records_replayed, recordsReplayedCount);
        }

        unsafe void RunAofReplayBenchDirect(int threadId)
        {
            var buffers = aofGen.GetPageBuffers(threadId);
            var offset = 0;
            var currentAddress = 64L;
            var nextAddress = 64L;
            var pagesSend = 0L;
            var totalBytes = 0L;
            var recordsReplayedCount = 0L;
            var singlePass = options.AofReplayReader > 0;

            waiter.Wait();

            // Initialize stream for replay
            aofReplayStream[threadId].InitializeReplayStream();

            while (!done)
            {
                if (singlePass && offset >= buffers.Length)
                {
                    done = true;
                    break;
                }
                var pos = offset++ % buffers.Length;
                var currPage = buffers[pos];
                fixed (byte* payloadPtr = currPage.payload)
                {
                    nextAddress = currentAddress + currPage.payloadLength;
                    aofReplayStream[threadId].ConsumeDirect(payloadPtr, currPage.payloadLength, currentAddress, nextAddress, isProtected: false);

                    // First page has a valid address from 64.
                    // After that currentAddress starts from beginning of bage (i.e. multiple of page size)
                    currentAddress = currentAddress == 64 ? currPage.Length : currentAddress + currPage.Length;
                    pagesSend++;
                    totalBytes += currPage.payloadLength;
                    recordsReplayedCount += currPage.recordCount;
                }
            }

            if (singlePass)
                RaiseSublogFrontierToMax(threadId);
            _ = Interlocked.Add(ref total_pages_processed, pagesSend);
            _ = Interlocked.Add(ref total_bytes_processed, totalBytes);
            _ = Interlocked.Add(ref total_records_replayed, recordsReplayedCount);
        }

        unsafe void RunReader(int threadId, RespServerSession session, LongHistogram hist)
        {
            var keys = aofGen.GlobalKeys;
            var keyLen = aofGen.KeyLen;
            var keyCount = keys.Length / keyLen;
            var rng = new Random(0xCAFE + threadId);
            var opsCompleted = 0L;

            // Pre-format GET command frame:
            //   "*2\r\n$3\r\nGET\r\n$<keyLen>\r\n<key bytes>\r\n"
            var prefix = $"*2\r\n$3\r\nGET\r\n${keyLen}\r\n";
            var prefixBytes = Encoding.ASCII.GetBytes(prefix);
            var totalLen = prefixBytes.Length + keyLen + 2;
            var buf = GC.AllocateArray<byte>(totalLen, pinned: true);
            Buffer.BlockCopy(prefixBytes, 0, buf, 0, prefixBytes.Length);
            buf[totalLen - 2] = (byte)'\r';
            buf[totalLen - 1] = (byte)'\n';

            waiter.Wait();

            fixed (byte* bufPtr = buf)
            fixed (byte* keysPtr = keys)
            {
                var keyDst = bufPtr + prefixBytes.Length;
                var prev = Stopwatch.GetTimestamp();
                while (!done)
                {
                    var idx = rng.Next(keyCount);
                    Buffer.MemoryCopy(keysPtr + idx * keyLen, keyDst, keyLen, keyLen);
                    session.TryConsumeMessages(bufPtr, totalLen);
                    var now = Stopwatch.GetTimestamp();
                    hist.RecordValue(now - prev);
                    prev = now;
                    opsCompleted++;
                }
            }
            _ = Interlocked.Add(ref readerOperationsCompleted, opsCompleted);
        }
    }
}
