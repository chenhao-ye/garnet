// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using System.Text;
using HdrHistogram;
using GarnetClientSession = Garnet.client.GarnetClientSession;

namespace Resp.benchmark
{
    /// <summary>
    /// Load-generating client of the end-to-end replication benchmark:
    /// writer threads issue SETs to a primary while reader threads issue GETs to a replica,
    /// reporting per-pass throughput and latency plus the replication lag.
    /// The primary/replica pair is a real Garnet deployment managed externally.
    /// The client preloads the keyspace, waits for the replica to catch up, then self-paces Repeat passes of RunTime seconds each.
    /// </summary>
    public class ReplicationBench
    {
        readonly Options options;
        readonly ManualResetEventSlim waiter = new();
        volatile bool done = false;

        // Keyset: deterministic from dbsize/keylength (same generator as AofBench), so the
        // preloaded keys and the measured uniform-random keys always agree.
        readonly byte[] keys;
        readonly int keyLen;
        readonly string valuePayload;

        long writerOperationsCompleted;
        long readerOperationsCompleted;
        static readonly long HistogramLowerBound = 1;
        static readonly long HistogramUpperBound = TimeStamp.Seconds(100);
        const int HistogramSigFigs = 2;

        public ReplicationBench(Options options)
        {
            this.options = options;
            if (options.Client != ClientType.GarnetClientSession)
                throw new Exception("--replication-bench requires --client GarnetClientSession");
            if (options.PrimaryPort <= 0 || options.ReplicaPort <= 0)
                throw new Exception("--replication-bench requires --primary-port and --replica-port");
            if (options.ReplicationWriters < 0 || options.ReplicationReaders < 0 || options.ReplicationWriters + options.ReplicationReaders == 0)
                throw new Exception("--replication-bench requires --replication-writers and/or --replication-readers > 0");
            keyLen = AofGen.DeriveKeyLen(options);
            keys = AofGen.BuildGlobalKeys(options.DbSize, keyLen);
            valuePayload = new string('v', options.ValueLength);
        }

        static GarnetClientSession ConnectClient(string host, int port, int sendBufferSize = 1 << 17)
        {
            var c = new GarnetClientSession(new IPEndPoint(IPAddress.Parse(host), port), new(sendBufferSize));
            c.Connect();
            return c;
        }

        static string GetReplicationInfo(GarnetClientSession client)
            => client.ExecuteAsync("INFO", "replication").GetAwaiter().GetResult();

        static string GetInfoField(string info, string name)
        {
            foreach (var line in info.Split('\n'))
            {
                var trimmed = line.TrimEnd('\r');
                if (trimmed.StartsWith(name + ":", StringComparison.Ordinal))
                    return trimmed[(name.Length + 1)..];
            }
            return null;
        }

        // The replication offset is an AofAddress: one offset per physical sublog, printed as
        // a comma-separated vector. On the primary it is the AOF tail; on the replica it is
        // the replayed offset.
        static long[] GetReplicationOffset(GarnetClientSession client)
        {
            var field = GetInfoField(GetReplicationInfo(client), "master_repl_offset")
                ?? throw new Exception("INFO replication did not report master_repl_offset");
            return [.. field.Split(',').Select(long.Parse)];
        }

        // Max over physical sublogs of (primary tail - replica replayed offset), in AOF bytes.
        static long ReplicationLagBytes(GarnetClientSession primary, GarnetClientSession replica)
        {
            var p = GetReplicationOffset(primary);
            var r = GetReplicationOffset(replica);
            var lag = 0L;
            for (var i = 0; i < Math.Min(p.Length, r.Length); i++)
                lag = Math.Max(lag, p[i] - r[i]);
            return lag;
        }

        /// <summary>
        /// Client driver: verifies the replica is attached, preloads the full keyspace into
        /// the primary, waits for the replica to catch up, then runs Repeat passes of
        /// RunTime seconds with writer threads (SET to primary) and reader threads (GET from
        /// replica), printing per-pass throughput/latency blocks and the replication lag.
        /// </summary>
        public void Run()
        {
            using var primaryInfo = ConnectClient(options.PrimaryHost, options.PrimaryPort);
            using var replicaInfo = ConnectClient(options.ReplicaHost, options.ReplicaPort);

            // The harness completes the cluster bootstrap (slots, attach, initial sync)
            // before launching this client; a mis-ordered manual launch fails fast here
            // instead of measuring a non-replicating setup.
            var info = GetReplicationInfo(replicaInfo);
            if (GetInfoField(info, "role") != "slave" || GetInfoField(info, "master_sync_in_progress") != "False")
                throw new Exception(
                    $"{options.ReplicaHost}:{options.ReplicaPort} is not a synced replica; " +
                    "bootstrap the primary/replica pair before starting the client");

            // Size the send buffer to hold a full intra-thread-parallel batch of commands:
            // per op, the key and value payloads plus a 64-byte allowance for RESP framing.
            var sendBufferSize = Math.Max(1 << 17, (64 + keyLen + options.ValueLength) * options.IntraThreadParallelism);
            var writerClients = new GarnetClientSession[options.ReplicationWriters];
            var readerClients = new GarnetClientSession[options.ReplicationReaders];
            try
            {
                for (var i = 0; i < options.ReplicationWriters; i++)
                    writerClients[i] = ConnectClient(options.PrimaryHost, options.PrimaryPort, sendBufferSize);
                for (var i = 0; i < options.ReplicationReaders; i++)
                {
                    var c = ConnectClient(options.ReplicaHost, options.ReplicaPort, sendBufferSize);
                    // The replica rejects reads on a read-write session.
                    c.Execute("READONLY");
                    c.CompletePending();
                    readerClients[i] = c;
                }

                Preload(writerClients);
                WaitReplicaCaughtUp(primaryInfo, replicaInfo);

                Console.WriteLine(
                    $">>> Replication client: {options.ReplicationWriters} writer(s) -> {options.PrimaryHost}:{options.PrimaryPort}, " +
                    $"{options.ReplicationReaders} reader(s) -> {options.ReplicaHost}:{options.ReplicaPort}, " +
                    $"{options.Repeat} pass(es) x {options.RunTime}s >>>");

                for (var pass = 0; pass < options.Repeat; pass++)
                {
                    var writerHists = NewHistograms(options.ReplicationWriters);
                    var readerHists = NewHistograms(options.ReplicationReaders);
                    var threads = new Thread[options.ReplicationWriters + options.ReplicationReaders];
                    for (var i = 0; i < options.ReplicationWriters; i++)
                    {
                        var x = i;
                        threads[i] = new Thread(() => RunWriter(x, writerClients[x], writerHists[x]));
                    }
                    for (var i = 0; i < options.ReplicationReaders; i++)
                    {
                        var x = i;
                        threads[options.ReplicationWriters + i] = new Thread(() => RunReader(x, readerClients[x], readerHists[x]));
                    }
                    foreach (var t in threads)
                        t.Start();

                    var swatch = Stopwatch.StartNew();
                    waiter.Set();
                    Thread.Sleep(TimeSpan.FromSeconds(options.RunTime));
                    done = true;
                    foreach (var t in threads)
                        t.Join();
                    swatch.Stop();

                    var lag = ReplicationLagBytes(primaryInfo, replicaInfo);
                    var seconds = swatch.ElapsedMilliseconds / 1000.0;
                    Console.WriteLine($"[Total time]: {swatch.ElapsedMilliseconds:N2} ms for pass {pass}");
                    PrintStats("Writer", writerOperationsCompleted, seconds, writerHists);
                    PrintStats("Reader", readerOperationsCompleted, seconds, readerHists);
                    Console.WriteLine($"[Replication lag bytes]: {lag:N0}");
                    Console.WriteLine("------------------------------");

                    done = false;
                    waiter.Reset();
                    writerOperationsCompleted = 0;
                    readerOperationsCompleted = 0;
                }
            }
            finally
            {
                foreach (var c in writerClients)
                    c?.Dispose();
                foreach (var c in readerClients)
                    c?.Dispose();
            }
        }

        static LongHistogram[] NewHistograms(int count)
        {
            var hists = new LongHistogram[count];
            for (var i = 0; i < count; i++)
                hists[i] = new LongHistogram(HistogramLowerBound, HistogramUpperBound, HistogramSigFigs);
            return hists;
        }

        // Loads every key once so measured GETs always hit and measured SETs are in-place
        // updates. Partitioned across the writer connections (one extra connection when the
        // run is read-only), each pipelined itp-deep.
        void Preload(GarnetClientSession[] writerClients)
        {
            var loaders = writerClients.Length > 0
                ? writerClients
                : [ConnectClient(options.PrimaryHost, options.PrimaryPort)];
            var keyCount = keys.Length / keyLen;
            var swatch = Stopwatch.StartNew();
            var threads = new Thread[loaders.Length];
            for (var i = 0; i < loaders.Length; i++)
            {
                var x = i;
                threads[i] = new Thread(() => PreloadWorker(loaders[x], x, loaders.Length));
            }
            foreach (var t in threads)
                t.Start();
            foreach (var t in threads)
                t.Join();
            swatch.Stop();
            if (writerClients.Length == 0)
                loaders[0].Dispose();
            Console.WriteLine($"[Preload]: {keyCount:N0} keys in {swatch.ElapsedMilliseconds:N2} ms");
        }

        void PreloadWorker(GarnetClientSession client, int idx, int total)
        {
            var keyCount = keys.Length / keyLen;
            var begin = (int)((long)keyCount * idx / total);
            var end = (int)((long)keyCount * (idx + 1) / total);
            var parallel = Math.Max(1, options.IntraThreadParallelism);
            var inFlight = 0;
            for (var i = begin; i < end; i++)
            {
                client.ExecuteBatch("SET", Encoding.ASCII.GetString(keys, i * keyLen, keyLen), valuePayload);
                if (++inFlight == parallel)
                {
                    client.CompletePending(true);
                    inFlight = 0;
                }
            }
            client.CompletePending(true);
        }

        // Blocks until the replica's replayed offset vector equals the primary's tail vector.
        void WaitReplicaCaughtUp(GarnetClientSession primary, GarnetClientSession replica)
        {
            var swatch = Stopwatch.StartNew();
            while (!GetReplicationOffset(primary).SequenceEqual(GetReplicationOffset(replica)))
                Thread.Sleep(10);
            swatch.Stop();
            Console.WriteLine($"[Catch up]: replica reached the primary offset in {swatch.ElapsedMilliseconds:N2} ms");
        }

        // Writer thread: keeps `itp` SETs to the primary in flight, recording per-batch latency
        // (per-op when itp = 1). Uniform-random keys over the preloaded keyspace.
        void RunWriter(int threadId, GarnetClientSession client, LongHistogram hist)
        {
            var keyCount = keys.Length / keyLen;
            var rng = new Random(0xBEEF + threadId);
            var parallel = Math.Max(1, options.IntraThreadParallelism);
            var wait = !options.Burst;
            var opsCompleted = 0L;

            waiter.Wait();

            while (!done)
            {
                var start = Stopwatch.GetTimestamp();
                for (var i = 0; i < parallel; i++)
                    client.ExecuteBatch("SET", Encoding.ASCII.GetString(keys, rng.Next(keyCount) * keyLen, keyLen), valuePayload);
                client.CompletePending(wait);
                hist.RecordValue(Stopwatch.GetTimestamp() - start);
                opsCompleted += parallel;
            }
            _ = Interlocked.Add(ref writerOperationsCompleted, opsCompleted);
        }

        // Reader thread: keeps `itp` GETs to the replica in flight, recording per-batch latency
        // (per-op when itp = 1). Uniform-random keys over the preloaded keyspace.
        void RunReader(int threadId, GarnetClientSession client, LongHistogram hist)
        {
            var keyCount = keys.Length / keyLen;
            var rng = new Random(0xCAFE + threadId);
            var parallel = Math.Max(1, options.IntraThreadParallelism);
            var wait = !options.Burst;
            var opsCompleted = 0L;

            waiter.Wait();

            while (!done)
            {
                var start = Stopwatch.GetTimestamp();
                for (var i = 0; i < parallel; i++)
                    client.ExecuteBatch("GET", Encoding.ASCII.GetString(keys, rng.Next(keyCount) * keyLen, keyLen));
                client.CompletePending(wait);
                hist.RecordValue(Stopwatch.GetTimestamp() - start);
                opsCompleted += parallel;
            }
            _ = Interlocked.Add(ref readerOperationsCompleted, opsCompleted);
        }

        // Prints the per-pass stats block ([<label> operations/throughput/latency]) from the
        // merged per-thread histograms; parse.py keys on these labels.
        static void PrintStats(string label, long operations, double seconds, LongHistogram[] histograms)
        {
            Console.WriteLine($"[{label} operations]: {operations:N0}");
            Console.WriteLine($"[{label} throughput]: {operations / seconds:N2} ops/sec");
            var merged = new LongHistogram(HistogramLowerBound, HistogramUpperBound, HistogramSigFigs);
            foreach (var h in histograms)
                merged.Add(h);
            if (merged.TotalCount > 0)
            {
                var s = OutputScalingFactor.TimeStampToMicroseconds;
                Console.WriteLine(
                    $"[{label} latency us] " +
                    $"p50={Math.Round(merged.GetValueAtPercentile(50) / s, 2)} " +
                    $"p90={Math.Round(merged.GetValueAtPercentile(90) / s, 2)} " +
                    $"p99={Math.Round(merged.GetValueAtPercentile(99) / s, 2)} " +
                    $"p99.9={Math.Round(merged.GetValueAtPercentile(99.9) / s, 2)} " +
                    $"max={Math.Round(merged.GetMaxValue() / s, 2)}");
            }
        }
    }
}