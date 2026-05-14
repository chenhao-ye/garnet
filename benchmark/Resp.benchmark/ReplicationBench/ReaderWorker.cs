// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using HdrHistogram;

namespace Resp.benchmark.ReplicationBenchImpl
{
    /// <summary>
    /// One reader thread: owns a single GarnetClientSession to the replica, issues
    /// READONLY once, then drives GETs in a tight loop. Latency is recorded into a
    /// per-thread HdrHistogram only after BeginMeasure() is called. Keys come from
    /// OnlineReqGen so the keyspace matches the writer side.
    /// </summary>
    public sealed class ReaderWorker
    {
        readonly Options opts;
        readonly IPEndPoint replicaEp;
        readonly int threadId;
        readonly Thread thread;

        public LongHistogram Histogram { get; }

        long localOps;
        volatile bool measuring;
        volatile bool stop;

        public long TotalOps => Interlocked.Read(ref localOps);

        public ReaderWorker(Options opts, IPEndPoint replicaEp, int threadId)
        {
            this.opts = opts;
            this.replicaEp = replicaEp;
            this.threadId = threadId;
            this.Histogram = new LongHistogram(1, TimeStamp.Seconds(100), 2);
            this.thread = new Thread(Run) { Name = $"repl-reader-{threadId}", IsBackground = true };
        }

        public void Start() => thread.Start();
        public void BeginMeasure() => measuring = true;
        public void RequestStop() => stop = true;
        public void Join() => thread.Join();

        void Run()
        {
            using var session = ReplicationDriver.NewSession(replicaEp, $"repl-reader-{threadId}");
            // Cluster replicas reject reads without READONLY.
            var ack = session.ExecuteAsync("READONLY").GetAwaiter().GetResult();
            if (!string.Equals(ack, "OK", StringComparison.Ordinal))
                throw new Exception($"READONLY on replica returned `{ack}`");

            var req = new OnlineReqGen(threadId, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength);
            var itp = Math.Max(1, opts.IntraThreadParallelism);

            if (itp == 1)
            {
                while (!stop)
                {
                    var start = Stopwatch.GetTimestamp();
                    session.ExecuteAsync("GET", req.GenerateKey()).GetAwaiter().GetResult();
                    var elapsed = Stopwatch.GetTimestamp() - start;
                    if (measuring)
                    {
                        Histogram.RecordValue(elapsed);
                        Interlocked.Increment(ref localOps);
                    }
                }
                return;
            }

            // Pipelined path: track each outstanding op's start timestamp so latency
            // accounts for queue time.
            var outstanding = new Task<string>[itp];
            var starts = new long[itp];
            for (int i = 0; i < itp; i++)
            {
                starts[i] = Stopwatch.GetTimestamp();
                outstanding[i] = session.ExecuteAsync("GET", req.GenerateKey());
            }

            while (!stop)
            {
                var done = Task.WhenAny(outstanding).GetAwaiter().GetResult();
                _ = done.GetAwaiter().GetResult();
                var idx = Array.IndexOf(outstanding, done);
                var elapsed = Stopwatch.GetTimestamp() - starts[idx];
                if (measuring)
                {
                    Histogram.RecordValue(elapsed);
                    Interlocked.Increment(ref localOps);
                }
                starts[idx] = Stopwatch.GetTimestamp();
                outstanding[idx] = session.ExecuteAsync("GET", req.GenerateKey());
            }

            for (int i = 0; i < itp; i++)
            {
                try { outstanding[i]?.GetAwaiter().GetResult(); } catch { /* drained */ }
            }
        }
    }
}
