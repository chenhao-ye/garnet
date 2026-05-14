// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;

namespace Resp.benchmark.ReplicationBenchImpl
{
    /// <summary>
    /// One writer thread: owns a single GarnetClientSession to the primary and drives
    /// SET commands in a tight loop. Optional intra-thread pipelining via --itp.
    /// Keys and values come from OnlineReqGen so the on-the-wire format matches the
    /// existing online benchmark (X-padded uniform keys; --zipf supported).
    /// </summary>
    public sealed class WriterWorker
    {
        readonly Options opts;
        readonly IPEndPoint primaryEp;
        readonly int threadId;
        readonly Thread thread;

        long localOps;
        volatile bool stop;

        public long TotalOps => Interlocked.Read(ref localOps);

        public WriterWorker(Options opts, IPEndPoint primaryEp, int threadId)
        {
            this.opts = opts;
            this.primaryEp = primaryEp;
            this.threadId = threadId;
            this.thread = new Thread(Run) { Name = $"repl-writer-{threadId}", IsBackground = true };
        }

        public void Start() => thread.Start();
        public void RequestStop() => stop = true;
        public void Join() => thread.Join();

        void Run()
        {
            using var session = ReplicationDriver.NewSession(primaryEp, $"repl-writer-{threadId}");
            var req = new OnlineReqGen(threadId, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength);
            var value = new string('V', Math.Max(1, opts.ValueLength));
            var itp = Math.Max(1, opts.IntraThreadParallelism);

            if (itp == 1)
            {
                while (!stop)
                {
                    session.ExecuteAsync("SET", req.GenerateKey(), value).GetAwaiter().GetResult();
                    Interlocked.Increment(ref localOps);
                }
                return;
            }

            // Pipelined path: keep `itp` outstanding SETs at all times.
            var outstanding = new Task<string>[itp];
            for (int i = 0; i < itp; i++)
                outstanding[i] = session.ExecuteAsync("SET", req.GenerateKey(), value);

            while (!stop)
            {
                var done = Task.WhenAny(outstanding).GetAwaiter().GetResult();
                _ = done.GetAwaiter().GetResult();
                var idx = Array.IndexOf(outstanding, done);
                Interlocked.Increment(ref localOps);
                outstanding[idx] = session.ExecuteAsync("SET", req.GenerateKey(), value);
            }

            for (int i = 0; i < itp; i++)
            {
                try { outstanding[i]?.GetAwaiter().GetResult(); } catch { /* drained */ }
            }
        }
    }
}
