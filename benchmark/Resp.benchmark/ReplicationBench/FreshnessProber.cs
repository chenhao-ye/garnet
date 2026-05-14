// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using HdrHistogram;

namespace Resp.benchmark.ReplicationBenchImpl
{
    /// <summary>
    /// Single-thread freshness prober.
    ///
    /// One cycle: SET the freshness key to a monotonically increasing version v on
    /// the primary, then poll GET on the replica until it reflects exactly v; the
    /// elapsed time from SET to first observation is one freshness sample.
    /// </summary>
    public sealed class FreshnessProber
    {
        const string FreshKey = "__FRESH";

        readonly Options opts;
        readonly IPEndPoint primaryEp;
        readonly IPEndPoint replicaEp;
        readonly long intervalTicks;
        readonly Thread thread;

        public LongHistogram Histogram { get; }
        long localSamples;
        volatile bool measuring;
        volatile bool stop;

        public long TotalSamples => Interlocked.Read(ref localSamples);

        public FreshnessProber(Options opts, IPEndPoint primaryEp, IPEndPoint replicaEp)
        {
            this.opts = opts;
            this.primaryEp = primaryEp;
            this.replicaEp = replicaEp;
            this.intervalTicks = Math.Max(0L, Stopwatch.Frequency * opts.ReplFreshnessIntervalUs / 1_000_000L);
            this.Histogram = new LongHistogram(1, TimeStamp.Seconds(100), 2);
            this.thread = new Thread(Run) { Name = "repl-fresh-probe", IsBackground = true };
        }

        public void Start() => thread.Start();
        public void BeginMeasure() => measuring = true;
        public void RequestStop() => stop = true;
        public void Join() => thread.Join();

        void Run()
        {
            using var pSess = ReplicationDriver.NewSession(primaryEp, "repl-fresh-primary");
            using var rSess = ReplicationDriver.NewSession(replicaEp, "repl-fresh-replica");

            var ack = rSess.ExecuteAsync("READONLY").GetAwaiter().GetResult();
            if (!string.Equals(ack, "OK", StringComparison.Ordinal))
                throw new Exception($"READONLY on replica returned `{ack}`");

            var rng = new Random();
            long v = 0;
            while (!stop)
            {
                v++;
                var writeTs = Stopwatch.GetTimestamp();
                pSess.ExecuteAsync("SET", FreshKey, v.ToString()).GetAwaiter().GetResult();

                // Poll the replica until it reflects exactly v. The interval between
                // successive GETs is throttled to --repl-freshness-interval-us so the
                // poll does not flood the replica.
                while (!stop)
                {
                    var pollStart = Stopwatch.GetTimestamp();
                    var resp = rSess.ExecuteAsync("GET", FreshKey).GetAwaiter().GetResult();
                    if (!string.IsNullOrEmpty(resp)
                        && long.TryParse(resp, out var vObs)
                        && vObs == v)
                    {
                        break;
                    }
                    if (intervalTicks > 0)
                    {
                        var pollDeadline = pollStart + intervalTicks;
                        while (!stop && Stopwatch.GetTimestamp() < pollDeadline) { /* busy-wait */ }
                    }
                }
                if (stop) break;

                var elapsed = Stopwatch.GetTimestamp() - writeTs;
                if (measuring && elapsed >= 0)
                {
                    Histogram.RecordValue(elapsed);
                    Interlocked.Increment(ref localSamples);
                }

                // Post-probe cooldown: wait between 1x and 20x the GET interval
                // before starting the next SET. This randomizes the timing to
                // insert the prober key
                if (intervalTicks > 0)
                {
                    var x = rng.Next(1, 20);
                    var cooldownDeadline = Stopwatch.GetTimestamp() + intervalTicks * x;
                    while (!stop && Stopwatch.GetTimestamp() < cooldownDeadline) { /* busy-wait */ }
                }
            }
        }
    }
}
