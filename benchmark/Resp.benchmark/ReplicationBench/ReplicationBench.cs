// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using HdrHistogram;

namespace Resp.benchmark.ReplicationBenchImpl
{
    /// <summary>
    /// MultiLog primary+replica read/write/freshness microbenchmark.
    ///
    /// Topology: one primary process and one replica process launched by the driver.
    /// Workload: N writer threads SETting uniformly random 8B keys/values on the
    /// primary; M reader threads GETting on the replica. A single-thread prober
    /// writes a versioned value to a fixed key every ~100us on the primary and
    /// continuously polls it on the replica to sample replication freshness.
    /// </summary>
    public sealed class ReplicationBench
    {
        readonly Options opts;

        public ReplicationBench(Options opts)
        {
            this.opts = opts;
        }

        public void Run()
        {
            PrintConfig();

            var driver = new ReplicationDriver(opts);
            driver.Connect();

            // Spawn workers.
            var writers = new WriterWorker[opts.ReplWriters];
            for (int i = 0; i < opts.ReplWriters; i++)
                writers[i] = new WriterWorker(opts, driver.PrimaryEndpoint, threadId: i + 1);

            var readers = new ReaderWorker[opts.ReplReaders];
            for (int i = 0; i < opts.ReplReaders; i++)
                readers[i] = new ReaderWorker(opts, driver.ReplicaEndpoint, threadId: i + 1);

            var prober = new FreshnessProber(opts, driver.PrimaryEndpoint, driver.ReplicaEndpoint);

            foreach (var w in writers) w.Start();
            foreach (var r in readers) r.Start();
            prober.Start();

            Console.WriteLine($"[repl-bench] Warmup for {opts.ReplWarmupSecs}s ...");
            Thread.Sleep(TimeSpan.FromSeconds(opts.ReplWarmupSecs));

            // Snapshot writer ops at the start of the measurement window.
            long startWriterOps = 0;
            foreach (var w in writers) startWriterOps += w.TotalOps;

            foreach (var r in readers) r.BeginMeasure();
            prober.BeginMeasure();

            Console.WriteLine($"[repl-bench] Measuring for {opts.RunTime}s ...");
            var sw = Stopwatch.StartNew();
            Thread.Sleep(TimeSpan.FromSeconds(opts.RunTime));
            sw.Stop();

            // Stop and join workers.
            foreach (var w in writers) w.RequestStop();
            foreach (var r in readers) r.RequestStop();
            prober.RequestStop();
            foreach (var w in writers) w.Join();
            foreach (var r in readers) r.Join();
            prober.Join();

            long endWriterOps = 0;
            foreach (var w in writers) endWriterOps += w.TotalOps;

            var elapsedSec = sw.Elapsed.TotalSeconds;
            var writerOps = endWriterOps - startWriterOps;
            var writerTput = writerOps / elapsedSec;

            long readerOps = 0;
            var readerHist = new LongHistogram(1, TimeStamp.Seconds(100), 2);
            foreach (var r in readers)
            {
                readerOps += r.TotalOps;
                readerHist.Add(r.Histogram);
            }
            var readerTput = readerOps / elapsedSec;

            ReportResults(elapsedSec, writerOps, writerTput, readerOps, readerTput, readerHist, prober.Histogram, prober.TotalSamples);
        }

        void PrintConfig()
        {
            Console.WriteLine("=== Replication Benchmark Configuration ===");
            Console.WriteLine($"  primary-port            : {opts.ReplPrimaryPort}");
            Console.WriteLine($"  replica-port            : {opts.ReplReplicaPort}");
            Console.WriteLine($"  writers                 : {opts.ReplWriters}");
            Console.WriteLine($"  readers                 : {opts.ReplReaders}");
            Console.WriteLine($"  freshness-interval-us   : {opts.ReplFreshnessIntervalUs}");
            Console.WriteLine($"  warmup-secs             : {opts.ReplWarmupSecs}");
            Console.WriteLine($"  runtime-secs            : {opts.RunTime}");
            Console.WriteLine($"  dbsize                  : {opts.DbSize}");
            Console.WriteLine($"  keylength               : {opts.KeyLength}");
            Console.WriteLine($"  valuelength             : {opts.ValueLength}");
            Console.WriteLine($"  itp                     : {opts.IntraThreadParallelism}");
            Console.WriteLine("============================================");
        }

        static void ReportResults(double elapsedSec, long writerOps, double writerTput,
            long readerOps, double readerTput, LongHistogram readerHist,
            LongHistogram freshHist, long freshSamples)
        {
            Console.WriteLine();
            Console.WriteLine("=== Replication Benchmark Results ===");
            Console.WriteLine($"  measurement window      : {elapsedSec:N3} s");
            Console.WriteLine();
            Console.WriteLine($"  writer ops              : {writerOps:N0}");
            Console.WriteLine($"  writer throughput       : {writerTput:N0} ops/s");
            Console.WriteLine();
            Console.WriteLine($"  reader ops              : {readerOps:N0}");
            Console.WriteLine($"  reader throughput       : {readerTput:N0} ops/s");
            if (readerHist.TotalCount > 0)
            {
                Console.WriteLine($"  reader latency (us)     : p50={ToUs(readerHist, 50)}  p99={ToUs(readerHist, 99)}  p99.9={ToUs(readerHist, 99.9)}  max={ToUs(readerHist, 100)}  mean={MeanUs(readerHist)}");
            }
            else
            {
                Console.WriteLine("  reader latency (us)     : <no samples>");
            }
            Console.WriteLine();
            Console.WriteLine($"  freshness samples       : {freshSamples:N0}");
            if (freshHist.TotalCount > 0)
            {
                Console.WriteLine($"  freshness (us)          : p50={ToUs(freshHist, 50)}  p99={ToUs(freshHist, 99)}  p99.9={ToUs(freshHist, 99.9)}  max={ToUs(freshHist, 100)}  mean={MeanUs(freshHist)}");
            }
            else
            {
                Console.WriteLine("  freshness (us)          : <no samples>");
            }
            Console.WriteLine("======================================");
        }

        static string ToUs(LongHistogram h, double pct)
        {
            var v = h.GetValueAtPercentile(pct) / OutputScalingFactor.TimeStampToMicroseconds;
            return v.ToString("N2");
        }

        static string MeanUs(LongHistogram h)
        {
            var v = h.GetMean() / OutputScalingFactor.TimeStampToMicroseconds;
            return v.ToString("N2");
        }
    }
}
