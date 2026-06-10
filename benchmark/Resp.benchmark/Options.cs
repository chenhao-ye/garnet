// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;
using Microsoft.Extensions.Logging;

namespace Resp.benchmark
{
    public partial class Options
    {
        [Option('p', "port", Required = false, Default = 6379, HelpText = "Port to connect to")]
        public int Port { get; set; }

        [Option('h', "host", Required = false, Default = "127.0.0.1", HelpText = "IP address to connect to")]
        public string Address { get; set; }

        [Option("clientaddr", Required = false, HelpText = "IP address of client")]
        public string ClientAddress { get; set; }

        [Option('s', "skipload", Required = false, Default = false, HelpText = "Skip loading phase")]
        public bool SkipLoad { get; set; }

        [Option("dbsize", Required = false, Default = 1 << 10, HelpText = "DB size")]
        public int DbSize { get; set; }

        [Option("totalops", Required = false, Default = 1 << 25, HelpText = "Total ops")]
        public int TotalOps { get; set; }

        [Option("op", Required = false, Default = OpType.GET, HelpText = "Operation type (GET, MGET, INCR, PING, ZADDREM, PFADD, ZADDCARD)")]
        public OpType Op { get; set; }

        [Option("keylength", Required = false, Default = 1, HelpText = "Key length (bytes) - padded, 0 indicates pad to max DB size")]
        public int KeyLength { get; set; }

        [Option("valuelength", Required = false, Default = 8, HelpText = "Value length (bytes) - 0 indicates use key as value")]
        public int ValueLength { get; set; }

        [Option('b', "batchsize", Separator = ',', Required = false, Default = new[] { 4096 }, HelpText = "Batch size, number of requests (comma separated)")]
        public IEnumerable<int> BatchSize { get; set; }

        [Option("runtime", Required = false, Default = 15, HelpText = "Run time (seconds)")]
        public int RunTime { get; set; }

        [Option("repeat", Required = false, Default = 1, HelpText = "Repeat the benchmark this many times back-to-back (reusing generated data)")]
        public int Repeat { get; set; }

        [Option('t', "threads", Separator = ',', Default = new[] { 1, 2, 4, 8, 16, 32 }, HelpText = "Number of threads (comma separated)")]
        public IEnumerable<int> NumThreads { get; set; }

        [Option('a', "auth", Required = false, Default = null, HelpText = "Authentication password")]
        public string Auth { get; set; }

        [Option("burst", Required = false, Default = false, HelpText = "Wait for response or burst the system (GarnetClientSession)")]
        public bool Burst { get; set; }

        [Option("lset", Required = false, Default = false, HelpText = "Use set instead of mset to load data for benchmarking.")]
        public bool LSet { get; set; }

        [Option("zipf", Required = false, Default = false, HelpText = "Zipf data distribution (0.99)")]
        public bool Zipf { get; set; }

        [Option("client", Required = false, Default = ClientType.LightClient, HelpText = "Choose ClientType to run benchmark (LightClient, SERedis, GarnetClientSession)")]
        public ClientType Client { get; set; }

        [Option("pool", Required = false, Default = false, HelpText = "Pool client instances. Supports SERedis, GarnetClient and GarnetClientSession (online bench only).")]
        public bool Pool { get; set; }

        [Option("tls", Required = false, Default = false, HelpText = "Enable TLS.")]
        public bool EnableTLS { get; set; }

        [Option("tlshost", Required = false, Default = "GarnetTest", HelpText = "TLS remote host name.")]
        public string TlsHost { get; set; }

        [Option("cert-file-name", Required = false, HelpText = "TLS certificate file name (example: testcert.pfx).")]
        public string CertFileName { get; set; }

        [Option("cert-password", Required = false, HelpText = "TLS certificate password (example: placeholder).")]
        public string CertPassword { get; set; }

        [Option('o', "online", Required = false, Default = false, HelpText = "Online get/set mix based on --readpercent.")]
        public bool Online { get; set; }

        [Option('x', "txn", Required = false, Default = false, HelpText = "Transaction micro benchmark")]
        public bool Txn { get; set; }

        [Option("itp", Required = false, Default = 1, HelpText = "Intra-thread parallelism (online bench only).")]
        public int IntraThreadParallelism { get; set; }

        [Option("sync", Required = false, Default = false, HelpText = "Sync mode (online bench GarnetClient only).")]
        public bool SyncMode { get; set; }

        [Option("ttl", Required = false, Default = 0, HelpText = "Ttl for keys, required if --op is SETEX, or can be used to generate keys with expiration (both string and zset) in online benchmarks. ")]
        public int Ttl { get; set; }

        [Option("sscardinality", Required = false, Default = 0, HelpText = "Number of unique sorted sets. Same key will always go to the same sorted set.")]
        public int SortedSetCardinality { get; set; }

        [Option("client-hist", Required = false, Default = false, HelpText = "Enable client side latency tracking through internal client histogram.")]
        public bool ClientHistogram { get; set; }

        [Option("op-percent", Separator = ',', Default = new[] { 60, 30, 10 }, HelpText = "Percent of commands executed from workload")]
        public IEnumerable<int> OpPercent { get; set; }

        [Option("op-workload", Separator = ',', Default = new[] { OpType.GET, OpType.SET, OpType.DEL }, HelpText = "Workload of commands for online bench.")]
        public IEnumerable<OpType> OpWorkload { get; set; }

        [Option("save-freq", Required = false, Default = 0, HelpText = "Save (checkpoint) frequency in seconds")]
        public int SaveFreqSecs { get; set; }

        [Option("logger-level", Required = false, Default = LogLevel.Information, HelpText = "Logging level")]
        public LogLevel LogLevel { get; set; }

        [Option("disable-console-logger", Required = false, Default = false, HelpText = "Disable console logger.")]
        public bool DisableConsoleLogger { get; set; }

        [Option("file-logger", Required = false, Default = null, HelpText = "Enable file logger and write to the specified path.")]
        public string FileLogger { get; set; }

        [Option("aof-bench", Required = false, Default = false, HelpText = "Run AOF bench at replica.")]
        public bool AofBench { get; set; }

        [Option("aof-bench-type", Required = false, Default = AofBenchType.Replay, HelpText = "Run AOF bench at replica.")]
        public AofBenchType AofBenchType { get; set; }

        [Option("aof-bench-role", Required = false, Default = AofBenchRole.Combined, HelpText = "Topology role of this AOF bench process: Combined (replica+client in one process), Replica (replay + server, paces a remote client over the control channel), Client (GarnetClientSession readers driven by a Replica), Primary (reserved).")]
        public AofBenchRole AofBenchRole { get; set; }

        [Option("aof-bench-control-port", Required = false, Default = 0, HelpText = "TCP port of the bench control channel between Replica and Client roles. 0 = --port + 10000.")]
        public int AofBenchControlPort { get; set; }

        [Option("aof-gen-pages", Required = false, Default = 64, HelpText = "DB size")]
        public int AofGenPages { get; set; }

        [Option("aof-replay-reader", Required = false, Default = 0, HelpText = "Reader threads to spawn during replay bench (0 = disabled). Switches replay to single-pass. Ignored for non-replay benches.")]
        public int AofReplayReader { get; set; }

        [Option("aof-reader-skip", Required = false, Default = false, HelpText = "Pre-set every physical sublog's max sequence number to long.MaxValue at run start. Readers' consistency check is always pass, isolating the consistent-read fast-path cost from the wait path.")]
        public bool AofReaderSkip { get; set; }

        [Option("aof-replay-dist", Required = false, Default = KeyDistribution.Uniform, HelpText = "Key distribution of the generated AOF records that AofBench replays: Uniform, Zipf, or ZipfRev (Zipf with the hotness order reversed).")]
        public KeyDistribution AofReplayDist { get; set; }

        [Option("aof-read-dist", Required = false, Default = KeyDistribution.Uniform, HelpText = "Key distribution of AofBench reader GETs over the global keyset: Uniform, Zipf, or ZipfRev (Zipf with the hotness order reversed).")]
        public KeyDistribution AofReadDist { get; set; }

        [Option("zipf-theta", Required = false, Default = 0.99, HelpText = "Theta of the Zipf key distributions used by --aof-replay-dist and --aof-read-dist.")]
        public double ZipfTheta { get; set; }

        [Option("pseudo-timestamp-pace", Required = false, Default = 2000, HelpText = "Average pseudo-timestamp ticks between consecutive generated AOF records of the same sublog. Generated records emulate one global stream advancing pace/#sublogs ticks per record, dealt across sublogs. Emulates a wall-clock sequence generator (at ~2 GHz, 2000 is ~1us per record).")]
        public int PseudoTimestampPace { get; set; }

        /*
         * ReplicationBench options
         */
        [Option("replication-bench", Required = false, Default = false, HelpText = "Run the replication bench client: writer threads issuing SETs to the primary plus reader threads issuing GETs to the replica. The primary/replica servers and their cluster setup are managed externally (e.g. by the experiment harness).")]
        public bool ReplicationBench { get; set; }

        [Option("primary-host", Required = false, Default = "127.0.0.1", HelpText = "Host of the primary node the writer threads target.")]
        public string PrimaryHost { get; set; }

        [Option("primary-port", Required = false, Default = 0, HelpText = "Port of the primary node the writer threads target.")]
        public int PrimaryPort { get; set; }

        [Option("replica-host", Required = false, Default = "127.0.0.1", HelpText = "Host of the replica node the reader threads target.")]
        public string ReplicaHost { get; set; }

        [Option("replica-port", Required = false, Default = 0, HelpText = "Port of the replica node the reader threads target.")]
        public int ReplicaPort { get; set; }

        [Option("replication-writers", Required = false, Default = 1, HelpText = "ReplicationBench: writer threads issuing SETs to the primary.")]
        public int ReplicationWriters { get; set; }

        [Option("replication-readers", Required = false, Default = 1, HelpText = "ReplicationBench: reader threads issuing GETs to the replica.")]
        public int ReplicationReaders { get; set; }

        /*
         * InProc/AofBench server options
         */
        [Option("aof", Required = false, Default = false, HelpText = "Enable AOF")]
        public bool EnableAOF { get; set; }

        [Option("cluster", Required = false, Default = false, HelpText = "Enable Cluster")]
        public bool EnableCluster { get; set; }

        [Option('i', "index", Required = false, Default = "1g", HelpText = "Start size of hash index in bytes (rounds down to power of 2)")]
        public string IndexMemorySize { get; set; }

        [Option("aof-null-device", Required = false, HelpText = "With main-memory replication, use null device for AOF. Ensures no disk IO, but can cause data loss during replication.")]
        public bool UseAofNullDevice { get; set; }

        [Option("aof-commit-freq", Required = false, Default = 0, HelpText = "Write ahead logging (append-only file) commit issue frequency in milliseconds. 0 = issue an immediate commit per operation, -1 = manually issue commits using COMMITAOF command")]
        public int CommitFrequencyMs { get; set; }

        [Option("aof-physical-sublog-count", Required = false, Default = 1, HelpText = "Number of sublogs used for AOF.")]
        public int AofPhysicalSublogCount { get; set; }

        [Option("aof-replay-task-count", Required = false, Default = 1, HelpText = "Number of replay tasks per physical sublog at the replica.")]
        public int AofReplayTaskCount { get; set; }

        [Option("aof-replay-drift-threshold", Required = false, Default = 10000, HelpText = "Cross-sublog replay drift, in sequence-number units, tolerated on a replica before a replay-align barrier round is triggered. -1 disables the barrier.")]
        public int AofReplayDriftThreshold { get; set; }

        [Option("aof-replay-drift-check-freq", Required = false, Default = 1, HelpText = "How often the cross-sublog drift is re-checked during replay, as a multiple of --aof-replay-drift-threshold: one scan per (this value x threshold) sequence-number window system-wide, rotated across replay threads (window index mod virtual sublog count), firing a replay-align round when the drift exceeds the threshold. 0 = readers about to wait are the only round source.")]
        public int AofReplayDriftCheckFreq { get; set; }

        [Option("aof-barrier-spin-us", Required = false, Default = -1, HelpText = "How long a replay thread spins at the replay-align barrier before sleeping: <0 = spin forever (never sleep), 0 = never spin (pure sleep), >0 = spin up to N microseconds then sleep for the remainder.")]
        public int AofBarrierSpinUs { get; set; }

        [Option("aof-reader-spin-us", Required = false, Default = 0, HelpText = "How long a replica reader session spins polling the sublog frontier before parking on the consistent-read wait: <0 = spin forever (never park), 0 = never spin (park immediately), >0 = spin up to N microseconds then park.")]
        public int AofReaderSpinUs { get; set; }

        [Option("aof-memory-size", Required = false, Default = "64m", HelpText = "Total AOF memory buffer used in bytes (rounds down to power of 2) - spills to disk after this limit.")]
        public string AofMemorySize { get; set; }

        [Option("aof-page-size", Required = false, Default = "4m", HelpText = "Size of each AOF page in bytes(rounds down to power of 2)")]
        public string AofPageSize { get; set; }

        /// <summary>
        /// Parse size from string specification
        /// </summary>
        /// <param name="value"></param>
        /// <param name="bytesRead"></param>
        /// <returns></returns>
        public static long ParseSize(string value, out int bytesRead)
        {
            ReadOnlySpan<char> suffix = ['k', 'm', 'g', 't', 'p'];
            long result = 0;
            bytesRead = 0;
            for (var i = 0; i < value.Length; i++)
            {
                var c = value[i];
                if (char.IsDigit(c))
                {
                    result = (result * 10) + (byte)c - '0';
                    bytesRead++;
                }
                else
                {
                    for (var s = 0; s < suffix.Length; s++)
                    {
                        if (char.ToLower(c) == suffix[s])
                        {
                            result *= (long)Math.Pow(1024, s + 1);
                            bytesRead++;

                            if (i + 1 < value.Length && char.ToLower(value[i + 1]) == 'b')
                                bytesRead++;

                            return result;
                        }
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Get AOF Page size in bits
        /// </summary>
        /// <returns></returns>
        public int AofPageSizeBits()
        {
            var size = ParseSize(AofPageSize, out _);
            var adjustedSize = PreviousPowerOf2(size);
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Previous power of 2
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        internal static long PreviousPowerOf2(long v)
        {
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            return v - (v >> 1);
        }

        public bool IsReplayEnabled
            => AofBenchType is AofBenchType.Replay or AofBenchType.ReplayNoResp or AofBenchType.ReplayDirect;
    }
}