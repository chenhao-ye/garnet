// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using Garnet.client;
using Garnet.common;

namespace Resp.benchmark.ReplicationBenchImpl
{
    /// <summary>
    /// Client-side replication setup: connects to an already-running primary and
    /// replica, wires them via cluster commands if they are not already attached,
    /// and provides a session factory used by the workload threads. Servers are
    /// launched externally (e.g. by experiment/run.py).
    /// </summary>
    public sealed class ReplicationDriver
    {
        readonly Options opts;

        public IPEndPoint PrimaryEndpoint { get; }
        public IPEndPoint ReplicaEndpoint { get; }

        public ReplicationDriver(Options opts)
        {
            this.opts = opts;
            PrimaryEndpoint = new IPEndPoint(IPAddress.Loopback, opts.ReplPrimaryPort);
            ReplicaEndpoint = new IPEndPoint(IPAddress.Loopback, opts.ReplReplicaPort);
        }

        public void Connect()
        {
            using var admP = NewSession(PrimaryEndpoint, "driver-admin-p");
            using var admR = NewSession(ReplicaEndpoint, "driver-admin-r");

            if (IsReplicaAttached(admP))
            {
                Console.WriteLine("[driver] Replica already attached; skipping cluster wiring.");
                return;
            }

            Console.WriteLine("[driver] Wiring cluster ...");
            WireCluster(admP, admR);
            Console.WriteLine("[driver] Cluster wired; replica attached.");
        }

        static bool IsReplicaAttached(GarnetClientSession admP)
        {
            try
            {
                var info = admP.ExecuteAsync("INFO", "replication").GetAwaiter().GetResult() ?? "";
                return ContainsLine(info, "connected_slaves:", out var v)
                       && int.TryParse(v, out var n) && n >= 1;
            }
            catch
            {
                return false;
            }
        }

        void WireCluster(GarnetClientSession admP, GarnetClientSession admR)
        {
            // Slot assignment + epochs. Each is best-effort: a re-run against a
            // pre-wired primary will see "ERR Slot already assigned", which is fine.
            TryExecOk(admP, "CLUSTER", "ADDSLOTSRANGE", "0", "16383");
            TryExecOk(admP, "CLUSTER", "SET-CONFIG-EPOCH", "1");
            TryExecOk(admR, "CLUSTER", "SET-CONFIG-EPOCH", "2");

            ExecOk(admR, "CLUSTER", "MEET", "127.0.0.1", PrimaryEndpoint.Port.ToString());

            var primaryId = admP.ExecuteAsync("CLUSTER", "MYID").GetAwaiter().GetResult()
                ?? throw new Exception("CLUSTER MYID on primary returned null");
            primaryId = primaryId.Trim();

            var sw = Stopwatch.StartNew();
            while (true)
            {
                if (sw.ElapsedMilliseconds > 30_000)
                    throw new Exception($"Replica did not learn primary id {primaryId} within 30s");
                var nodes = admR.ExecuteAsync("CLUSTER", "NODES").GetAwaiter().GetResult() ?? "";
                if (nodes.Contains(primaryId, StringComparison.Ordinal)) break;
                Thread.Sleep(50);
            }

            ExecOk(admR, "CLUSTER", "REPLICATE", primaryId);

            sw.Restart();
            while (true)
            {
                if (sw.ElapsedMilliseconds > 60_000)
                    throw new Exception("Replica did not finish attaching within 60s (INFO REPLICATION never reported connected_slaves >= 1)");
                if (IsReplicaAttached(admP)) break;
                Thread.Sleep(50);
            }
        }

        public static GarnetClientSession NewSession(IPEndPoint ep, string clientName)
        {
            // 128 KB send/recv buffers are plenty for an 8B/8B workload; matches the
            // floor RespOnlineBench uses.
            var session = new GarnetClientSession(
                endpoint: ep,
                networkBufferSettings: new NetworkBufferSettings(1 << 17),
                clientName: clientName);
            session.Connect();
            return session;
        }

        static void ExecOk(GarnetClientSession s, params string[] command)
        {
            var resp = s.ExecuteAsync(command).GetAwaiter().GetResult();
            if (!string.Equals(resp, "OK", StringComparison.Ordinal))
                throw new Exception($"Expected OK for `{string.Join(' ', command)}`, got `{resp}`");
        }

        static void TryExecOk(GarnetClientSession s, params string[] command)
        {
            try
            {
                var resp = s.ExecuteAsync(command).GetAwaiter().GetResult();
                if (!string.Equals(resp, "OK", StringComparison.Ordinal))
                    Console.WriteLine($"[driver] `{string.Join(' ', command)}` -> `{resp}` (ignored)");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[driver] `{string.Join(' ', command)}` failed: {ex.Message} (ignored)");
            }
        }

        static bool ContainsLine(string info, string prefix, out string value)
        {
            foreach (var line in info.Split('\n'))
            {
                var l = line.Trim();
                if (l.StartsWith(prefix, StringComparison.Ordinal))
                {
                    value = l.Substring(prefix.Length).Trim();
                    return true;
                }
            }
            value = null;
            return false;
        }
    }
}
