// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Text;
using Embedded.server;
using Garnet.common;
using Garnet.server;

namespace Resp.benchmark
{
    public class GarnetServerInstance
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

        internal EmbeddedRespServer server;
        internal RespServerSession[] sessions;
        internal readonly string primaryId;

        // Loopback endpoint the server listens on for network reader clients; null when the server is
        // purely embedded (in-process readers).
        internal readonly EndPoint endpoint;

        public GarnetServerInstance(Options options)
        {
            var serverOptions = AofBench.GetServerOptions(options);
            primaryId = Generator.CreateHexId();

            // Start a network endpoint when local GarnetClientSession readers will connect, or when
            // this process is the Replica role of a split-process run (the readers are remote).
            if (options.IsReplayEnabled
                && (options.AofBenchRole == AofBenchRole.Replica
                    || (options.AofReplayReader > 0 && options.Client == ClientType.GarnetClientSession)))
            {
                endpoint = new IPEndPoint(IPAddress.Parse(options.Address), options.Port);
                serverOptions.EndPoints = [endpoint];
                // No embedded network server => the base GarnetServer creates a GarnetServerTcp listener.
                server = new EmbeddedRespServer(serverOptions, Program.loggerFactory);
                server.Start();
            }
            else // In-process readers use the embedded server with no listener.
            {
                server = new EmbeddedRespServer(serverOptions, Program.loggerFactory, new GarnetServerEmbedded());
            }

            sessions = server.GetRespSessions(options.AofPhysicalSublogCount);
            AddAllSlots();
            sessions[0].clusterSession.UnsafeSetConfig(replicaOf: primaryId);
        }

        unsafe void AddAllSlots()
        {
            // RESP for: CLUSTER ADDSLOTSRANGE 0 16383
            var req = Encoding.ASCII.GetBytes(
                "*4\r\n$7\r\nCLUSTER\r\n$13\r\nADDSLOTSRANGE\r\n$1\r\n0\r\n$5\r\n16383\r\n");
            fixed (byte* p = req)
                _ = sessions[0].TryConsumeMessages(p, req.Length);
        }

        public IClusterSession GetClusterSession(int idx)
            => sessions[idx].clusterSession;

        internal RespServerSession GetRespServerSession(int idx)
            => sessions[idx];
    }
}