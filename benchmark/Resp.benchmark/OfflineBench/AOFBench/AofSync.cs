// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using Embedded.server;
using Garnet.client;
using Garnet.cluster;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Tsavorite.core;

namespace Resp.benchmark
{
    internal sealed class AofSync : IBulkLogEntryConsumer, IDisposable
    {
        const int maxChunkSize = 1 << 20;
        readonly Options options;
        readonly AofBench aofBench;
        readonly int threadId;
        readonly CancellationTokenSource cts = new();
        GarnetClientSession garnetClient;
        string primaryId;
        readonly long startAddress;
        long previousAddress;
        bool initialized = false;
        readonly ILogger logger = null;

        // Direct replay mode (InProc): bypass RESP, call ReplicaReplayDriver directly
        ReplicaReplayDriver replayDriver;

        public long Size => previousAddress - startAddress;

        public AofSync(AofBench aofBench, int threadId, long startAddress, Options options, AofGen aofGen)
        {
            this.options = options;
            this.aofBench = aofBench;
            this.threadId = threadId;
            primaryId = null;
            garnetClient = null;
            this.startAddress = startAddress;
            previousAddress = startAddress;

            if (options.Client != ClientType.InProc)
            {
                // Get replica information
                var replicaNode = GetClusterNodes(options);
                primaryId = replicaNode.ParentNodeId;

                // Initialize client
                var aofSyncNetworkBufferSettings = aofGen.GetAofSyncNetworkBufferSettings();
                garnetClient = new GarnetClientSession(
                            replicaNode.EndPoint,
                            aofSyncNetworkBufferSettings,
                            aofSyncNetworkBufferSettings.CreateBufferPool(),
                            tlsOptions: null,
                            logger: logger);
                garnetClient.Connect();
            }
        }

        public void Dispose()
        {
            replayDriver?.SuspendReplay();
            cts.Cancel();
            cts.Dispose();
        }

        ClusterNode GetClusterNodes(Options opts)
        {
            var redis = ConnectionMultiplexer.Connect(
                BenchUtils.GetConfig(
                    opts.Address,
                    opts.Port,
                    useTLS: opts.EnableTLS,
                    tlsHost: opts.TlsHost,
                    allowAdmin: true));

            var servers = redis.GetServers();
            if (servers.Length < 2)
                throw new Exception("Too few nodes for AOF bench to run");

            var endpoint = new IPEndPoint(IPAddress.Parse(opts.Address), opts.Port);
            var primaryServer = redis.GetServer(endpoint);
            var nodes = primaryServer.ClusterNodes();
            var primaryNodeId = (string)primaryServer.Execute("cluster", "myid");

            ClusterNode replicaNode = null;
            foreach (var node in nodes.Nodes)
            {
                if (node.ParentNodeId != null && node.ParentNodeId.Equals(primaryNodeId))
                    replicaNode = node;
            }

            if (replicaNode == null)
                throw new Exception($"No replica found for [{endpoint}] to run AOF bench!");
            return replicaNode;
        }

        void InitializeReplayStream()
        {
            if (options.Client == ClientType.InProc)
            {
                // Direct mode: initialize ReplicaReplayDriver without going through RESP
                var clusterProvider = (ClusterProvider)aofBench.server.StoreWrapper.clusterProvider;
                var networkSender = new EmbeddedNetworkSender();
                clusterProvider.replicationManager.InitializeReplicaReplayDriver(threadId, networkSender);
                replayDriver = clusterProvider.replicationManager.ReplicaReplayDriverStore.GetReplayDriver(threadId);
                replayDriver.ResumeReplay();
            }
            else
            {
                garnetClient.ExecuteClusterAppendLog(
                    primaryId,
                    physicalSublogIdx: threadId,
                    previousAddress: -1,
                    currentAddress: -1,
                    nextAddress: -1,
                    payloadPtr: -1,
                    payloadLength: 0);
                garnetClient.CompletePending(false);
            }
        }

        public unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected)
        {
            try
            {
                if (!initialized)
                {
                    InitializeReplayStream();
                    initialized = true;
                }

                if (options.Client == ClientType.InProc)
                {
                    // Direct replay: bypass RESP serialization entirely
                    replayDriver.Consume(payloadPtr, payloadLength, currentAddress, nextAddress, isProtected: false);
                }
                else
                {
                    garnetClient.ExecuteClusterAppendLog(
                        primaryId,
                        physicalSublogIdx: threadId,
                        previousAddress,
                        currentAddress,
                        nextAddress,
                        (long)payloadPtr,
                        payloadLength);
                }

                previousAddress = nextAddress;
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.AofSyncTaskInfo.Consume");
                throw;
            }
        }

        public void Throttle()
        {
            // Trigger flush while we are out of epoch protection
            garnetClient.CompletePending(false);
            garnetClient.Throttle();
        }
    }
}
