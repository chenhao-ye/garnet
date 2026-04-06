// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed class ReplicaReplayTask(
        int replayIdx,
        ReplicaReplayDriver replayDriver,
        ClusterProvider clusterProvider,
        CancellationTokenSource cts,
        ILogger logger = null)
    {
        readonly int replayTaskIdx = replayIdx;
        readonly ReplicaReplayDriver replayDriver = replayDriver;
        readonly ReplicationManager replicationManager = clusterProvider.replicationManager;
        readonly GarnetAppendOnlyFile appendOnlyFile = clusterProvider.storeWrapper.appendOnlyFile;
        readonly ReplayBatchContext replayBatchContext = replayDriver.replayBatchContext;
        readonly CancellationTokenSource cts = cts;
        readonly TsavoriteLog replaySublog = clusterProvider.storeWrapper.appendOnlyFile.Log.GetSubLog(replayDriver.physicalSublogIdx);
        readonly ILogger logger = logger;

        /// <summary>
        /// Asynchronously replays log entries using SemaphoreSlim coordination, processing and applying them for replication
        /// and consistency across sublogs.
        /// </summary>
        /// <returns>A task representing the asynchronous replay operation.</returns>
        internal async Task ContinuousBackgroundReplay()
        {
            var physicalSublogIdx = replayDriver.physicalSublogIdx;
            var virtualSublogIdx = appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, replayTaskIdx);

            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    await replayBatchContext.LeaderFollowerBarrier.WaitReadyWorkAsync(cancellationToken: cts.Token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} failed at WaitAsync", nameof(ContinuousBackgroundReplay));
                    cts.Cancel();
                    break;
                }

                unsafe
                {
                    var isProtected = replayBatchContext.IsProtected;
                    var descriptors = replayBatchContext.Descriptors;
                    var replayTaskCount = clusterProvider.serverOptions.AofReplayTaskCount;
                    var headerSize = (int)appendOnlyFile.HeaderSize;

                    try
                    {
                        var consumed = 0;
                        while (true)
                        {
                            var produced = Volatile.Read(ref replayBatchContext.ProducedCount);
                            for (var i = consumed; i < produced; i++)
                            {
                                cts.Token.ThrowIfCancellationRequested();
                                ref var desc = ref descriptors[i];

                                // Fast path: ShardedHeader with non-matching hash — skip without dereferencing
                                if (desc.KeyHash >= 0 && desc.KeyHash % replayTaskCount != replayTaskIdx)
                                    continue;

                                // Must dereference to determine entry type and process
                                var payloadLength = replaySublog.UnsafeGetLength(desc.Ptr);
                                if (payloadLength > 0)
                                {
                                    var entryPtr = desc.Ptr + headerSize;

                                    if (desc.KeyHash >= 0)
                                    {
                                        // ShardedHeader, matched — process directly
                                        replicationManager.AofProcessor.ProcessAofRecordInternal(virtualSublogIdx, entryPtr, payloadLength, true, out var isCheckpointStart);
                                        if (isCheckpointStart)
                                        {
                                            replicationManager.ReplicationCheckpointStartOffset[physicalSublogIdx] = replicationManager.GetSublogReplicationOffset(physicalSublogIdx);
                                        }
                                    }
                                    else if (ReplayBatchContext.IsTransactionParticipant(entryPtr, replayTaskIdx))
                                    {
                                        // TransactionHeader, participating — process
                                        replicationManager.AofProcessor.ProcessAofRecordInternal(virtualSublogIdx, entryPtr, payloadLength, true, out var isCheckpointStart);
                                        if (isCheckpointStart)
                                        {
                                            replicationManager.ReplicationCheckpointStartOffset[physicalSublogIdx] = replicationManager.GetSublogReplicationOffset(physicalSublogIdx);
                                        }
                                    }
                                }
                                else if (payloadLength < 0)
                                {
                                    if (!clusterProvider.serverOptions.EnableFastCommit)
                                        throw new GarnetException("Received FastCommit request at replica AOF processor, but FastCommit is not enabled", clientResponse: false);

                                    // Only a single thread should commit metadata
                                    if (replayTaskIdx == 0)
                                    {
                                        var entryPtr = desc.Ptr + headerSize;
                                        TsavoriteLogRecoveryInfo info = new();
                                        info.Initialize(new ReadOnlySpan<byte>(entryPtr, -payloadLength));
                                        replaySublog.UnsafeCommitMetadataOnly(info, isProtected);
                                    }
                                }
                            }
                            consumed = produced;

                            // Check if all descriptors have been produced and consumed
                            if (Volatile.Read(ref replayBatchContext.ProductionComplete) && consumed >= replayBatchContext.TotalCount)
                                break;

                            Thread.Yield();
                        }

                        // Update max sequence number for this virtual sublog (batch-wide max computed by producer)
                        appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, replayBatchContext.MaxBatchSequenceNumber);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "{method} failed at replaying", nameof(ContinuousBackgroundReplay));
                        cts.Cancel();
                        break;
                    }
                    finally
                    {
                        // Signal work completion after processing
                        replayBatchContext.LeaderFollowerBarrier.SignalCompleted();
                    }
                }
            }
        }
    }
}