// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Slot type carried by the replay <see cref="RingBufferChannel{TRecord}"/>.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Size = 8)]
    internal readonly unsafe struct ReplayRecord
    {
        public readonly byte* Ptr;
        public ReplayRecord(byte* ptr) { Ptr = ptr; }
    }

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
        readonly RingBufferChannel<ReplayRecord> channel = new(clusterProvider.serverOptions.AofReplayRingSize, clusterProvider.serverOptions.AofReplayRingBatch);

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
                    var record = replayBatchContext.Record;
                    var recordLength = replayBatchContext.RecordLength;
                    var currentAddress = replayBatchContext.CurrentAddress;
                    var nextAddress = replayBatchContext.NextAddress;
                    var isProtected = replayBatchContext.IsProtected;
                    var ptr = record;

                    var maxSequenceNumber = 0L;
                    try
                    {
                        // logger?.LogError("[{sublogIdx},{replayIdx}] = {currentAddress} -> {nextAddress}", sublogIdx, replayIdx, currentAddress, nextAddress);                        
                        while (ptr < record + recordLength)
                        {
                            cts.Token.ThrowIfCancellationRequested();
                            var entryLength = appendOnlyFile.HeaderSize;
                            var payloadLength = replaySublog.UnsafeGetLength(ptr);
                            if (payloadLength > 0)
                            {
                                var entryPtr = ptr + entryLength;
                                if (replicationManager.AofProcessor.CanReplay(entryPtr, replayTaskIdx, out var sequenceNumber))
                                {
                                    replicationManager.AofProcessor.ProcessAofRecordInternal(virtualSublogIdx, entryPtr, payloadLength, true, out var isCheckpointStart);
                                    // Encountered checkpoint start marker, log the ReplicationCheckpointStartOffset so we know the correct AOF truncation
                                    // point when we take a checkpoint at the checkpoint end marker
                                    if (isCheckpointStart)
                                    {
                                        // logger?.LogError("[{sublogIdx}] CheckpointStart {address}", sublogIdx, clusterProvider.replicationManager.GetSublogReplicationOffset(sublogIdx));
                                        replicationManager.ReplicationCheckpointStartOffset[physicalSublogIdx] = replicationManager.GetSublogReplicationOffset(physicalSublogIdx);
                                    }
                                }
                                maxSequenceNumber = Math.Max(sequenceNumber, maxSequenceNumber);
                                entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
                            }
                            else if (payloadLength < 0)
                            {
                                if (!clusterProvider.serverOptions.EnableFastCommit)
                                    throw new GarnetException("Received FastCommit request at replica AOF processor, but FastCommit is not enabled", clientResponse: false);

                                // Only a single thread should commit metadata
                                if (replayTaskIdx == 0)
                                {
                                    TsavoriteLogRecoveryInfo info = new();
                                    info.Initialize(new ReadOnlySpan<byte>(ptr + entryLength, -payloadLength));
                                    replaySublog.UnsafeCommitMetadataOnly(info, isProtected);
                                }
                                entryLength += TsavoriteLog.UnsafeAlign(-payloadLength);
                            }
                            ptr += entryLength;
                        }

                        // A zero-length batch staged by the driver is a broadcast time pulse:
                        // advance this task's virtual sublog (and arrive at any active
                        // replay-align round) without a record.
                        var pulseSequenceNumber = replayDriver.StagedPulseSequenceNumber;
                        if (pulseSequenceNumber > 0)
                            appendOnlyFile.readConsistencyManager.AdvanceVirtualSublogTime(virtualSublogIdx, pulseSequenceNumber);

                        // Update max sequence number for this virtual sublog. Only the timestamp
                        // read protocol consults it; the snapshot protocol skips this maintenance.
                        if (clusterProvider.serverOptions.AofReadWithTimestamp)
                            appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, maxSequenceNumber);
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

        /// <summary>
        /// Enqueue a pre-header AOF record pointer. May block if the ring buffer is full (i.e., the replay task does not consume records fast enough).
        /// </summary>
        public unsafe void AddRecord(byte* ptr) => channel.Write(new ReplayRecord(ptr));

        /// <summary>
        /// Enqueue a null-pointer control slot instructing the consumer to apply the driver's
        /// staged time pulse (<see cref="ReplicaReplayDriver.StagedPulseSequenceNumber"/>) to its
        /// virtual sublog. Same producer rules as <see cref="AddRecord"/>; the driver broadcasts
        /// the marker to every task's ring and runs the batch lifecycle to completion before
        /// producing anything else, so the staged value is stable while the marker is in flight.
        /// </summary>
        public unsafe void AddPulseMarker() => channel.Write(new ReplayRecord(null));

        /// <summary>
        /// Mark the ring as batch-complete (flushes Tail and sets the Completed flag).
        /// The consumer observes <see cref="RingBufferChannel{TRecord}.IsCompleted"/> after draining
        /// and uses the barrier to acknowledge; driver calls <see cref="ResetBatch"/>
        /// after <see cref="LeaderFollowerBarrier.WaitCompleted"/>.
        /// </summary>
        internal void CompleteBatch() => channel.Complete();

        /// <summary>
        /// Clear the ring's Completed flag so the next batch starts fresh. Must be called
        /// by the driver while consumer is parked on the barrier's resetReady.
        /// </summary>
        internal void ResetBatch() => channel.Reset();

        internal unsafe Task ChannelBackgroundReplay()
        {
            var physicalSublogIdx = replayDriver.physicalSublogIdx;
            var virtualSublogIdx = appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, replayTaskIdx);
            var headerSize = appendOnlyFile.HeaderSize;

            // Ensure cancel/teardown breaks the Read spin loop by completing the channel.
            cts.Token.Register(channel.Complete);

            try
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    while (channel.Read(out var record))
                    {
                        if (record.Ptr == null)
                        {
                            // Broadcast time pulse: advance this task's virtual sublog (and arrive
                            // at any active replay-align round) without a record.
                            appendOnlyFile.readConsistencyManager.AdvanceVirtualSublogTime(virtualSublogIdx, replayDriver.StagedPulseSequenceNumber);
                            continue;
                        }
                        var payloadLength = replaySublog.UnsafeGetLength(record.Ptr);
                        var entryPtr = record.Ptr + headerSize;
                        replicationManager.AofProcessor.ProcessAofRecordInternal(virtualSublogIdx, entryPtr, payloadLength, true, out var isCheckpointStart);

                        // Encountered checkpoint start marker, log the ReplicationCheckpointStartOffset so we know the correct AOF truncation
                        // point when we take a checkpoint at the checkpoint end marker
                        if (isCheckpointStart)
                        {
                            // logger?.LogError("[{sublogIdx}] CheckpointStart {address}", sublogIdx, clusterProvider.replicationManager.GetSublogReplicationOffset(sublogIdx));
                            replicationManager.ReplicationCheckpointStartOffset[physicalSublogIdx] = replicationManager.GetSublogReplicationOffset(physicalSublogIdx);
                        }
                    }
                    // Read returned false => batch completed (or cancellation set the Completed flag).
                    replayBatchContext.LeaderFollowerBarrier.SignalCompleted(cts.Token);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                logger?.LogError(ex, "{method} failed at replaying", nameof(ChannelBackgroundReplay));
                cts.Cancel();
            }
            return Task.CompletedTask;
        }

    }
}
