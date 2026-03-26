// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Wrapper for store and store-specific information
    /// </summary>
    public sealed unsafe partial class AofProcessor
    {
        readonly StoreWrapper storeWrapper;
        readonly RespServerSession[] respServerSessions;
        readonly AofReplayCoordinator aofReplayCoordinator;

        int activeDbId;

        /// <summary>
        /// Set ReadWriteSession on the cluster session (NOTE: used for replaying stored procedures only)
        /// </summary>
        public void SetReadWriteSession()
        {
            foreach (var respServerSession in respServerSessions)
                respServerSession.clusterSession.SetReadWriteSession();
        }

        /// <summary>Basic (Ephemeral locking) Session Context for main store</summary>
        StringBasicContext stringBasicContext;

        /// <summary>Basic (Ephemeral locking) Session Context for object store</summary>
        ObjectBasicContext objectBasicContext;

        /// <summary>Basic (Ephemeral locking) Session Context for unified store</summary>
        UnifiedBasicContext unifiedBasicContext;

        readonly StoreWrapper replayAofStoreWrapper;
        readonly IClusterProvider clusterProvider;
        readonly ILogger logger;

        /// <summary>
        /// Create new AOF processor
        /// </summary>
        public AofProcessor(
            StoreWrapper storeWrapper,
            IClusterProvider clusterProvider = null,
            bool recordToAof = false,
            ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;

            this.clusterProvider = clusterProvider;
            replayAofStoreWrapper = new StoreWrapper(storeWrapper, recordToAof);

            this.activeDbId = 0;
            this.respServerSessions = [.. Enumerable.Range(0, storeWrapper.serverOptions.AofVirtualSublogCount).Select(_ => ObtainServerSession())];

            // Switch current contexts to match the default database
            SwitchActiveDatabaseContext(storeWrapper.DefaultDatabase, true);
            aofReplayCoordinator = new AofReplayCoordinator(storeWrapper.serverOptions, this, logger);
            this.logger = logger;
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            foreach (var respServerSession in respServerSessions)
            {
                var databaseSessionsSnapshot = respServerSession.GetDatabaseSessionsSnapshot();
                foreach (var dbSession in databaseSessionsSnapshot)
                {
                    dbSession.StorageSession.stringBasicContext.Session?.Dispose();
                    dbSession.StorageSession.objectBasicContext.Session?.Dispose();
                }
            }
        }

        private RespServerSession ObtainServerSession()
            => new(0, networkSender: null, storeWrapper: replayAofStoreWrapper, subscribeBroker: null, authenticator: null, enableScripts: false, clusterProvider: clusterProvider);

        private void SwitchActiveDatabaseContext(GarnetDatabase db, bool initialSetup = false)
        {
            foreach (var respServerSession in respServerSessions)
            {
                // Switch the session's context to match the specified database, if necessary
                if (respServerSession.activeDbId != db.Id)
                {
                    var switchDbSuccessful = respServerSession.TrySwitchActiveDatabaseSession(db.Id);
                    Debug.Assert(switchDbSuccessful);
                }

                // Switch the storage context to match the session, if necessary
                if (activeDbId != db.Id || initialSetup)
                {
                    stringBasicContext = respServerSession.storageSession.stringBasicContext;
                    unifiedBasicContext = respServerSession.storageSession.unifiedBasicContext;

                    if (!storeWrapper.serverOptions.DisableObjects)
                        objectBasicContext = respServerSession.storageSession.objectBasicContext.Session.BasicContext;
                    activeDbId = db.Id;
                }
            }
        }

        /// <summary>
        /// Process AOF record internal
        /// NOTE: This method is shared between recover replay and replication replay
        /// </summary>
        /// <param name="virtualSublogIdx"></param>
        /// <param name="ptr"></param>
        /// <param name="length"></param>
        /// <param name="asReplica"></param>
        /// <param name="isCheckpointStart"></param>
        public void ProcessAofRecordInternal(int virtualSublogIdx, byte* ptr, int length, bool asReplica, out bool isCheckpointStart, int replayTaskIdx = -1)
        {
            var processRecordStart = Stopwatch.GetTimestamp();
            var processRecordSetupStart = processRecordStart;
            var timingStats = GetReplayTimingStats(virtualSublogIdx, replayTaskIdx);
            var txnHandlingTicks = 0L;
            var controlOpsTicks = 0L;
            var replayOpTicks = 0L;
            var processRecordSetupTicks = 0L;
            var header = *(AofHeader*)ptr;
            var shardedHeader = default(AofShardedHeader);
            var replayContext = aofReplayCoordinator.GetReplayContext(virtualSublogIdx);
            isCheckpointStart = false;
            var shardedLog = storeWrapper.serverOptions.AofPhysicalSublogCount > 1;
            var updateSequenceNumber = shardedLog && storeWrapper.serverOptions.AofReadWithTimestamp;
            if (timingStats != null)
            {
                processRecordSetupTicks = Stopwatch.GetTimestamp() - processRecordSetupStart;
                timingStats.Add(AofReplayTimingPhase.ReplayProcessRecordSetup, processRecordSetupTicks);
            }

            // Handle transactions
            var txnHandlingStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
            if (aofReplayCoordinator.AddOrReplayTransactionOperation(virtualSublogIdx, ptr, length, asReplica))
            {
                if (timingStats != null)
                {
                    txnHandlingTicks = Stopwatch.GetTimestamp() - txnHandlingStart;
                    timingStats.Add(AofReplayTimingPhase.ReplayTxnHandling, txnHandlingTicks);
                }
                return;
            }
            if (timingStats != null)
            {
                txnHandlingTicks = Stopwatch.GetTimestamp() - txnHandlingStart;
                timingStats.Add(AofReplayTimingPhase.ReplayTxnHandling, txnHandlingTicks);
            }

            switch (header.opType)
            {
                case AofEntryType.CheckpointStartCommit:
                    var controlOpsStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    // Inform caller that we processed a checkpoint start marker so that it can record ReplicationCheckpointStartOffset if this is a replica replay
                    isCheckpointStart = true;
                    if (header.aofHeaderVersion > 1)
                    {
                        if (replayContext.inFuzzyRegion)
                        {
                            logger?.LogInformation("Encountered new CheckpointStartCommit before prior CheckpointEndCommit. Clearing {fuzzyRegionBufferCount} records from previous fuzzy region", aofReplayCoordinator.FuzzyRegionBufferCount(virtualSublogIdx));
                            aofReplayCoordinator.ClearFuzzyRegionBuffer(virtualSublogIdx);
                        }
                        Debug.Assert(!replayContext.inFuzzyRegion);
                        replayContext.inFuzzyRegion = true;
                    }

                    if (updateSequenceNumber)
                    {
                        shardedHeader = *(AofShardedHeader*)ptr;
                        storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, shardedHeader.sequenceNumber);
                    }
                    if (timingStats != null)
                    {
                        controlOpsTicks = Stopwatch.GetTimestamp() - controlOpsStart;
                        timingStats.Add(AofReplayTimingPhase.ReplayControlOps, controlOpsTicks);
                    }
                    break;
                case AofEntryType.CheckpointEndCommit:
                    controlOpsStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    if (header.aofHeaderVersion > 1)
                    {
                        if (!replayContext.inFuzzyRegion)
                        {
                            logger?.LogInformation("Encountered CheckpointEndCommit without a prior CheckpointStartCommit - ignoring");
                        }
                        else
                        {
                            Debug.Assert(replayContext.inFuzzyRegion);
                            replayContext.inFuzzyRegion = false;
                            // Take checkpoint after the fuzzy region
                            if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                            {
                                if (!shardedLog)
                                {
                                    _ = storeWrapper.TakeCheckpoint(background: false, logger);
                                }
                                else
                                {
                                    aofReplayCoordinator.ProcessSynchronizedOperation(
                                        virtualSublogIdx,
                                        ptr,
                                        (int)LeaderBarrierType.CHECKPOINT,
                                        () => storeWrapper.TakeCheckpoint(background: false, logger));
                                }
                            }

                            // Process buffered records
                            aofReplayCoordinator.ProcessFuzzyRegionOperations(virtualSublogIdx, storeWrapper.store.CurrentVersion, asReplica);
                            aofReplayCoordinator.ClearFuzzyRegionBuffer(virtualSublogIdx);
                        }
                    }
                    if (timingStats != null)
                    {
                        controlOpsTicks = Stopwatch.GetTimestamp() - controlOpsStart;
                        timingStats.Add(AofReplayTimingPhase.ReplayControlOps, controlOpsTicks);
                    }
                    break;
                case AofEntryType.MainStoreStreamingCheckpointStartCommit:
                case AofEntryType.ObjectStoreStreamingCheckpointStartCommit:
                    controlOpsStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                    {
                        if (!shardedLog)
                        {
                            storeWrapper.store.SetVersion(header.storeVersion);
                        }
                        else
                        {
                            aofReplayCoordinator.ProcessSynchronizedOperation(
                                virtualSublogIdx,
                                ptr,
                                (int)LeaderBarrierType.STREAMING_CHECKPOINT,
                                () => storeWrapper.store.SetVersion(header.storeVersion));
                        }
                    }
                    if (timingStats != null)
                    {
                        controlOpsTicks = Stopwatch.GetTimestamp() - controlOpsStart;
                        timingStats.Add(AofReplayTimingPhase.ReplayControlOps, controlOpsTicks);
                    }
                    break;
                case AofEntryType.MainStoreStreamingCheckpointEndCommit:
                case AofEntryType.ObjectStoreStreamingCheckpointEndCommit:
                    controlOpsStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    if (updateSequenceNumber)
                    {
                        shardedHeader = *(AofShardedHeader*)ptr;
                        storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, shardedHeader.sequenceNumber);
                    }
                    if (timingStats != null)
                    {
                        controlOpsTicks = Stopwatch.GetTimestamp() - controlOpsStart;
                        timingStats.Add(AofReplayTimingPhase.ReplayControlOps, controlOpsTicks);
                    }
                    break;
                case AofEntryType.FlushAll:
                    controlOpsStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    if (!shardedLog)
                    {
                        storeWrapper.FlushAllDatabases(unsafeTruncateLog: header.unsafeTruncateLog == 1);
                    }
                    else
                    {
                        aofReplayCoordinator.ProcessSynchronizedOperation(
                            virtualSublogIdx,
                            ptr,
                            (int)LeaderBarrierType.FLUSH_DB_ALL,
                            () => storeWrapper.FlushAllDatabases(unsafeTruncateLog: header.unsafeTruncateLog == 1));
                    }
                    if (timingStats != null)
                    {
                        controlOpsTicks = Stopwatch.GetTimestamp() - controlOpsStart;
                        timingStats.Add(AofReplayTimingPhase.ReplayControlOps, controlOpsTicks);
                    }
                    break;
                case AofEntryType.FlushDb:
                    controlOpsStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    if (!shardedLog)
                    {
                        storeWrapper.FlushDatabase(unsafeTruncateLog: header.unsafeTruncateLog == 1, dbId: header.databaseId);
                    }
                    else
                    {
                        aofReplayCoordinator.ProcessSynchronizedOperation(
                            virtualSublogIdx,
                            ptr,
                            (int)LeaderBarrierType.FLUSH_DB,
                            () => storeWrapper.FlushDatabase(unsafeTruncateLog: header.unsafeTruncateLog == 1, dbId: header.databaseId));
                    }
                    if (timingStats != null)
                        timingStats.Add(AofReplayTimingPhase.ReplayControlOps, Stopwatch.GetTimestamp() - controlOpsStart);
                    break;
                default:
                    var replayOpStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    _ = ReplayOp(virtualSublogIdx, stringBasicContext, objectBasicContext, unifiedBasicContext, ptr, length, asReplica, timingStats);
                    if (timingStats != null)
                    {
                        replayOpTicks = Stopwatch.GetTimestamp() - replayOpStart;
                        timingStats.Add(AofReplayTimingPhase.ReplayOpTotal, replayOpTicks);
                    }
                    break;
            }

            if (timingStats != null)
            {
                var accountedTicks = processRecordSetupTicks + txnHandlingTicks + controlOpsTicks + replayOpTicks;
                var totalElapsed = Stopwatch.GetTimestamp() - processRecordStart;
                var scaffoldingTicks = totalElapsed - accountedTicks;
                if (scaffoldingTicks > 0)
                    timingStats.Add(AofReplayTimingPhase.ReplayProcessRecordScaffolding, scaffoldingTicks);
            }
        }

        private unsafe bool ReplayOp<TStringContext, TObjectContext, TUnifiedContext>(
                int sublogIdx,
                TStringContext stringContext, TObjectContext objectContext, TUnifiedContext unifiedContext,
                byte* entryPtr, int length, bool asReplica, AofReplayTimingStats timingStats = null)
            where TStringContext : ITsavoriteContext<StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var header = *(AofHeader*)entryPtr;
            var replayContext = aofReplayCoordinator.GetReplayContext(sublogIdx);

            // Skips (1) entries with versions that were part of prior checkpoint; and (2) future entries in fuzzy region
            var skipRecordStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
            if (SkipRecord(sublogIdx, replayContext.inFuzzyRegion, entryPtr, length, asReplica))
            {
                if (timingStats != null)
                    timingStats.Add(AofReplayTimingPhase.ReplaySkipRecord, Stopwatch.GetTimestamp() - skipRecordStart);
                return false;
            }
            if (timingStats != null)
                timingStats.Add(AofReplayTimingPhase.ReplaySkipRecord, Stopwatch.GetTimestamp() - skipRecordStart);

            var bufferPtr = (byte*)Unsafe.AsPointer(ref replayContext.objectOutputBuffer[0]);
            var bufferLength = replayContext.objectOutputBuffer.Length;

            var needUpdateSequenceNumber = storeWrapper.serverOptions.AofPhysicalSublogCount > 1 && storeWrapper.serverOptions.AofReadWithTimestamp;
            switch (header.opType)
            {
                case AofEntryType.StoreUpsert:
                    var replayOpInnerStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    if (needUpdateSequenceNumber) UpdateKeySequenceNumber(sublogIdx, entryPtr);
                    StoreUpsert(stringContext, AofHeader.SkipHeader(entryPtr), timingStats);
                    if (timingStats != null)
                        timingStats.Add(AofReplayTimingPhase.ReplayStoreUpsert, Stopwatch.GetTimestamp() - replayOpInnerStart);
                    break;
                case AofEntryType.StoreRMW:
                    replayOpInnerStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    if (needUpdateSequenceNumber) UpdateKeySequenceNumber(sublogIdx, entryPtr);
                    StoreRMW(stringContext, AofHeader.SkipHeader(entryPtr));
                    if (timingStats != null)
                        timingStats.Add(AofReplayTimingPhase.ReplayOtherReplayOp, Stopwatch.GetTimestamp() - replayOpInnerStart);
                    break;
                case AofEntryType.StoreDelete:
                    replayOpInnerStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    if (needUpdateSequenceNumber) UpdateKeySequenceNumber(sublogIdx, entryPtr);
                    StoreDelete(stringContext, AofHeader.SkipHeader(entryPtr));
                    if (timingStats != null)
                        timingStats.Add(AofReplayTimingPhase.ReplayOtherReplayOp, Stopwatch.GetTimestamp() - replayOpInnerStart);
                    break;
                case AofEntryType.ObjectStoreRMW:
                    replayOpInnerStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    if (needUpdateSequenceNumber) UpdateKeySequenceNumber(sublogIdx, entryPtr);
                    ObjectStoreRMW(objectContext, AofHeader.SkipHeader(entryPtr), bufferPtr, bufferLength);
                    if (timingStats != null)
                        timingStats.Add(AofReplayTimingPhase.ReplayOtherReplayOp, Stopwatch.GetTimestamp() - replayOpInnerStart);
                    break;
                case AofEntryType.ObjectStoreUpsert:
                    replayOpInnerStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    if (needUpdateSequenceNumber) UpdateKeySequenceNumber(sublogIdx, entryPtr);
                    ObjectStoreUpsert(objectContext, storeWrapper.GarnetObjectSerializer, AofHeader.SkipHeader(entryPtr), bufferPtr, bufferLength);
                    if (timingStats != null)
                        timingStats.Add(AofReplayTimingPhase.ReplayOtherReplayOp, Stopwatch.GetTimestamp() - replayOpInnerStart);
                    break;
                case AofEntryType.ObjectStoreDelete:
                    replayOpInnerStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    if (needUpdateSequenceNumber) UpdateKeySequenceNumber(sublogIdx, entryPtr);
                    ObjectStoreDelete(objectContext, AofHeader.SkipHeader(entryPtr));
                    if (timingStats != null)
                        timingStats.Add(AofReplayTimingPhase.ReplayOtherReplayOp, Stopwatch.GetTimestamp() - replayOpInnerStart);
                    break;
                case AofEntryType.UnifiedStoreRMW:
                    replayOpInnerStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    if (needUpdateSequenceNumber) UpdateKeySequenceNumber(sublogIdx, entryPtr);
                    UnifiedStoreRMW(unifiedContext, AofHeader.SkipHeader(entryPtr), bufferPtr, bufferLength);
                    if (timingStats != null)
                        timingStats.Add(AofReplayTimingPhase.ReplayOtherReplayOp, Stopwatch.GetTimestamp() - replayOpInnerStart);
                    break;
                case AofEntryType.UnifiedStoreStringUpsert:
                    replayOpInnerStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    if (needUpdateSequenceNumber) UpdateKeySequenceNumber(sublogIdx, entryPtr);
                    UnifiedStoreStringUpsert(unifiedContext, AofHeader.SkipHeader(entryPtr), bufferPtr, bufferLength);
                    if (timingStats != null)
                        timingStats.Add(AofReplayTimingPhase.ReplayOtherReplayOp, Stopwatch.GetTimestamp() - replayOpInnerStart);
                    break;
                case AofEntryType.UnifiedStoreObjectUpsert:
                    replayOpInnerStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    if (needUpdateSequenceNumber) UpdateKeySequenceNumber(sublogIdx, entryPtr);
                    UnifiedStoreObjectUpsert(unifiedContext, storeWrapper.GarnetObjectSerializer, AofHeader.SkipHeader(entryPtr), bufferPtr, bufferLength);
                    if (timingStats != null)
                        timingStats.Add(AofReplayTimingPhase.ReplayOtherReplayOp, Stopwatch.GetTimestamp() - replayOpInnerStart);
                    break;
                case AofEntryType.UnifiedStoreDelete:
                    replayOpInnerStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    if (needUpdateSequenceNumber) UpdateKeySequenceNumber(sublogIdx, entryPtr);
                    UnifiedStoreDelete(unifiedContext, AofHeader.SkipHeader(entryPtr));
                    if (timingStats != null)
                        timingStats.Add(AofReplayTimingPhase.ReplayOtherReplayOp, Stopwatch.GetTimestamp() - replayOpInnerStart);
                    break;
                case AofEntryType.StoredProcedure:
                    replayOpInnerStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    aofReplayCoordinator.ReplayStoredProc(sublogIdx, header.procedureId, entryPtr);
                    if (timingStats != null)
                        timingStats.Add(AofReplayTimingPhase.ReplayOtherReplayOp, Stopwatch.GetTimestamp() - replayOpInnerStart);
                    break;
                case AofEntryType.TxnCommit:
                    replayOpInnerStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
                    aofReplayCoordinator.ProcessFuzzyRegionTransactionGroup(sublogIdx, entryPtr, asReplica);
                    if (timingStats != null)
                        timingStats.Add(AofReplayTimingPhase.ReplayOtherReplayOp, Stopwatch.GetTimestamp() - replayOpInnerStart);
                    break;
                default:
                    throw new GarnetException($"Unknown AOF header operation type {header.opType}");
            }

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        AofReplayTimingStats GetReplayTimingStats(int virtualSublogIdx, int replayTaskIdx)
        {
            var timingContext = storeWrapper.serverOptions.AofReplayTimingContext;
            if (timingContext == null || replayTaskIdx < 0)
                return null;

            var replayTaskCount = Math.Max(storeWrapper.serverOptions.AofReplayTaskCount, 1);
            var physicalSublogIdx = virtualSublogIdx / replayTaskCount;
            return timingContext.GetReplayTaskStats(physicalSublogIdx, replayTaskIdx);
        }

        private void UpdateKeySequenceNumber(int sublogIdx, byte* ptr)
        {
            Debug.Assert(storeWrapper.serverOptions.AofPhysicalSublogCount > 1);
            var shardedHeader = *(AofShardedHeader*)ptr;
            var curr = ptr + sizeof(AofShardedHeader);
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr).ReadOnlySpan;
            storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogKeySequenceNumber(sublogIdx, key, shardedHeader.sequenceNumber);
        }

        static void StoreUpsert<TStringContext>(TStringContext stringContext, byte* keyPtr, AofReplayTimingStats timingStats = null)
            where TStringContext : ITsavoriteContext<StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var materializeStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
            var key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
            var curr = keyPtr + key.TotalSize();

            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += value.TotalSize;

            var stringInput = new StringInput();
            _ = stringInput.DeserializeFrom(curr);
            if (timingStats != null)
                timingStats.Add(AofReplayTimingPhase.ReplayStoreUpsertMaterialize, Stopwatch.GetTimestamp() - materializeStart);

            StringOutput output = default;
            var sharedUpsertStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
            _ = stringContext.Upsert(key, ref stringInput, value, ref output);
            if (timingStats != null)
                timingStats.Add(AofReplayTimingPhase.ReplaySharedUpsertPrimitive, Stopwatch.GetTimestamp() - sharedUpsertStart);

            var cleanupStart = timingStats != null ? Stopwatch.GetTimestamp() : 0L;
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
            if (timingStats != null)
                timingStats.Add(AofReplayTimingPhase.ReplayStoreUpsertCleanup, Stopwatch.GetTimestamp() - cleanupStart);
        }

        static void StoreRMW<TStringContext>(TStringContext stringContext, byte* keyPtr)
            where TStringContext : ITsavoriteContext<StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
            var curr = keyPtr + key.TotalSize();

            var stringInput = new StringInput();
            _ = stringInput.DeserializeFrom(curr);

            var output = StringOutput.FromPinnedSpan(stackalloc byte[32]);

            var status = stringContext.RMW(key, ref stringInput, ref output);
            if (status.IsPending)
                StorageSession.CompletePendingForSession(ref status, ref output, ref stringContext);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void StoreDelete<TStringContext>(TStringContext stringContext, byte* keyPtr)
            where TStringContext : ITsavoriteContext<StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            => stringContext.Delete(SpanByte.FromLengthPrefixedPinnedPointer(keyPtr));

        static void ObjectStoreUpsert<TObjectContext>(TObjectContext objectContext, GarnetObjectSerializer garnetObjectSerializer, byte* keyPtr, byte* outputPtr, int outputLength)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
            var curr = keyPtr + key.TotalSize();

            var valueSpan = SpanByte.FromLengthPrefixedPinnedPointer(curr);
            var valueObject = garnetObjectSerializer.Deserialize(valueSpan.ToArray()); // TODO native deserializer to avoid alloc and copy

            var output = ObjectOutput.FromPinnedPointer(outputPtr, outputLength);
            _ = objectContext.Upsert(key, valueObject);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void ObjectStoreRMW<TObjectContext>(TObjectContext objectContext, byte* keyPtr, byte* outputPtr, int outputLength)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
            var curr = keyPtr + key.TotalSize();

            var objectInput = new ObjectInput();
            _ = objectInput.DeserializeFrom(curr);

            // Call RMW with the reconstructed key & ObjectInput
            var output = ObjectOutput.FromPinnedPointer(outputPtr, outputLength);
            var status = objectContext.RMW(key, ref objectInput, ref output);
            if (status.IsPending)
                StorageSession.CompletePendingForObjectStoreSession(ref status, ref output, ref objectContext);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void ObjectStoreDelete<TObjectContext>(TObjectContext objectContext, byte* keyPtr)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => objectContext.Delete(SpanByte.FromLengthPrefixedPinnedPointer(keyPtr));

        static void UnifiedStoreStringUpsert<TUnifiedContext>(TUnifiedContext unifiedContext, byte* keyPtr, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
            var curr = keyPtr + key.TotalSize();

            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += value.TotalSize;

            var unifiedInput = new UnifiedInput();
            _ = unifiedInput.DeserializeFrom(curr);

            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            _ = unifiedContext.Upsert(key, ref unifiedInput, value, ref output);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void UnifiedStoreObjectUpsert<TUnifiedContext>(TUnifiedContext unifiedContext, GarnetObjectSerializer garnetObjectSerializer, byte* keyPtr, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
            var curr = keyPtr + key.TotalSize();

            var valueSpan = SpanByte.FromLengthPrefixedPinnedPointer(curr);
            var valueObject = garnetObjectSerializer.Deserialize(valueSpan.ToArray()); // TODO native deserializer to avoid alloc and copy

            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            _ = unifiedContext.Upsert(key, valueObject);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void UnifiedStoreRMW<TUnifiedContext>(TUnifiedContext unifiedContext, byte* keyPtr, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
            var curr = keyPtr + key.TotalSize();

            var unifiedInput = new UnifiedInput();
            _ = unifiedInput.DeserializeFrom(curr);

            // Call RMW with the reconstructed key & UnifiedInput
            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            var status = unifiedContext.RMW(key, ref unifiedInput, ref output);
            if (status.IsPending)
                StorageSession.CompletePendingForUnifiedStoreSession(ref status, ref output, ref unifiedContext);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void UnifiedStoreDelete<TUnifiedContext>(TUnifiedContext unifiedContext, byte* keyPtr)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
            => unifiedContext.Delete(SpanByte.FromLengthPrefixedPinnedPointer(keyPtr));

        /// <summary>
        /// On recovery apply records with header.version greater than CurrentVersion.
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="inFuzzyRegion"></param>
        /// <param name="entryPtr"></param>
        /// <param name="length"></param>
        /// <param name="asReplica"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        bool SkipRecord(int sublogIdx, bool inFuzzyRegion, byte* entryPtr, int length, bool asReplica)
        {
            var header = *(AofHeader*)entryPtr;
            return (asReplica && inFuzzyRegion) ? // Buffer logic only for AOF version > 1
                BufferNewVersionRecord(sublogIdx, header, entryPtr, length) :
                IsOldVersionRecord(header);

            bool BufferNewVersionRecord(int sublogIdx, AofHeader header, byte* entryPtr, int length)
            {
                if (IsNewVersionRecord(header))
                {
                    aofReplayCoordinator.AddFuzzyRegionOperation(sublogIdx, new ReadOnlySpan<byte>(entryPtr, length));
                    return true;
                }
                return false;
            }

            bool IsOldVersionRecord(AofHeader header)
                => header.storeVersion < storeWrapper.store.CurrentVersion;

            bool IsNewVersionRecord(AofHeader header)
                => header.storeVersion > storeWrapper.store.CurrentVersion;
        }

        /// <summary>
        /// Check if the calling parallel replay task should replay this entry
        /// </summary>
        /// <param name="ptr"></param>
        /// <param name="replayTaskIdx"></param>
        /// <param name="sequenceNumber"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        public bool CanReplay(byte* ptr, int replayTaskIdx, out long sequenceNumber)
        {
            var header = *(AofHeader*)ptr;
            var replayHeaderType = (AofHeaderType)header.padding;
            sequenceNumber = 0L;
            switch (replayHeaderType)
            {
                // Check if should replay entry by inspecting key
                case AofHeaderType.ShardedHeader:
                    var shardedHeader = *(AofShardedHeader*)ptr;
                    sequenceNumber = shardedHeader.sequenceNumber;
                    var curr = AofHeader.SkipHeader(ptr);
                    var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr).ReadOnlySpan;
                    var hash = GarnetLog.HASH(key);
                    var _replayTaskIdx = hash % storeWrapper.serverOptions.AofReplayTaskCount;
                    return replayTaskIdx == _replayTaskIdx;
                // If no key to inspect, check bit vector for participating replay tasks in the transaction
                // NOTE: HeaderType transactions include MULTI-EXEC transactions, custom txn procedures, and any operation that executes across physical and virtual sublogs (e.g. checkpoint, flushdb)
                case AofHeaderType.TransactionHeader:
                    var txnHeader = *(AofTransactionHeader*)ptr;
                    sequenceNumber = txnHeader.shardedHeader.sequenceNumber;
                    var bitVector = BitVector.CopyFrom(new Span<byte>(txnHeader.replayTaskAccessVector, AofTransactionHeader.ReplayTaskAccessVectorBytes));
                    return bitVector.IsSet(replayTaskIdx);
                default:
                    throw new GarnetException($"Replay header type {replayHeaderType} not supported!");
            }
        }

        /// <summary>
        /// Determines whether the specified log entry should be skipped during replay based on its sequence number.
        /// </summary>
        /// <param name="ptr">A pointer to the start of the log entry header in memory. Must point to a valid header structure.</param>
        /// <param name="untilSequenceNumber">The sequence number threshold. Entries with a sequence number greater than this value will be skipped.
        /// Specify -1 to skip all entries.</param>
        /// <param name="entrySequenceNumber">When this method returns, contains the sequence number of the current log entry, or -1 if unavailable.</param>
        /// <returns>true if the log entry should be skipped; otherwise, false.</returns>
        /// <exception cref="GarnetException">Thrown if the log entry header type is not supported.</exception>
        public bool SkipReplay(byte* ptr, long untilSequenceNumber, out long entrySequenceNumber)
        {
            entrySequenceNumber = -1;
            if (untilSequenceNumber == -1)
                return true;
            var header = *(AofHeader*)ptr;
            var replayHeaderType = (AofHeaderType)header.padding;
            switch (replayHeaderType)
            {
                case AofHeaderType.ShardedHeader:
                    var shardedHeader = *(AofShardedHeader*)ptr;
                    entrySequenceNumber = shardedHeader.sequenceNumber;
                    return shardedHeader.sequenceNumber > untilSequenceNumber;
                case AofHeaderType.TransactionHeader:
                    var txnHeader = *(AofTransactionHeader*)ptr;
                    entrySequenceNumber = txnHeader.shardedHeader.sequenceNumber;
                    return txnHeader.shardedHeader.sequenceNumber > untilSequenceNumber;
                default:
                    throw new GarnetException($"Replay header type {replayHeaderType} not supported!");
            }
        }
    }
}
