// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
        readonly AofReplayCoordinator aofReplayCoordinator;

        int activeDbId;
        VectorManager activeVectorManager;

        /// <summary>
        /// Set ReadWriteSession on the cluster session (NOTE: used for replaying stored procedures only)
        /// </summary>
        public void SetReadWriteSession()
        {
            for (var i = 0; i < storeWrapper.serverOptions.AofVirtualSublogCount; i++)
            {
                var respServerSession = aofReplayCoordinator.GetReplayContext(i).respServerSession;
                respServerSession.clusterSession.SetReadWriteSession();
            }
        }

        readonly StoreWrapper replayAofStoreWrapper;
        readonly IClusterProvider clusterProvider;

        readonly Func<RespServerSession> obtainServerSession;

        readonly ILogger logger;
        readonly bool usingShardedLog;

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
            this.replayAofStoreWrapper = new StoreWrapper(storeWrapper, recordToAof);

            this.activeDbId = 0;
            this.usingShardedLog = storeWrapper.serverOptions.AofPhysicalSublogCount > 1 || storeWrapper.serverOptions.AofReplayTaskCount > 1;
            this.obtainServerSession = () => new(0, networkSender: null, storeWrapper: replayAofStoreWrapper, subscribeBroker: null, authenticator: null, enableScripts: false, clusterProvider: clusterProvider);

            this.aofReplayCoordinator = new AofReplayCoordinator(storeWrapper.serverOptions, this, logger);
            this.logger = logger;

            // Switch current contexts to match the default database
            SwitchActiveDatabaseContext(storeWrapper.DefaultDatabase, true);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            activeVectorManager?.WaitForVectorOperationsToComplete();
            activeVectorManager?.ShutdownReplayTasks();
            aofReplayCoordinator?.Dispose();
        }

        private RespServerSession ObtainServerSession()
            => new(0, networkSender: null, storeWrapper: replayAofStoreWrapper, subscribeBroker: null, authenticator: null, enableScripts: false, clusterProvider: clusterProvider);

        private void SwitchActiveDatabaseContext(GarnetDatabase db, bool initialSetup = false)
        {
            for (var i = 0; i < storeWrapper.serverOptions.AofVirtualSublogCount; i++)
            {
                var respServerSession = aofReplayCoordinator.GetReplayContext(i).respServerSession;
                // Switch the session's context to match the specified database, if necessary
                if (respServerSession.activeDbId != db.Id)
                {
                    var switchDbSuccessful = respServerSession.TrySwitchActiveDatabaseSession(db.Id);
                    Debug.Assert(switchDbSuccessful);
                }

                // Switch the storage context to match the session, if necessary
                if (activeDbId != db.Id || initialSetup)
                {
                    activeDbId = db.Id;
                    activeVectorManager = db.VectorManager;
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
        public void ProcessAofRecordInternal(int virtualSublogIdx, byte* ptr, int length, bool asReplica, out bool isCheckpointStart)
        {
            var header = *(AofHeader*)ptr;
            var shardedHeader = default(AofShardedHeader);
            var replayContext = aofReplayCoordinator.GetReplayContext(virtualSublogIdx);
            isCheckpointStart = false;

            // StoreRMW can queue VADDs onto different threads
            // but everything else needs to WAIT for those to complete
            // otherwise we might loose consistency
            if (header.opType != AofEntryType.StoreRMW)
            {
                activeVectorManager.WaitForVectorOperationsToComplete();
            }

            // Handle transactions
            if (aofReplayCoordinator.AddOrReplayTransactionOperation(virtualSublogIdx, ptr, length, asReplica))
                return;

            switch (header.opType)
            {
                case AofEntryType.CheckpointStartCommit:
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

                    if (usingShardedLog)
                    {
                        shardedHeader = *(AofShardedHeader*)ptr;
                        storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, shardedHeader.sequenceNumber);
                    }
                    break;
                case AofEntryType.CheckpointEndCommit:
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
                                if (!usingShardedLog)
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
                    break;
                case AofEntryType.MainStoreStreamingCheckpointStartCommit:
                case AofEntryType.ObjectStoreStreamingCheckpointStartCommit:
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                    {
                        if (!usingShardedLog)
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
                    break;
                case AofEntryType.MainStoreStreamingCheckpointEndCommit:
                case AofEntryType.ObjectStoreStreamingCheckpointEndCommit:
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    if (usingShardedLog)
                    {
                        shardedHeader = *(AofShardedHeader*)ptr;
                        storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, shardedHeader.sequenceNumber);
                    }
                    break;
                case AofEntryType.FlushAll:
                    if (!usingShardedLog)
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
                    break;
                case AofEntryType.FlushDb:
                    if (!usingShardedLog)
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
                    break;
                default:
                    _ = ReplayOp(
                        virtualSublogIdx,
                        replayContext.StringBasicContext,
                        replayContext.ObjectBasicContext,
                        replayContext.UnifiedBasicContext,
                        ptr,
                        length,
                        asReplica);
                    break;
            }
        }

        private bool ReplayOp<TStringContext, TObjectContext, TUnifiedContext>(
                int sublogIdx,
                TStringContext stringContext, TObjectContext objectContext, TUnifiedContext unifiedContext,
                byte* entryPtr, int length, bool asReplica)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var header = *(AofHeader*)entryPtr;
            var replayContext = aofReplayCoordinator.GetReplayContext(sublogIdx);

            // StoreRMW can queue VADDs onto different threads
            // but everything else needs to WAIT for those to complete
            // otherwise we might loose consistency
            if (header.opType != AofEntryType.StoreRMW)
            {
                activeVectorManager.WaitForVectorOperationsToComplete();
            }

            // Skips (1) entries with versions that were part of prior checkpoint; and (2) future entries in fuzzy region
            if (SkipRecord(sublogIdx, replayContext.inFuzzyRegion, entryPtr, length, asReplica))
                return false;

            var bufferPtr = (byte*)Unsafe.AsPointer(ref replayContext.objectOutputBuffer[0]);
            var bufferLength = replayContext.objectOutputBuffer.Length;
            switch (header.opType)
            {
                case AofEntryType.StoreUpsert:
                {
                    PrepareKey(sublogIdx, entryPtr, out var key, out var keyHash, out var postKeyPtr);
                    StoreUpsert(stringContext, ref replayContext.parseState, key, keyHash, postKeyPtr);
                    break;
                }
                case AofEntryType.StoreRMW:
                {
                    PrepareKey(sublogIdx, entryPtr, out var key, out var keyHash, out var postKeyPtr);
                    StoreRMW(stringContext, ref replayContext.parseState, activeVectorManager, replayContext.respServerSession, obtainServerSession, key, keyHash, postKeyPtr);
                    break;
                }
                case AofEntryType.StoreDelete:
                {
                    PrepareKey(sublogIdx, entryPtr, out var key, out var keyHash, out _);
                    StoreDelete(stringContext, key, keyHash);
                    break;
                }
                case AofEntryType.ObjectStoreRMW:
                {
                    PrepareKey(sublogIdx, entryPtr, out var key, out var keyHash, out var postKeyPtr);
                    ObjectStoreRMW(objectContext, ref replayContext.parseState, key, keyHash, postKeyPtr, bufferPtr, bufferLength);
                    break;
                }
                case AofEntryType.ObjectStoreUpsert:
                {
                    PrepareKey(sublogIdx, entryPtr, out var key, out var keyHash, out var postKeyPtr);
                    ObjectStoreUpsert(objectContext, storeWrapper.GarnetObjectSerializer, key, keyHash, postKeyPtr, bufferPtr, bufferLength);
                    break;
                }
                case AofEntryType.ObjectStoreDelete:
                {
                    PrepareKey(sublogIdx, entryPtr, out var key, out var keyHash, out _);
                    ObjectStoreDelete(objectContext, key, keyHash);
                    break;
                }
                case AofEntryType.UnifiedStoreRMW:
                {
                    PrepareKey(sublogIdx, entryPtr, out var key, out var keyHash, out var postKeyPtr);
                    UnifiedStoreRMW(unifiedContext, ref replayContext.parseState, key, keyHash, postKeyPtr, bufferPtr, bufferLength);
                    break;
                }
                case AofEntryType.UnifiedStoreStringUpsert:
                {
                    PrepareKey(sublogIdx, entryPtr, out var key, out var keyHash, out var postKeyPtr);
                    UnifiedStoreStringUpsert(unifiedContext, ref replayContext.parseState, key, keyHash, postKeyPtr, bufferPtr, bufferLength);
                    break;
                }
                case AofEntryType.UnifiedStoreObjectUpsert:
                {
                    PrepareKey(sublogIdx, entryPtr, out var key, out var keyHash, out var postKeyPtr);
                    UnifiedStoreObjectUpsert(unifiedContext, storeWrapper.GarnetObjectSerializer, key, keyHash, postKeyPtr, bufferPtr, bufferLength);
                    break;
                }
                case AofEntryType.UnifiedStoreDelete:
                {
                    PrepareKey(sublogIdx, entryPtr, out var key, out var keyHash, out _);
                    UnifiedStoreDelete(unifiedContext, activeVectorManager, replayContext.respServerSession.storageSession, key, keyHash);
                    break;
                }
                case AofEntryType.StoredProcedure:
                {
                    aofReplayCoordinator.ReplayStoredProc(sublogIdx, header.procedureId, entryPtr);
                    break;
                }
                case AofEntryType.TxnCommit:
                {
                    aofReplayCoordinator.ProcessFuzzyRegionTransactionGroup(sublogIdx, entryPtr, asReplica);
                    break;
                }
                default:
                    throw new GarnetException($"Unknown AOF header operation type {header.opType}");
            }

            return true;
        }

        // Extract key from entryPtr.
        // In sharded mode, additionally hashes the key and updates the ReadConsistencyManager,
        // returning the hash via keyHash for the caller to forward to Tsavorite via opts.KeyHash.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PrepareKey(int sublogIdx, byte* entryPtr, out Span<byte> key, out long? keyHash, out byte* postKeyPtr)
        {
            if (usingShardedLog)
            {
                var keyPtr = entryPtr + sizeof(AofShardedHeader);
                key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
                postKeyPtr = keyPtr + key.TotalSize();
                var hash = GarnetKeyComparer.StaticGetHashCode64((FixedSpanByteKey)key);
                var seqNum = ((AofShardedHeader*)entryPtr)->sequenceNumber;
                storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogKeySequenceNumber(sublogIdx, hash, seqNum);
                keyHash = hash;
            }
            else
            {
                var keyPtr = entryPtr + sizeof(AofHeader);
                key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
                postKeyPtr = keyPtr + key.TotalSize();
                keyHash = null;
            }
        }

        static void StoreUpsert<TStringContext>(TStringContext stringContext, ref SessionParseState parseState, Span<byte> key, long? keyHash, byte* postKeyPtr)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(postKeyPtr);
            var curr = postKeyPtr + value.TotalSize;

            var stringInput = new StringInput { parseState = parseState };
            _ = stringInput.DeserializeFrom(curr);

            StringOutput output = default;
            var opts = new UpsertOptions { KeyHash = keyHash };
            _ = stringContext.Upsert((FixedSpanByteKey)key, ref stringInput, value, ref output, ref opts);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void StoreRMW<TStringContext>(TStringContext stringContext, ref SessionParseState parseState, VectorManager vectorManager, RespServerSession activeServerSession, Func<RespServerSession> obtainServerSession, Span<byte> key, long? keyHash, byte* postKeyPtr)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var stringInput = new StringInput { parseState = parseState };
            _ = stringInput.DeserializeFrom(postKeyPtr);

            // VADD requires special handling, shove it over to the VectorManager
            if (stringInput.header.cmd == RespCommand.VADD)
            {
                vectorManager.HandleVectorSetAddReplication(activeServerSession.storageSession, obtainServerSession, key, ref stringInput);
                return;
            }
            else
            {
                // Any other op (include other vector ops) need to wait for pending VADDs to complete
                vectorManager.WaitForVectorOperationsToComplete();

                // VREM is also read-like, so requires special handling - shove it over to the VectorManager
                if (stringInput.header.cmd == RespCommand.VREM)
                {
                    vectorManager.HandleVectorSetRemoveReplication(activeServerSession.storageSession, key, ref stringInput);
                    return;
                }
            }

            var output = StringOutput.FromPinnedSpan(stackalloc byte[32]);
            var opts = new RMWOptions { KeyHash = keyHash };
            var status = stringContext.RMW((FixedSpanByteKey)key, ref stringInput, ref output, ref opts);
            if (status.IsPending)
                StorageSession.CompletePendingForSession(ref status, ref output, ref stringContext);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void StoreDelete<TStringContext>(TStringContext stringContext, Span<byte> key, long? keyHash)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var opts = new DeleteOptions { KeyHash = keyHash };
            _ = stringContext.Delete((FixedSpanByteKey)key, ref opts);
        }

        static void ObjectStoreUpsert<TObjectContext>(TObjectContext objectContext, GarnetObjectSerializer garnetObjectSerializer, Span<byte> key, long? keyHash, byte* postKeyPtr, byte* outputPtr, int outputLength)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var valueSpan = SpanByte.FromLengthPrefixedPinnedPointer(postKeyPtr);
            var valueObject = garnetObjectSerializer.Deserialize(valueSpan.ToArray()); // TODO native deserializer to avoid alloc and copy

            var output = ObjectOutput.FromPinnedPointer(outputPtr, outputLength);
            var opts = new UpsertOptions { KeyHash = keyHash };
            _ = objectContext.Upsert((FixedSpanByteKey)key, valueObject, ref opts);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void ObjectStoreRMW<TObjectContext>(TObjectContext objectContext, ref SessionParseState parseState, Span<byte> key, long? keyHash, byte* postKeyPtr, byte* outputPtr, int outputLength)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var objectInput = new ObjectInput { parseState = parseState };
            _ = objectInput.DeserializeFrom(postKeyPtr);

            var output = ObjectOutput.FromPinnedPointer(outputPtr, outputLength);
            var opts = new RMWOptions { KeyHash = keyHash };
            var status = objectContext.RMW((FixedSpanByteKey)key, ref objectInput, ref output, ref opts);
            if (status.IsPending)
                StorageSession.CompletePendingForObjectStoreSession(ref status, ref output, ref objectContext);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void ObjectStoreDelete<TObjectContext>(TObjectContext objectContext, Span<byte> key, long? keyHash)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var opts = new DeleteOptions { KeyHash = keyHash };
            _ = objectContext.Delete((FixedSpanByteKey)key, ref opts);
        }

        static void UnifiedStoreStringUpsert<TUnifiedContext>(TUnifiedContext unifiedContext, ref SessionParseState parseState, Span<byte> key, long? keyHash, byte* postKeyPtr, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(postKeyPtr);
            var curr = postKeyPtr + value.TotalSize;

            var unifiedInput = new UnifiedInput { parseState = parseState };
            _ = unifiedInput.DeserializeFrom(curr);

            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            var opts = new UpsertOptions { KeyHash = keyHash };
            _ = unifiedContext.Upsert((FixedSpanByteKey)key, ref unifiedInput, value, ref output, ref opts);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void UnifiedStoreObjectUpsert<TUnifiedContext>(TUnifiedContext unifiedContext, GarnetObjectSerializer garnetObjectSerializer, Span<byte> key, long? keyHash, byte* postKeyPtr, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var valueSpan = SpanByte.FromLengthPrefixedPinnedPointer(postKeyPtr);
            var valueObject = garnetObjectSerializer.Deserialize(valueSpan.ToArray()); // TODO native deserializer to avoid alloc and copy

            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            var opts = new UpsertOptions { KeyHash = keyHash };
            _ = unifiedContext.Upsert((FixedSpanByteKey)key, valueObject, ref opts);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void UnifiedStoreRMW<TUnifiedContext>(TUnifiedContext unifiedContext, ref SessionParseState parseState, Span<byte> key, long? keyHash, byte* postKeyPtr, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var unifiedInput = new UnifiedInput { parseState = parseState };
            _ = unifiedInput.DeserializeFrom(postKeyPtr);

            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            var opts = new RMWOptions { KeyHash = keyHash };
            var status = unifiedContext.RMW((FixedSpanByteKey)key, ref unifiedInput, ref output, ref opts);
            if (status.IsPending)
                StorageSession.CompletePendingForUnifiedStoreSession(ref status, ref output, ref unifiedContext);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void UnifiedStoreDelete<TUnifiedContext>(TUnifiedContext unifiedContext, VectorManager vectorManager, StorageSession storageSession, Span<byte> key, long? keyHash)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var opts = new DeleteOptions { KeyHash = keyHash };
            var res = unifiedContext.Delete((FixedSpanByteKey)key, ref opts);

            if (res.IsCanceled)
            {
                // Might be a vector set
                res = vectorManager.TryDeleteVectorSet(storageSession, key, out _);
                if (res.IsPending)
                    _ = unifiedContext.CompletePending(true);
            }
        }

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
            sequenceNumber = 0L;
            switch (header.headerType)
            {
                // Check replay tag embedded in header to determine if this task should replay the entry
                case AofHeaderType.ShardedHeader:
                    var shardedHeader = *(AofShardedHeader*)ptr;
                    sequenceNumber = shardedHeader.sequenceNumber;
                    var _replayTaskIdx = header.replayTag % storeWrapper.serverOptions.AofReplayTaskCount;
                    return replayTaskIdx == _replayTaskIdx;
                // If no key to inspect, check bit vector for participating replay tasks in the transaction
                // NOTE: HeaderType transactions include MULTI-EXEC transactions, custom txn procedures, and any operation that executes across physical and virtual sublogs (e.g. checkpoint, flushdb)
                case AofHeaderType.TransactionHeader:
                    var txnHeader = *(AofTransactionHeader*)ptr;
                    sequenceNumber = txnHeader.shardedHeader.sequenceNumber;
                    var bitVector = BitVector.CopyFrom(new Span<byte>(txnHeader.replayTaskAccessVector, AofTransactionHeader.ReplayTaskAccessVectorBytes));
                    return bitVector.IsSet(replayTaskIdx);
                default:
                    throw new GarnetException($"Replay header type {header.headerType} not supported!");
            }
        }

        /// <summary>
        /// Calculates the index of the replay task associated with the specified AOF header pointer.
        /// </summary>
        /// <param name="ptr">A pointer to a byte array representing the AOF header.</param>
        /// <returns>The zero-based index of the replay task to which the entry should be assigned. Returns -1 if the header type
        /// does not contain a key for task assignment.</returns>
        /// <exception cref="GarnetException">Thrown when the AOF header type referenced by <paramref name="ptr"/> is not supported.</exception>
        public int GetReplayTaskIdx(byte* ptr)
        {
            var header = *(AofHeader*)ptr;
            switch (header.headerType)
            {
                // Extract replay tag from header to determine target replay task
                case AofHeaderType.ShardedHeader:
                    return header.replayTag % storeWrapper.serverOptions.AofReplayTaskCount;
                // If no key to inspect, check bit vector for participating replay tasks in the transaction
                // NOTE: HeaderType transactions include MULTI-EXEC transactions, custom txn procedures, and any operation that executes across physical and virtual sublogs (e.g. checkpoint, flushdb)
                case AofHeaderType.TransactionHeader:
                    return -1;
                default:
                    throw new GarnetException($"Replay header type {header.headerType} not supported!");
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
            switch (header.headerType)
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
                    throw new GarnetException($"Replay header type {header.headerType} not supported!");
            }
        }
    }
}