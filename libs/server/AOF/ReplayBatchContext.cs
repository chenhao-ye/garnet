// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Threading;
using Garnet.common;
using Tsavorite.core;

using static Garnet.server.GarnetLog;

namespace Garnet.server
{
    /// <summary>
    /// Minimal descriptor for a single AOF log entry. Carries only a pointer and
    /// pre-computed key hash so replay tasks can decide whether to dereference
    /// without redundant hashing.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct ReplayEntryDescriptor
    {
        /// <summary>
        /// Pointer to the TsavoriteLog record start (before the TsavoriteLog header).
        /// Tasks call <see cref="TsavoriteLog.UnsafeGetLength"/> on this to get the payload length,
        /// then add headerSize to reach the AofHeader.
        /// </summary>
        public byte* Ptr;

        /// <summary>
        /// Pre-computed key hash for routing.
        /// >= 0: ShardedHeader key hash — task checks <c>KeyHash % AofReplayTaskCount == myIdx</c> to skip without dereferencing.
        /// &lt; 0: must dereference to determine entry type (TransactionHeader or commit metadata).
        /// </summary>
        public long KeyHash;
    }

    /// <summary>
    /// Replay work item used with recover/replication replay.
    /// </summary>
    /// <param name="replayTasks"></param>
    public unsafe class ReplayBatchContext(int replayTasks)
    {
        const int InitialDescriptorCapacity = 1 << 16; // 64K entries
        const int PublishInterval = 16;

        /// <summary>
        /// Record pointer.
        /// </summary>
        public byte* Record;
        /// <summary>
        /// Record length.
        /// </summary>
        public int RecordLength;
        /// <summary>
        /// Represents the current address value for a given TsavoriteLog page.
        /// </summary>
        public long CurrentAddress;
        /// <summary>
        /// Represents the next address value for a given TsavoriteLog page.
        /// </summary>
        public long NextAddress;
        /// <summary>
        /// Whether replay occurs under epoch protections.
        /// </summary>
        public bool IsProtected;
        /// <summary>
        /// Leader barrier to coordinate replication offset update.
        /// </summary>
        public LeaderFollowerBarrier LeaderFollowerBarrier = new(replayTasks);

        /// <summary>
        /// Pre-computed entry descriptors for parallel replay tasks.
        /// </summary>
        public ReplayEntryDescriptor[] Descriptors = new ReplayEntryDescriptor[InitialDescriptorCapacity];

        /// <summary>
        /// Number of descriptors produced so far (updated via Volatile.Write for cross-thread visibility).
        /// </summary>
        public int ProducedCount;

        /// <summary>
        /// Total number of descriptors in the current batch (valid after production completes).
        /// </summary>
        public int TotalCount;

        /// <summary>
        /// Whether the driver has finished producing all descriptors for the current batch.
        /// </summary>
        public bool ProductionComplete;

        /// <summary>
        /// Maximum sequence number across all entries in the current batch, computed by the producer.
        /// Tasks use this to update their virtual sublog max sequence number.
        /// </summary>
        public long MaxBatchSequenceNumber;

        /// <summary>
        /// Reset production state for a new batch.
        /// </summary>
        public void ResetProduction()
        {
            Volatile.Write(ref ProductionComplete, false);
            Volatile.Write(ref ProducedCount, 0);
            TotalCount = 0;
            MaxBatchSequenceNumber = 0;
        }

        /// <summary>
        /// Ensure the descriptor array has enough capacity.
        /// </summary>
        void EnsureDescriptorCapacity(int needed)
        {
            if (Descriptors.Length >= needed)
                return;
            var newCapacity = Descriptors.Length;
            while (newCapacity < needed)
                newCapacity <<= 1;
            Descriptors = new ReplayEntryDescriptor[newCapacity];
        }

        /// <summary>
        /// Pre-scan the batch to build entry descriptors with pre-computed key hashes.
        /// Descriptors are published incrementally via Volatile.Write so replay tasks
        /// can begin consuming before the full batch is scanned.
        /// Also computes <see cref="MaxBatchSequenceNumber"/> across all entries.
        /// </summary>
        /// <param name="record">Pointer to the batch record buffer.</param>
        /// <param name="recordLength">Length of the batch in bytes.</param>
        /// <param name="headerSize">TsavoriteLog header size (appendOnlyFile.HeaderSize).</param>
        /// <param name="sublog">The TsavoriteLog sublog for reading entry lengths.</param>
        public void ProduceDescriptors(byte* record, int recordLength, int headerSize, TsavoriteLog sublog)
        {
            // Estimate max entries and ensure capacity
            var minEntrySize = headerSize + AofHeader.TotalSize;
            var estimatedEntries = (recordLength / minEntrySize) + 1;
            EnsureDescriptorCapacity(estimatedEntries);

            var descriptors = Descriptors;
            var ptr = record;
            var idx = 0;
            var maxSeqNo = 0L;

            while (ptr < record + recordLength)
            {
                var entryLength = headerSize;
                var payloadLength = sublog.UnsafeGetLength(ptr);

                if (payloadLength > 0)
                {
                    var entryPtr = ptr + entryLength;
                    var header = *(AofHeader*)entryPtr;
                    var headerType = (AofHeaderType)header.padding;

                    long keyHash;
                    switch (headerType)
                    {
                        case AofHeaderType.ShardedHeader:
                            var shardedHeader = *(AofShardedHeader*)entryPtr;
                            maxSeqNo = Math.Max(maxSeqNo, shardedHeader.sequenceNumber);
                            var curr = AofHeader.SkipHeader(entryPtr);
                            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr).ReadOnlySpan;
                            keyHash = HASH(key);
                            break;
                        case AofHeaderType.TransactionHeader:
                            var txnHeader = *(AofTransactionHeader*)entryPtr;
                            maxSeqNo = Math.Max(maxSeqNo, txnHeader.shardedHeader.sequenceNumber);
                            keyHash = -1;
                            break;
                        default:
                            throw new GarnetException($"Replay header type {headerType} not supported!");
                    }

                    descriptors[idx] = new ReplayEntryDescriptor { Ptr = ptr, KeyHash = keyHash };
                    idx++;
                    entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
                }
                else if (payloadLength < 0)
                {
                    // Commit metadata — KeyHash < 0 forces tasks to dereference
                    descriptors[idx] = new ReplayEntryDescriptor { Ptr = ptr, KeyHash = -1 };
                    idx++;
                    entryLength += TsavoriteLog.UnsafeAlign(-payloadLength);
                }

                // Publish descriptors in batches for amortized fence cost
                if (idx > 0 && (idx % PublishInterval == 0))
                    Volatile.Write(ref ProducedCount, idx);

                ptr += entryLength;
            }

            // Publish final batch and mark production complete
            MaxBatchSequenceNumber = maxSeqNo;
            TotalCount = idx;
            Volatile.Write(ref ProducedCount, idx);
            Volatile.Write(ref ProductionComplete, true);
        }

        /// <summary>
        /// Check if a replay task participates in a transaction entry.
        /// Used by replay tasks for entries with KeyHash &lt; 0 that are TransactionHeaders.
        /// </summary>
        /// <param name="entryPtr">Pointer to the entry (AofTransactionHeader).</param>
        /// <param name="replayTaskIdx">The replay task index to check.</param>
        /// <returns>True if the task participates in this transaction.</returns>
        public static bool IsTransactionParticipant(byte* entryPtr, int replayTaskIdx)
        {
            var txnHeader = (AofTransactionHeader*)entryPtr;
            var bitVector = BitVector.CopyFrom(new Span<byte>(txnHeader->replayTaskAccessVector, AofTransactionHeader.ReplayTaskAccessVectorBytes));
            return bitVector.IsSet(replayTaskIdx);
        }
    }
}