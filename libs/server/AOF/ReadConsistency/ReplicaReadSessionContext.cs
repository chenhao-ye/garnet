// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Tsavorite.core;

namespace Garnet.server
{
    [StructLayout(LayoutKind.Explicit, Size = 40)]
    public struct ReplicaReadSessionContext
    {
        /// <summary>
        /// Session version
        /// </summary>
        [FieldOffset(0)]
        public long sessionVersion;

        /// <summary>
        /// Maximum session sequence number established from all keys read so far
        /// </summary>
        [FieldOffset(8)]
        public long maximumSessionSequenceNumber;

        /// <summary>
        /// Last read hash
        /// </summary>
        [FieldOffset(16)]
        public long lastHash;

        /// <summary>
        /// Last read sublogIdx
        /// </summary>
        [FieldOffset(24)]
        public short lastVirtualSublogIdx;

        /// <summary>
        /// Per-session cached lower-bound view of each virtual sublog's published max sequence number.
        /// Lets the freshness check skip reading the constantly-replay-written global published max when
        /// the cached value already proves freshness. Allocated and reset by CheckConsistencyManagerVersion.
        /// </summary>
        [FieldOffset(32)]
        public long[] cachedSublogMax;
    }

    public class ReadSessionState : IDisposable
    {
        /// <summary>
        /// GarnetAppendOnlyFile instance
        /// </summary>
        readonly GarnetAppendOnlyFile appendOnlyFile;

        /// <summary>
        /// Replica read context used with sharded log
        /// </summary>
        ReplicaReadSessionContext replicaReadContext;

        /// <summary>
        /// Read context for batch reads. Used to track max sequence number of all keys involved in the read.
        /// </summary>
        ReplicaReadSessionContext batchReadContext;

        /// <summary>
        /// Session-lifetime cancellation.
        /// Canceled in Dispose to wake any in-flight wait inside the read consistency manager.
        /// </summary>
        readonly CancellationTokenSource cts;

        /// <summary>
        /// Reusable wakeup primitive for the consistent-read wait path. Initialized in the constructor
        /// and replaced in-place by <see cref="VirtualSublogReplayState.WaitForSequenceNumber"/> if the
        /// previous wait was cancelled or timed out (the retired instance lingers as a tombstone in the
        /// virtual sublog's wait queue until the next signal pass dequeues it).
        /// </summary>
        ConsistentReadWaiter consistentReadWaiter;

        /// <summary>
        /// Array of key hashes used for consistent read key batch.
        /// </summary>
        long[] keyHashCache = null;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static int GetPowerOfTwoSize(int value)
            => value <= 1 ? 1 : (int)BitOperations.RoundUpToPowerOf2((uint)value);

        void ExpandKeyHashCache(int keyCount)
        {
            var newSize = GetPowerOfTwoSize(keyCount);
            keyHashCache = GC.AllocateArray<long>(newSize, pinned: true);
        }

        void ShrinkKeyHashCache(int keyCount)
        {
            var newSize = GetPowerOfTwoSize(keyCount);
            keyHashCache = GC.AllocateArray<long>(newSize, pinned: true);
        }

        /// <summary>
        /// Read session state constructor
        /// </summary>
        /// <param name="appendOnlyFile"></param>
        public ReadSessionState(GarnetAppendOnlyFile appendOnlyFile)
        {
            this.appendOnlyFile = appendOnlyFile;
            replicaReadContext = new() { sessionVersion = -1, maximumSessionSequenceNumber = 0, lastVirtualSublogIdx = -1 };
            cts = new();
            consistentReadWaiter = new ConsistentReadWaiter();
        }

        /// <summary>
        /// Releases all resources used by the current instance of the class.
        /// </summary>
        public void Dispose()
        {
            cts.Cancel();
            cts.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void BeforeConsistentReadKeyCallback(long hash)
            => appendOnlyFile.readConsistencyManager.BeforeConsistentReadKey(
                hash, ref replicaReadContext, ref consistentReadWaiter, cts.Token);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AfterConsistentReadKeyCallback()
            => appendOnlyFile.readConsistencyManager.AfterConsistentReadKey(ref replicaReadContext);

        /// <summary>
        /// Initialize context for read key batch.
        /// </summary>
        /// <param name="parameters"></param>
        public void BeforeConsistentReadKeyBatch(ReadOnlySpan<PinnedSpanByte> parameters)
        {
            var keyCount = parameters.Length;
            var consistencyManager = appendOnlyFile.readConsistencyManager;
            // First check if version of consistency mananger has changed
            consistencyManager.CheckConsistencyManagerVersion(ref replicaReadContext);

            // Allocate array to cache key hashes for batch read
            if (keyHashCache == null || keyCount > keyHashCache.Length)
                ExpandKeyHashCache(keyCount);
            else if ((keyCount << 2) < keyHashCache.Length)
                ShrinkKeyHashCache(keyCount);

            // NOTE: this context is a copy used to emulate standalone reads.
            // The actual update of the session max will happen after the read succeeds.
            batchReadContext = replicaReadContext;
            for (var i = 0; i < parameters.Length; i++)
            {
                var key = parameters[i];
                consistencyManager.BeforeConsistentReadKeyBatch(
                    key.ReadOnlySpan, ref batchReadContext, ref consistentReadWaiter, cts.Token, out var hash);
                keyHashCache[i] = hash;
            }
        }

        /// <summary>
        /// Validate keys have not changed after reading a key batch.
        /// </summary>
        /// <param name="keyCount"></param>
        /// <returns></returns>
        public bool AfterConsistentReadKeyBatch(int keyCount)
        {
            var consistencyManager = appendOnlyFile.readConsistencyManager;
            for (var i = 0; i < keyCount; i++)
            {
                var hash = keyHashCache[i];
                if (!consistencyManager.AfterConsistentReadKeyBatch(hash, ref batchReadContext))
                    return false;
            }

            return true;
        }
    }
}