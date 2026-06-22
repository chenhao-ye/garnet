// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Manages read consistency for append-only file operations, tracking sequence numbers and ensuring consistent
    /// reads across virtual sublogs and keys.
    /// </summary>
    /// <param name="currentVersion"></param>
    /// <param name="appendOnlyFile"></param>
    /// <param name="serverOptions"></param>
    public class ReadConsistencyManager(long currentVersion, GarnetAppendOnlyFile appendOnlyFile, GarnetServerOptions serverOptions)
    {
        /// <summary>
        /// Read consistency manager version.
        /// </summary>
        public long CurrentVersion { get; private set; } = currentVersion;
        readonly GarnetServerOptions serverOptions = serverOptions;

        /// <summary>
        /// Maximum total time in milliseconds the consistent read wait may block before throwing.
        /// </summary>
        readonly int replicaSyncTimeoutMs = (int)serverOptions.ReplicaSyncTimeout.TotalMilliseconds;

        /// <summary>
        /// Reader spin budget before parking, in Stopwatch ticks: -1 spins forever, 0 parks
        /// immediately, &gt; 0 spins up to the budget (AofReaderSpinUs) and then parks.
        /// </summary>
        readonly long readerSpinTicks =
            serverOptions.AofReaderSpinUs < 0 ? -1 : serverOptions.AofReaderSpinUs * Stopwatch.Frequency / 1_000_000;

        readonly VirtualSublogReplayState[] vsrs = [.. Enumerable.Range(0, serverOptions.AofVirtualSublogCount).Select(virtualSublogIdx => new VirtualSublogReplayState(appendOnlyFile.Log.physicalSublogShift + appendOnlyFile.Log.replayTaskShift, serverOptions, virtualSublogIdx))];

        /// <summary>
        /// Maximum allowed drift (in sequence-number units) between leading and trailing sublog
        /// before a replay-side synchronization barrier round is triggered. -1 disables the
        /// barrier so no round is ever activated.
        /// </summary>
        readonly long replayDriftThreshold = serverOptions.AofReplayDriftThreshold;

        /// <summary>
        /// Whether replay drift is bounded at all: false when the barrier is disabled
        /// (threshold -1) or there is a single virtual sublog (no cross-sublog drift to bound).
        /// </summary>
        readonly bool driftBoundingEnabled = serverOptions.AofReplayDriftThreshold >= 0 && serverOptions.AofVirtualSublogCount > 1;

        /// <summary>
        /// Interval, in sequence-number units, between two consecutive drift scans by the same
        /// virtual sublog: window length (AofReplayDriftCheckFreq x AofReplayDriftThreshold)
        /// x virtual sublog count. The shared sequence-number timeline is divided into windows,
        /// each scanned by exactly one replay thread (window index mod sublog count), so the
        /// system-wide scan spacing is one window while each sublog checks once per this
        /// interval. Drift is thereby bounded proactively rather than only when a reader is
        /// about to wait (most reads never wait, so drift could otherwise accumulate unchecked
        /// between waits and hurt read tail latency). The Math.Max keeps the interval positive
        /// for threshold 0 (a legal setting where any drift fires).
        /// </summary>
        readonly long replayDriftCheckInterval =
            Math.Max(1, (long)serverOptions.AofReplayDriftCheckFreq * serverOptions.AofReplayDriftThreshold) * serverOptions.AofVirtualSublogCount;

        /// <summary>
        /// Cooperative barrier used to bound inter-virtual-sublog replay drift. The reader activates it
        /// on demand when it observes a large drift while about to wait; replay threads align on it via
        /// per-record CheckAndWait calls. One participant per virtual sublog (one replay thread each).
        /// </summary>
        public readonly ReplayAlignBarrier replayBarrier = new(serverOptions.AofVirtualSublogCount, serverOptions.AofBarrierSpinUs);

        /// <summary>
        /// Get sequence number for provided key: the key's sketch entry, or with
        /// <paramref name="frontier"/> the published max sequence number of the key's sublog.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="frontier"></param>
        /// <returns></returns>
        public long GetKeySequenceNumber(ReadOnlySpan<byte> key, bool frontier = false)
        {
            var hash = GarnetLog.HASH(key);
            return frontier ? GetSublogMaxSequenceNumber(hash) : GetKeySequenceNumber(hash);
        }

        /// <summary>
        /// Get snapshot of maximum replayed timestamp for all physical sublogs
        /// </summary>
        /// <returns></returns>
        public AofAddress GetPhysicalSublogMaxReplayedSequenceNumber()
        {
            var physicalSublogCount = serverOptions.AofPhysicalSublogCount;
            var replayTaskCount = serverOptions.AofReplayTaskCount;
            var maxKeySeqNumVector = AofAddress.Create(physicalSublogCount, 0);
            for (var physicalSublogIdx = 0; physicalSublogIdx < physicalSublogCount; physicalSublogIdx++)
            {
                for (var rt = 0; rt < replayTaskCount; rt++)
                    maxKeySeqNumVector[physicalSublogIdx] = Math.Max(maxKeySeqNumVector[physicalSublogIdx], vsrs[appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, rt)].Max);
            }
            return maxKeySeqNumVector;
        }

        /// <summary>
        /// Get the published max sequence number of the sublog the hash maps to.
        /// </summary>
        /// <param name="keyHash"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long GetSublogMaxSequenceNumber(long keyHash)
            => vsrs[appendOnlyFile.Log.GetVirtualSublogIdx(keyHash)].Max;

        /// <summary>
        /// Get key specific sequence number for provided hash
        /// </summary>
        /// <param name="keyHash"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long GetKeySequenceNumber(long keyHash)
            => vsrs[appendOnlyFile.Log.GetVirtualSublogIdx(keyHash)].GetKeySequenceNumber(keyHash);

        /// <summary>
        /// Update physical sublog max sequence number
        /// </summary>
        /// <param name="physicalSublogIdx"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdatePhysicalSublogMaxSequenceNumber(int physicalSublogIdx, long sequenceNumber)
        {
            var replayTaskCount = serverOptions.AofReplayTaskCount;
            // Update virtual sublog maximum value for all virtual sublogs
            for (var rt = 0; rt < replayTaskCount; rt++)
                vsrs[appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, rt)].UpdateMaxSequenceNumber(sequenceNumber);
        }

        /// <summary>
        /// Update max sequence number of virtual sublog associated with the specified virtual sublogIdx.
        /// </summary>
        /// <param name="virtualSublogIdx"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdateVirtualSublogMaxSequenceNumber(int virtualSublogIdx, long sequenceNumber)
            => vsrs[virtualSublogIdx].UpdateMaxSequenceNumber(sequenceNumber);
        /// <summary>
        /// Update key sequence number when both the virtual sublog index and key hash are already known.
        /// Caller must guarantee virtualSublogIdx == appendOnlyFile.Log.GetVirtualSublogIdx(keyHash).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpdateVirtualSublogKeySequenceNumber(int virtualSublogIdx, long keyHash, long sequenceNumber)
        {
            vsrs[virtualSublogIdx].UpdateMaxSequenceNumber(sequenceNumber);

            // Replay-driven drift bounding: scan when this sublog's replay enters a timeline window it
            // owns (see replayDriftCheckInterval) and arm a barrier round at the leading frontier when
            // the drift exceeds the threshold.
            if (sequenceNumber >= vsrs[virtualSublogIdx].NextDriftCheckSequenceNumber)
            {
                // Whole-interval advances keep the boundary on this sublog's owned windows.
                var next = vsrs[virtualSublogIdx].NextDriftCheckSequenceNumber + replayDriftCheckInterval;
                if (next <= sequenceNumber)
                {
                    // First record, or fell a full interval behind (e.g. parked at a barrier
                    // round): jump to the first owned boundary past the record.
                    next += ((sequenceNumber - next) / replayDriftCheckInterval + 1) * replayDriftCheckInterval;
                }
                vsrs[virtualSublogIdx].NextDriftCheckSequenceNumber = next;
                BoundReplayDrift();
            }

            // Park this replay thread while it leads an active round, bounding drift from the lagging
            // sublogs. This runs BEFORE the sketch entry is published below: while parked, the key's
            // sketch entry still holds its previous value, so a reader that touches this key advances
            // its session sequence number only to that previous value, never to the just-published
            // frontier. The advance is thereby deferred until the lagging sublogs converge toward the
            // frontier, at which point it no longer forces cross-sublog reads to wait. Fast path is a
            // single Volatile.Read + compare when no round is active.
            replayBarrier.CheckAndWait(vsrs[virtualSublogIdx].Max);

            // Publish the key's sketch entry. The caller applies the store mutation after this
            // returns, so the new value never becomes visible before its sketch entry.
            vsrs[virtualSublogIdx].UpdateKeySequenceNumber(keyHash, sequenceNumber);
        }

        /// <summary>
        /// Update key sequence number of virtual sublog associated with the specified keyHash.
        /// </summary>
        /// <param name="keyHash"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdateVirtualSublogKeySequenceNumber(long keyHash, long sequenceNumber)
            => UpdateVirtualSublogKeySequenceNumber(appendOnlyFile.Log.GetVirtualSublogIdx(keyHash), keyHash, sequenceNumber);

        // Cold reset path: runs only on first read and on a version change (replica re-attach).
        // Split out of the per-read version check so that check stays tiny.
        void ResetSessionContext(ref ReplicaReadSessionContext replicaReadSessionContext)
        {
            replicaReadSessionContext.sessionVersion = CurrentVersion;
            replicaReadSessionContext.lastVirtualSublogIdx = -1;
            replicaReadSessionContext.maximumSessionSequenceNumber = 0;
            if (replicaReadSessionContext.cachedSublogMax == null)
                replicaReadSessionContext.cachedSublogMax = new long[serverOptions.AofVirtualSublogCount];
            else
                Array.Clear(replicaReadSessionContext.cachedSublogMax);
        }

        /// <summary>
        /// Synchronize the session context with the current manager version, resetting it on first use
        /// or on a version change (replica re-attach). Entry point for the batch read path; the
        /// single-key path (<see cref="BeforeConsistentReadKey"/>) inlines this same check directly.
        /// </summary>
        /// <param name="replicaReadSessionContext">A reference to the session context to check and update.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CheckConsistencyManagerVersion(ref ReplicaReadSessionContext replicaReadSessionContext)
        {
            if (replicaReadSessionContext.sessionVersion != CurrentVersion)
                ResetSessionContext(ref replicaReadSessionContext);
        }

        /// <summary>
        /// Verify key freshness before allowing reads.
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="replicaReadSessionContext"></param>
        /// <param name="waiter">Session-owned reusable wakeup primitive; replaced with a fresh
        /// instance by <see cref="VirtualSublogReplayState.WaitForSequenceNumber"/> on cancel/timeout.</param>
        /// <param name="ct">Cancellation token that aborts the wait when the session ends.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void VerifyKeyFreshness(long hash, ref ReplicaReadSessionContext replicaReadSessionContext,
                                ref ConsistentReadWaiter waiter, CancellationToken ct)
        {
            var virtualSublogIdx = (short)appendOnlyFile.Log.GetVirtualSublogIdx(hash);

            // Prefetch the key's sketch slot for later post-read update (AfterConsistentReadKey)
            vsrs[virtualSublogIdx].PrefetchKeySequenceNumber(hash);

            // Wait for replay to catch up only when reading a different sublog than the last read:
            // consecutive same-sublog reads are prefix-consistent by construction.
            if (replicaReadSessionContext.lastVirtualSublogIdx != -1 && replicaReadSessionContext.lastVirtualSublogIdx != virtualSublogIdx
                && replicaReadSessionContext.maximumSessionSequenceNumber >= replicaReadSessionContext.cachedSublogMax[virtualSublogIdx])
            {
                RefreshAndMaybeWait(virtualSublogIdx, hash, ref replicaReadSessionContext, ref waiter, ct);
            }

            // Store for future update
            replicaReadSessionContext.lastVirtualSublogIdx = virtualSublogIdx;
            replicaReadSessionContext.lastHash = hash;
        }

        // Cold path of VerifyKeyFreshness: the session has read past its cached view of this sublog's
        // published max, so refresh from the live value and block until replay catches up if even the
        // refreshed value is behind. Kept out of line so the no-wait fast path inlines.
        void RefreshAndMaybeWait(short virtualSublogIdx, long hash, ref ReplicaReadSessionContext replicaReadSessionContext,
                                 ref ConsistentReadWaiter waiter, CancellationToken ct)
        {
            var maxSessionSeqNum = replicaReadSessionContext.maximumSessionSequenceNumber;
            var publishedMax = vsrs[virtualSublogIdx].Max;
            replicaReadSessionContext.cachedSublogMax[virtualSublogIdx] = publishedMax;
            if (maxSessionSeqNum >= publishedMax)
            {
                // About to wait. If the replay-side drift is large enough to be worth bounding, install a barrier round
                BoundReplayDrift();
                if (SpinForSublogMax(virtualSublogIdx, maxSessionSeqNum, ref replicaReadSessionContext, ct))
                    return;
                vsrs[virtualSublogIdx].WaitForSequenceNumber(maxSessionSeqNum, ref waiter, ct, replicaSyncTimeoutMs);
            }
        }

        /// <summary>
        /// Spin-poll the sublog's published max for up to the reader spin budget instead of
        /// parking; returns true once it passes the session's sequence number. A spinning
        /// reader enqueues no waiter, so the replay thread's per-record waiter-signal pass
        /// stays on its lock-free empty fast path (no wake train) while readers wait.
        /// </summary>
        bool SpinForSublogMax(short virtualSublogIdx, long maxSessionSeqNum,
                              ref ReplicaReadSessionContext replicaReadSessionContext, CancellationToken ct)
        {
            if (readerSpinTicks == 0)
                return false;
            var deadline = readerSpinTicks > 0 ? Stopwatch.GetTimestamp() + readerSpinTicks : long.MaxValue;
            var spins = 0;
            while (true)
            {
                Thread.SpinWait(32);
                var publishedMax = vsrs[virtualSublogIdx].Max;
                if (maxSessionSeqNum < publishedMax)
                {
                    replicaReadSessionContext.cachedSublogMax[virtualSublogIdx] = publishedMax;
                    return true;
                }
                // Deadline and cancellation are polled coarsely so the hot poll loop stays a
                // single shared read; an unbounded spin still observes session teardown.
                if ((++spins & 0xFF) == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    if (Stopwatch.GetTimestamp() >= deadline)
                        return false;
                }
            }
        }

        /// <summary>
        /// Scan all virtual sublogs' current max sequence numbers; if the spread exceeds
        /// <see cref="replayDriftThreshold"/>, install a barrier round at the leader's value so that
        /// replayers pause once they reach it and the laggards have time to catch up.
        /// Invoked from two rare paths that dedupe on the active round: a reader about to wait
        /// (<see cref="RefreshAndMaybeWait"/>) and a replay thread crossing its progress gate
        /// (<see cref="UpdateVirtualSublogKeySequenceNumber(int, long, long)"/>).
        /// </summary>
        void BoundReplayDrift()
        {
            if (!driftBoundingEnabled) return;
            // A round already in progress is bounding the drift; a disabled barrier also reports an
            // in-progress round (one that never completes), so the scan is skipped in both cases.
            if (replayBarrier.IsActive) return;

            var virtualSublogCount = serverOptions.AofVirtualSublogCount;
            long minFrontier = long.MaxValue, maxFrontier = long.MinValue;
            for (var v = 0; v < virtualSublogCount; v++)
            {
                var frontier = vsrs[v].Max;
                if (frontier < minFrontier) minFrontier = frontier;
                if (frontier > maxFrontier) maxFrontier = frontier;
            }
            if (maxFrontier - minFrontier <= replayDriftThreshold) return;
            // A sublog that has published no progress at all has no record stream yet (e.g. its
            // replay session is still being established during attach). Barrier arrivals happen only
            // on the per-record replay path, so a round could not complete until that sublog both
            // starts and reaches the target; firing now would only park every started replay thread.
            // Skip until all sublogs have progress.
            if (minFrontier == 0) return;
            replayBarrier.TryActivate(maxFrontier);
        }

        /// <summary>
        /// This method implements part of the consistent read protocol for a single key when shared AOF is enabled.
        /// NOTE:
        ///     This method waits until the log sequence number of the associated key is lesser or equal than the maximum session log sequence number.
        ///     It executes before store.Read is processed to ensure that the log sequence number of the associated key is ahead of the last read in accordance to the consistent read protocol
        ///     The replica read context is updated (<seealso cref="T:Garnet.server.ReplicaReadConsistencyManager.ConsistentReadSequenceNumberUpdate"/>) after the actual store.Read call to ensure that we don't underestimate the true log sequence number.
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="replicaReadSessionContext"></param>
        /// <param name="waiter">Session-owned reusable wakeup primitive.</param>
        /// <param name="ct">Cancellation token that aborts the wait when the session ends.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void BeforeConsistentReadKey(long hash, ref ReplicaReadSessionContext replicaReadSessionContext,
                                            ref ConsistentReadWaiter waiter, CancellationToken ct)
        {
            if (replicaReadSessionContext.sessionVersion != CurrentVersion)
                ResetSessionContext(ref replicaReadSessionContext);

            // Verify key freshness
            VerifyKeyFreshness(hash, ref replicaReadSessionContext, ref waiter, ct);
        }

        /// <summary>
        /// This method implements part of the consistent read protocol for a single key when shared AOF is enabled.
        /// NOTE:
        ///     This method is used to update the log sequence number after store.Read was processed.
        ///     This is done to ensure that the log sequence number tracked by the ReadConsistencyManager is an overestimate of the actual sequence number since
        ///     we cannot be certain at prepare phase what is the actual sequence number.
        /// </summary>
        /// <param name="replicaReadSessionContext"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AfterConsistentReadKey(ref ReplicaReadSessionContext replicaReadSessionContext)
        {
            var keySequenceNumber = vsrs[replicaReadSessionContext.lastVirtualSublogIdx].GetKeySequenceNumber(replicaReadSessionContext.lastHash);
            replicaReadSessionContext.maximumSessionSequenceNumber = Math.Max(
                replicaReadSessionContext.maximumSessionSequenceNumber, keySequenceNumber);
        }

        /// <summary>
        /// Verify key freshness and keep track hash and maximum session sequence number to check for updates after batch read.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="batchReadContext"></param>
        /// <param name="waiter">Session-owned reusable wakeup primitive.</param>
        /// <param name="ct">Cancellation token that aborts the wait when the session ends.</param>
        /// <param name="hash"></param>
        public void BeforeConsistentReadKeyBatch(ReadOnlySpan<byte> key, ref ReplicaReadSessionContext batchReadContext,
                                                 ref ConsistentReadWaiter waiter, CancellationToken ct, out long hash)
        {
            // Verify key freshness
            hash = GarnetLog.HASH(key);
            VerifyKeyFreshness(hash, ref batchReadContext, ref waiter, ct);

            // Keep track of max sequence number to check for updates after batch read.
            batchReadContext.maximumSessionSequenceNumber = Math.Max(
                batchReadContext.maximumSessionSequenceNumber, GetKeySequenceNumber(batchReadContext.lastHash));
        }

        /// <summary>
        /// Validate that key sequence number has not progressed beyond the snapshot used for batch key read.
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="batchReadContext"></param>
        /// <returns></returns>
        public bool AfterConsistentReadKeyBatch(long hash, ref ReplicaReadSessionContext batchReadContext)
        {
            var keySequenceNumber = GetKeySequenceNumber(hash);
            var mSSN = batchReadContext.maximumSessionSequenceNumber;
            // NOTE: Read key batch is prefix consistent at boundary because maximumSessionSequenceNumber (mSSN) == maxof(batch key sequence numbers)
            // and freshness check would have prevented boundary read of the corresponding key.
            // In other words, T_k (timestamp of key k) < T_f (frontier timestamp where read was allowed to proceed) and because mSSN == max of all T_k in the batch
            // mSSN < T_f, hence time has advanced beyond the point where it is safe to read.
            return keySequenceNumber <= mSSN;
        }
    }
}