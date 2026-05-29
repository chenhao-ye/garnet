// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Tsavorite.core;

namespace Garnet.server
{
    internal struct VirtualSublogReplayState
    {
        const int SketchSlotSize = 1 << 15;
        const int SketchSlotMask = SketchSlotSize - 1;

        // Records the replay thread applies between flushes of its private running max to the shared
        // sketchMaxValue. Batching the flush keeps the reader-read sketchMaxValue cache line stable
        // between flushes instead of being invalidated on every replayed record. Configured via
        // serverOptions.AofReplayFlushFreq (1 = flush every record, i.e. no batching).
        readonly int flushFreq;

        readonly int sketchShift;  // physicalSublogShift + replayTaskShift

        readonly long[] sketch = new long[SketchSlotSize];
        long sketchMaxValue;
        readonly object @lock = new();

        // Min-heap of pending readers keyed by their target sequence number. Mutated only under @lock.
        // The replay thread pops and signals the prefix of waiters whose target sketchMaxValue has
        // crossed; cancelled/timed-out waiters are tombstoned via ConsistentReadWaiter.Cancelled and
        // dropped lazily during the next signal pass.
        readonly PriorityQueue<ConsistentReadWaiter, long> waitQueue = new();

        // Mirror of waitQueue's head priority (or long.MaxValue if empty). Updated under @lock when
        // the queue changes; volatile-readable from the replay thread for the lock-free fast-path skip.
        // May briefly point at a cancelled tombstone after a timeout/ct; the next signal pass cleans it.
        long minWaiterTarget = long.MaxValue;

        // Per-sublog single-writer accumulator, separately allocated so the replay thread's per-record
        // writes to it do not invalidate the cache line holding sketchMaxValue (which the reader reads).
        readonly WriterState writer = new();

        // Padded to 128 bytes with the data fields placed in the middle so that adjacent heap
        // allocations (e.g. neighbouring sublogs' WriterStates) cannot share the cache line holding
        // localMax/flushCounter -- preventing inter-replay-thread false sharing. 128 bytes also
        // covers the x86 adjacent-line prefetcher; matches the BCL false-sharing-padding convention.
        [StructLayout(LayoutKind.Explicit, Size = 128)]
        sealed class WriterState
        {
            [FieldOffset(64)] public long localMax;
            [FieldOffset(72)] public int flushCounter;
        }

        // True running max: the flushed value or the not-yet-flushed accumulator, whichever is larger.
        // The hot reader path reads the sketchMaxValue field directly (GetFrontierSequenceNumber); this
        // property is used only off the hot path (drift detection, recovery), where the un-flushed max
        // must remain visible for correctness.
        public readonly long Max => Math.Max(sketchMaxValue, writer.localMax);

        public VirtualSublogReplayState(int sketchShift, int flushFreq)
        {
            var size = SketchSlotSize;
            if ((size & (size - 1)) != 0)
                throw new InvalidOperationException($"Size ({SketchSlotSize}) must be a power of 2");
            Array.Clear(sketch);
            sketchMaxValue = 0;
            this.sketchShift = sketchShift;
            this.flushFreq = flushFreq;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        readonly int GetSketchSlot(long hash) => (int)(((ulong)hash >> sketchShift) & SketchSlotMask);

        /// <summary>
        /// Gets the current frontier sequence number associated with the specified hash value.
        /// </summary>
        /// <param name="hash">The hash value for which to retrieve the frontier sequence number.</param>
        /// <returns>The frontier sequence number corresponding to the specified hash value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetFrontierSequenceNumber(long hash)
            => Math.Max(sketch[GetSketchSlot(hash)], sketchMaxValue);

        /// <summary>
        /// Gets the sequence number associated with the specified hash key.
        /// </summary>
        /// <param name="hash">The hash value for which to retrieve the sequence number.</param>
        /// <returns>The sequence number corresponding to the given hash key.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetKeySequenceNumber(long hash)
            => sketch[GetSketchSlot(hash)];

        /// <summary>
        /// Updates the maximum observed sequence number.
        /// </summary>
        /// <remarks>Updates are thread-safe and guaranteed to be monotonically increasing.</remarks>
        /// <param name="sequenceNumber">The sequence number to compare against the current maximum.</param>
        public void UpdateMaxSequenceNumber(long sequenceNumber)
        {
            _ = Utility.MonotonicUpdate(ref sketchMaxValue, sequenceNumber, out _);
            SignalIfFrontierAdvanced();
        }

        /// <summary>
        /// Updates the sequence number associated with the specified key hash.
        /// Returns true if this update crossed a flush boundary (sketchMaxValue was advanced and any
        /// pending readers were signaled). The caller uses the flush-boundary signal to amortize
        /// expensive cross-thread coordination (barrier checks, reader wakeups) to roughly once
        /// per <c>flushFreq</c> records.
        /// </summary>
        /// <remarks>Updates are thread-safe and guaranteed to be monotonically increasing.</remarks>
        /// <param name="hash">The hash value identifying the key whose sequence number is to be updated.</param>
        /// <param name="sequenceNumber">The new sequence number to associate with the specified key hash. Must be greater than or equal to the
        /// current value to have an effect.</param>
        public bool UpdateKeySequenceNumber(long hash, long sequenceNumber)
        {
            _ = Utility.MonotonicUpdate(ref sketch[GetSketchSlot(hash)], sequenceNumber, out _);
            // Accumulate the sublog max in the private writer state and flush it to the shared
            // sketchMaxValue only every FlushFreq records. One replay thread per sublog, so the
            // accumulator needs no synchronization; the flush is atomic to coexist with
            // UpdateMaxSequenceNumber (e.g. a time-advance on this sublog).
            if (sequenceNumber > writer.localMax)
                writer.localMax = sequenceNumber;
            if (++writer.flushCounter >= flushFreq)
            {
                writer.flushCounter = 0;
                _ = Utility.MonotonicUpdate(ref sketchMaxValue, writer.localMax, out _);
                // Only signal at flush boundaries: between flushes sketchMaxValue is constant and the
                // gate would skip anyway. A slot-specific satisfaction (a record whose sketch[slot] write
                // alone would unblock a waiter on that slot) is therefore delayed by at most flushFreq
                // records, bounded by replicaSyncTimeoutMs.
                SignalIfFrontierAdvanced();
                return true;
            }
            return false;
        }

        /// <summary>
        /// Pops and signals the prefix of waiters whose target sequence number has been crossed by
        /// the current sketchMaxValue. The fast path (no waiters or smallest target still ahead of
        /// sketchMaxValue) is lock-free.
        /// </summary>
        void SignalIfFrontierAdvanced()
        {
            // Fast path: smallest live target is still ahead of sketchMaxValue (or the queue is empty,
            // in which case minWaiterTarget is long.MaxValue).
            if (Volatile.Read(ref sketchMaxValue) <= Volatile.Read(ref minWaiterTarget))
                return;

            lock (@lock)
            {
                long maxVal = sketchMaxValue;
                while (waitQueue.TryPeek(out var top, out var target))
                {
                    // Tombstones (cancelled/timed-out waiters) are dropped unconditionally.
                    if (top.Cancelled)
                    {
                        waitQueue.Dequeue();
                        continue;
                    }
                    // Min-heap ordering: once we see an unsatisfied target, all remaining targets are larger.
                    if (maxVal <= target)
                        break;
                    waitQueue.Dequeue();
                    top.Event.Set();
                }
                minWaiterTarget = waitQueue.TryPeek(out _, out var t) ? t : long.MaxValue;
            }
        }

        /// <summary>
        /// Waits until the session's frontier sequence number for the specified hash reaches or exceeds
        /// the given maximum sequence number.
        /// </summary>
        /// <param name="hash">The hash value identifying the key whose frontier is being monitored.</param>
        /// <param name="maximumSessionSequenceNumber">The target sequence number to wait for.</param>
        /// <param name="waiter">Caller's session-owned reusable waiter. Must be non-null and not Cancelled
        /// on entry; replaced with a fresh instance if this call cancels or times out.</param>
        /// <param name="ct">Cancellation token that aborts the wait when signaled.</param>
        /// <param name="timeoutMs">Maximum time in milliseconds to wait before throwing.</param>
        /// <exception cref="OperationCanceledException">Thrown when ct is canceled or the wait times out.</exception>
        public void WaitForSequenceNumber(long hash, long maximumSessionSequenceNumber,
                                          ref ConsistentReadWaiter waiter,
                                          CancellationToken ct, int timeoutMs)
        {
            // Reset outside @lock. Safe because the waiter is NOT in waitQueue at this point: previous
            // wait was either signaled (replay Dequeued under @lock before Set) or cancelled (the catch
            // below already replaced the field with a fresh instance). The MRES is owned by this
            // session's single worker thread, so Reset/Wait are sequential here.
            waiter.Event.Reset();

            lock (@lock)
            {
                // Re-check under @lock — atomic with Enqueue so a signal cannot race in between.
                if (maximumSessionSequenceNumber < GetFrontierSequenceNumber(hash))
                    return;
                waitQueue.Enqueue(waiter, maximumSessionSequenceNumber);
                if (maximumSessionSequenceNumber < minWaiterTarget)
                    minWaiterTarget = maximumSessionSequenceNumber;
            }

            try
            {
                if (!waiter.Event.Wait(timeoutMs, ct))
                    throw new OperationCanceledException(
                        $"{nameof(WaitForSequenceNumber)} timed out after {timeoutMs}ms");
                // Successfully signaled. Waiter is already out of the queue (replay Dequeued before Set).
            }
            catch (OperationCanceledException)
            {
                // Retire the cancelled waiter as a tombstone in waitQueue and install a fresh waiter
                // in the session field so the next wait skips the null/cancelled check.
                waiter.Cancelled = true;
                waiter = new ConsistentReadWaiter();
                throw;
            }
        }
    }
}