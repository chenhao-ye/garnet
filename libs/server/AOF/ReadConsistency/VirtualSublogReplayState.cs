// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;
using System.Threading;
using Tsavorite.core;

namespace Garnet.server
{
    internal struct VirtualSublogReplayState
    {
        const int SketchSlotSize = 1 << 15;
        const int SketchSlotMask = SketchSlotSize - 1;

        readonly int sketchShift;  // physicalSublogShift + replayTaskShift

        readonly long[] sketch = new long[SketchSlotSize];

        // The published sublog max sequence number, written by the replay thread on every record (and
        // on time-advance) and read by the reader on the frontier check, so it is true-shared and
        // bounces between cores. It lives in its own cache-line-isolated object so its writes never
        // invalidate the reader's immutable sketch / sketchShift fields.
        readonly PublishedMax sketchMax = new();

        readonly object @lock = new();

        // Min-heap of pending readers keyed by their target sequence number. Mutated only under @lock.
        // The replay thread pops and signals the prefix of waiters whose target the published max has
        // crossed; cancelled/timed-out waiters are tombstoned via ConsistentReadWaiter.Cancelled and
        // dropped lazily during the next signal pass.
        readonly PriorityQueue<ConsistentReadWaiter, long> waitQueue = new();

        // Mirror of waitQueue's head priority (or long.MaxValue if empty). Updated under @lock when
        // the queue changes; volatile-readable from the replay thread for the lock-free fast-path skip.
        // May briefly point at a cancelled tombstone after a timeout/ct; the next signal pass cleans it.
        long minWaiterTarget = long.MaxValue;

        // Cache-line-isolated cell for the published sketch max
        [StructLayout(LayoutKind.Explicit, Size = 128)]
        sealed class PublishedMax
        {
            [FieldOffset(64)] public long value;
        }

        public readonly long Max => sketchMax.value;

        public VirtualSublogReplayState(int sketchShift)
        {
            var size = SketchSlotSize;
            if ((size & (size - 1)) != 0)
                throw new InvalidOperationException($"Size ({SketchSlotSize}) must be a power of 2");
            Array.Clear(sketch);
            sketchMax.value = 0;
            this.sketchShift = sketchShift;
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
            => Math.Max(sketch[GetSketchSlot(hash)], sketchMax.value);

        /// <summary>
        /// Gets the sequence number associated with the specified hash key.
        /// </summary>
        /// <param name="hash">The hash value for which to retrieve the sequence number.</param>
        /// <returns>The sequence number corresponding to the given hash key.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetKeySequenceNumber(long hash)
            => sketch[GetSketchSlot(hash)];

        /// <summary>
        /// Issues a temporal prefetch of the sketch slot for the given hash so the post-read update
        /// finds it resident. The replay thread writes this slot, so an uncached read of it is a
        /// cross-core coherence miss on the post-read critical path; prefetching here overlaps that
        /// miss with the store read.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly unsafe void PrefetchKeySequenceNumber(long hash)
        {
            if (Sse.IsSupported)
                Sse.Prefetch0(Unsafe.AsPointer(ref sketch[GetSketchSlot(hash)]));
        }

        /// <summary>
        /// Updates the maximum observed sequence number.
        /// </summary>
        /// <remarks>Updates are thread-safe and guaranteed to be monotonically increasing.</remarks>
        /// <param name="sequenceNumber">The sequence number to compare against the current maximum.</param>
        public void UpdateMaxSequenceNumber(long sequenceNumber)
        {
            _ = Utility.MonotonicUpdate(ref sketchMax.value, sequenceNumber, out _);
            SignalIfFrontierAdvanced();
        }

        /// <summary>
        /// Updates the sequence number associated with the specified key hash.
        /// </summary>
        /// <remarks>Updates are thread-safe and guaranteed to be monotonically increasing.</remarks>
        /// <param name="hash">The hash value identifying the key whose sequence number is to be updated.</param>
        /// <param name="sequenceNumber">The new sequence number to associate with the specified key hash. Must be greater than or equal to the
        /// current value to have an effect.</param>
        public void UpdateKeySequenceNumber(long hash, long sequenceNumber)
        {
            _ = Utility.MonotonicUpdate(ref sketch[GetSketchSlot(hash)], sequenceNumber, out _);
            // Publish the sublog max on every record. The reader's frontier is max(sketch[slot],
            // sketchMax.value); sketchMax.value is advanced here in lockstep with the slot, so it is
            // always >= any slot value. A waiter's release condition therefore reduces to the published
            // max crossing its target (see SignalIfFrontierAdvanced) with no per-slot lag. The published
            // max lives on its own cache line, so this per-record write does not invalidate the reader's
            // immutable sketch / sketchShift fields.
            _ = Utility.MonotonicUpdate(ref sketchMax.value, sequenceNumber, out _);
            SignalIfFrontierAdvanced();
        }

        /// <summary>
        /// Pops and signals the prefix of waiters whose target sequence number has been crossed by the
        /// current published max. The fast path (no waiters, or the smallest target still ahead of the
        /// published max) is lock-free.
        /// </summary>
        void SignalIfFrontierAdvanced()
        {
            // Fast path: smallest live target is still ahead of the published max (or the queue is
            // empty, in which case minWaiterTarget is long.MaxValue).
            if (Volatile.Read(ref sketchMax.value) <= Volatile.Read(ref minWaiterTarget))
                return;

            lock (@lock)
            {
                long maxVal = sketchMax.value;
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
                // Re-check under @lock -- atomic with Enqueue so a signal cannot race in between.
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
