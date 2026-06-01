// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        readonly SemaphoreSlim updateSignal = new(0);
        int waiterCount;

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
            SignalAdvanceTime();
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
            _ = Utility.MonotonicUpdate(ref sketchMax.value, sequenceNumber, out _);
            SignalAdvanceTime();
        }

        /// <summary>
        /// Signals that time should advance, allowing any awaiting operations to proceed.
        /// </summary>
        void SignalAdvanceTime()
        {
            if (Volatile.Read(ref waiterCount) == 0)
                return;

            int releaseCount;
            lock (@lock)
            {
                releaseCount = waiterCount;
            }

            if (releaseCount > 0)
                updateSignal.Release(releaseCount);
        }

        /// <summary>
        /// Waits until the session's frontier sequence number for the specified hash reaches or exceeds
        /// the given maximum sequence number.
        /// </summary>
        /// <param name="hash">The hash value identifying the session whose sequence number is being monitored.</param>
        /// <param name="maximumSessionSequenceNumber">The target sequence number to wait for.</param>
        /// <param name="ct">Cancellation token that aborts the wait when signaled.</param>
        /// <param name="timeoutMs">Maximum time in milliseconds to wait for a single broadcast wakeup.</param>
        /// <exception cref="OperationCanceledException">Thrown when ct is canceled or when an iteration times out.</exception>
        public void WaitForSequenceNumber(long hash, long maximumSessionSequenceNumber, CancellationToken ct, int timeoutMs)
        {
            while (true)
            {
                lock (@lock)
                {
                    if (maximumSessionSequenceNumber < GetFrontierSequenceNumber(hash))
                        return;

                    waiterCount++;
                }

                try
                {
                    if (!updateSignal.Wait(timeoutMs, ct))
                        throw new OperationCanceledException($"{nameof(WaitForSequenceNumber)} timed out after {timeoutMs}ms");
                }
                finally
                {
                    lock (@lock)
                    {
                        waiterCount--;
                    }
                }
            }
        }
    }
}