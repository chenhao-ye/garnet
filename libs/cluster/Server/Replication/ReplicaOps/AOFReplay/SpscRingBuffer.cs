// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Garnet.cluster
{
    /// <summary>
    /// Single-producer, single-consumer lock-free ring buffer for ReplayRecord.
    /// Uses spinning for both producer and consumer to minimize wakeup overhead.
    /// Head and tail are placed on separate cache lines to avoid false sharing.
    ///
    /// The producer batches tail updates: the visible tail is advanced only once per
    /// cache-line-worth of writes (8 records × 8 bytes = 64 bytes). This reduces
    /// cross-core coherence traffic on the tail cache line by 8×. Call Flush() to
    /// publish any partial trailing batch (e.g., before enqueuing a sentinel).
    /// </summary>
    internal sealed class SpscRingBuffer
    {
        const int CacheLineSize = 64;

        // Number of records per cache line: 64 bytes / 8 bytes (sizeof ReplayRecord = pointer only) = 8.
        const int TailBatchSize = 8;
        const int TailBatchMask = TailBatchSize - 1;

        // Cache-line-padded counters to prevent false sharing between producer and consumer.
        [StructLayout(LayoutKind.Explicit, Size = CacheLineSize)]
        struct PaddedLong
        {
            [FieldOffset(0)]
            public long Value;
        }

        PaddedLong head;  // consumer side — written by consumer, read by producer
        PaddedLong tail;  // shared — written by producer in batches, read by consumer

        // Producer's private write cursor. Updated every record; tail.Value lags behind
        // by up to TailBatchSize - 1 records and is flushed on batch boundary or Flush().
        // Lives on its own cache line (after the two PaddedLongs) — producer-private.
        long internalTail;

        readonly int mask;
        readonly ReplayRecord[] buffer;

        /// <summary>
        /// Creates a new SPSC ring buffer with the given capacity (rounded up to power of 2).
        /// </summary>
        public SpscRingBuffer(int capacity = 1024)
        {
            capacity = RoundUpPowerOf2(capacity);
            mask = capacity - 1;
            buffer = new ReplayRecord[capacity];
            head.Value = 0;
            tail.Value = 0;
            internalTail = 0;
        }

        /// <summary>
        /// Number of records visible to the consumer (may lag behind internalTail by up to TailBatchSize - 1).
        /// </summary>
        public int Count
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)(Volatile.Read(ref tail.Value) - Volatile.Read(ref head.Value));
        }

        /// <summary>
        /// Enqueue a record. Spins if the buffer is full.
        /// Advances the visible tail every TailBatchSize records; call Flush() for partial batches.
        /// Must be called from the single producer thread only.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Enqueue(ReplayRecord item)
        {
            var localTail = internalTail;
            // Spin until there is space (capacity check against internal write cursor)
            while (localTail - Volatile.Read(ref head.Value) >= buffer.Length)
                Thread.SpinWait(1);

            buffer[localTail & mask] = item;
            localTail++;
            internalTail = localTail;

            // Publish to the consumer every TailBatchSize records (one cache line of 8-byte pointers)
            if ((localTail & TailBatchMask) == 0)
                Volatile.Write(ref tail.Value, localTail);
        }

        /// <summary>
        /// Publishes any buffered (not yet visible) records to the consumer immediately.
        /// Must be called by the producer after a partial batch — in particular, before
        /// enqueuing a sentinel so the consumer sees all preceding records.
        /// Must be called from the single producer thread only.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Flush()
        {
            var localTail = internalTail;
            if (tail.Value != localTail)
                Volatile.Write(ref tail.Value, localTail);
        }

        /// <summary>
        /// Try to dequeue a record without blocking.
        /// Must be called from the single consumer thread only.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue(out ReplayRecord item)
        {
            var localHead = head.Value;
            if (localHead == Volatile.Read(ref tail.Value))
            {
                item = default;
                return false;
            }

            item = buffer[localHead & mask];
            Volatile.Write(ref head.Value, localHead + 1);
            return true;
        }

        /// <summary>
        /// Spin-wait until at least one item is available, then dequeue it.
        /// Must be called from the single consumer thread only.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReplayRecord SpinDequeue()
        {
            var localHead = head.Value;
            while (localHead == Volatile.Read(ref tail.Value))
                Thread.SpinWait(1);

            var item = buffer[localHead & mask];
            Volatile.Write(ref head.Value, localHead + 1);
            return item;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static int RoundUpPowerOf2(int v)
        {
            v--;
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            return v + 1;
        }
    }
}
