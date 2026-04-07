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
    /// </summary>
    internal sealed class SpscRingBuffer
    {
        const int CacheLineSize = 64;

        // Cache-line-padded counters to prevent false sharing between producer and consumer.
        [StructLayout(LayoutKind.Explicit, Size = CacheLineSize)]
        struct PaddedLong
        {
            [FieldOffset(0)]
            public long Value;
        }

        PaddedLong head;  // consumer side
        PaddedLong tail;  // producer side

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
        }

        /// <summary>
        /// Number of items currently in the buffer.
        /// </summary>
        public int Count
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)(Volatile.Read(ref tail.Value) - Volatile.Read(ref head.Value));
        }

        /// <summary>
        /// Enqueue a record. Spins if the buffer is full.
        /// Must be called from the single producer thread only.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Enqueue(ReplayRecord item)
        {
            var localTail = tail.Value;
            // Spin until there is space
            while (localTail - Volatile.Read(ref head.Value) >= buffer.Length)
                Thread.SpinWait(1);

            buffer[localTail & mask] = item;
            Volatile.Write(ref tail.Value, localTail + 1);
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
