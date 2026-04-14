// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Garnet.common
{
    // These control structs live outside the generic class because explicit
    // layout is disallowed on types nested inside a generic class. Kept
    // internal to this assembly; do not use outside RingBufferChannel.

    /// <summary>
    /// Producer-owned state. Own cacheline to avoid false sharing with the
    /// consumer. Tail is written by the producer, read by the consumer with
    /// acquire/release semantics.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    internal struct ProducerControl
    {
        /// <summary>
        /// Producer's cached view of the consumer's <see cref="ConsumerControl.Head"/>.
        /// Refreshed only when the ring looks full; lets the producer skip a volatile
        /// read of the consumer's cacheline on the fast path.
        /// </summary>
        [FieldOffset(0)] public long CachedHead;

        /// <summary>
        /// Producer-private write cursor. Advances on every write but is not yet
        /// visible to the consumer. Writes beyond <see cref="Tail"/> are buffered
        /// here until batched publish.
        /// </summary>
        [FieldOffset(8)] public long BufferTail;

        /// <summary>
        /// Published write cursor — the highest index the consumer may read.
        /// Written by the producer with release semantics every batchSize writes
        /// (or on Flush / Complete); read by the consumer with acquire semantics.
        /// </summary>
        [FieldOffset(16)] public long Tail;
    }

    /// <summary>
    /// Consumer-owned state. Own cacheline. Head is written by the consumer,
    /// read by the producer with acquire/release semantics. Completed is
    /// written once by the producer on shutdown.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    internal struct ConsumerControl
    {
        /// <summary>
        /// Read cursor — the next index the consumer will read. Written by the
        /// consumer with release semantics on each successful read; read by the
        /// producer with acquire semantics when checking for free slots.
        /// </summary>
        [FieldOffset(0)] public long Head;

        /// <summary>
        /// Consumer's cached view of the producer's <see cref="ProducerControl.Tail"/>.
        /// Refreshed only when the ring looks empty; lets the consumer skip a volatile
        /// read of the producer's cacheline on the fast path.
        /// </summary>
        [FieldOffset(8)] public long CachedTail;

        /// <summary>
        /// Completion flag. Set once by the producer on shutdown; read by the
        /// consumer to know it may exit after draining. Read as a volatile int
        /// (treated as bool).
        /// </summary>
        [FieldOffset(16)] public int Completed;
    }

    /// <summary>
    /// Single-producer, single-consumer ring buffer parameterized by the slot
    /// record type. Used to hand off small fixed-size records across threads
    /// without per-record atomics.
    ///
    /// Layout is cacheline (64B) aligned. The producer's working state sits on
    /// one cacheline, the consumer's working state on another, and the ring of
    /// entries follows. For optimal cache behavior choose
    /// <c>batchSize * sizeof(TRecord)</c> to be a multiple of 64 (e.g. with an
    /// 8-byte record and batchSize=8, each publish makes exactly one
    /// freshly-written cacheline visible to the consumer).
    ///
    /// The producer batches writes: it only publishes the tail (making prior
    /// writes visible to the consumer) every <c>batchSize</c> writes, or when
    /// <see cref="Flush"/> is called explicitly.
    /// </summary>
    public sealed unsafe class RingBufferChannel<TRecord> where TRecord : unmanaged
    {
        const int CacheLineBytes = 64;

        readonly long[] controlRaw;
        readonly TRecord[] ringRaw;
        readonly ProducerControl* producer;
        readonly ConsumerControl* consumer;
        readonly TRecord* ring;
        readonly int mask;
        readonly int capacity;
        readonly int batchSize;

        public RingBufferChannel(int capacity, int batchSize)
        {
            if (capacity < 64 || (capacity & (capacity - 1)) != 0)
                throw new ArgumentException("capacity must be a power of two and >= 64", nameof(capacity));
            if (batchSize < 1)
                throw new ArgumentOutOfRangeException(nameof(batchSize));

            this.capacity = capacity;
            this.mask = capacity - 1;
            this.batchSize = batchSize;

            // Control: 2 cachelines + 1 cacheline of alignment slack.
            const int LongsPerLine = CacheLineBytes / sizeof(long);
            controlRaw = GC.AllocateArray<long>(3 * LongsPerLine, pinned: true);
            var cRaw = (long)Unsafe.AsPointer(ref controlRaw[0]);
            var cAligned = (cRaw + CacheLineBytes - 1) & ~((long)CacheLineBytes - 1);
            producer = (ProducerControl*)cAligned;
            consumer = (ConsumerControl*)(cAligned + CacheLineBytes);

            // Ring: `capacity` slots + 1 cacheline of alignment slack (rounded up).
            var slotsPerLineSlack = (CacheLineBytes + sizeof(TRecord) - 1) / sizeof(TRecord);
            ringRaw = GC.AllocateArray<TRecord>(capacity + slotsPerLineSlack, pinned: true);
            var rRaw = (long)Unsafe.AsPointer(ref ringRaw[0]);
            var rAligned = (rRaw + CacheLineBytes - 1) & ~((long)CacheLineBytes - 1);
            ring = (TRecord*)rAligned;
        }

        /// <summary>
        /// Enqueue a record. Spins if the ring is full. Publishes every
        /// batchSize writes. Only the writer thread may call this, and the
        /// writer is also the only caller of <see cref="Complete"/>, so no
        /// completion check is needed on the write path.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(TRecord record)
        {
            var bufferTail = producer->BufferTail;
            if (bufferTail - producer->CachedHead >= capacity)
            {
                producer->CachedHead = Volatile.Read(ref consumer->Head);
                if (bufferTail - producer->CachedHead >= capacity)
                    WaitForSlot(bufferTail);
            }

            ring[bufferTail & mask] = record;
            bufferTail++;
            producer->BufferTail = bufferTail;

            if (bufferTail - producer->Tail >= batchSize)
                Volatile.Write(ref producer->Tail, bufferTail);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void WaitForSlot(long bufferTail)
        {
            // Ensure the consumer can see all our pending writes so it can make
            // progress; otherwise we could deadlock here waiting for Head to
            // advance while the consumer sees an older Tail.
            Volatile.Write(ref producer->Tail, bufferTail);

            var spinner = new SpinWait();
            while (bufferTail - producer->CachedHead >= capacity)
            {
                spinner.SpinOnce();
                producer->CachedHead = Volatile.Read(ref consumer->Head);
            }
        }

        /// <summary>
        /// Make any buffered writes visible to the consumer immediately.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Flush()
        {
            Volatile.Write(ref producer->Tail, producer->BufferTail);
        }

        /// <summary>
        /// Read the next record. Spins if the ring is empty. Returns false
        /// only when the channel is <see cref="Complete">completed</see> and
        /// fully drained; the caller should then acknowledge the batch end
        /// (and check its own cancellation if applicable).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read(out TRecord record)
        {
            // Fast path: record available, or batch already completed and empty.
            if (TryRead(out record)) return true;
            if (Volatile.Read(ref consumer->Completed) != 0) return false;

            // Slow path: spin until either a record is available or the channel is completed.
            var spinner = new SpinWait();
            do
            {
                spinner.SpinOnce();
                if (TryRead(out record)) return true;
            } while (Volatile.Read(ref consumer->Completed) == 0);
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool TryRead(out TRecord record)
        {
            var head = consumer->Head;
            if (head == consumer->CachedTail)
            {
                consumer->CachedTail = Volatile.Read(ref producer->Tail);
                if (head == consumer->CachedTail)
                {
                    record = default;
                    return false;
                }
            }

            record = ring[head & mask];
            Volatile.Write(ref consumer->Head, head + 1);
            return true;
        }

        /// <summary>
        /// Signal to the consumer that no more records will be written.
        /// Any in-flight writes are flushed so the consumer can drain them.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Complete()
        {
            Volatile.Write(ref producer->Tail, producer->BufferTail);
            Volatile.Write(ref consumer->Completed, 1);
        }

        /// <summary>
        /// Producer-side: clear the completion flag so the channel can be reused
        /// for a new batch. Caller must ensure no consumer is currently observing
        /// <see cref="IsCompleted"/> (e.g. all consumers parked on a barrier).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            Volatile.Write(ref consumer->Completed, 0);
        }

        /// <summary>
        /// True once <see cref="Complete"/> has been called and the consumer has drained all remaining entries.
        /// </summary>
        public bool IsCompleted
        {
            get
            {
                if (Volatile.Read(ref consumer->Completed) == 0) return false;
                return consumer->Head == Volatile.Read(ref producer->Tail);
            }
        }
    }
}
