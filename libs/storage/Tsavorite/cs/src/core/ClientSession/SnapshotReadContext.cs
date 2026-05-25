// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Consistent read context using the snapshot-based read protocol. Each Read is resolved at a
    /// fixed snapshot address provided by <see cref="getSnapshotAddress"/>; older record versions
    /// at-or-before that address are returned even if newer versions exist in the log.
    /// </summary>
    public readonly struct SnapshotReadContext<TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>
        : ITsavoriteContext<TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>
        where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        where TFunctions : ISessionFunctions<TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        public readonly BasicContext<TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> BasicContext { get; }

        /// <summary>
        /// Provides the current snapshot upper-bound address. Each Read serves the most recent record
        /// version strictly before this address.
        /// </summary>
        private readonly Func<long> getSnapshotAddress;

        /// <inheritdoc/>
        public long GetKeyHash<TOpKey>(TOpKey key)
            where TOpKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => Session.store.GetKeyHash(key);

        internal SnapshotReadContext(ClientSession<TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession, Func<long> getSnapshotAddress)
        {
            BasicContext = new BasicContext<TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>(clientSession);
            this.getSnapshotAddress = getSnapshotAddress;
        }

        /// <inheritdoc/>
        public bool IsNull => BasicContext.IsNull;

        /// <inheritdoc/>
        public ClientSession<TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> Session => BasicContext.Session;

        #region Snapshot Read Support

        internal struct SnapshotVersionScanFunctions : IScanIteratorFunctions
        {
            private readonly long snapshotMaxAddress;
            internal long foundAddress;

            internal SnapshotVersionScanFunctions(long snapshotMaxAddress)
            {
                this.snapshotMaxAddress = snapshotMaxAddress;
                foundAddress = LogAddress.kInvalidAddress;
            }

            public bool OnStart(long beginAddress, long endAddress) => true;

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata,
                long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept;
                // Skip records at or beyond snapshot boundary (too new)
                if (recordMetadata.Address >= snapshotMaxAddress)
                    return true;
                // Found most recent version at or before snapshot boundary
                if (!logRecord.Info.Tombstone)
                    foundAddress = recordMetadata.Address;
                return false; // Stop iteration
            }

            public void OnStop(bool completed, long numberOfRecords) { }
            public void OnException(Exception exception, long numberOfRecords) { }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status SnapshotRead(TKey key, ref TInput input, ref TOutput output, TContext userContext)
        {
            var snapshotAddr = getSnapshotAddress();
            var scanFn = new SnapshotVersionScanFunctions(snapshotAddr);
            Session.store.Log.IterateKeyVersions(ref scanFn, key);
            if (scanFn.foundAddress != LogAddress.kInvalidAddress)
            {
                var readOptions = default(ReadOptions);
                return BasicContext.ReadAtAddress(scanFn.foundAddress, key, ref input, ref output, ref readOptions, out _, userContext);
            }
            return new Status(StatusCode.NotFound);
        }

        #endregion

        #region ITsavoriteContext/Read

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(TKey key, ref TInput input, ref TOutput output, TContext userContext = default)
            => SnapshotRead(key, ref input, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
            => Read(key, ref input, ref output, ref readOptions, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(TKey key, ref TOutput output, TContext userContext = default)
        {
            TInput input = default;
            return Read(key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(TKey key, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
        {
            TInput input = default;
            return Read(key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput output) Read(TKey key, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return (Read(key, ref input, ref output, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput output) Read(TKey key, ref ReadOptions readOptions, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return (Read(key, ref input, ref output, ref readOptions, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            recordMetadata = default;
            return SnapshotRead(key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow reads from address!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow reads from address!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadWithPrefetch<TBatch>(ref TBatch batch, TContext userContext = default)
            where TBatch : IReadArgBatch<TKey, TInput, TOutput>
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => throw new TsavoriteException("Snapshot read context does not currently support ReadWithPrefetch!");

        #endregion

        #region ITsavoriteContext

        /// <inheritdoc/>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
            => BasicContext.CompletePending(wait, spinWaitForCommit);

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            => BasicContext.CompletePendingWithOutputs(out completedOutputs, wait, spinWaitForCommit);

        /// <inheritdoc/>
        public async ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
            => await BasicContext.CompletePendingAsync(waitForCommit, token).ConfigureAwait(false);

        /// <inheritdoc/>
        public async ValueTask<CompletedOutputIterator<TInput, TOutput, TContext>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
            => await BasicContext.CompletePendingWithOutputsAsync(waitForCommit, token).ConfigureAwait(false);

        /// <inheritdoc/>
        public Status Upsert(TKey key, ReadOnlySpan<byte> desiredValue, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, ReadOnlySpan<byte> desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, IHeapObject desiredValue, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, IHeapObject desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, ref TInput input, IHeapObject desiredValue, ref TOutput output, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, ref TInput input, IHeapObject desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, ref TInput input, IHeapObject desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(in TSourceLogRecord diskLogRecord) where TSourceLogRecord : ISourceLogRecord
            => BasicContext.Upsert(diskLogRecord);

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(TKey key, in TSourceLogRecord diskLogRecord) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(TKey key, ref TInput input, in TSourceLogRecord diskLogRecord) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(ref TInput input, in TSourceLogRecord diskLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(TKey key, ref TInput input, in TSourceLogRecord diskLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        public Status Upsert<TOpKey, TSourceLogRecord>(TOpKey key, in TSourceLogRecord diskLogRecord)
            where TOpKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord => throw new TsavoriteException("Snapshot read context does not allow writes!");
        public Status Upsert<TOpKey, TSourceLogRecord>(TOpKey key, ref TInput input, in TSourceLogRecord diskLogRecord)
            where TOpKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord => throw new TsavoriteException("Snapshot read context does not allow writes!");
        public Status Upsert<TOpKey, TSourceLogRecord>(TOpKey key, ref TInput input, in TSourceLogRecord diskLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TOpKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(TKey key, ref TInput input, ref TOutput output, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(TKey key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(TKey key, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(TKey key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(TKey key, ref TInput input, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(TKey key, ref TInput input, ref RMWOptions rmwOptions, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Delete(TKey key, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public Status Delete(TKey key, ref DeleteOptions deleteOptions, TContext userContext = default)
            => throw new TsavoriteException("Snapshot read context does not allow writes!");

        /// <inheritdoc/>
        public void ResetModified(TKey key)
            => throw new TsavoriteException("Snapshot read context does not reset ResetModified!");

        /// <inheritdoc/>
        public void Refresh()
            => throw new TsavoriteException("Snapshot read context does not reset Refresh!");
        #endregion
    }
}
