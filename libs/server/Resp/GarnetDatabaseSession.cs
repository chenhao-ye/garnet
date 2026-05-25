// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    /// <summary>
    /// Represents a logical database session in Garnet
    /// </summary>
    internal class GarnetDatabaseSession : IDisposable
    {
        /// <summary>
        /// Database ID
        /// </summary>
        public int Id { get; }

        /// <summary>
        /// Storage session 
        /// </summary>
        public StorageSession StorageSession { get; }

        /// <summary>
        /// Garnet API
        /// </summary>
        public BasicGarnetApi GarnetApi { get; }

        /// <summary>
        /// Consistent Garnet API
        /// </summary>
        public ConsistentReadGarnetApi ConsistentGarnetApi { get; }

        /// <summary>
        /// Lockable Garnet API
        /// </summary>
        public TransactionalGarnetApi TransactionalGarnetApi { get; }

        /// <summary>
        /// Lockable Consistent Garnet API
        /// </summary>
        public TransactionalConsistentReadGarnetApi TransactionalConsistentGarnetApi { get; }

        /// <summary>
        /// Snapshot Garnet API (populated only when AofReadWithTimestamp is false).
        /// </summary>
        public SnapshotReadGarnetApi SnapshotGarnetApi { get; }

        /// <summary>
        /// Lockable Snapshot Garnet API
        /// </summary>
        public TransactionalSnapshotReadGarnetApi TransactionalSnapshotGarnetApi { get; }

        /// <summary>
        /// Transaction manager
        /// </summary>
        public TransactionManager TransactionManager { get; }

        bool disposed = false;

        public GarnetDatabaseSession(
            int id,
            StorageSession storageSession,
            BasicGarnetApi garnetApi,
            TransactionalGarnetApi lockableGarnetApi,
            TransactionManager txnManager,
            ConsistentReadGarnetApi consistentGarnetApi = default,
            TransactionalConsistentReadGarnetApi transactionalConsistentGarnetApi = default,
            SnapshotReadGarnetApi snapshotGarnetApi = default,
            TransactionalSnapshotReadGarnetApi transactionalSnapshotGarnetApi = default)
        {
            this.Id = id;
            this.StorageSession = storageSession;
            this.GarnetApi = garnetApi;
            this.TransactionalGarnetApi = lockableGarnetApi;
            this.TransactionManager = txnManager;
            this.ConsistentGarnetApi = consistentGarnetApi;
            this.TransactionalConsistentGarnetApi = transactionalConsistentGarnetApi;
            this.SnapshotGarnetApi = snapshotGarnetApi;
            this.TransactionalSnapshotGarnetApi = transactionalSnapshotGarnetApi;
        }

        public GarnetDatabaseSession(int id, GarnetDatabaseSession srcSession)
        {
            this.Id = id;
            this.StorageSession = srcSession.StorageSession;
            this.GarnetApi = srcSession.GarnetApi;
            this.TransactionalGarnetApi = srcSession.TransactionalGarnetApi;
            this.TransactionManager = srcSession.TransactionManager;
        }

        /// <summary>
        /// Dispose method
        /// </summary>
        public void Dispose()
        {
            if (disposed)
                return;

            StorageSession?.Dispose();

            disposed = true;
        }
    }
}