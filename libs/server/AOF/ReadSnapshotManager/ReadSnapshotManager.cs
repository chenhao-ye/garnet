// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Stub read manager for the Snapshot read protocol.
    /// All session callbacks are no-ops: reads proceed immediately without any ordering enforcement.
    /// </summary>
    /// <param name="currentVersion">Initial version for this manager instance.</param>
    /// <param name="appendOnlyFile">The owning append-only file.</param>
    /// <param name="serverOptions">Server configuration options.</param>
    public class ReadSnapshotManager(long currentVersion, GarnetAppendOnlyFile appendOnlyFile, GarnetServerOptions serverOptions)
    {
        readonly GarnetAppendOnlyFile appendOnlyFile = appendOnlyFile;
        readonly GarnetServerOptions serverOptions = serverOptions;

        /// <inheritdoc/>
        public long CurrentVersion { get; private set; } = currentVersion;

        /// <inheritdoc/>
        public AofAddress GetPhysicalSublogMaxReplayedSequenceNumber()
            => throw new NotImplementedException();

        /// <inheritdoc/>
        public void ConsistentReadKeyPrepare(ReadOnlySpan<byte> key, ref ReplicaReadSessionContext ctx, CancellationToken ct)
        {
            // No-op: snapshot protocol allows reads to proceed without blocking.
        }

        /// <inheritdoc/>
        public void ConsistentReadSequenceNumberUpdate(ref ReplicaReadSessionContext ctx)
        {
            // No-op: snapshot protocol does not track per-session sequence numbers.
        }
    }
}