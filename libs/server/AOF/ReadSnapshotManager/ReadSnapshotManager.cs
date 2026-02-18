// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Manages read consistency for append-only file operations through snapshots.
    /// </summary>
    /// <param name="appendOnlyFile">The owning append-only file.</param>
    /// <param name="serverOptions">Server configuration options.</param>
    public class ReadSnapshotManager(GarnetAppendOnlyFile appendOnlyFile, GarnetServerOptions serverOptions)
    {
        readonly GarnetAppendOnlyFile appendOnlyFile = appendOnlyFile;
        readonly GarnetServerOptions serverOptions = serverOptions;

        private long snapshotAddress = long.MaxValue;

        /// <summary>Returns the max log address at which snapshot reads are bounded. long.MaxValue = read latest.</summary>
        public long GetSnapshotAddress() => Interlocked.Read(ref snapshotAddress);

        /// <summary>Updates the snapshot address boundary used for snapshot reads.</summary>
        internal void UpdateSnapshotAddress(long newAddress) => Interlocked.Exchange(ref snapshotAddress, newAddress);
    }
}