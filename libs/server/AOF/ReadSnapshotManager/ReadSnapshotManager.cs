// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
    }
}