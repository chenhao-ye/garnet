// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Manages read consistency for append-only file operations through snapshots.
    /// </summary>
    /// <param name="currentVersion">Initial version for this manager instance.</param>
    /// <param name="appendOnlyFile">The owning append-only file.</param>
    /// <param name="serverOptions">Server configuration options.</param>
    public class ReadSnapshotManager(GarnetAppendOnlyFile appendOnlyFile, GarnetServerOptions serverOptions)
    {
        readonly GarnetAppendOnlyFile appendOnlyFile = appendOnlyFile;
        readonly GarnetServerOptions serverOptions = serverOptions;
    }
}