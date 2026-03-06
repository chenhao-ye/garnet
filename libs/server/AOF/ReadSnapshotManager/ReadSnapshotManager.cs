// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Manages read consistency for append-only file operations through snapshots.
    /// Periodically takes a FoldOver checkpoint and advances the snapshot address boundary.
    /// </summary>
    public sealed class ReadSnapshotManager : IDisposable
    {
        readonly GarnetServerOptions serverOptions;
        readonly StoreWrapper storeWrapper;
        readonly CancellationTokenSource cts = new();
        readonly Task backgroundTask;

        private long snapshotAddress = long.MaxValue;
        private long lastSnapshotTail = long.MinValue;

        /// <param name="serverOptions">Server configuration options.</param>
        /// <param name="storeWrapper">When non-null, the background snapshot task is started immediately.</param>
        public ReadSnapshotManager(GarnetServerOptions serverOptions, StoreWrapper storeWrapper = null)
        {
            this.serverOptions = serverOptions;
            this.storeWrapper = storeWrapper;
            if (storeWrapper != null)
                backgroundTask = Task.Run(RunAsync);
        }

        /// <summary>Returns the max log address at which snapshot reads are bounded. long.MaxValue = read latest.</summary>
        public long GetSnapshotAddress() => Interlocked.Read(ref snapshotAddress);

        /// <summary>Updates the snapshot address boundary used for snapshot reads.</summary>
        internal void UpdateSnapshotAddress(long newAddress) => Interlocked.Exchange(ref snapshotAddress, newAddress);

        async Task RunAsync()
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(serverOptions.AofSnapshotFreq, cts.Token);
                }
                catch (OperationCanceledException) { break; }

                var currentTail = storeWrapper.store.Log.TailAddress;
                if (currentTail == lastSnapshotTail)
                    continue;

                var (success, _) = await storeWrapper.store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver, cancellationToken: cts.Token);
                if (success)
                {
                    UpdateSnapshotAddress(currentTail);
                    lastSnapshotTail = currentTail;
                }
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            cts.Cancel();
            backgroundTask?.Wait();
            cts.Dispose();
        }
    }
}
