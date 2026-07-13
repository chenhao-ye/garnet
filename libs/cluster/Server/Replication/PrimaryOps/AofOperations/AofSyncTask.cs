// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class AofSyncDriver : IDisposable
    {
        public class AofSyncTask : IBulkLogEntryConsumer, IDisposable
        {
            readonly ClusterProvider clusterProvider;
            readonly AofSyncDriverStore aofSyncDriverStore;
            readonly int physicalSublogIdx;
            public readonly GarnetClientSession garnetClient;
            readonly string localNodeId;
            readonly string remoteNodeId;
            readonly CancellationTokenSource cts;
            readonly long startAddress;
            TsavoriteLogScanSingleIterator iter;
            long previousAddress;

            // Byte-progress gate for backpressure lag publishing: republish only after shipping
            // publishDeltaBytes since the last publish. previousAddress advances only when a chunk
            // ships, so a caught-up (idle) sublog never publishes, and a busy one batches many
            // chunks per publish. Accessed only by this task's consume loop; with backpressure
            // disabled the publish block is skipped entirely.
            readonly bool backpressureEnabled;
            readonly long publishDeltaBytes;
            long lastPublishedShippedAddress;

            // In-band time pulse state (multi-log timestamp mode). When this sublog ships nothing
            // for AofTailWitnessFreq while some sublog's tail moved since this task's last pulse,
            // the task sends CLUSTER ADVANCE_TIME on its own sync connection so the replica's
            // logical time keeps flowing on this sublog (see MaybeSendTimePulse).
            readonly bool timePulseEnabled;
            readonly GarnetAppendOnlyFile appendOnlyFile;
            readonly TsavoriteLog physicalSublog;
            readonly long[] pulseTailSnapshot;
            readonly long[] pulseTailScratch;
            long lastSendTicks;

            /// <summary>
            /// Return start address for this AofSyncTask
            /// </summary>
            public long StartAddress => startAddress;

            /// <summary>
            /// Return previous address for this AofSyncTask
            /// </summary>
            public long PreviousAddress => previousAddress;

            /// <summary>
            /// Check if client connection is healthy
            /// </summary>
            public bool IsConnected => garnetClient != null && garnetClient.IsConnected;

            /// <summary>
            /// Logger instance
            /// </summary>
            readonly ILogger logger;

            /// <summary>
            /// AofSyncTask constructor
            /// </summary>
            /// <param name="clusterProvider"></param>
            /// <param name="physicalSublogIdx"></param>
            /// <param name="endPoint"></param>
            /// <param name="startAddress"></param>
            /// <param name="localNodeId"></param>
            /// <param name="remoteNodeId"></param>
            /// <param name="cts"></param>
            /// <param name="logger"></param>
            public AofSyncTask(
                ClusterProvider clusterProvider,
                AofSyncDriverStore aofSyncDriverStore,
                int physicalSublogIdx,
                IPEndPoint endPoint,
                long startAddress,
                string localNodeId,
                string remoteNodeId,
                CancellationTokenSource cts,
                ILogger logger)
            {
                var currentConfig = clusterProvider.clusterManager.CurrentConfig;
                this.clusterProvider = clusterProvider;
                this.aofSyncDriverStore = aofSyncDriverStore;
                this.physicalSublogIdx = physicalSublogIdx;
                this.startAddress = startAddress;
                previousAddress = startAddress;
                this.localNodeId = localNodeId;
                this.remoteNodeId = remoteNodeId;
                this.cts = cts;
                appendOnlyFile = clusterProvider.storeWrapper.appendOnlyFile;
                backpressureEnabled = appendOnlyFile.backpressure != null;
                publishDeltaBytes = appendOnlyFile.backpressure?.PublishDeltaBytes ?? 0;
                lastPublishedShippedAddress = startAddress;
                timePulseEnabled = clusterProvider.serverOptions.MultiLogEnabled && clusterProvider.serverOptions.AofReadWithTimestamp;
                if (timePulseEnabled)
                {
                    physicalSublog = appendOnlyFile.Log.GetSubLog(physicalSublogIdx);
                    pulseTailSnapshot = new long[clusterProvider.serverOptions.AofPhysicalSublogCount];
                    pulseTailScratch = new long[clusterProvider.serverOptions.AofPhysicalSublogCount];
                    // -1 differs from any real tail, forcing one initial pulse even on a fully
                    // idle system so a fresh replica's sessions are not stuck at max 0.
                    Array.Fill(pulseTailSnapshot, -1L);
                }
                garnetClient = new GarnetClientSession(
                            endPoint,
                            this.clusterProvider.replicationManager.GetAofSyncNetworkBufferSettings,
                            this.clusterProvider.replicationManager.GetNetworkPool,
                            tlsOptions: this.clusterProvider.serverOptions.TlsOptions?.TlsClientOptions,
                            authUsername: this.clusterProvider.ClusterUsername,
                            authPassword: this.clusterProvider.ClusterPassword,
                            clientName: $"AofSyncTask-{physicalSublogIdx}:({currentConfig.LocalNodeEndpoint})",
                            logger: logger);
                this.logger = logger;
            }

            public void Dispose()
            {
                try
                {
                    // Dispose GarnetClient
                    garnetClient?.Dispose();
                }
                catch { }

                try
                {
                    // This forces the background sync task to stop,
                    // unless the cancelled cts already signaled it to stop
                    iter?.Dispose();
                    iter = null;
                }
                catch { }
            }

            /// <summary>
            /// Consume AOF records generated at the primary
            /// </summary>
            /// <param name="payloadPtr"></param>
            /// <param name="payloadLength"></param>
            /// <param name="currentAddress"></param>
            /// <param name="nextAddress"></param>
            /// <param name="isProtected"></param>
            public unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected)
            {
                try
                {
                    ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Aof_Sync_Task_Consume);

                    // logger?.LogInformation("Sending {payloadLength} bytes to {remoteNodeId} at address {currentAddress}-{nextAddress}", payloadLength, remoteNodeId, currentAddress, nextAddress);

                    // This is called under epoch protection, so we have to wait for appending to complete
                    garnetClient.ExecuteClusterAppendLog(
                        localNodeId,
                        physicalSublogIdx,
                        previousAddress,
                        currentAddress,
                        nextAddress,
                        (long)payloadPtr,
                        payloadLength);

                    // Set task address to nextAddress, as the iterator is currently at nextAddress
                    // (records at currentAddress are already sent above)
                    previousAddress = nextAddress;
                    // Shipped records carry time themselves; restart the idle window for pulses.
                    lastSendTicks = Environment.TickCount64;
                }
                catch (Exception ex)
                {
                    logger?.LogError(
                        ex,
                        "{Consume}[{taskId}]: exception consuming AOF payload to sync {remoteNodeId} ({currenAddress}, {nextAddress})",
                        nameof(AofSyncTask.Consume),
                        physicalSublogIdx,
                        remoteNodeId,
                        currentAddress,
                        nextAddress);
                    throw;
                }
            }

            public void Throttle()
            {
                cts.Token.ThrowIfCancellationRequested();

                if (!garnetClient.IsConnected)
                    ExceptionUtils.ThrowException(new GarnetException($"AOF stream client disconnected! [{physicalSublogIdx}]:({startAddress},{previousAddress})"));

                // Trigger flush while we are out of epoch protection
                garnetClient.CompletePending(false);
                garnetClient.Throttle();

                // Publish replication lag to the backpressure gate outside epoch protection,
                // gated on shipped byte-progress: republish once this task has shipped
                // publishDeltaBytes since its last publish. previousAddress advances only in
                // Consume, so a caught-up sublog never republishes (no lock/scan while idle),
                // while a draining one batches many chunks per publish yet still releases
                // stalled appenders as it ships.
                if (backpressureEnabled && previousAddress - lastPublishedShippedAddress >= publishDeltaBytes)
                {
                    lastPublishedShippedAddress = previousAddress;
                    aofSyncDriverStore.PublishReplicationLag();
                }

                // The consume loop invokes Throttle on every poll, including empty ones, so this
                // is also the idle hook for time pulses.
                MaybeSendTimePulse();
            }

            /// <summary>
            /// Sends an in-band CLUSTER ADVANCE_TIME pulse when this sublog has shipped nothing
            /// for AofTailWitnessFreq, so the replica's logical time keeps flowing on this sublog
            /// while other sublogs make progress and reader sessions advance their timestamps.
            /// Runs on the sync task's own thread (the consume loop calls it on every poll,
            /// outside epoch protection), so it shares the connection safely with Consume's
            /// APPENDLOG stream and is ordered after every record shipped so far.
            /// </summary>
            void MaybeSendTimePulse()
            {
                if (!timePulseEnabled || iter == null)
                    return;
                var now = Environment.TickCount64;
                if (now - lastSendTicks < clusterProvider.serverOptions.AofTailWitnessFreq)
                    return;

                // Converged check: a pulse is useful only if some sublog's tail moved since this
                // task's last pulse (only a moved sublog's records can advance a session timestamp
                // past this idle sublog's published max). Under full quiescence every task thus
                // sends one trailing pulse and then goes silent until any tail moves again. The
                // tails are snapshotted BEFORE acquiring the pulse timestamp below, so the
                // timestamp provably exceeds the stamp of every record inside the snapshot;
                // records appended after the snapshot un-converge the next check.
                var anyTailMoved = false;
                for (var i = 0; i < pulseTailScratch.Length; i++)
                {
                    pulseTailScratch[i] = appendOnlyFile.Log.GetTailAddress(i);
                    anyTailMoved |= pulseTailScratch[i] != pulseTailSnapshot[i];
                }
                if (!anyTailMoved)
                {
                    // Converged; back off a full idle window before re-checking.
                    lastSendTicks = now;
                    return;
                }

                // Acquire the pulse timestamp BEFORE observing the allocation tail below. The +1
                // makes the pulse strictly larger than every stamp already acquired (an earlier
                // counter read returns at most this read's value), which quiescence convergence
                // requires: session timestamps are always drawn from record stamps, so after the
                // trailing pulse every session sits strictly below this sublog's max and no
                // reader stays blocked once pulses cease. Records are stamped before they reserve
                // log space, so every record holding log space here is covered by that bound, and
                // the tail comparison below aborts the pulse until all of them have shipped. A
                // record that has not yet reserved space ships after the pulse on this
                // connection, and operations depending on it stamp only after its bucket latch
                // releases, hence after this read -- so no session can reach a timestamp that
                // requires seeing that record while this pulse is what unblocks it. For the +1
                // this last step additionally assumes one counter tick is shorter than such an
                // append-then-observe chain, so a dependent cannot stamp within the same tick as
                // this read and undercut the pulse (the granularity assumption; record-only
                // replay never needs it because the read gate treats ties conservatively).
                var sequenceNumber = appendOnlyFile.seqNumGen.GetSequenceNumber() + 1;
                // Gate on the allocation tail, not the safe tail: a completed record that sits
                // above a still-copying straggler is excluded from the safe tail yet already
                // visible to dependents through the store, so a safe-tail comparison could report
                // caught-up while a stamped record remains unshipped. iter.NextAddress can never
                // exceed the safe tail, which never exceeds the allocation tail, so passing this
                // check means all three coincide.
                if (iter.NextAddress < physicalSublog.TailAddress)
                    return; // unshipped records exist; they ship next and carry time themselves

                garnetClient.ExecuteClusterAdvanceTime(physicalSublogIdx, sequenceNumber);
                garnetClient.CompletePending(false);

                for (var i = 0; i < pulseTailSnapshot.Length; i++)
                    pulseTailSnapshot[i] = pulseTailScratch[i];
                lastSendTicks = now;
            }

            public async Task RunAofSyncTask(AofSyncDriver aofSyncDriver)
            {
                var enteredMonitor = false;
                try
                {
                    enteredMonitor = aofSyncDriver.activeWorkerMonitor.TryEnter();
                    if (!enteredMonitor)
                        ExceptionUtils.ThrowException(new GarnetException($"[{physicalSublogIdx}] Failed to acquire lock at {nameof(RunAofSyncTask)}"));

                    logger?.LogInformation(
                        "{RunAofSyncTask}[{taskId}]: syncing {remoteNodeId} starting from address {address}",
                        nameof(AofSyncTask.RunAofSyncTask),
                        physicalSublogIdx,
                        aofSyncDriver.remoteNodeId,
                        startAddress);

                    if (!IsConnected)
                        garnetClient.Connect();

                    iter = clusterProvider.storeWrapper.appendOnlyFile.Log.ScanSingle(physicalSublogIdx, startAddress, long.MaxValue, scanUncommitted: true, recover: false, logger: logger);

                    // Send ping to initialize replication stream
                    garnetClient.ExecuteClusterAppendLog(aofSyncDriver.localNodeId, physicalSublogIdx, -1, -1, -1, -1, 0);
                    garnetClient.CompletePending(false);
                    lastSendTicks = Environment.TickCount64;

                    await iter.BulkConsumeAllAsync(
                        this,
                        aofSyncDriver.clusterProvider.serverOptions.ReplicaSyncDelayMs,
                        maxChunkSize: aofSyncDriver.clusterProvider.serverOptions.AofReplicationChunkSize,
                        cts.Token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "[{sublogIdx}]({method})", physicalSublogIdx, nameof(RunAofSyncTask));
                }
                finally
                {
                    if (enteredMonitor)
                        _ = aofSyncDriver.activeWorkerMonitor.Exit();
                    garnetClient?.Dispose();
                }
            }
        }
    }
}