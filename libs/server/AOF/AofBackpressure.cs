// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Primary-side replication backpressure for the AOF, constructed only when AofShipMaxLag
    /// is set (a null gate means disabled; call sites gate with ?.). MultiLog is one logical
    /// log split across physical
    /// sublogs, so the cluster layer publishes the logical log's replication lag: the slowest
    /// attached replica's tail-minus-shipped diff, summed across all sublogs. Appenders stall
    /// while it exceeds the configured budget. Without this gate, an in-memory AOF (null device
    /// with fast-aof-truncate) recycles pages past unshipped records and silently drops them
    /// for lagging replicas. Wrap protection assumes lag spreads across sublogs; a single
    /// sublog still wraps at its own buffer size regardless of the total.
    ///
    /// Stalled appenders sleep-poll the published lag; publishers only write a volatile field
    /// and never wake anyone, keeping the shipping threads' cost flat under load. The stall
    /// and resume thresholds differ (hysteresis), so the system alternates between longer
    /// free-run and drain phases instead of oscillating at the stall boundary.
    ///
    /// A replica that ships slowly throttles the primary indefinitely, which is the intended
    /// contract; one that stops shipping entirely stalls appends until its connection faults
    /// and its sync driver is removed (the removal publish then releases the stall), the same
    /// contract as the replica-side ReplicationOffsetMaxLag throttle.
    /// </summary>
    public sealed class AofBackpressure : IDisposable
    {
        /// <summary>
        /// Sleep interval while stalled. Also bounds how quickly a stall reacts to a publish,
        /// so publishers gain nothing from refreshing the lag more often than this.
        /// </summary>
        public const int PollIntervalMs = 1;

        /// <summary>
        /// Byte-progress interval for republishing lag. A sync task republishes only after
        /// shipping this many bytes since its last publish, so a caught-up (idle) sublog never
        /// publishes and a busy one batches many chunks per publish. Derived from the stall
        /// budget so a stalled appender is released within one interval of shipping past the
        /// resume threshold (a small fraction of the hysteresis band).
        /// </summary>
        public long PublishDeltaBytes { get; }

        readonly long stallLagBytes;
        readonly long resumeLagBytes;

        // Stall decision, owned by the publishers: the threshold comparison and hysteresis run
        // on the shipping threads, and this flag is rewritten only on state transitions, so the
        // cache line appenders read stays quiet while the state is steady.
        bool stalled;

        volatile bool disposed;

        readonly ILogger logger;

        public AofBackpressure(GarnetServerOptions serverOptions, ILogger logger = null)
        {
            this.logger = logger;
            stallLagBytes = serverOptions.AofShipMaxLag;
            resumeLagBytes = stallLagBytes / 2;
            // ~16 publishes span the full stall budget (~8 the hysteresis band), so the release
            // lag past resumeLagBytes is bounded by one PublishDeltaBytes of shipping.
            PublishDeltaBytes = Math.Max(1, stallLagBytes / 32);
            logger?.LogInformation("AofBackpressure enabled: stall lag {stallLagBytes} bytes, resume lag {resumeLagBytes} bytes, publish delta {PublishDeltaBytes} bytes, summed across sublogs", stallLagBytes, resumeLagBytes, PublishDeltaBytes);
        }

        /// <summary>
        /// Stall while the publishers report the replication lag over budget.
        /// Callers must not hold sublog locks (transaction paths gate before LockSublogs).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Wait()
        {
            if (!Volatile.Read(ref stalled)) return;
            WaitSlow();
        }

        void WaitSlow()
        {
            while (!disposed && Volatile.Read(ref stalled))
                Thread.Sleep(PollIntervalMs);
        }

        /// <summary>
        /// Publish the replication lag (slowest replica's tail-minus-shipped diff summed across
        /// sublogs). Pass 0 when no replica is attached. The threshold comparison and hysteresis
        /// run here, on the publishing thread; the stall flag is written only when the decision
        /// changes. Concurrent publishers may race a transition, which is benign: the next
        /// publish re-derives it from fresher lag within a poll interval.
        /// </summary>
        /// <param name="totalLag">Replication lag in bytes, summed across all sublogs.</param>
        public void PublishReplicationLag(long totalLag)
        {
            var isStalled = Volatile.Read(ref stalled);
            if (!isStalled && totalLag > stallLagBytes)
                Volatile.Write(ref stalled, true);
            else if (isStalled && totalLag <= resumeLagBytes)
                Volatile.Write(ref stalled, false);
        }

        /// <summary>
        /// Release all stalled appenders permanently (server shutdown).
        /// </summary>
        public void Dispose() => disposed = true;
    }
}