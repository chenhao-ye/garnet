// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Bounds inter-virtual-sublog replay drift on a replica. When a large drift is observed -- by a
    /// reader that is about to block on a lagging virtual sublog, or by a replay thread crossing its
    /// progress gate (see ReadConsistencyManager.BoundReplayDrift) -- a "round" is installed targeting
    /// the leading sublog's frontier sequence number. Each replay thread that reaches the target then
    /// arrives at the barrier and waits there (spinning, then sleeping, per the constructor's spin
    /// budget); when every participant has arrived, the last one releases them all together. The
    /// threads thus align at the target before any leader pulls further ahead, which bounds the drift.
    /// The firing side only installs the round; it never tears it down. The barrier is a performance
    /// aid only -- prefix consistency is enforced by the reader's wait, so a round completing early or
    /// late never affects correctness.
    ///
    /// Arrivals come from the thread that owns each virtual sublog's replay, from two sources:
    /// per replayed record via <see cref="CheckAndWait"/> (blocking -- the leader pauses so
    /// laggards catch up), and per applied time pulse via <see cref="CheckAndArrive"/>
    /// (non-blocking -- an idle sublog whose max is advanced by the primary's quiescence pulses
    /// counts toward the round instead of stranding it, but has no replay work to pause and its
    /// delivering thread must not park). A per-participant last-arrived-round slot makes arrivals
    /// idempotent within a round, so the plain countdown stays correct when a non-blocking arrival
    /// continues processing. A round therefore completes only while every participant keeps
    /// replaying records or receiving pulses: a participant that is about to stop (exiting at end
    /// of run, pausing at a phase boundary, owning manager replaced) calls <see cref="Disable"/>,
    /// which releases the active round and rejects new rounds until <see cref="Enable"/>, so
    /// peers are never stranded waiting for an arrival that cannot come.
    ///
    /// Known limitation: cross-sublog synchronized replay (transaction groups, custom
    /// procedures) arrives through the same per-record path while its peers are parked at a
    /// LeaderBarrier. A thread that parks at a round from inside such a scope, or a coordinator
    /// arriving for a sublog it does not own, can stall the round until the synchronization
    /// timeout. Transactions are out of scope for the current barrier work; the analysis and the
    /// intended fix (thread-scoped arrival suppression) are recorded in the design notes.
    ///
    /// Fast path (no round active): a single Volatile.Read of a class field plus a long compare.
    /// </summary>
    public sealed class ReplayAlignBarrier
    {
        // Number of Thread.SpinWait iterations between release-flag checks while spinning.
        const int SpinWaitIterations = 16;

        sealed class Round
        {
            public long target;  // target frontier sequence number
            public int remaining;
            public volatile bool released;  // set by the last arrival / Disable; spinners poll this
            public readonly ManualResetEventSlim release = new(false);
        }

        readonly int participantCount;

        // Last round each participant arrived at, indexed by virtual sublog. Written by the
        // thread that owns the participant's replay (the same thread that calls CheckAndWait /
        // CheckAndArrive for it), so plain reads/writes suffice on the record and pulse paths;
        // synchronized replay can violate this (see the known limitation in the class remarks).
        // Makes arrivals idempotent per round: a non-blocking pulse arrival continues processing
        // and would otherwise decrement the countdown again on its next record.
        readonly Round[] lastArrivedRound;

        // How long an arrived thread spins before falling back to a kernel wait:
        //   < 0 => spin forever (never sleep); 0 => never spin (pure kernel wait); > 0 => spin up to
        // this many Stopwatch ticks, then sleep for the remainder. Spinning avoids the park/wake cost
        // when rounds are short and frequent, at the cost of burning a core while parked.
        readonly long spinTicks;

        Round currentRound;

        public ReplayAlignBarrier(int participantCount, int spinMicroseconds)
        {
            this.participantCount = participantCount;
            this.lastArrivedRound = new Round[participantCount];
            this.spinTicks = spinMicroseconds < 0
                ? -1
                : (long)(spinMicroseconds * (Stopwatch.Frequency / 1_000_000.0));
        }

        /// <summary>
        /// True while a round is in progress. A disabled barrier reports active: <see cref="Disable"/>
        /// occupies the slot with a round that never completes.
        /// </summary>
        public bool IsActive => Volatile.Read(ref currentRound) != null;

        /// <summary>
        /// Called when a large cross-sublog drift is observed (by a reader about to wait, or by a
        /// replay thread at its progress gate). Installs a round at the given target that expects
        /// every participant to arrive. No-op if a round is already in progress (including the
        /// never-completing round installed by <see cref="Disable"/>).
        /// </summary>
        public void TryActivate(long target)
        {
            if (Volatile.Read(ref currentRound) != null) return;
            var round = new Round { target = target, remaining = participantCount };
            _ = Interlocked.CompareExchange(ref currentRound, round, null);
        }

        /// <summary>
        /// Called by a replay thread after advancing its virtual sublog's frontier with a replayed
        /// record. When a round is active, this participant has reached the target, and it has not
        /// already arrived at this round (e.g. via a pulse), it arrives and blocks until every
        /// participant arrives. Lock-free fast path when no round is active.
        /// </summary>
        /// <param name="virtualSublogIdx">The arriving participant; must be the thread that owns
        /// this virtual sublog's replay.</param>
        /// <param name="frontier">The participant's current published max sequence number.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CheckAndWait(int virtualSublogIdx, long frontier)
        {
            var r = Volatile.Read(ref currentRound);
            if (r == null || frontier < r.target) return;
            if (lastArrivedRound[virtualSublogIdx] == r) return;
            lastArrivedRound[virtualSublogIdx] = r;
            Arrive(r);
        }

        /// <summary>
        /// Non-blocking arrival, used when a time pulse advances an idle virtual sublog's frontier.
        /// The participant is counted toward the round but never parks: an idle sublog has no
        /// replay work to pause, and the threads that deliver pulses (a replica session thread in
        /// synchronous replay, or the consume loop a session waits on through the ring batch)
        /// must not block for the rest of the round, which can take arbitrarily long or never
        /// complete during shutdown. The per-round arrival dedup keeps the countdown correct if
        /// records for this sublog resume before the round completes.
        /// </summary>
        /// <param name="virtualSublogIdx">The arriving participant; must be the thread that owns
        /// this virtual sublog's replay.</param>
        /// <param name="frontier">The participant's current published max sequence number.</param>
        public void CheckAndArrive(int virtualSublogIdx, long frontier)
        {
            var r = Volatile.Read(ref currentRound);
            if (r == null || frontier < r.target) return;
            if (lastArrivedRound[virtualSublogIdx] == r) return;
            lastArrivedRound[virtualSublogIdx] = r;
            if (Interlocked.Decrement(ref r.remaining) > 0)
                return;
            ReleaseRound(r);
        }

        void Arrive(Round r)
        {
            if (Interlocked.Decrement(ref r.remaining) > 0)
            {
                if (spinTicks < 0)  // Spin forever (never sleep).
                {
                    while (!r.released)
                        Thread.SpinWait(SpinWaitIterations);
                    return;
                }
                if (spinTicks > 0)
                {
                    var deadline = Stopwatch.GetTimestamp() + spinTicks;
                    while (Stopwatch.GetTimestamp() < deadline)
                    {
                        if (r.released)
                            return;
                        Thread.SpinWait(SpinWaitIterations);
                    }
                }
                r.release.Wait();
            }
            else
            {
                ReleaseRound(r);
            }
        }

        // Last arrival: tear the round down only if it is still current, so a thread acting on a
        // stale round reference cannot clobber a freshly activated one, then release the rest:
        // the flag wakes spinners, the event wakes sleepers.
        void ReleaseRound(Round r)
        {
            _ = Interlocked.CompareExchange(ref currentRound, null, r);
            r.released = true;
            r.release.Set();
        }

        /// <summary>
        /// Releases the active round and rejects new ones until <see cref="Enable"/>, by occupying
        /// the round slot with a round that can never complete: its target is long.MaxValue, which
        /// no frontier reaches, so no thread arrives at it, and <see cref="TryActivate"/> always
        /// finds a round in progress. Called by a participant that is about to stop arriving on the
        /// per-record replay path -- a replay worker exiting at end of run, workers pausing at a
        /// phase boundary (e.g. a benchmark warmup), or the owning
        /// <see cref="ReadConsistencyManager"/> being replaced -- so no peer is left stranded in a
        /// round that can no longer complete.
        /// </summary>
        public void Disable()
        {
            // The inert round is pre-released with effectively infinite remaining: if any path ever
            // did arrive at the unreachable target, it would return immediately instead of parking,
            // and the last-arrival teardown could never clear the slot.
            var inert = new Round { target = long.MaxValue, remaining = int.MaxValue, released = true };
            inert.release.Set();
            var r = Interlocked.Exchange(ref currentRound, inert);
            if (r != null)
            {
                r.released = true;
                r.release.Set();
            }
        }

        /// <summary>
        /// Re-allows round activation after <see cref="Disable"/> by clearing the round slot,
        /// releasing whatever round it held (normally Disable's never-completing round, which no
        /// thread waits on). Called once every participant is again arriving on the per-record
        /// replay path (e.g. when a benchmark's measured pass starts after warmup).
        /// </summary>
        public void Enable()
        {
            var r = Interlocked.Exchange(ref currentRound, null);
            if (r != null)
            {
                r.released = true;
                r.release.Set();
            }
        }
    }
}