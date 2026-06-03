// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Bounds inter-virtual-sublog replay drift on a replica. A reader that is about to block on a
    /// lagging virtual sublog installs a "round" targeting the leading sublog's frontier sequence
    /// number. Each replay thread that reaches the target then arrives at the barrier and waits there
    /// (spinning, then sleeping, per the constructor's spin budget); when every participant has arrived,
    /// the last one releases them all together. The threads thus align
    /// at the target before any leader pulls further ahead, which bounds the drift. The reader only
    /// installs the round; it never tears it down. The barrier is a performance aid only -- prefix
    /// consistency is enforced by the reader's wait, so a round completing early or late never affects
    /// correctness.
    ///
    /// There is a single arrival source: the replay threads' per-record <see cref="CheckAndWait"/>.
    /// Each thread arrives at most once per round (it blocks immediately after arriving and does not
    /// process another record until released), so a plain countdown of outstanding participants
    /// suffices -- no per-sublog arrival tracking. A replay thread that exits before a round completes
    /// (e.g. at shutdown) calls <see cref="Disable"/> to release any peer it would otherwise strand.
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

        // How long an arrived thread spins before falling back to a kernel wait:
        //   < 0 => spin forever (never sleep); 0 => never spin (pure kernel wait); > 0 => spin up to
        // this many Stopwatch ticks, then sleep for the remainder. Spinning avoids the park/wake cost
        // when rounds are short and frequent, at the cost of burning a core while parked.
        readonly long spinTicks;

        Round currentRound;

        public ReplayAlignBarrier(int participantCount, int spinMicroseconds)
        {
            this.participantCount = participantCount;
            this.spinTicks = spinMicroseconds < 0
                ? -1
                : (long)(spinMicroseconds * (Stopwatch.Frequency / 1_000_000.0));
        }

        /// <summary>True while a round is in progress.</summary>
        public bool IsActive => Volatile.Read(ref currentRound) != null;

        /// <summary>
        /// Called by a reader that observes a large cross-sublog drift. Installs a round at the given
        /// target that expects every participant to arrive. No-op if a round is already in progress.
        /// </summary>
        public void TryActivate(long target)
        {
            if (Volatile.Read(ref currentRound) != null) return;
            var round = new Round { target = target, remaining = participantCount };
            _ = Interlocked.CompareExchange(ref currentRound, round, null);
        }

        /// <summary>
        /// Called by a replay thread after advancing its virtual sublog's frontier. When a round is
        /// active and this thread has reached the target, it arrives and blocks until every participant
        /// arrives. Lock-free fast path when no round is active.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CheckAndWait(long frontier)
        {
            var r = Volatile.Read(ref currentRound);
            if (r == null || frontier < r.target) return;
            Arrive(r);
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
                // Last to arrive. Tear the round down only if it is still current, so a thread acting on
                // a stale round reference cannot clobber a freshly activated one, then release the rest:
                // the flag wakes spinners, the event wakes sleepers.
                _ = Interlocked.CompareExchange(ref currentRound, null, r);
                r.released = true;
                r.release.Set();
            }
        }

        /// <summary>
        /// Tears down the active round and releases all paused threads. Called when the owning
        /// <see cref="ReadConsistencyManager"/> is replaced so threads parked in the old round resume.
        /// </summary>
        public void Disable()
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