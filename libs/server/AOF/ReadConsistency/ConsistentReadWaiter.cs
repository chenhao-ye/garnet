// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Per-session wakeup primitive used by the consistent-read protocol. A session keeps one
    /// instance in <see cref="ReadSessionState"/> and reuses it across waits. The instance is
    /// retired (replaced with a fresh one) only when a wait is cancelled or times out, in which
    /// case the old instance remains in the virtual sublog's wait queue as a tombstone and is
    /// reclaimed lazily by the next signal pass that crosses its target.
    /// </summary>
    public sealed class ConsistentReadWaiter
    {
        public readonly ManualResetEventSlim Event = new(initialState: false);
        public volatile bool Cancelled;
    }
}
