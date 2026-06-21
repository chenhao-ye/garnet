// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.server
{
    /// <summary>
    /// Measurement-only per-reader-session counters for the reader-breakdown study: how many
    /// consistency checks a session ran and how many of them had to wait (spin or park). The
    /// fields sit on their own cache line and one probe is allocated per session, so a reader
    /// only ever writes its own probe -- no false sharing between reader threads. Only the
    /// owning reader thread writes; totals are read after threads quiesce.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 128)]
    public sealed class ReaderWaitProbe
    {
        /// <summary>Consistency checks performed (one per key freshness check).</summary>
        [FieldOffset(64)] public long checks;

        /// <summary>Checks that could not proceed immediately and had to wait (spin or park).</summary>
        [FieldOffset(72)] public long waits;
    }
}