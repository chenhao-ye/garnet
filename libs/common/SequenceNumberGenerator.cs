// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.common
{
    /// <summary>
    /// Sequence number generator. Reads the invariant TSC via a native helper
    /// (libgarnet_tsc, built by Garnet.common.csproj) when available; falls
    /// back to Stopwatch.GetTimestamp() otherwise. The native path costs
    /// ~7 ns/call vs ~23 ns for Stopwatch on a 2.4 GHz Xeon.
    /// </summary>
    /// <param name="startingOffset"></param>
    public sealed partial class SequenceNumberGenerator(long startingOffset)
    {
        readonly long baseTimestamp = ReadCounter();
        readonly long startingOffset = startingOffset;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetSequenceNumber() => ReadCounter() - baseTimestamp + startingOffset;

        public override string ToString() => $"{startingOffset},{baseTimestamp},{ReadCounter()}";

        // ---- Native rdtsc fast path -----------------------------------------

        [LibraryImport("garnet_tsc", EntryPoint = "garnet_read_tsc")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl), typeof(CallConvSuppressGCTransition)])]
        private static partial ulong GarnetReadTsc();

        // Probe once at startup. If the native library is loadable and the call
        // succeeds, use it; otherwise fall back to Stopwatch.GetTimestamp().
        // The branch on `useNative` is a static readonly bool — the JIT treats
        // it as a constant after tier-up and erases the unused side.
        static readonly bool useNative = ProbeNative();

        static bool ProbeNative()
        {
            try
            {
                _ = GarnetReadTsc();
                return true;
            }
            catch
            {
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static long ReadCounter()
            => useNative ? (long)GarnetReadTsc() : Stopwatch.GetTimestamp();
    }
}
