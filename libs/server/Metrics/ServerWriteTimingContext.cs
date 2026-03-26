// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Garnet.server
{
    internal enum ServerWriteTimingPhase
    {
        SetCommandTotal = 0,
        SetStore = 1,
        SetResponseWrite = 2,
        SetAofAppend = 3,
        SetStoreEngineUpsert = 4,
    }

    internal sealed class ServerWriteTimingContext
    {
        readonly long[] ticks = new long[Enum.GetValues<ServerWriteTimingPhase>().Length];
        long setCount;
        int reported;

        public void Add(ServerWriteTimingPhase phase, long elapsedTicks)
        {
            if (elapsedTicks > 0)
                Interlocked.Add(ref ticks[(int)phase], elapsedTicks);
        }

        public void IncrementSetCount() => Interlocked.Increment(ref setCount);

        static double TicksToMilliseconds(long value) => value * 1000.0 / Stopwatch.Frequency;

        static double TicksToNanoseconds(long value) => value * 1_000_000_000.0 / Stopwatch.Frequency;

        public void Report()
        {
            if (Interlocked.Exchange(ref reported, 1) != 0)
                return;

            var totalSets = Interlocked.Read(ref setCount);
            if (totalSets == 0)
                return;

            var totalTicks = Interlocked.Read(ref ticks[(int)ServerWriteTimingPhase.SetCommandTotal]);
            var storeTicks = Interlocked.Read(ref ticks[(int)ServerWriteTimingPhase.SetStore]);
            var responseTicks = Interlocked.Read(ref ticks[(int)ServerWriteTimingPhase.SetResponseWrite]);
            var aofTicks = Interlocked.Read(ref ticks[(int)ServerWriteTimingPhase.SetAofAppend]);
            var storeEngineUpsertTicks = Interlocked.Read(ref ticks[(int)ServerWriteTimingPhase.SetStoreEngineUpsert]);
            var storeCoreTicks = Math.Max(0, storeTicks - aofTicks);
            var sharedPrimitiveNoAofTicks = Math.Max(0, storeEngineUpsertTicks - aofTicks);
            var liveOnlyOverheadTicks = Math.Max(0, totalTicks - storeEngineUpsertTicks);
            var wrapperTicks = Math.Max(0, totalTicks - storeTicks - responseTicks);

            Console.WriteLine($"[Server SET Count]: {totalSets:N0}");
            ReportPhase("Server SET Total", totalTicks, totalTicks, totalSets);
            ReportPhase("Server SET Store", storeTicks, totalTicks, totalSets);
            ReportPhase("Server SET Shared Upsert Primitive", storeEngineUpsertTicks, totalTicks, totalSets);
            ReportPhase("Server SET Shared Upsert Primitive No AOF", sharedPrimitiveNoAofTicks, totalTicks, totalSets);
            ReportPhase("Server SET Store Core", storeCoreTicks, totalTicks, totalSets);
            ReportPhase("Server SET AOF Append", aofTicks, totalTicks, totalSets);
            ReportPhase("Server SET Response Write", responseTicks, totalTicks, totalSets);
            ReportPhase("Server SET Wrapper", wrapperTicks, totalTicks, totalSets);
            ReportPhase("Server SET Live-Only Overhead", liveOnlyOverheadTicks, totalTicks, totalSets);

            var topPhases = new[]
            {
                ("Server SET Shared Upsert Primitive", storeEngineUpsertTicks),
                ("Server SET AOF Append", aofTicks),
                ("Server SET Response Write", responseTicks),
                ("Server SET Wrapper", wrapperTicks)
            }
            .OrderByDescending(x => x.Item2)
            .Take(3)
            .Select(x => $"{x.Item1} {(totalTicks == 0 ? 0 : x.Item2 * 100.0 / totalTicks):N2}%");

            Console.WriteLine($"[Server SET Top Phases]: {string.Join("; ", topPhases)}");
        }

        static void ReportPhase(string label, long phaseTicks, long totalTicks, long count)
        {
            var percent = totalTicks == 0 ? 0 : phaseTicks * 100.0 / totalTicks;
            var avgNs = count == 0 ? 0 : TicksToNanoseconds(phaseTicks) / count;
            Console.WriteLine($"[{label}]: {TicksToMilliseconds(phaseTicks):N2} ms ({percent:N2}% of set total, sets={count:N0}, avg={avgNs:N2} ns/set)");
        }
    }
}
