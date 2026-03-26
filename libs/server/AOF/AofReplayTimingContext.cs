// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Garnet.server
{
    public enum AofReplayTimingPhase
    {
        PageTotal = 0,
        AppendLogBuild = 1,
        SessionConsume = 2,
        ClusterAppendLogParseValidate = 3,
        PrimaryStreamTotal = 4,
        PrimaryStreamPrechecks = 5,
        RawEnqueue = 6,
        ReplayTotal = 7,
        ReplayRecordApply = 8,
        ReplayFastCommitMetadata = 9,
        ReplayPostchecks = 10,
        ReplayTxnHandling = 11,
        ReplayControlOps = 12,
        ReplayOpTotal = 13,
        ReplayStoreUpsert = 14,
        ReplayOtherReplayOp = 15,
        ReplayProcessRecordScaffolding = 16,
        ReplaySkipRecord = 17,
        ReplayProcessRecordSetup = 18,
        ReplaySharedUpsertPrimitive = 19,
        ReplayStoreUpsertMaterialize = 20,
        ReplayStoreUpsertCleanup = 21,
        Count = 22
    }

    public sealed class AofReplayTimingStats
    {
        readonly long[] ticks = new long[(int)AofReplayTimingPhase.Count];
        readonly long[] counts = new long[(int)AofReplayTimingPhase.Count];

        public void Add(AofReplayTimingPhase phase, long elapsedTicks, long eventCount = 1)
        {
            ticks[(int)phase] += elapsedTicks;
            counts[(int)phase] += eventCount;
        }

        public long GetTicks(AofReplayTimingPhase phase) => ticks[(int)phase];

        public long GetCount(AofReplayTimingPhase phase) => counts[(int)phase];

        public void Reset()
        {
            Array.Clear(ticks, 0, ticks.Length);
            Array.Clear(counts, 0, counts.Length);
        }
    }

    public sealed class AofReplayTimingContext
    {
        readonly AofReplayTimingStats[] sublogStats;
        readonly AofReplayTimingStats[][] replayTaskStats;

        public AofReplayTimingContext(int physicalSublogCount, int replayTaskCount)
        {
            sublogStats = Enumerable.Range(0, physicalSublogCount).Select(_ => new AofReplayTimingStats()).ToArray();
            replayTaskStats = Enumerable.Range(0, physicalSublogCount)
                .Select(_ => Enumerable.Range(0, Math.Max(replayTaskCount, 1)).Select(__ => new AofReplayTimingStats()).ToArray())
                .ToArray();
        }

        public AofReplayTimingStats GetSublogStats(int physicalSublogIdx) => sublogStats[physicalSublogIdx];

        public AofReplayTimingStats GetReplayTaskStats(int physicalSublogIdx, int replayTaskIdx) => replayTaskStats[physicalSublogIdx][replayTaskIdx];

        public void Reset()
        {
            foreach (var stats in sublogStats)
                stats.Reset();

            foreach (var replayTasks in replayTaskStats)
                foreach (var stats in replayTasks)
                    stats.Reset();
        }

        public IEnumerable<string> GetReportLines(long[] pagesBySublog, long[] recordsBySublog)
        {
            var aggregate = AggregateStats();
            var totalPages = pagesBySublog?.Sum() ?? aggregate.GetCount(AofReplayTimingPhase.PageTotal);
            var totalRecords = recordsBySublog?.Sum() ?? 0L;
            var pageTotalTicks = aggregate.GetTicks(AofReplayTimingPhase.PageTotal);
            if (pageTotalTicks <= 0)
                yield break;

            yield return FormatPhaseLine("Replay Page Total", pageTotalTicks, pageTotalTicks, totalPages, totalRecords, includeRecordRate: false);
            yield return FormatPhaseLine("Replay AppendLog Build", aggregate.GetTicks(AofReplayTimingPhase.AppendLogBuild), pageTotalTicks, totalPages, totalRecords, includeRecordRate: false);
            yield return FormatPhaseLine("Replay Session Consume", aggregate.GetTicks(AofReplayTimingPhase.SessionConsume), pageTotalTicks, totalPages, totalRecords, includeRecordRate: false);
            yield return FormatPhaseLine("Replay Cluster Parse Validate", aggregate.GetTicks(AofReplayTimingPhase.ClusterAppendLogParseValidate), pageTotalTicks, totalPages, totalRecords, includeRecordRate: false);
            yield return FormatPhaseLine("Replay PrimaryStream Total", aggregate.GetTicks(AofReplayTimingPhase.PrimaryStreamTotal), pageTotalTicks, totalPages, totalRecords, includeRecordRate: false);
            yield return FormatPhaseLine("Replay PrimaryStream Prechecks", aggregate.GetTicks(AofReplayTimingPhase.PrimaryStreamPrechecks), pageTotalTicks, totalPages, totalRecords, includeRecordRate: false);
            yield return FormatPhaseLine("Replay Raw Enqueue", aggregate.GetTicks(AofReplayTimingPhase.RawEnqueue), pageTotalTicks, totalPages, totalRecords, includeRecordRate: false);
            yield return FormatPhaseLine("Replay Total", aggregate.GetTicks(AofReplayTimingPhase.ReplayTotal), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay Record Apply", aggregate.GetTicks(AofReplayTimingPhase.ReplayRecordApply), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay Fast Commit Metadata", aggregate.GetTicks(AofReplayTimingPhase.ReplayFastCommitMetadata), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay Postchecks", aggregate.GetTicks(AofReplayTimingPhase.ReplayPostchecks), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay Txn Handling", aggregate.GetTicks(AofReplayTimingPhase.ReplayTxnHandling), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay Control Ops", aggregate.GetTicks(AofReplayTimingPhase.ReplayControlOps), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay Op Total", aggregate.GetTicks(AofReplayTimingPhase.ReplayOpTotal), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay Store Upsert", aggregate.GetTicks(AofReplayTimingPhase.ReplayStoreUpsert), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay Shared Upsert Primitive", aggregate.GetTicks(AofReplayTimingPhase.ReplaySharedUpsertPrimitive), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay Store Upsert Materialize", aggregate.GetTicks(AofReplayTimingPhase.ReplayStoreUpsertMaterialize), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay Store Upsert Cleanup", aggregate.GetTicks(AofReplayTimingPhase.ReplayStoreUpsertCleanup), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay Other ReplayOp", aggregate.GetTicks(AofReplayTimingPhase.ReplayOtherReplayOp), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay ProcessRecord Scaffolding", aggregate.GetTicks(AofReplayTimingPhase.ReplayProcessRecordScaffolding), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay SkipRecord", aggregate.GetTicks(AofReplayTimingPhase.ReplaySkipRecord), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay ProcessRecord Setup", aggregate.GetTicks(AofReplayTimingPhase.ReplayProcessRecordSetup), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatPhaseLine("Replay Replay-Only Overhead", Math.Max(0, aggregate.GetTicks(AofReplayTimingPhase.ReplayRecordApply) - aggregate.GetTicks(AofReplayTimingPhase.ReplaySharedUpsertPrimitive)), pageTotalTicks, totalPages, totalRecords, includeRecordRate: true);
            yield return FormatTopPhasesLine("Replay Top Phases", aggregate, pageTotalTicks);

            if (pagesBySublog == null || recordsBySublog == null)
                yield break;

            for (var sublogIdx = 0; sublogIdx < sublogStats.Length; sublogIdx++)
            {
                var sublogAggregate = AggregateSublogStats(sublogIdx);
                var sublogPageTicks = sublogAggregate.GetTicks(AofReplayTimingPhase.PageTotal);
                if (sublogPageTicks <= 0)
                    continue;

                var sublogPages = sublogIdx < pagesBySublog.Length ? pagesBySublog[sublogIdx] : sublogAggregate.GetCount(AofReplayTimingPhase.PageTotal);
                var sublogRecords = sublogIdx < recordsBySublog.Length ? recordsBySublog[sublogIdx] : 0L;
                yield return FormatSublogSummaryLine(sublogIdx, sublogAggregate, sublogPages, sublogRecords);
            }
        }

        AofReplayTimingStats AggregateStats()
        {
            var aggregate = new AofReplayTimingStats();
            for (var sublogIdx = 0; sublogIdx < sublogStats.Length; sublogIdx++)
            {
                MergeStats(aggregate, sublogStats[sublogIdx]);
                foreach (var taskStats in replayTaskStats[sublogIdx])
                    MergeStats(aggregate, taskStats);
            }
            return aggregate;
        }

        AofReplayTimingStats AggregateSublogStats(int sublogIdx)
        {
            var aggregate = new AofReplayTimingStats();
            MergeStats(aggregate, sublogStats[sublogIdx]);
            foreach (var taskStats in replayTaskStats[sublogIdx])
                MergeStats(aggregate, taskStats);
            return aggregate;
        }

        static void MergeStats(AofReplayTimingStats destination, AofReplayTimingStats source)
        {
            for (var idx = 0; idx < (int)AofReplayTimingPhase.Count; idx++)
            {
                var phase = (AofReplayTimingPhase)idx;
                destination.Add(phase, source.GetTicks(phase), source.GetCount(phase));
            }
        }

        static string FormatPhaseLine(string label, long ticks, long pageTotalTicks, long pages, long records, bool includeRecordRate)
        {
            var ms = TicksToMilliseconds(ticks);
            var pct = pageTotalTicks > 0 ? (ticks * 100.0) / pageTotalTicks : 0;
            var usPerPage = pages > 0 ? TicksToMicroseconds(ticks) / pages : 0;
            var builder = new StringBuilder();
            _ = builder.Append('[').Append(label).Append("]: ")
                .Append(ms.ToString("N2")).Append(" ms")
                .Append(" (").Append(pct.ToString("N2")).Append("% of page total")
                .Append(", pages=").Append(pages.ToString("N0"))
                .Append(", avg=").Append(usPerPage.ToString("N2")).Append(" us/page");

            if (records > 0)
            {
                _ = builder.Append(", records=").Append(records.ToString("N0"));
                if (includeRecordRate)
                {
                    var nsPerRecord = TicksToNanoseconds(ticks) / records;
                    _ = builder.Append(", avg=").Append(nsPerRecord.ToString("N2")).Append(" ns/record");
                }
            }

            _ = builder.Append(')');
            return builder.ToString();
        }

        static string FormatTopPhasesLine(string label, AofReplayTimingStats stats, long pageTotalTicks)
        {
            var candidates = new[]
            {
                AofReplayTimingPhase.ReplayTotal,
                AofReplayTimingPhase.RawEnqueue,
                AofReplayTimingPhase.PrimaryStreamPrechecks,
                AofReplayTimingPhase.ClusterAppendLogParseValidate,
                AofReplayTimingPhase.AppendLogBuild
            };

            var top = candidates
                .Select(phase => new
                {
                    Phase = phase,
                    Ticks = stats.GetTicks(phase)
                })
                .OrderByDescending(x => x.Ticks)
                .Take(3)
                .Where(x => x.Ticks > 0)
                .Select(x => $"{GetPhaseLabel(x.Phase)} {(pageTotalTicks > 0 ? (x.Ticks * 100.0 / pageTotalTicks).ToString("N2") : "0.00")}%");

            return $"[{label}]: {string.Join("; ", top)}";
        }

        static string FormatSublogSummaryLine(int sublogIdx, AofReplayTimingStats stats, long pages, long records)
        {
            var pageTicks = stats.GetTicks(AofReplayTimingPhase.PageTotal);
            var totalMs = TicksToMilliseconds(pageTicks);
            var avgUsPerPage = pages > 0 ? TicksToMicroseconds(pageTicks) / pages : 0;
            return $"[Replay Sublog {sublogIdx}]: pages={pages:N0}, records={records:N0}, page_total={totalMs:N2} ms, avg={avgUsPerPage:N2} us/page, top={FormatTopPhasesLine("unused", stats, pageTicks).Replace("[unused]: ", string.Empty)}";
        }

        static string GetPhaseLabel(AofReplayTimingPhase phase)
            => phase switch
            {
                AofReplayTimingPhase.AppendLogBuild => "AppendLog Build",
                AofReplayTimingPhase.ClusterAppendLogParseValidate => "Cluster Parse Validate",
                AofReplayTimingPhase.PrimaryStreamPrechecks => "PrimaryStream Prechecks",
                AofReplayTimingPhase.RawEnqueue => "Raw Enqueue",
                AofReplayTimingPhase.ReplayTotal => "Replay Total",
                _ => phase.ToString()
            };

        static double TicksToMilliseconds(long ticks) => ticks * 1000.0 / Stopwatch.Frequency;

        static double TicksToMicroseconds(long ticks) => ticks * 1_000_000.0 / Stopwatch.Frequency;

        static double TicksToNanoseconds(long ticks) => ticks * 1_000_000_000.0 / Stopwatch.Frequency;
    }
}
