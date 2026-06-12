// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Numerics;
using System.Text;
using Allure.NUnit;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster.MultiLogTests
{
    [AllureNUnit]
    [TestFixture]
    [NonParallelizable]
    public class ClusterMultiLogQuiescenceTests : AllureTestBase
    {
        ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown()
        {
            context?.TearDown();
        }

        /// <summary>
        /// Finds a printable key whose hash routes to the requested virtual sublog under the given
        /// (physical sublog count, replay task count) configuration, mirroring GarnetLog's hash
        /// decomposition (physical index from the low bits, replay task from the replay tag).
        /// </summary>
        static byte[] FindKeyForVirtualSublog(int virtualSublogIdx, int sublogCount, int replayTaskCount)
        {
            const int maxAttempts = 1 << 16;
            var physicalShift = BitOperations.Log2((uint)sublogCount);
            for (var i = 0; i < maxAttempts; i++)
            {
                var key = Encoding.ASCII.GetBytes($"qkey_{i}");
                var hash = (ulong)GarnetLog.HASH(key);
                var physicalIdx = (int)(hash & (ulong)(sublogCount - 1));
                var replayTag = (int)((hash >> physicalShift) & 0x3F);
                var taskIdx = replayTag & (replayTaskCount - 1);
                if ((physicalIdx * replayTaskCount) + taskIdx == virtualSublogIdx)
                    return key;
            }
            Assert.Fail($"No key found for virtual sublog {virtualSublogIdx} within {maxAttempts} attempts");
            return null;
        }

        /// <summary>
        /// A session that has advanced its timestamp by reading a freshly written key must not
        /// block indefinitely when it then reads a key on a sublog that has gone quiescent: the
        /// primary's in-band time pulses (CLUSTER ADVANCE_TIME, sent by each sublog's AOF sync
        /// task after AofTailWitnessFreq of idleness) keep the idle sublog's logical time flowing
        /// on the replica. Covers both sharding axes: physical sublogs (m=2, n=1) and virtual
        /// sublogs over a single physical log (m=1, n=2).
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        public void ClusterMultiLogQuiescentSublogReadTest([Values] bool physicalSharding)
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var sublogCount = physicalSharding ? 2 : 1;
            var replayTaskCount = physicalSharding ? 1 : 2;

            context.CreateInstances(2,
                disableObjects: true,
                enableAOF: true,
                sublogCount: sublogCount,
                replayTaskCount: replayTaskCount);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            // One key per virtual sublog; the LAST write goes to virtual sublog 1, so its sequence
            // number is the largest in the system and virtual sublog 0 is quiescent afterwards.
            var keyOnSublog0 = FindKeyForVirtualSublog(0, sublogCount, replayTaskCount);
            var keyOnSublog1 = FindKeyForVirtualSublog(1, sublogCount, replayTaskCount);

            var respState = context.clusterTestUtils.SetKey(primaryIndex, keyOnSublog0, Encoding.ASCII.GetBytes("v0"), out _, out _, logger: context.logger);
            ClassicAssert.AreEqual(ResponseState.OK, respState);
            respState = context.clusterTestUtils.SetKey(primaryIndex, keyOnSublog1, Encoding.ASCII.GetBytes("v1"), out _, out _, logger: context.logger);
            ClassicAssert.AreEqual(ResponseState.OK, respState);

            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);

            // Read the freshest key first: the session's timestamp advances to the system's
            // largest sequence number. The subsequent read on quiescent sublog 0 then fails the
            // freshness check until a time pulse advances that sublog, so it must complete
            // promptly instead of waiting out the replica sync timeout.
            var value1 = context.clusterTestUtils.GetKey(replicaIndex, keyOnSublog1, out _, out _, out var state1, logger: context.logger);
            ClassicAssert.AreEqual(ResponseState.OK, state1);
            ClassicAssert.AreEqual("v1", value1);

            var stopwatch = Stopwatch.StartNew();
            var value0 = context.clusterTestUtils.GetKey(replicaIndex, keyOnSublog0, out _, out _, out var state0, logger: context.logger);
            stopwatch.Stop();
            ClassicAssert.AreEqual(ResponseState.OK, state0);
            ClassicAssert.AreEqual("v0", value0);
            ClassicAssert.Less(stopwatch.ElapsedMilliseconds, 5000, "read on a quiescent sublog should be unblocked by a time pulse");
        }
    }
}