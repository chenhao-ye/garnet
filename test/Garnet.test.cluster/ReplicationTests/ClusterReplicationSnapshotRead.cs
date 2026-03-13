// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using System.Threading;
using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    [AllureNUnit]
    [TestFixture]
    [NonParallelizable]
    public class ClusterReplicationSnapshotRead : AllureTestBase
    {
        const int TestReplayTaskCount = 2;

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
        /// Verifies that ConsistentReadContext in snapshot mode (AofReadWithTimestamp=false) correctly
        /// reads replicated data when snapshotAddress=long.MaxValue (initial state — reads latest version).
        /// </summary>
        [Test, Order(1)]
        [Category("REPLICATION")]
        public void ClusterSnapshotReadBasicTest()
        {
            var primaryIndex = 0;
            var replicaIndex = 1;

            context.CreateInstances(2,
                disableObjects: true,
                enableAOF: true,
                sublogCount: 1,
                replayTaskCount: TestReplayTaskCount,
                aofReadWithTimestamp: false,
                aofSnapshotFreq: 5);
            context.CreateConnection();
            context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            var keyLength = 16;
            var kvpairCount = 32;
            context.kvPairs = [];

            context.SimplePopulateDB(disableObjects: true, keyLength, kvpairCount, primaryIndex);
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);
            Thread.Sleep(1000);
            context.SimpleValidateDB(disableObjects: true, replicaIndex);
        }

        /// <summary>
        /// Verifies that in snapshot mode, once replica replay catches up, reads expose the latest flushed value.
        /// With the flush-based snapshot advancement, there is no intentional delay window in which older values
        /// remain visible after the newer writes have fully replayed on the replica.
        /// </summary>
        [Test, Order(2)]
        [Category("REPLICATION")]
        public void ClusterSnapshotReadLatestTest()
        {
            const int snapshotFreqMs = 100;
            var primaryIndex = 0;
            var replicaIndex = 1;

            context.CreateInstances(2,
                disableObjects: true,
                enableAOF: true,
                sublogCount: 1,
                replayTaskCount: TestReplayTaskCount,
                aofReadWithTimestamp: false,
                aofSnapshotFreq: snapshotFreqMs);
            context.CreateConnection();
            context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            // Batch 1: write initial values to primary
            string[] keys = ["key_a", "key_b", "key_c"];
            string[] batch1 = ["snap_v1a", "snap_v1b", "snap_v1c"];
            string[] batch2 = ["snap_v2a", "snap_v2b", "snap_v2c"];

            for (var i = 0; i < keys.Length; i++)
            {
                var resp = context.clusterTestUtils.SetKey(primaryIndex,
                    Encoding.ASCII.GetBytes(keys[i]),
                    Encoding.ASCII.GetBytes(batch1[i]),
                    out _, out _, logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, resp);
            }

            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);

            // Batch 2: overwrite the same keys with new values
            for (var i = 0; i < keys.Length; i++)
            {
                var resp = context.clusterTestUtils.SetKey(primaryIndex,
                    Encoding.ASCII.GetBytes(keys[i]),
                    Encoding.ASCII.GetBytes(batch2[i]),
                    out _, out _, logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, resp);
            }

            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);

            for (var i = 0; i < keys.Length; i++)
            {
                var value = context.clusterTestUtils.GetKey(replicaIndex,
                    Encoding.ASCII.GetBytes(keys[i]),
                    out _, out _, out var responseState,
                    logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, responseState);
                ClassicAssert.AreEqual(batch2[i], value, $"Replica should expose latest replayed value for {keys[i]}");
            }
        }
    }
}
