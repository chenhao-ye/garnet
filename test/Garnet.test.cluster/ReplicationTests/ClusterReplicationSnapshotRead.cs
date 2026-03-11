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
        const int TestSublogCount = 2;

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
                sublogCount: TestSublogCount,
                aofReadWithTimestamp: false,
                aofSnapshotFreq: 5);
            context.CreateConnection();
            context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            var keyLength = 16;
            var kvpairCount = 32;
            context.kvPairs = [];

            context.SimplePopulateDB(disableObjects: true, keyLength, kvpairCount, primaryIndex);
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);
            context.SimpleValidateDB(disableObjects: true, replicaIndex);
        }

        /// <summary>
        /// Verifies that ConsistentReadContext snapshot reads respect the snapshotAddress boundary.
        /// Writes batch1, waits for snapshot to advance past batch1, overwrites with batch2, then
        /// asserts replica reads return batch1 (snapshot-bounded) before the next snapshot fires,
        /// and batch2 after the snapshot advances past batch2.
        /// </summary>
        [Test, Order(2)]
        [Category("REPLICATION")]
        public void ClusterSnapshotReadSnapshotBoundaryTest()
        {
            // aofSnapshotFreq = 100ms; snapshotWaitMs >> freq to ensure at least one checkpoint fires.
            // After batch2 AOF sync completes, reads happen before the next 100ms interval elapses.
            const int snapshotFreqMs = 100;
            const int snapshotWaitMs = 1000;
            var primaryIndex = 0;
            var replicaIndex = 1;

            context.CreateInstances(2,
                disableObjects: true,
                enableAOF: true,
                sublogCount: TestSublogCount,
                aofReadWithTimestamp: false,
                aofSnapshotFreq: snapshotFreqMs);
            context.CreateConnection();
            context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            // Batch 1: write initial values to primary
            string[] keys = ["{_}sk1", "{_}sk2", "{_}sk3"];
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

            // Wait for batch1 to sync to replica, then sleep so the snapshot fires
            // and advances snapshotAddress to cover batch1's log tail on the replica.
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);
            Thread.Sleep(snapshotWaitMs);

            // Batch 2: overwrite the same keys with new values
            for (var i = 0; i < keys.Length; i++)
            {
                var resp = context.clusterTestUtils.SetKey(primaryIndex,
                    Encoding.ASCII.GetBytes(keys[i]),
                    Encoding.ASCII.GetBytes(batch2[i]),
                    out _, out _, logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, resp);
            }

            // Wait for batch2 to sync. Reads happen immediately — well within one snapshot interval
            // (100ms) — so snapshotAddress still points to batch1's tail. SnapshotVersionScanFunctions
            // skips batch2 records (address > snapshotAddress) and returns batch1 versions.
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);

            for (var i = 0; i < keys.Length; i++)
            {
                var value = context.clusterTestUtils.GetKey(replicaIndex,
                    Encoding.ASCII.GetBytes(keys[i]),
                    out _, out _, out var responseState,
                    logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, responseState);
                ClassicAssert.AreEqual(batch1[i], value, $"Snapshot read should return batch1 value for {keys[i]}");
            }

            // Sleep again so the snapshot fires and advances past batch2.
            Thread.Sleep(snapshotWaitMs);

            // Now reads should see batch2 values.
            for (var i = 0; i < keys.Length; i++)
            {
                var value = context.clusterTestUtils.GetKey(replicaIndex,
                    Encoding.ASCII.GetBytes(keys[i]),
                    out _, out _, out var responseState,
                    logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, responseState);
                ClassicAssert.AreEqual(batch2[i], value, $"Snapshot should have advanced; expected batch2 value for {keys[i]}");
            }
        }
    }
}
