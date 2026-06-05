// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Resp.benchmark
{
    /// <summary>
    /// Which replication-topology role this AofBench process plays. Combined fuses Replica and
    /// Client in one process; the split roles let the harness place each process on its own
    /// NUMA node (CPU and memory) or, in the future, on separate machines.
    /// </summary>
    public enum AofBenchRole
    {
        /// <summary>
        /// Replica and Client in one process (in-process or loopback readers).
        /// </summary>
        Combined,
        /// <summary>
        /// Reserved for a future bench that streams AOF from a real primary; not implemented.
        /// </summary>
        Primary,
        /// <summary>
        /// AOF generation + embedded Garnet server + replay workers. Serves remote Client-role
        /// readers, paces them over the bench control channel, and idles after the final pass
        /// until killed.
        /// </summary>
        Replica,
        /// <summary>
        /// GarnetClientSession GET readers driven by the Replica's control channel. Regenerates
        /// the key set locally (key generation is deterministic from dbsize/keylength).
        /// </summary>
        Client,
    }
}