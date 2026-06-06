// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Resp.benchmark
{
    /// <summary>
    /// Key-popularity distribution of a benchmark workload. Hotness is defined over the global
    /// key index (key 0 is the hottest), so paths that share a keyset share the same hot keys.
    /// </summary>
    public enum KeyDistribution
    {
        /// <summary>
        /// Every key is equally likely.
        /// </summary>
        Uniform,
        /// <summary>
        /// Key g is weighed 1/(g+1)^theta with theta = <see cref="AofGen.ZipfTheta"/>.
        /// </summary>
        Zipf,
    }
}