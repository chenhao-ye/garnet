// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Resp.benchmark
{
    /// <summary>
    /// Key-popularity distribution of a benchmark workload. Hotness is defined over the global
    /// key index, so paths that share a keyset and a variant share the same hot keys; combining
    /// Zipf and ZipfRev across the replay/read knobs gives equally skewed but hot-set-disjoint
    /// workloads.
    /// </summary>
    public enum KeyDistribution
    {
        /// <summary>
        /// Every key is equally likely.
        /// </summary>
        Uniform,
        /// <summary>
        /// Key g is weighed 1/(g+1)^theta (theta from --zipf-theta, default 0.99); the hottest
        /// key is the first key (index 0).
        /// </summary>
        Zipf,
        /// <summary>
        /// Zipf weights with the hotness order reversed: the hottest key is the last key
        /// (index dbsize-1).
        /// </summary>
        ZipfRev,
    }
}