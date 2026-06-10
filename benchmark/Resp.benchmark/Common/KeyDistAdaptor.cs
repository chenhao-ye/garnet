// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Resp.benchmark
{
    /// <summary>
    /// Samples global key indices under a <see cref="KeyDistribution"/>. Owns its RNG, seeded
    /// deterministically from the constructor seed, so callers only invoke <see cref="Next"/>.
    /// </summary>
    public sealed class KeyDistAdaptor
    {
        readonly KeyDistribution dist;
        readonly int keyCount;
        readonly Random rng;
        readonly ZipfGenerator zipfg;

        public KeyDistAdaptor(KeyDistribution dist, int keyCount, int seed, double theta)
        {
            this.dist = dist;
            this.keyCount = keyCount;
            // Only build the generator the distribution needs: the zipf setup is O(keyCount)
            // (zeta sum), so Uniform must not pay it.
            if (dist == KeyDistribution.Uniform)
                rng = new Random(seed);
            else
                zipfg = new ZipfGenerator(new RandomGenerator((uint)seed), keyCount, theta);
        }

        /// <summary>
        /// Next key index in [0, keyCount). The zipf variants map rank 0 (the hottest) to the
        /// first key (Zipf) or the last key (ZipfRev).
        /// </summary>
        public int Next() => dist switch
        {
            KeyDistribution.Uniform => rng.Next(keyCount),
            KeyDistribution.Zipf => zipfg.Next(),
            KeyDistribution.ZipfRev => keyCount - 1 - zipfg.Next(),
            _ => throw new ArgumentOutOfRangeException(nameof(dist), dist, "Unknown key distribution"),
        };
    }
}