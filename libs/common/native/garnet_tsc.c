// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
//
// TSC reader for Garnet.common's SequenceNumberGenerator. Built into
// libgarnet_tsc.so by the MSBuild target in Garnet.common.csproj and
// PInvoked with SuppressGCTransition.

#include <stdint.h>

#if defined(__x86_64__) || defined(_M_X64)

__attribute__((visibility("default")))
uint64_t garnet_read_tsc(void)
{
    uint32_t lo, hi;
    __asm__ volatile ("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
}

#else

#include <time.h>

__attribute__((visibility("default")))
uint64_t garnet_read_tsc(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

#endif
