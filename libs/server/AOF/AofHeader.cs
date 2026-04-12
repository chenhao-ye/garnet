// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;

namespace Garnet.server
{
    internal enum AofHeaderType : byte
    {
        BasicHeader,
        ShardedHeader,
        TransactionHeader
    }

    /// <summary>
    /// Used for coordinated operations
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    unsafe struct AofTransactionHeader
    {
        public const int TotalSize = AofShardedHeader.TotalSize + 2 + 32;
        // maximum 256 replay tasks per physical sublog, hence 32 bytes bitmap
        public const int ReplayTaskAccessVectorBytes = 32;

        /// <summary>
        /// AofShardedHeader used with multi-log
        /// </summary>
        [FieldOffset(0)]
        public AofShardedHeader shardedHeader;

        /// <summary>
        /// Used for synchronizing virtual sublog replay
        /// NOTE: This stores the total number of replay tasks that participate in a given transaction.
        /// </summary>
        [FieldOffset(AofShardedHeader.TotalSize)]
        public short participantCount;

        /// <summary>
        /// Used to track replay task participating in the txn
        /// </summary>
        [FieldOffset(AofShardedHeader.TotalSize + 2)]
        public fixed byte replayTaskAccessVector[ReplayTaskAccessVectorBytes];
    }

    /// <summary>
    /// Used for sharded log to add a k
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    struct AofShardedHeader
    {
        public const int TotalSize = AofHeader.TotalSize + 8;

        /// <summary>
        /// Basic AOF header used with single log.
        /// </summary>
        [FieldOffset(0)]
        public AofHeader basicHeader;

        /// <summary>
        /// Used with multi-log to implement read consistency protocol.
        /// </summary>
        [FieldOffset(AofHeader.TotalSize)]
        public long sequenceNumber;
    };

    /// <summary>
    /// Basic AOF header
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    struct AofHeader
    {
        public static unsafe byte* SkipHeader(byte* entryPtr)
        {
            var header = *(AofHeader*)entryPtr;
            return header.headerType switch
            {
                AofHeaderType.BasicHeader => entryPtr + TotalSize,
                AofHeaderType.ShardedHeader => entryPtr + AofShardedHeader.TotalSize,
                AofHeaderType.TransactionHeader => entryPtr + AofTransactionHeader.TotalSize,
                _ => throw new GarnetException($"Type not supported {header.headerType}"),
            };
        }

        public const int TotalSize = 16;

        // Important: Update version number whenever any of the following change:
        // * Layout, size, contents of this struct
        // * Any of the AofEntryType or AofStoreType enums' existing value mappings
        // * SpanByte format or header
        const byte AofHeaderVersion = 2;

        /// <summary>
        /// 0-bit in padding is used to indicate that the log contains AofExtendedHeader
        /// </summary>
        internal const byte ShardedLogFlag = 1;

        /// <summary>
        /// Mask for extracting AofHeaderType from padding (bits 0-1)
        /// </summary>
        internal const byte HeaderTypeMask = 0x03;

        /// <summary>
        /// Bit shift for replay tag within padding (bits 2-7)
        /// </summary>
        internal const int ReplayTagShift = 2;

        /// <summary>
        /// Mask for replay tag after shifting (6 bits, values 0-63)
        /// </summary>
        internal const byte ReplayTagMask = 0x3F;

        /// <summary>
        /// Construct padding byte from header type and replay tag
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte MakePadding(AofHeaderType type, byte replayTag)
            => (byte)((byte)type | ((replayTag & ReplayTagMask) << ReplayTagShift));

        /// <summary>
        /// Version of AOF
        /// </summary>
        [FieldOffset(0)]
        public byte aofHeaderVersion;
        /// <summary>
        /// Padding: bits 0-1 store AofHeaderType, bits 2-7 store replay tag
        /// </summary>
        [FieldOffset(1)]
        public byte padding;

        /// <summary>
        /// Header type extracted from padding (bits 0-1)
        /// </summary>
        internal readonly AofHeaderType headerType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (AofHeaderType)(padding & HeaderTypeMask);
        }

        /// <summary>
        /// Replay tag extracted from padding (bits 2-7, 6-bit value 0-63)
        /// </summary>
        internal readonly byte replayTag
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (byte)((padding >> ReplayTagShift) & ReplayTagMask);
        }
        /// <summary>
        /// Type of operation
        /// </summary>
        [FieldOffset(2)]
        public AofEntryType opType;
        /// <summary>
        /// Procedure ID
        /// </summary>
        [FieldOffset(3)]
        public byte procedureId;
        /// <summary>
        /// Store version
        /// </summary>
        [FieldOffset(4)]
        public long storeVersion;
        /// <summary>
        /// Session ID
        /// </summary>
        [FieldOffset(12)]
        public int sessionID;
        /// <summary>
        /// Transaction ID
        /// </summary>
        [FieldOffset(12)]
        public int txnID;
        /// <summary>
        /// Unsafe truncate log (used with FLUSH command)
        /// </summary>
        [FieldOffset(1)]
        public byte unsafeTruncateLog;
        /// <summary>
        /// Database ID (used with FLUSH command)
        /// </summary>
        [FieldOffset(3)]
        public byte databaseId;

        public AofHeader()
        {
            this.aofHeaderVersion = AofHeaderVersion;
        }
    }
}