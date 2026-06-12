// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Reflection;
using Tsavorite.core;

namespace Resp.benchmark
{
    internal static class TsavoriteExtension
    {
        private static readonly MethodInfo AlignMethod;
        private static readonly FieldInfo HeaderSizeField;
        private static readonly MethodInfo SetHeaderMethod;

        static TsavoriteExtension()
        {
            var type = typeof(TsavoriteLog);
            var flags = BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;

            AlignMethod = type.GetMethod("Align", BindingFlags.NonPublic | BindingFlags.Static);
            HeaderSizeField = type.GetField("headerSize", flags);
            SetHeaderMethod = type.GetMethod("SetHeader", flags);
        }

        // Serialized payload length of one record, before the log's entry header and alignment.
        static unsafe int RecordLength<THeader, TInput>(ReadOnlySpan<byte> item1, ReadOnlySpan<byte> item2, ref TInput input)
            where THeader : unmanaged where TInput : IStoreInput
            => sizeof(THeader) + item1.TotalSize() + item2.TotalSize() + input.SerializedLength;

        /// <summary>
        /// Bytes one <see cref="DummyEnqueue{THeader, TInput}"/> record occupies in a page
        /// (entry header plus aligned payload). All records with the same header type and item
        /// lengths occupy the same number of bytes, so page capacity is pageSize / this value.
        /// </summary>
        public static int DummyEnqueueLength<THeader, TInput>(
            this TsavoriteLog log,
            ReadOnlySpan<byte> item1,
            ReadOnlySpan<byte> item2,
            ref TInput input)
            where THeader : unmanaged where TInput : IStoreInput
        {
            var headerSize = (int)HeaderSizeField.GetValue(log);
            return headerSize + (int)AlignMethod.Invoke(null, [RecordLength<THeader, TInput>(item1, item2, ref input)]);
        }

        /// <summary>
        /// DummyEnqueue to provided buffer if there is enough space.
        /// Used to simulate AOF layout
        /// </summary>
        /// <typeparam name="THeader"></typeparam>
        /// <typeparam name="TInput"></typeparam>
        /// <param name="log"></param>
        /// <param name="beginPageAddress"></param>
        /// <param name="endPageAddress"></param>
        /// <param name="userHeader"></param>
        /// <param name="item1"></param>
        /// <param name="item2"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        public static unsafe bool DummyEnqueue<THeader, TInput>(
            this TsavoriteLog log,
            ref byte* beginPageAddress,
            byte* endPageAddress,
            THeader userHeader,
            ReadOnlySpan<byte> item1,
            ReadOnlySpan<byte> item2,
            ref TInput input)
            where THeader : unmanaged where TInput : IStoreInput
        {
            var headerSize = (int)HeaderSizeField.GetValue(log);
            var length = RecordLength<THeader, TInput>(item1, item2, ref input);
            var allocatedLength = headerSize + (int)AlignMethod.Invoke(null, [length]);

            if (beginPageAddress + allocatedLength > endPageAddress)
                return false;

            var physicalAddress = beginPageAddress;
            *(THeader*)(physicalAddress + headerSize) = userHeader;
            var offset = headerSize + sizeof(THeader);
            item1.SerializeTo(new Span<byte>(physicalAddress + offset, allocatedLength - offset));
            offset += item1.TotalSize();
            item2.SerializeTo(new Span<byte>(physicalAddress + offset, allocatedLength - offset));
            offset += item2.TotalSize();
            input.CopyTo(physicalAddress + offset, input.SerializedLength);

            SetHeaderMethod.Invoke(log, [length, (IntPtr)beginPageAddress]);
            beginPageAddress += allocatedLength;
            return true;
        }
    }
}