using Mach.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mach.Core
{
    public sealed class BufferedByteReader
    {
        //public BufferedByteReader() => m_buffer = new byte[16384];
        public BufferedByteReader() { }

        public ArraySegment<byte> ReadBytes(SocketHandler aStreamHdl, int aCount)
        {

            byte[] sRecvByte = new byte[aCount];
            var sRecvArraySeg = new ArraySegment<byte>(sRecvByte, 0, aCount);
            int sRecvCount = 0;

            while (sRecvCount < aCount)
            {
                sRecvCount += aStreamHdl.ReadBytes(sRecvArraySeg);
                sRecvArraySeg = new ArraySegment<byte>(sRecvByte, sRecvCount, aCount - sRecvCount);
            }

            if (aCount != sRecvCount)
                throw new InvalidOperationException();

            return new ArraySegment<byte>(sRecvByte, 0, sRecvByte.Length);

            //// check if read can be satisfied from the buffer
            //if (m_remainingData.Count >= aCount)
            //{
            //    var readBytes = m_remainingData.Slice(0, aCount);
            //    m_remainingData = m_remainingData.Slice(aCount);
            //    return readBytes;
            //}

            //// get a buffer big enough to hold all the data, and move any buffered data to the beginning
            //var buffer = aCount > m_buffer.Length ? new byte[aCount] : m_buffer;
            //if (m_remainingData.Count > 0)
            //{
            //    Buffer.BlockCopy(m_remainingData.Array, m_remainingData.Offset, buffer, 0, m_remainingData.Count);
            //    m_remainingData = new ArraySegment<byte>(buffer, 0, m_remainingData.Count);
            //}

            //return ReadBytesInternal(aStreamHdl, new ArraySegment<byte>(buffer, m_remainingData.Count, buffer.Length - m_remainingData.Count), aCount);
        }

        //private ArraySegment<byte> ReadBytesInternal(SocketHandler aStreamHandler, ArraySegment<byte> buffer, int totalBytesToRead)
        //{
        //    while (true)
        //    {
        //        var readBytesCount = aStreamHandler.ReadBytes(buffer);
        //        if (readBytesCount == 0)
        //        {
        //            var data = m_remainingData;
        //            m_remainingData = new ArraySegment<byte>();
        //            return data;
        //        }

        //        var bufferSize = buffer.Offset + readBytesCount;
        //        if (bufferSize >= totalBytesToRead)
        //        {
        //            var bufferBytes = new ArraySegment<byte>(buffer.Array, 0, bufferSize);
        //            var requestedBytes = bufferBytes.Slice(0, totalBytesToRead);
        //            m_remainingData = bufferBytes.Slice(totalBytesToRead);
        //            return requestedBytes;
        //        }

        //        buffer = buffer.Slice(readBytesCount);
        //    }
        //}

        //readonly byte[] m_buffer;
        //ArraySegment<byte> m_remainingData;
    }
}
