using Mach.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mach.Comm
{
    public class Packet
    {
        private int m_length;

        public Packet(PacketType aProtocolType, DataType aDataType, byte[] aByte, int aOffset)
        {
            PacketType = aProtocolType;
            DataType = aDataType;
            m_length = 0;

            if (DataType == DataType.DATA_STRING_TYPE ||
                DataType == DataType.DATA_BINARY_TYPE ||
                DataType == DataType.DATA_ROWS_TYPE)
            {
                ulong sLen = BitConverter.ToUInt64(aByte, aOffset);
                aOffset += 8;
                m_length += 8;
                Segment = new ArraySegment<byte>(aByte, aOffset, (int)sLen);
                // align to 8
                m_length += (int)((sLen / 8) * 8);
                m_length += (int)(((sLen % 8 == 0) ? 0 : 1) * 8);
            }
            else
            {
                if (DataType == DataType.DATA_SCHAR_TYPE ||
                    DataType == DataType.DATA_UCHAR_TYPE)
                {
                    m_length += 1;
                }
                else if (DataType == DataType.DATA_SSHORT_TYPE ||
                         DataType == DataType.DATA_USHORT_TYPE)
                {
                    m_length += 2;
                }
                else if (DataType == DataType.DATA_SINT_TYPE ||
                         DataType == DataType.DATA_UINT_TYPE)
                {
                    m_length += 4;
                }
                else if (DataType == DataType.DATA_SLONG_TYPE ||
                         DataType == DataType.DATA_ULONG_TYPE)
                {
                    m_length += 8;
                }

                if (m_length != 0)
                {
                    Segment = new ArraySegment<byte>(aByte, aOffset, 8);
                }
                else
                {
                    Segment = new ArraySegment<byte>();
                }
            }
        }

        public ArraySegment<byte> Segment { get; set; }
        public PacketType PacketType { get; set; }
        public DataType DataType { get; set; }
        public int Length { get => m_length; }
    }
}
