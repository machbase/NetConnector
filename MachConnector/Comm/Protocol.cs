using Mach.Data.MachClient;
using Mach.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mach.Comm
{
    public abstract class Protocol
    {
        protected ProtocolType m_protocolType;
        private PacketHeader m_header;
        private byte[] m_sendData;
        private byte[] m_recvData;
        private int m_length;
        private int m_offset;

        public PacketHeader Header { get => m_header; set => m_header = value; }

        internal byte[] SendData {
            get => m_sendData;
            set
            {
                m_sendData = value;
                m_length = m_sendData.Length;
            }
        }
        internal byte[] RecvData
        {
            get => m_recvData;
            set
            {
                m_recvData = value;
                m_length = m_recvData.Length;
            }
        }

        internal int Length { get => m_length; }
        internal int Offset { get => m_offset; set => m_offset = value; }
        public ProtocolType ProtocolType { get => m_protocolType; protected set => m_protocolType = value;
        }

        public Protocol()
        {
            Offset = 0;
        }

        public virtual void Interpret()
        {
            Offset = 0;
        }
    }

    public static class ProtocolConverter
    {
        public static Packet ReadNext(this Protocol aProtocol)
        {
            return ReadNext(aProtocol, PacketType.UNKNOWN_TYPE);
        }

        public static Packet ReadNext(this Protocol aProtocol, PacketType aPacketType)
        {
            if (aProtocol.Offset + 8 >= aProtocol.Length)
            {
                return null;
            }
            else
            {
                PacketType sPacketType = (PacketType)BitConverter.ToUInt32(aProtocol.RecvData, aProtocol.Offset);
                aProtocol.Offset += 4;
                DataType sDataType = (DataType)BitConverter.ToUInt32(aProtocol.RecvData, aProtocol.Offset);
                aProtocol.Offset += 4;

                Packet sReturnPacket = new Packet(sPacketType, sDataType, aProtocol.RecvData, aProtocol.Offset);
                aProtocol.Offset += sReturnPacket.Length; // move to packet's size

                if (aPacketType != PacketType.UNKNOWN_TYPE)
                    sReturnPacket.CheckPacketType(aPacketType);

                return sReturnPacket;
            }
        }

        public static void CheckResult(this Protocol aProtocol, Packet aPacket)
        {
            long sErrNum = aPacket.GetInt();
            Result sResult = (Result)aPacket.GetInt(4);

            if (sResult != Result.OK)
            {
                Packet sNext = aProtocol.ReadNext();
                string sMsg = null;

                while (sMsg == null)
                {
                    switch (sNext.PacketType)
                    {
                        case PacketType.RETURN_MESSAGE_ID:
                            sMsg = sNext.GetString();
                            break;
                        default:
                            break;
                    }
                    sNext = aProtocol.ReadNext();
                }

                throw new MachException(String.Format("[ERR-{0:00000} : {1}]", sErrNum, sMsg));
            }
        }

        public static void CheckAppendResult(this Protocol aProtocol, Packet aPacket)
        {
            long sErrNum = aPacket.GetInt();
            Result sResult = (Result)aPacket.GetInt(4);

            if (sResult != Result.OK)
            {
                Packet sNext = aProtocol.ReadNext();
                string sMsg = null;
                string sRowBuffer = null;

                while (sMsg == null || sRowBuffer == null)
                {
                    switch (sNext.PacketType)
                    {
                        case PacketType.RETURN_MESSAGE_ID:
                            sMsg = sNext.GetString();
                            break;
                        case PacketType.RETURN_EMESSAGE_ID:
                            sRowBuffer = sNext.GetString();
                            break;
                        default:
                            break;
                    }
                    sNext = aProtocol.ReadNext();
                }

                throw new MachAppendException(String.Format("[ERR-{0:00000} : {1}]", sErrNum, sMsg), sRowBuffer);
            }
        }
    }
}
