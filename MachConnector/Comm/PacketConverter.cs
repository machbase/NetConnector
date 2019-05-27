using Mach.Data.MachClient;
using Mach.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mach.Comm
{
    public struct PacketHeader
    {
        public ProtocolType Protocol;
        public ModeType     Flag;
        public int          Length;
    }

    public static class PacketConverter
    {
        public static byte[] CreateProtocolHeader(ProtocolType aPayloadType, ModeType aModeType, int aStmtId, int aLength)
        {
#pragma warning disable CS0675 // 부호 확장된 피연산자에 비트 OR 연산자를 사용했습니다.
            ulong sPacketHeader = ((ulong)aStmtId << 48 | (ulong)aPayloadType << 32 | (ulong)aModeType.ToValue() << 30 | (ulong)aLength);
#pragma warning restore CS0675 // 부호 확장된 피연산자에 비트 OR 연산자를 사용했습니다.
            return BitConverter.GetBytes(sPacketHeader);
        }

        public static byte[] CreateDataHeader(PacketType aProtocol, DataType aData)
        {
            var sList = new List<byte[]>
            {
                BitConverter.GetBytes(aProtocol.ToValue()),
                BitConverter.GetBytes(aData.ToValue())
            };

            return sList.MakeArray();
        }

        public static int GetInt(this Packet aPacket)
        {
            return BitConverter.ToInt32(aPacket.Segment.Array, aPacket.Segment.Offset);
        }

        public static int GetInt(this Packet aPacket, int aOffset)
        {
            return BitConverter.ToInt32(aPacket.Segment.Array, aPacket.Segment.Offset + aOffset);
        }

        public static long GetLong(this Packet aPacket)
        {
            return BitConverter.ToInt64(aPacket.Segment.Array, aPacket.Segment.Offset);
        }

        public static long GetLong(this Packet aPacket, int aOffset)
        {
            return BitConverter.ToInt64(aPacket.Segment.Array, aPacket.Segment.Offset + aOffset);
        }

        public static string GetString(this Packet aPacket)
        {
            return aPacket.Segment.GetString();
        }

        public static void CheckPacketType(this Packet aPacket, PacketType aType)
        {
            if (aPacket.PacketType != aType)
                throw new MachException(MachErrorMsg.INVALID_NEXT_PROTOCOL.FormatInvariant(aType));
        }

        public static byte[] WriteBytesWithLength(byte[] aByte)
        {
            // write its length and string
            var sList = new List<byte[]>
            {
                WriteLength(aByte.Length),
                WriteBytes(aByte)
            };
            return sList.MakeArray();
        }

        public static byte[] WriteStringWithLength(string aString)
        {
            // write its length and string
            var sList = new List<byte[]>
            {
                WriteLength(aString.Length),
                WriteString(aString)
            };
            return sList.MakeArray();
        }

        public static byte[] WriteBytes(byte[] aBytes)
        {
            int sLength = aBytes.Length.AlignLength();
            byte[] sByte = new byte[sLength];
            Array.Copy(aBytes, sByte, aBytes.Length);

            return sByte;
        }

        public static byte[] WriteString(string aString)
        {
            /** NOTE : UTF8 only && No Endian considered **/
            int sLength = aString.Length.AlignLength();
            
            byte[] sByte = new byte[sLength];
            Encoding.UTF8.GetBytes(aString, 0, aString.Length, sByte, 0);

            // null-terminating
            if (aString.Length < sLength)
                sByte[aString.Length] = (byte)'\0';

            return sByte;
        }

        public static byte[] WriteLength(long aLong)
        {
            // NOTE align
            return BitConverter.GetBytes(aLong).FitToEndian();
        }

        public static byte[] FitToEndian(this byte[] aByte)
        {
            if (!BitConverter.IsLittleEndian)
                Array.Reverse(aByte);
            return aByte;
        }

        public static byte[] GetRealArray(this ArraySegment<byte> aSeg)
        {
            byte[] sReturn = new byte[aSeg.Count];
            Array.Copy(aSeg.Array, aSeg.Offset, sReturn, 0, aSeg.Count);
            return sReturn;
        }

        public static string GetString(this ArraySegment<byte> aSeg)
        {
            return Encoding.UTF8.GetString(aSeg.Array, aSeg.Offset, aSeg.Count);
        }

        public static byte[] GetArray(this ArraySegment<byte> aSeg, int aLength)
        {
            byte[] sReturn = new byte[aLength];
            if (aLength > (aSeg.Count - aSeg.Offset))
            {
                return null;
            }
            else
            {
                Array.Copy(aSeg.Array, aSeg.Offset, sReturn, 0, aLength);
                return sReturn;
            }
        }
    }
}
