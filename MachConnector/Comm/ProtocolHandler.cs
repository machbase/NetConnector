using Mach.Comm;
using Mach.Data.MachClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.Common;
using System.Data;
using System.Text;
using Mach.Core;
using Mach.Utility;
using System.Net.Sockets;

namespace Mach.Comm
{
    internal class ProtocolHandler
    {
        private SocketHandler m_socketHandler;
        private BufferedByteReader m_bufferedByteReader;

        public ProtocolHandler(SocketHandler socketHandler, BufferedByteReader bufferedByteReader)
        {
            m_socketHandler = socketHandler;
            m_bufferedByteReader = bufferedByteReader;
        }

        public void Request(Protocol aProtocol)
        {
            try
            {
                m_socketHandler.WriteBytes(aProtocol.SendData);
            }
            catch (SocketException se)
            {
                throw se;
            }
        }

        public void SetTimeout(int aTimeout)
        {
            m_socketHandler.RemainingTimeout = aTimeout;
        }

        public void UnsetTimeout()
        {
            m_socketHandler.RemainingTimeout = Constants.InfiniteTimeout;
        }

        public void ResponseInit()
        {
            // special response method
            InitProtocol sReturnPayload = new InitProtocol();
            ArraySegment<byte> sReturn = m_bufferedByteReader.ReadBytes(m_socketHandler, InitProtocol.CONNECT_PAYLOAD_RESPONSE_LEN);
            sReturnPayload.Interpret(sReturn); // read and verify
        }

        public Protocol Response()
        {
            return Response(null);
        }

        public Protocol Response(Protocol aProtocol)
        {
            return Response(aProtocol, null);
        }

        public Protocol Response(Protocol aProtocol, Protocol aAlterProtocol)
        {
            // from JDBC's readFromSoc()
            ArraySegment<byte> sRecvByte;
            PacketHeader sHeader = new PacketHeader();

            try
            {
                ArraySegment<byte> sRecvPartialByte = new ArraySegment<byte>();
                bool sSequentialMode = false;

                ReadPacketHeader(ref sHeader);

                // create new packet and generates error if it contains
                Protocol sProtocol = null;
                if (aProtocol != null)
                {
                    if (aProtocol.ProtocolType != sHeader.Protocol)
                    {
                        if (aAlterProtocol != null)
                        { 
                            if (aAlterProtocol.ProtocolType != sHeader.Protocol)
                            {
                                throw new MachException(MachErrorMsg.INVALID_PROTOCOL.FormatInvariant(
                                                                                        aAlterProtocol.ProtocolType.ToString(),
                                                                                        sHeader.Protocol));
                            }
                            else
                            {
                                sProtocol = aAlterProtocol;
                            }
                        }
                        else
                        {
                            throw new MachException(MachErrorMsg.INVALID_PROTOCOL.FormatInvariant(
                                                                                    aProtocol.ProtocolType.ToString(),
                                                                                    sHeader.Protocol));
                        }
                    }
                    else
                    {
                        sProtocol = aProtocol;
                    }
                }
                else
                {
                    sProtocol = CreateNewProtocol(sHeader);
                }

                switch ((ModeType)sHeader.Flag)
                {
                    case ModeType.MODE_COMPLETE:
                        sRecvPartialByte = m_bufferedByteReader.ReadBytes(m_socketHandler, sHeader.Length);
                        break;
                    case ModeType.MODE_BEGIN:
                        sRecvPartialByte = m_bufferedByteReader.ReadBytes(m_socketHandler, sHeader.Length);
                        sSequentialMode = true;
                        break;
                    case ModeType.MODE_MIDDLE:
                    case ModeType.MODE_END:
                    default:
                        throw new MachException(String.Format(MachErrorMsg.INVALID_INIT_PROTOCOL, sHeader.Flag));
                }

                if (sSequentialMode == true)
                {
                    List<ArraySegment<byte>> sRecvList = new List<ArraySegment<byte>>{
                        sRecvPartialByte
                    };
                    PacketHeader sContinueHeader = new PacketHeader();
                    bool isEnded = false;
                    while (isEnded == false)
                    {
                        ReadPacketHeader(ref sContinueHeader);

                        switch ((ModeType)sContinueHeader.Flag)
                        {
                            case ModeType.MODE_MIDDLE:
                                sRecvList.Add(m_bufferedByteReader.ReadBytes(m_socketHandler, sContinueHeader.Length));
                                break;
                            case ModeType.MODE_END:
                                sRecvList.Add(m_bufferedByteReader.ReadBytes(m_socketHandler, sContinueHeader.Length));
                                isEnded = true;
                                break;
                            case ModeType.MODE_BEGIN:
                            case ModeType.MODE_COMPLETE:
                            default:
                                throw new MachException(String.Format(MachErrorMsg.INVALID_NEXT_PROTOCOL, sContinueHeader.Flag));
                        }
                    }

                    sRecvByte = new ArraySegment<byte>(sRecvList.SelectMany(a => a.GetRealArray()).ToArray());
                }
                else
                {
                    sRecvByte = sRecvPartialByte;
                }

                // setting and interpret
                sProtocol.Header   = sHeader;
                sProtocol.RecvData = sRecvByte.GetRealArray();
                sProtocol.Interpret();

                return sProtocol;
            }
            catch (SocketException se)
            {
                throw se;
            }
            catch (MachException e)
            {
                throw e;
            }
        }

        private void ReadPacketHeader(ref PacketHeader aHeader)
        {
            var sHeader = m_bufferedByteReader.ReadBytes(m_socketHandler, 8); // header

            if (sHeader.Count != 8)
                throw new NotSupportedException();

            ulong sHeaderLong = BitConverter.ToUInt64(sHeader.Array, sHeader.Offset);

            aHeader.Protocol = (ProtocolType)((sHeaderLong & 0x000000FF00000000L) >> 32);
            aHeader.Flag = (ModeType)((sHeaderLong & 0x00000000C0000000L) >> 30);
            aHeader.Length = (int)(sHeaderLong & 0x000000003FFFFFFFL);
        }

        private Protocol CreateNewProtocol(PacketHeader aHeader)
        {
            Protocol sReturn = null;

            switch (aHeader.Protocol)
            {
                case ProtocolType.CONNECT_PROTOCOL:
                    sReturn = new ConnectProtocol();
                    break;
                case ProtocolType.EXECDIRECT_PROTOCOL:
                    sReturn = new ExecDirectProtocol();
                    break;
                case ProtocolType.ERROR_PROTOCOL:
                    sReturn = new ErrorProtocol();
                    break;
                case ProtocolType.DISCONNECT_PROTOCOL:
                case ProtocolType.INVALID_PROTOCOL:
                case ProtocolType.LARGE_PROTOCOL:
                case ProtocolType.TIMEOUT_PROTOCOL:
                case ProtocolType.ADMIN_PROTOCOL:
                case ProtocolType.PREPARE_PROTOCOL:
                case ProtocolType.EXECUTE_PROTOCOL:
                case ProtocolType.FETCH_PROTOCOL:
                case ProtocolType.FREE_PROTOCOL:
                case ProtocolType.APPEND_OPEN_PROTOCOL:
                case ProtocolType.APPEND_DATA_PROTOCOL:
                case ProtocolType.APPEND_CLOSE_PROTOCOL:
                    // TODO
                    break;
            }

            if (sReturn == null)
                throw new ArgumentNullException();

            return sReturn;
        }
    }
}
