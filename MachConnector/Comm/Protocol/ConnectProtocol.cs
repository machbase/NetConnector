using Mach.Core;
using Mach.Core.Statement;
using Mach.Data.MachClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Mach.Comm
{
    class ConnectProtocol : Protocol
    {
        public ConnectProtocol() 
            : base()
        {
            base.ProtocolType = ProtocolType.CONNECT_PROTOCOL;
        }

        /* ------------------------- */

        public void Generate(ConnectionSettings aCsb)
        {
            var sDict = new Dictionary<string, byte[]>
            {
                { "HEADER",       null },
                { "VERSION_H",    PacketConverter.CreateDataHeader(PacketType.CONNECT_VERSION_ID, DataType.DATA_ULONG_TYPE) },
                { "VERSION",      MakeVersion() },
                { "CLIENT_ID_H",  PacketConverter.CreateDataHeader(PacketType.CONNECT_CLIENT_ID, DataType.DATA_STRING_TYPE) },
                { "CLIENT_ID",    PacketConverter.WriteStringWithLength("CNET") },
                { "LANG_H",       PacketConverter.CreateDataHeader(PacketType.CONNECT_LANG_ID, DataType.DATA_STRING_TYPE) },
                { "LANG",         PacketConverter.WriteStringWithLength("UTF-8") }, // TODO is it safe? US7ASCII?
                { "DATABASE_H",   PacketConverter.CreateDataHeader(PacketType.CONNECT_DATABASE_ID, DataType.DATA_STRING_TYPE) },
                { "DATABASE",     PacketConverter.WriteStringWithLength(aCsb.Database) },
                { "USER_H",       PacketConverter.CreateDataHeader(PacketType.CONNECT_USER_ID, DataType.DATA_STRING_TYPE) },
                { "USER",         PacketConverter.WriteStringWithLength(aCsb.UserID) },
                { "PASSWORD_H",   PacketConverter.CreateDataHeader(PacketType.CONNECT_PASSWORD_ID, DataType.DATA_STRING_TYPE) },
                { "PASSWORD",     PacketConverter.WriteStringWithLength(aCsb.Password) },
                { "TIMEOUT_H",    PacketConverter.CreateDataHeader(PacketType.CONNECT_TIMEOUT_ID, DataType.DATA_ULONG_TYPE) },
                { "TIMEOUT",      BitConverter.GetBytes((long)aCsb.ConnectionTimeout) },
                { "IP_H",         PacketConverter.CreateDataHeader(PacketType.CONNECT_IP_ID, DataType.DATA_STRING_TYPE) },
                { "IP",           PacketConverter.WriteStringWithLength(GetLocalIPAddress()) },
                { "SHOWHIDDNE_H", PacketConverter.CreateDataHeader(PacketType.CONNECT_SHC_ID, DataType.DATA_UINT_TYPE) },
                { "SHOWHIDDNE",   BitConverter.GetBytes(aCsb.ShowHiddenColumns) },
                // NOTE if you want to leave the last field as string, please add more padding field such as (DUMMY_INT)
                //      or switch the string field with upper one if you can. (in this case, IP was the last, but it switched)
            };
            sDict["HEADER"] = PacketConverter.CreateProtocolHeader(ProtocolType,
                                                                  ModeType.MODE_COMPLETE,
                                                                  0, // unused
                                                                  sDict.Values.Sum(a => (a != null) ? a.Length : 0));

            base.SendData = sDict.Values.SelectMany(x => x).ToArray();
        }

        public override void Interpret()
        {
            base.Interpret();

            Packet sNext = this.ReadNext();

            while (sNext != null)
            {
                switch (sNext.PacketType)
                {
                    case PacketType.RETURN_RESULT_ID:
                        this.CheckResult(sNext);
                        break;
                    case PacketType.CONNECT_VERSION_ID:
                        SetVersion(sNext);
                        break;
                    default:
                        // TODO
                        break;
                }
                sNext = this.ReadNext();
            }
        }

        /* ------------------------- */

        public string m_versionString { get; private set; }

        private void SetVersion(Packet aPacket)
        {
            ulong versionLong = (ulong)aPacket.GetLong();
            int major = (int)((versionLong >> 48) & 0xFFFF);
            int minor = (int)((versionLong >> 32) & 0xFFFF);
            int patch = (int)((versionLong) & 0xFFFFFFFFL);
            m_versionString = String.Format("{0}.{1}.{2}", major, minor, patch);
        }

        private byte[] MakeVersion()
        {
            ulong sVersion = ((((ulong)Mach.Core.Version.MAJOR & 0xFFFF) << 48) |
                              (((ulong)Mach.Core.Version.MINOR & 0xFFFF) << 32) |
                              (((ulong)Mach.Core.Version.FIX & 0xFFFFFFFFL) << 32));
            return BitConverter.GetBytes(sVersion);
        }

        private static string GetLocalIPAddress()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());

            // first, search IPv4
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    return ip.ToString();
                }
            }
            // if no IP, search IPv6
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetworkV6)
                {
                    return ip.ToString();
                }
            }
            // if no result, just return..
            return null;
        }
    }
}
