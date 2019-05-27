using Mach.Data.MachClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mach.Comm
{
    class FreeProtocol : Protocol
    {
        public FreeProtocol() 
            : base()
        {
            base.ProtocolType = ProtocolType.FREE_PROTOCOL;
        }
        
        /* ------------------------- */

        public void Generate(int aStatementID)
        {
            var sDict = new Dictionary<string, byte[]>
            {
                { "HEADER",       null },
                { "H_ID",         PacketConverter.CreateDataHeader(PacketType.FREE_ID_ID, DataType.DATA_ULONG_TYPE) },
                { "STMTID",       BitConverter.GetBytes((long)aStatementID).FitToEndian() },
                // NOTE if you want to leave the last field as string, please add more padding field such as (DUMMY_INT)
                //      or switch the string field with upper one if you can. (in this case, IP was the last, but it switched)
            };
            sDict["HEADER"] = PacketConverter.CreateProtocolHeader(ProtocolType,
                                                                  ModeType.MODE_COMPLETE,
                                                                  0, // unused
                                                                  sDict.Values.Sum(a => (a != null) ? a.Length : 0));

            base.SendData = sDict.Values.SelectMany(x => x).ToArray();
        }

        public void Generate()
        {
            Generate(0);
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
                    default:
                        // TODO
                        break;
                }
                sNext = this.ReadNext();
            }
        }

        /* ------------------------- */

        public const int CONNECT_PAYLOAD_RESPONSE_LEN = 9;
    }
}
