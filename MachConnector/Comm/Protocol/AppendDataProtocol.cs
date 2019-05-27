using Mach.Core;
using Mach.Core.Statement;
using Mach.Data.MachClient;
using Mach.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mach.Comm
{
    class AppendDataProtocol : Protocol
    {
        public AppendDataProtocol()
            : base()
        {
            base.ProtocolType = ProtocolType.APPEND_DATA_PROTOCOL;
        }

        /* ------------------------- */

        public void Generate(int aStatementID, MachAppendWriter aWriter)
        {
            DerivedWriter = aWriter;

            var sDict = new Dictionary<string, byte[]>
            {
                { "H_PROTOCOL",  null },
                { "H_ID",        PacketConverter.CreateDataHeader(PacketType.EXEC_ID_ID, DataType.DATA_ULONG_TYPE) },
                { "ID",          BitConverter.GetBytes((long)aStatementID).FitToEndian() },
                { "ROWS",        aWriter.DataList.MakeArray() }, // ROWS header is inside of each rows
            };

            sDict["H_PROTOCOL"] = PacketConverter.CreateProtocolHeader(ProtocolType,
                                                                       ModeType.MODE_COMPLETE,
                                                                       0, // unused
                                                                       sDict.Values.Sum(a => (a != null) ? a.Length : 0));

            base.SendData = sDict.Values.MakeArray();
        }

        public void Generate(MachAppendWriter aWriter)
        {
            Generate(0, aWriter);
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
                        this.CheckAppendResult(sNext);
                        break;
                    default:
                        // TODO
                        break;
                }
                sNext = this.ReadNext();
            }
        }

        /* ------------------------- */

        internal MachAppendWriter DerivedWriter { get; set; }
    }
}
