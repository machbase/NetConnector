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
    class AppendOpenProtocol : Protocol
    {
        public AppendOpenProtocol()
            : base()
        {
            base.ProtocolType = ProtocolType.APPEND_OPEN_PROTOCOL;
        }

        /* ------------------------- */

        public void Generate(int aStatementID, string aTableName, MachAppendWriter aWriter)
        {
            DerivedWriter = aWriter;

            var sDict = new Dictionary<string, byte[]>
            {
                { "H_PROTOCOL",  null },
                { "H_ID",        PacketConverter.CreateDataHeader(PacketType.EXEC_ID_ID, DataType.DATA_ULONG_TYPE) },
                { "ID",          BitConverter.GetBytes((long)aStatementID).FitToEndian() },
                { "H_TABLENAME", PacketConverter.CreateDataHeader(PacketType.EXEC_TABLE_ID, DataType.DATA_STRING_TYPE) },
                { "TABLENAME",   PacketConverter.WriteStringWithLength(aTableName) },
                { "H_ENDIAN",    PacketConverter.CreateDataHeader(PacketType.EXEC_ENDIAN_ID, DataType.DATA_ULONG_TYPE) },
                { "ENDIAN",      BitConverter.GetBytes((long)(BitConverter.IsLittleEndian ? 0 : 1)) },
            };
            sDict["H_PROTOCOL"] = PacketConverter.CreateProtocolHeader(ProtocolType,
                                                                       ModeType.MODE_COMPLETE,
                                                                       0, // unused
                                                                       sDict.Values.Sum(a => (a != null) ? a.Length : 0));

            base.SendData = sDict.Values.MakeArray();
        }

        public void Generate(string aTableName, MachAppendWriter aWriter)
        {
            Generate(0, aTableName, aWriter);
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
                    case PacketType.DIRECT_COLUMNS_ID:
                        {
                            int sColumnCount = sNext.GetInt();
                            DerivedWriter.Meta.SetMeta(this, sColumnCount);
                            // TODO
                        }
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
