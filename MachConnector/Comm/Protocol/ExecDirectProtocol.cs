using Mach.Core.Statement;
using Mach.Data.MachClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mach.Core;
using Mach.Core.Result;

namespace Mach.Comm
{
    class ExecDirectProtocol : Protocol
    {
        public ExecDirectProtocol() 
            : base()
        {
            base.ProtocolType = ProtocolType.EXECDIRECT_PROTOCOL;
        }

        /* ------------------------- */

        public void Generate(string commandText, MachParameterCollection parameterCollection, MachDataReader aDataReader)
        {
            var preparer = new StatementPreparer(commandText, parameterCollection);
            Generate(preparer.ParseAndBindParameters(), 0, aDataReader); // TODO
        }

        public void Generate(string commandText, MachDataReader aDataReader)
        {
            Generate(commandText, null, aDataReader);
        }
        
        public void Generate(ArraySegment<byte> aByte, MachDataReader aDataReader)
        {
            Generate(aByte, 0, aDataReader); // TODO
        }

        private void Generate(ArraySegment<byte> aByte, int aStatementID, MachDataReader aDataReader)
        {
            var sDict = new Dictionary<string, byte[]>
            {
                { "H_PROTOCOL", null },
                { "H_QUERY",    PacketConverter.CreateDataHeader(PacketType.DIRECT_STATEMENT_ID, DataType.DATA_STRING_TYPE) },
                { "QUERY",      PacketConverter.WriteBytesWithLength(aByte.Array) },
                { "H_ID",       PacketConverter.CreateDataHeader(PacketType.DIRECT_ID_ID, DataType.DATA_ULONG_TYPE) },
                { "STMTID",     BitConverter.GetBytes((long)aStatementID).FitToEndian() },
            };
            sDict["H_PROTOCOL"] = PacketConverter.CreateProtocolHeader(ProtocolType,
                                                                       ModeType.MODE_COMPLETE,
                                                                       aStatementID,
                                                                       sDict.Values.Sum(a => (a != null) ? a.Length : 0));

            base.SendData = sDict.Values.SelectMany(x => x).ToArray();

            DerivedReader = aDataReader;
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
                    case PacketType.DIRECT_ROWS_ID:
                        DerivedReader.ResultSet.RecordsAffected = sNext.GetInt();
                        break;
                    case PacketType.DIRECT_ERROR_ROWS_ID:
                        DerivedReader.ResultSet.RecordsFailed = sNext.GetInt();
                        break;
                    case PacketType.DIRECT_COLUMNS_ID:
                        int sColumnCount = sNext.GetInt();
                        DerivedReader.ResultSet.SetMeta(this, sColumnCount);
                        break;
                    default:
                        break;
                }
                sNext = this.ReadNext();
            }
        }

        /* ------------------------- */

        internal MachDataReader DerivedReader { get; set; }
    }
}
