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
    class FetchProtocol : Protocol
    {
        
        public FetchProtocol()
            : base()
        {
            base.ProtocolType = ProtocolType.FETCH_PROTOCOL;
        }

        /* ------------------------- */

        public void Generate(MachDataReader aDataReader)
        {
            Generate(0, aDataReader);
        }

        private void Generate(int aStatementID, MachDataReader aDataReader)
        {
            var sDict = new Dictionary<string, byte[]>
            {
                { "H_PROTOCOL", null },
                { "H_ID",       PacketConverter.CreateDataHeader(PacketType.FETCH_ID_ID, DataType.DATA_ULONG_TYPE) },
                { "ID",         BitConverter.GetBytes((long)aStatementID).FitToEndian() },
                { "H_SIZE",     PacketConverter.CreateDataHeader(PacketType.FETCH_ROWS_ID, DataType.DATA_ULONG_TYPE) },
                { "STMTID",     BitConverter.GetBytes((long)aDataReader.FetchSize).FitToEndian() },
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
            bool isEndResponse = false;
            int sRowsFetched = 0;

            // TODO if m_fetchEnd is true, you don't need to!
            while (sNext != null && isEndResponse == false)
            {
                switch (sNext.PacketType)
                {
                    case PacketType.RETURN_RESULT_ID:
                        CheckResult(sNext);
                        break;
                    case PacketType.FETCH_VALUE_ID: // rows
                        DerivedReader.ResultSet.Write(sNext);
                        sRowsFetched++;
                        break;
                    //case PacketType.FETCH_ROWS_ID: // fetched count
                    //    sFetchedCount = sNext.GetLong();
                    //    if (sFetchedCount == 0)
                    //    {
                    //        // server waits for the next msg but records to be fetched remiain
                    //        isEndResponse = true;
                    //    }
                    //    break;
                    default:
                        break;
                }
                sNext = this.ReadNext();
            }

            DerivedReader.ResultSet.RecordsAffected += sRowsFetched;
        }

        /* ------------------------- */

        public MachDataReader DerivedReader { get; set; }

        private void CheckResult(Packet aPacket)
        {
            long sErrNum = aPacket.GetInt();
            Result sResult = (Result)aPacket.GetInt(4);

            if (sResult == Result.LAST)
            {
                DerivedReader.ResultSet.IsFetchEnd = true;
            }
            else if (sResult != Result.OK)
            {
                Packet sNext = this.ReadNext();
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
                    sNext = this.ReadNext();
                }

                throw new MachException(String.Format("[ERR-{0:00000} : {1}]", sErrNum, sMsg));
            }
        }
    }

}