using Mach.Comm;
using Mach.Core.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mach.Core.Result
{
    internal sealed class AppendMeta
    {
        public AppendMeta()
        {
            ColumnMetadataList = new List<ColumnMetadata>();
            ColumnCount = 0;
            RecordsAffected = 0;
            RecordsFailed = 0;
        }

        internal void SetMeta(AppendOpenProtocol aProtocol, int aColumnCount)
        {
            ColumnCount = aColumnCount;

            Packet sNext;

            // Column Meta List
            string sColName = null;
            ulong sColType = 0;

            for (int i = 0; i < ColumnCount; i++)
            {
                sNext = aProtocol.ReadNext(PacketType.DIRECT_TABLE_ID);
                // just skip..

                sNext = aProtocol.ReadNext(PacketType.DIRECT_COLNAME_ID);
                sColName = sNext.GetString();

                sNext = aProtocol.ReadNext(PacketType.DIRECT_COLTYPE_ID);
                sColType = (ulong)sNext.GetLong();

                ColumnMetadataList.Add(new ColumnMetadata(sColName, sColType));
            }
        }

        public int ColumnCount { get; set; }
        public List<ColumnMetadata> ColumnMetadataList { get; set; }

        public int RecordsAffected { get; set; }
        public int RecordsFailed { get; set; }
    }
}
