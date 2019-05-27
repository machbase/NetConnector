using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Mach.Core;
using Mach.Data.MachClient;
using Mach.Core.Types;
using Mach.Comm;
using Mach.Utility;

namespace Mach.Core.Result
{
    internal sealed class ResultSet
    {
        public ResultSet()
        {
            ColumnMetadataList = new List<ColumnMetadata>();
            ColumnCount = 0;
            RecordsAffected = 0;
            RecordsFailed = 0;
            IsFetchEnd = false;
        }

        internal void SetMeta(ExecDirectProtocol aProtocol, int aColumnCount)
        {
            ColumnCount = aColumnCount;

            // Default Date Format
            Packet sNext = aProtocol.ReadNext(PacketType.PREP_DEFAULT_DATE_FMT_ID);
            DefaultDataFormat = sNext.GetString();

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
        
        public int GetOrdinal(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));
            
            for (var column = 0; column < ColumnMetadataList.Count; column++)
            {
                if (name.Equals(ColumnMetadataList[column].ColumnName, StringComparison.OrdinalIgnoreCase))
                    return column;
            }

            throw new IndexOutOfRangeException(String.Format("The column name '{0}' does not exist in the result set.", name));
        }

        public Row GetCurrentRow()
        {
            if (State != ResultSetState.ReadingRows)
                throw new InvalidOperationException("Read must be called first.");
            return m_row ?? throw new InvalidOperationException("There is no current row.");
        }

        public string GetName(int ordinal)
        {
            if (ColumnMetadataList.Count == 0)
                throw new IndexOutOfRangeException("There is no current result set.");
            if (ordinal < 0 || ordinal > ColumnMetadataList.Count)
                throw new IndexOutOfRangeException("value must be between 0 and {0}".FormatInvariant(ColumnMetadataList.Count - 1));
            return ColumnMetadataList[ordinal].ColumnName;
        }

        public string GetDataTypeName(int ordinal)
        {
            if (ordinal < 0 || ordinal > ColumnMetadataList.Count)
                throw new ArgumentOutOfRangeException(nameof(ordinal), "value must be between 0 and {0}.".FormatInvariant(ColumnMetadataList.Count));

            return ColumnMetadataList[ordinal].ColumnType.GetDbTypeName();
        }

        public Type GetFieldType(int ordinal)
        {
            if (ordinal < 0 || ordinal > ColumnMetadataList.Count)
                throw new ArgumentOutOfRangeException(nameof(ordinal), "value must be between 0 and {0}.".FormatInvariant(ColumnMetadataList.Count));

            return ColumnMetadataList[ordinal].ColumnType.GetCompatibleType();
        }

        public bool Read()
        {
            m_row?.ClearData();
            if (m_rowQueue.Count > 0)
            {
                m_row = m_rowQueue.Dequeue();
                State = ResultSetState.ReadingRows;
                return true;
            }
            else
            {
                return false;
            }
        }

        public void Write(Packet aPacket)
        {
            Row sNewRow = new Row(this);
            sNewRow.SetData(aPacket);
            m_rowQueue.Enqueue(sNewRow);
        }

        public int ColumnCount { get; set; }
        public List<ColumnMetadata> ColumnMetadataList { get; set; }

        public int RecordsAffected { get; set; }
        public int RecordsFailed { get; set; }
        
        public string DefaultDataFormat { get; set; }
        public ResultSetState State { get; private set; }
        public bool HasRows {
            get
            {
                return (m_rowQueue.Count > 0);
            }
        }

        public bool IsFetchEnd { get; internal set; }

        readonly Queue<Row> m_rowQueue = new Queue<Row>();
        Row m_row;
    }

    internal enum ResultSetState
    {
        None,
        ReadResultSetHeader,
        ReadingRows,
        HasMoreData,
        NoMoreData,
    }
}
