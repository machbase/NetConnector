using Mach.Comm;
using Mach.Core.Types;
using Mach.Data.MachClient;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Mach.Core.Result
{
	internal sealed class Row : IDisposable
	{
        public Row(ResultSet resultSet)
        {
            ResultSet = resultSet;
            m_columns = new List<ArraySegment<byte>>();
            m_nulls = new ColumnNull[ResultSet.ColumnCount];
        }

		public void SetData(Packet aPacket)
		{
            ClearData();

            // split to columns
            ColumnMetadata sColumnMeta;
            int sOffset = 0;
            int sReadLength = 0;
            bool sNeedReverse = true;

            for (int i = 0; i < ResultSet.ColumnCount; i++)
            {
                sColumnMeta = ResultSet.ColumnMetadataList[i];
                sNeedReverse = true;

                switch (sColumnMeta.ColumnType)
                {
                    case MachDBType.TEXT:
                    case MachDBType.BINARY:
                    case MachDBType.VARCHAR:
                        {
                            // reverse its length
                            Array.Reverse(aPacket.Segment.Array,
                                          aPacket.Segment.Offset + sOffset,
                                          4);
                            sReadLength = aPacket.GetInt(sOffset);
                            sOffset += 4;
                            sNeedReverse = false;
                        }
                        break;
                    case MachDBType.BOOL:
                    case MachDBType.INT16:
                    case MachDBType.UINT16:
                        sReadLength = 2;
                        break;
                    case MachDBType.INT32:
                    case MachDBType.UINT32:
                    case MachDBType.FLT32:
                        sReadLength = 4;
                        break;
                    case MachDBType.INT64:
                    case MachDBType.UINT64:
                    case MachDBType.FLT64:
                    case MachDBType.DATE:
                        sReadLength = 8;
                        break;
                    case MachDBType.IPV4:
                        sReadLength = 5;
                        sNeedReverse = false;
                        break;
                    case MachDBType.IPV6:
                        sReadLength = 17;
                        sNeedReverse = false;
                        break;
                    default:
                        break;
                }

                if (sNeedReverse)
                {
                    Array.Reverse(aPacket.Segment.Array,
                                  aPacket.Segment.Offset + sOffset,
                                  sReadLength);
                }

                m_columns.Add(new ArraySegment<byte>(aPacket.Segment.Array,
                                                     aPacket.Segment.Offset + sOffset,
                                                     sReadLength));
                sOffset += sReadLength; // don't need to align!
            }
        }

		public void Dispose() => ClearData();

		public void ClearData()
		{
            m_columns.Clear();
            Array.Clear(m_nulls, 0, ResultSet.ColumnCount);
        }

		public bool GetBoolean(int ordinal)
		{
			var value = GetValue(ordinal);
			if (value is bool)
				return (bool) value;

			if (value is sbyte)
				return (sbyte) value != 0;
			if (value is byte)
				return (byte) value != 0;
			if (value is short)
				return (short) value != 0;
			if (value is ushort)
				return (ushort) value != 0;
			if (value is int)
				return (int) value != 0;
			if (value is uint)
				return (uint) value != 0;
			if (value is long)
				return (long) value != 0;
			if (value is ulong)
				return (ulong) value != 0;
			return (bool) value;
		}

		public sbyte GetSByte(int ordinal) => (sbyte) GetValue(ordinal);

		public byte GetByte(int ordinal) => (byte) GetValue(ordinal);

		public long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
		{
            var sColumnData = GetValue(ordinal);

            if (sColumnData == DBNull.Value)
            {
                throw new InvalidCastException("Column is NULL.");
            }

            if (!(sColumnData is byte[]))
            {
                var column = ResultSet.ColumnMetadataList[ordinal];
                throw new InvalidCastException(String.Format("Can't convert {0} to bytes.", column.ColumnType));
            }

            byte[] sByteData = (byte[])sColumnData;
            int lengthToCopy = Math.Min(sByteData.Length - (int)dataOffset, length);
            
			return lengthToCopy;
		}

		public char GetChar(int ordinal) => (char) GetValue(ordinal);

		public long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

		public short GetInt16(int ordinal)
		{
			object value = GetValue(ordinal);
			if (value is short)
				return (short) value;

			if (value is sbyte)
				return (sbyte) value;
			if (value is byte)
				return (byte) value;
			if (value is ushort)
				return checked((short) (ushort) value);
			if (value is int)
				return checked((short) (int) value);
			if (value is uint)
				return checked((short) (uint) value);
			if (value is long)
				return checked((short) (long) value);
			if (value is ulong)
				return checked((short) (ulong) value);
			if (value is decimal)
				return (short) (decimal) value;
			return (short) value;
		}

		public int GetInt32(int ordinal)
		{
			object value = GetValue(ordinal);
			if (value is int)
				return (int) value;

			if (value is sbyte)
				return (sbyte) value;
			if (value is byte)
				return (byte) value;
			if (value is short)
				return (short) value;
			if (value is ushort)
				return (ushort) value;
			if (value is uint)
				return checked((int) (uint) value);
			if (value is long)
				return checked((int) (long) value);
			if (value is ulong)
				return checked((int) (ulong) value);
			if (value is decimal)
				return (int) (decimal) value;
			return (int) value;
		}

		public long GetInt64(int ordinal)
		{
			object value = GetValue(ordinal);
			if (value is long)
				return (long) value;

			if (value is sbyte)
				return (sbyte) value;
			if (value is byte)
				return (byte) value;
			if (value is short)
				return (short) value;
			if (value is ushort)
				return (ushort) value;
			if (value is int)
				return (int) value;
			if (value is uint)
				return (uint) value;
			if (value is ulong)
				return checked((long) (ulong) value);
			if (value is decimal)
				return (long) (decimal) value;
			return (long) value;
		}

		public DateTime GetDateTime(int ordinal) => (DateTime) GetValue(ordinal);

		public DateTimeOffset GetDateTimeOffset(int ordinal) => new DateTimeOffset(DateTime.SpecifyKind(GetDateTime(ordinal), DateTimeKind.Utc));

		public string GetString(int ordinal) => (string) GetValue(ordinal);

		public decimal GetDecimal(int ordinal) => (decimal) GetValue(ordinal);

		public double GetDouble(int ordinal)
		{
			object value = GetValue(ordinal);
			return value is float floatValue ? floatValue : (double) value;
		}

		public float GetFloat(int ordinal) => (float) GetValue(ordinal);

		public int GetValues(object[] values)
		{
			int count = Math.Min(values.Length, ResultSet.ColumnMetadataList.Count);
			for (int i = 0; i < count; i++)
				values[i] = GetValue(i);
			return count;
		}

        public bool IsDBNull(int ordinal)
        {
            if (m_nulls[ordinal] == ColumnNull.UNKNOWN)
            {
                var value = GetValue(ordinal);
                if (value == DBNull.Value)
                { 
                    m_nulls[ordinal] = ColumnNull.NULL;
                    return true;
                }
                else
                {
                    m_nulls[ordinal] = ColumnNull.NOTNULL;
                    return false;
                }
            }
            else
            {
                return (m_nulls[ordinal] == ColumnNull.NULL);
            }
        }

		public object this[int ordinal] => GetValue(ordinal);

		public object this[string name] => GetValue(ResultSet.GetOrdinal(name));

		public object GetValue(int ordinal)
		{
            if (ordinal < 0 || ordinal > ResultSet.ColumnMetadataList.Count)
                throw new ArgumentOutOfRangeException(nameof(ordinal), String.Format("value must be between 0 and {0}.", ResultSet.ColumnMetadataList.Count));

            if (m_nulls[ordinal] == ColumnNull.NULL)
                return DBNull.Value;

            var sColumnData = m_columns[ordinal];
            ColumnMetadata sColumnMeta = ResultSet.ColumnMetadataList[ordinal];

            switch (sColumnMeta.ColumnType)
            {
                case MachDBType.TEXT:
                case MachDBType.BINARY:
                    if (sColumnData.Count == 0)
                        return DBNull.Value;
                    else
                        return sColumnData.GetRealArray();
                case MachDBType.VARCHAR:
                    if (sColumnData.Count == 0)
                        return DBNull.Value;
                    else
                        return sColumnData.GetString();
                case MachDBType.BOOL:
                case MachDBType.INT16:
                    {
                        short sRet = BitConverter.ToInt16(sColumnData.Array, sColumnData.Offset);
                        if (m_nulls[ordinal] == ColumnNull.UNKNOWN)
                        {
                            if ((ushort)sRet == NullType.SHORT_NULL)
                            {
                                m_nulls[ordinal] = ColumnNull.NULL;
                                return DBNull.Value;
                            }
                            else
                            {
                                m_nulls[ordinal] = ColumnNull.NOTNULL;
                                return sRet;
                            }
                        }
                        return sRet;
                    }
                case MachDBType.UINT16:
                    {
                        ushort sRet = BitConverter.ToUInt16(sColumnData.Array, sColumnData.Offset);
                        if (m_nulls[ordinal] == ColumnNull.UNKNOWN)
                        {
                            if (sRet == NullType.USHORT_NULL)
                            {
                                m_nulls[ordinal] = ColumnNull.NULL;
                                return DBNull.Value;
                            }
                            else
                            {
                                m_nulls[ordinal] = ColumnNull.NOTNULL;
                                return sRet;
                            }
                        }
                        return sRet;
                    }
                case MachDBType.INT32:
                    {
                        int sRet = BitConverter.ToInt32(sColumnData.Array, sColumnData.Offset);
                        if (m_nulls[ordinal] == ColumnNull.UNKNOWN)
                        {
                            if ((uint)sRet == NullType.INT_NULL)
                            {
                                m_nulls[ordinal] = ColumnNull.NULL;
                                return DBNull.Value;
                            }
                            else
                            {
                                m_nulls[ordinal] = ColumnNull.NOTNULL;
                                return sRet;
                            }
                        }
                        return sRet;
                    }
                case MachDBType.UINT32:
                    {
                        uint sRet = BitConverter.ToUInt32(sColumnData.Array, sColumnData.Offset);
                        if (m_nulls[ordinal] == ColumnNull.UNKNOWN)
                        {
                            if (sRet == NullType.UINT_NULL)
                            {
                                m_nulls[ordinal] = ColumnNull.NULL;
                                return DBNull.Value;
                            }
                            else
                            {
                                m_nulls[ordinal] = ColumnNull.NOTNULL;
                                return sRet;
                            }
                        }
                        return sRet;
                    }
                case MachDBType.INT64:
                    {
                        long sRet = BitConverter.ToInt64(sColumnData.Array, sColumnData.Offset);
                        if (m_nulls[ordinal] == ColumnNull.UNKNOWN)
                        {
                            if ((ulong)sRet == NullType.LONG_NULL)
                            {
                                m_nulls[ordinal] = ColumnNull.NULL;
                                return DBNull.Value;
                            }
                            else
                            {
                                m_nulls[ordinal] = ColumnNull.NOTNULL;
                                return sRet;
                            }
                        }
                        return sRet;
                    }
                case MachDBType.UINT64:
                    {
                        ulong sRet = BitConverter.ToUInt64(sColumnData.Array, sColumnData.Offset);
                        if (m_nulls[ordinal] == ColumnNull.UNKNOWN)
                        {
                            if (sRet == NullType.ULONG_NULL)
                            {
                                m_nulls[ordinal] = ColumnNull.NULL;
                                return DBNull.Value;
                            }
                            else
                            {
                                m_nulls[ordinal] = ColumnNull.NOTNULL;
                                return sRet;
                            }
                        }
                        return sRet;
                    }
                case MachDBType.FLT32:
                    {
                        float sRet = BitConverter.ToSingle(sColumnData.Array, sColumnData.Offset);
                        if (m_nulls[ordinal] == ColumnNull.UNKNOWN)
                        {
                            if (sRet == NullType.FLOAT_NULL)
                            {
                                m_nulls[ordinal] = ColumnNull.NULL;
                                return DBNull.Value;
                            }
                            else
                            {
                                m_nulls[ordinal] = ColumnNull.NOTNULL;
                                return sRet;
                            }
                        }
                        return sRet;
                    }
                case MachDBType.FLT64:
                    {
                        double sRet = BitConverter.ToDouble(sColumnData.Array, sColumnData.Offset);
                        if (m_nulls[ordinal] == ColumnNull.UNKNOWN)
                        {
                            if (sRet == NullType.DOUBLE_NULL)
                            {
                                m_nulls[ordinal] = ColumnNull.NULL;
                                return DBNull.Value;
                            }
                            else
                            {
                                m_nulls[ordinal] = ColumnNull.NOTNULL;
                                return sRet;
                            }
                        }
                        return sRet;
                    }
                case MachDBType.DATE:
                    {
                        object sRet = ParseDateTime(sColumnData);
                        if (m_nulls[ordinal] == ColumnNull.UNKNOWN)
                        {
                            if (sRet == DBNull.Value)
                                m_nulls[ordinal] = ColumnNull.NULL;
                            else
                                m_nulls[ordinal] = ColumnNull.NOTNULL;
                        }
                        return sRet;
                    }
                case MachDBType.IPV4:
                    {
                        object sRet = ParseIPv4(sColumnData);
                        if (m_nulls[ordinal] == ColumnNull.UNKNOWN)
                        {
                            if (sRet == DBNull.Value)
                                m_nulls[ordinal] = ColumnNull.NULL;
                            else
                                m_nulls[ordinal] = ColumnNull.NOTNULL;
                        }
                        return sRet;
                    }
                case MachDBType.IPV6:
                    {
                        object sRet = ParseIPv6(sColumnData);
                        if (m_nulls[ordinal] == ColumnNull.UNKNOWN)
                        {
                            if (sRet == DBNull.Value)
                                m_nulls[ordinal] = ColumnNull.NULL;
                            else
                                m_nulls[ordinal] = ColumnNull.NOTNULL;
                        }
                        return sRet;
                    }
                default:
                    throw new NotImplementedException(String.Format("Reading {0} not implemented", sColumnMeta.ColumnType));
            }
		}

		private object ParseDateTime(ArraySegment<byte> aColumnData)
		{
            ulong unixDate = BitConverter.ToUInt64(aColumnData.Array, aColumnData.Offset);
            if (unixDate == NullType.DATETIME_NULL)
                return DBNull.Value;
            else
            {
                unixDate /= 1000000; // truncate under milisecond
                DateTime start = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                start = start.AddMilliseconds(unixDate);
                start = start.AddTicks((long)((unixDate % 1000000) / 100)); // 100-nanosecond resolution
                return start.ToLocalTime();
            }
        }

        private object ParseIPv4(ArraySegment<byte> sValue)
        {
            if (sValue.Array[sValue.Offset] == (byte)0)
            {
                return DBNull.Value;
            }
            else
            {
                return String.Format("{0}.{1}.{2}.{3}",
                                     sValue.Array[sValue.Offset + 1],
                                     sValue.Array[sValue.Offset + 2],
                                     sValue.Array[sValue.Offset + 3],
                                     sValue.Array[sValue.Offset + 4]);
            }
        }

        private object ParseIPv6(ArraySegment<byte> sValue)
        {
            String ipv6 = "";

            if (sValue.Array[sValue.Offset] == (byte)0)
            {
                return DBNull.Value;
            }
            else
            {
                bool sV4Mapped = false;
                byte[] sTmp = new byte[16];

                Buffer.BlockCopy(sValue.Array, sValue.Offset + 1, sTmp, 0, 16);

                sV4Mapped = isV4Mapped(sTmp);

                if (sV4Mapped)
                {
                    ipv6 = String.Format("::{0}{1}.{2}.{3}.{4}",
                                 (sV4Mapped == true ? "ffff:" : ""),
                                 sTmp[12],
                                 sTmp[13],
                                 sTmp[14],
                                 sTmp[15]);
                }
                else
                {
                    IPAddress address = new IPAddress(sTmp);
                    ipv6 = address.ToString();
                }

                return ipv6;
            }
        }

        private static bool isV4Mapped(byte[] bytes)
        {
            if (bytes != null && bytes.Length == 16)
            {
                for (int i = 0; i < 10; i++)
                {
                    if (bytes[i] != 0)
                    {
                        return false;
                    }
                }

                for (int i = 10; i < 12; i++)
                {
                    if (bytes[i] != (byte)0xff)
                    {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        public readonly ResultSet ResultSet;
        List<ArraySegment<byte>> m_columns;
        private ColumnNull[] m_nulls;
    }

    internal enum ColumnNull
    {
        UNKNOWN = 0,
        NOTNULL,
        NULL
    }
}
