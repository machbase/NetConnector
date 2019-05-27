using Mach.Comm;
using Mach.Core.Result;
using Mach.Core.Types;
using Mach.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

namespace Mach.Data.MachClient
{
    [Flags]
    public enum MachAppendOption
    {
        None = 0,
        MicroSecTruncated = 1,
        //AutoOpenClose = 2,
    }

    public delegate void ErrorDelegateFuncType(MachAppendException e);

    public sealed class MachAppendWriter
    {
        internal static MachAppendWriter Create(MachCommand aCommand, string aTableName, MachAppendOption aOption, int aErrorCheckCount)
        {
            var sAppendWriter = new MachAppendWriter(aCommand, aTableName, aOption, aErrorCheckCount);
            return sAppendWriter;
        }

        internal static MachAppendWriter Create(MachCommand aCommand, string aTableName, MachAppendOption aOption)
        {
            var sAppendWriter = new MachAppendWriter(aCommand, aTableName, aOption, 0);
            return sAppendWriter;
        }

        internal static MachAppendWriter Create(MachCommand aCommand, string aTableName)
        {
            var sAppendWriter = new MachAppendWriter(aCommand, aTableName, MachAppendOption.None, 0);
            return sAppendWriter;
        }

        private MachAppendWriter(MachCommand aCommand, string aTableName, MachAppendOption aOption, int aErrorCheckCount)
        {
            Command = aCommand;
            TableName = aTableName;
            ErrorDelegateFunc = null;
            ErrorCheckCount = aErrorCheckCount;
            Meta = new AppendMeta();
            Option = aOption;

            m_dataList = new List<byte[]>();

            ClearData();
        }

        internal void CallErrorDelegator(MachAppendException e)
        {
            ErrorDelegateFunc?.Invoke(e);
        }

        public void SetErrorDelegator(ErrorDelegateFuncType aFunc)
        {
            ErrorDelegateFunc = aFunc;
        }

        internal void writeData(List<object> aDataList, ulong aArrivalTime)
        {
            if (Meta.ColumnCount == 0)
                throw new MachException(MachErrorMsg.INVALID_APPEND_OPEN);

            if (Meta.ColumnCount != (aDataList.Count + 1)) // aDataList excludes _arrival_time
                throw new MachException(MachErrorMsg.APPEND_COLUMN_COUNT_DIFFER
                                            .FormatInvariant(Meta.ColumnCount, aDataList.Count));

            int sSum = (Meta.ColumnCount / 8);
            sSum += (Meta.ColumnCount % 8 > 0) ? 1 : 0;
            byte[] sNullArray = new byte[sSum];

            var sRowList = new Dictionary<string, byte[]>
            {
                { "HEAD",        PacketConverter.CreateDataHeader(PacketType.EXEC_ROWS_ID, DataType.DATA_BINARY_TYPE) },
                { "LEN",         null },
                { "COMP",        new byte[]{0} }, // always non-compression
                { "NULLBIT_CNT", BitConverter.GetBytes(sSum) },
                { "NULLBITS",    sNullArray },
                { "DATA",        null },
                { "PAD",         null },
            };
            Array.Clear(sNullArray, 0, sSum);

            // create dataList locally to concat 
            List<byte[]> sAppendList = new List<byte[]>();

            if (aArrivalTime == 0)
                SetNullBit(0, sNullArray);
            else
                sAppendList.Add(BitConverter.GetBytes(aArrivalTime));

            // if DBNull.Value, then change to NULL value below
            for (int i = 0; i < Meta.ColumnCount - 1; i++)
            {
                if (aDataList[i] == DBNull.Value)
                {
                    switch (Meta.ColumnMetadataList[i + 1].ColumnType)
                    {
                        case MachDBType.BINARY:
                        case MachDBType.TEXT:
                        case MachDBType.VARCHAR:
                        case MachDBType.IPV4:
                        case MachDBType.IPV6:
                            sAppendList.Add(new byte[0]);
                            break;
                        case MachDBType.BOOL:
                        case MachDBType.INT16:
                            sAppendList.Add(BitConverter.GetBytes(MachAppendNullValue.SHORT_NULL));
                            break;
                        case MachDBType.UINT16:
                            sAppendList.Add(BitConverter.GetBytes(MachAppendNullValue.USHORT_NULL));
                            break;
                        case MachDBType.INT32:
                            sAppendList.Add(BitConverter.GetBytes(MachAppendNullValue.INTEGER_NULL));
                            break;
                        case MachDBType.UINT32:
                            sAppendList.Add(BitConverter.GetBytes(MachAppendNullValue.UINTEGER_NULL));
                            break;
                        case MachDBType.INT64:
                            sAppendList.Add(BitConverter.GetBytes(MachAppendNullValue.LONG_NULL));
                            break;
                        case MachDBType.UINT64:
                            sAppendList.Add(BitConverter.GetBytes(MachAppendNullValue.ULONG_NULL));
                            break;
                        case MachDBType.FLT32:
                            sAppendList.Add(BitConverter.GetBytes(MachAppendNullValue.FLOAT_NULL));
                            break;
                        case MachDBType.FLT64:
                            sAppendList.Add(BitConverter.GetBytes(MachAppendNullValue.DOUBLE_NULL));
                            break;
                        case MachDBType.DATE: // DateTime or unsigned long
                            sAppendList.Add(BitConverter.GetBytes(MachAppendNullValue.DATETIME_NULL));
                            break;
                        default:
                            break;
                    }
                    SetNullBit(i + 1, sNullArray);
                }
                else
                {
                    // compare its compatible type and check
                    // BUGUBG DateTime is multi-typed value so skip it
                    if (Meta.ColumnMetadataList[i + 1].ColumnType != MachDBType.DATE)
                    { 
                        if (Meta.ColumnMetadataList[i + 1].ColumnType.GetCompatibleType() != aDataList[i].GetType())
                        {
                            throw new MachException(
                                        MachErrorMsg.INVALID_APPEND_DATA_TYPE.FormatInvariant(i,
                                            Meta.ColumnMetadataList[i + 1].ColumnType.GetCompatibleType().FullName,
                                            aDataList[i].GetType().FullName));
                        }
                    }

                    switch (Meta.ColumnMetadataList[i + 1].ColumnType)
                    {
                        case MachDBType.BINARY:
                            {
                                byte[] sAppendBytes = null;
                                if (((byte[])aDataList[i]).Length > MAX_BINARY_LENGTH)
                                {
                                    sAppendBytes = new byte[MAX_BINARY_LENGTH];
                                    Array.Copy(((byte[])aDataList[i]), sAppendBytes, MAX_BINARY_LENGTH);
                                }
                                else
                                {
                                    sAppendBytes = ((byte[])aDataList[i]);
                                }
                                sAppendList.Add(BitConverter.GetBytes(sAppendBytes.Length));
                                sAppendList.Add(sAppendBytes);
                            }
                            break;
                        case MachDBType.TEXT:
                            {
                                byte[] sAppendBytes = null;
                                if (((string)aDataList[i]).Length > MAX_BINARY_LENGTH)
                                {
                                    sAppendBytes = new byte[MAX_BINARY_LENGTH];
                                }
                                else
                                {
                                    sAppendBytes = new byte[((string)aDataList[i]).Length];
                                }
                                Encoding.UTF8.GetBytes(((string)aDataList[i]), 0, sAppendBytes.Length, sAppendBytes, 0);
                                sAppendList.Add(BitConverter.GetBytes(sAppendBytes.Length));
                                sAppendList.Add(sAppendBytes);
                            }
                            break;
                        case MachDBType.VARCHAR:
                            {
                                byte[] sAppendBytes = Encoding.UTF8.GetBytes((string)aDataList[i]);
                                sAppendList.Add(BitConverter.GetBytes(sAppendBytes.Length));
                                sAppendList.Add(sAppendBytes);
                                break;
                            }
                        case MachDBType.IPV4:
                            sAppendList.Add(ParseIPv4ForAppend((string)aDataList[i]));
                            break;
                        case MachDBType.IPV6:
                            sAppendList.Add(ParseIPv6ForAppend((string)aDataList[i]));
                            break;
                        case MachDBType.BOOL:
                        case MachDBType.INT16:
                            sAppendList.Add(BitConverter.GetBytes((short)aDataList[i]));
                            break;
                        case MachDBType.UINT16:
                            sAppendList.Add(BitConverter.GetBytes((ushort)aDataList[i]));
                            break;
                        case MachDBType.INT32:
                            sAppendList.Add(BitConverter.GetBytes((int)aDataList[i]));
                            break;
                        case MachDBType.UINT32:
                            sAppendList.Add(BitConverter.GetBytes((uint)aDataList[i]));
                            break;
                        case MachDBType.INT64:
                            sAppendList.Add(BitConverter.GetBytes((long)aDataList[i]));
                            break;
                        case MachDBType.UINT64:
                            sAppendList.Add(BitConverter.GetBytes((ulong)aDataList[i]));
                            break;
                        case MachDBType.FLT32:
                            sAppendList.Add(BitConverter.GetBytes((float)aDataList[i]));
                            break;
                        case MachDBType.FLT64:
                            sAppendList.Add(BitConverter.GetBytes((double)aDataList[i]));
                            break;
                        case MachDBType.DATE: // DateTime or unsigned long or string
                            if (aDataList[i] is DateTime)
                            {
                                DateTime sAppendTime = (DateTime)aDataList[i];
                                DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

                                if ((Option & MachAppendOption.MicroSecTruncated) == MachAppendOption.MicroSecTruncated)
                                {
                                    sAppendTime = sAppendTime.AddTicks(-(sAppendTime.Ticks % TimeSpan.TicksPerMillisecond));
                                }

                                long sTicks = 0;

                                if (sAppendTime.Kind == DateTimeKind.Utc)
                                {
                                    DateTime myDt = DateTime.SpecifyKind(sAppendTime, DateTimeKind.Local);
                                    TimeSpan diff = myDt.ToUniversalTime() - origin;
                                    sTicks = diff.Ticks;
                                }
                                else if (sAppendTime.Kind == DateTimeKind.Local)
                                {
                                    TimeSpan diff = sAppendTime.ToUniversalTime() - origin;
                                    // sTicks = sAppendTime.Ticks - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Local).Ticks;
                                    sTicks = diff.Ticks;
                                }
                                else
                                {
                                    throw new MachException("DateTime to append at {0} is unspecified DateTimeKind."
                                                                .FormatInvariant(aDataList[i]));
                                }

                                if (sTicks < 0)
                                    throw new MachException("DateTime to append at {0} is less than 1970-01-01."
                                                                .FormatInvariant(aDataList[i]));

                                // Resolution of Ticks = 100 nanosecond
                                sAppendList.Add(BitConverter.GetBytes(sTicks * 100));
                            }
                            else if (aDataList[i] is ulong)
                            {
                                sAppendList.Add(BitConverter.GetBytes((ulong)aDataList[i]));
                            }
                            else if (aDataList[i] is string)
                            {
                                DateTime sAppendTime;
                                bool sInserted = false;

                                foreach (string sFormatString in m_dateFormat)
                                { 
                                    try
                                    {
                                        sAppendTime = DateTime.ParseExact((string)aDataList[i], sFormatString, System.Globalization.CultureInfo.InvariantCulture);
                                        long sTicks = sAppendTime.Ticks - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

                                        if (sTicks < 0)
                                        { 
                                            throw new MachException("DateTime to append at {0} is less than 1970-01-01.".FormatInvariant(aDataList[i]));
                                        }

                                        // Resolution of Ticks = 100 nanosecond
                                        sAppendList.Add(BitConverter.GetBytes(sTicks * 100));
                                        sInserted = true;
                                        break;
                                    }
                                    catch (FormatException e)
                                    {
                                        throw e;
                                    }
                                }

                                if (!sInserted)
                                {
                                    throw new MachException(MachErrorMsg.INVALID_APPEND_DATATIME_FORMAT.FormatInvariant(aDataList[i]));
                                }
                            }
                            else
                            {
                                throw new MachException(MachErrorMsg.INVALID_APPEND_DATATIME_TYPE.FormatInvariant(aDataList[i].GetType().FullName));
                            }
                            break;
                        default:
                            break;
                    }
                }
            }

            sRowList["DATA"] = sAppendList.MakeArray();

            // do not include HEAD's length
            long sLength = sRowList.Values.Sum(a => ((a == null) ? 0 : a.Length)) - sRowList["HEAD"].Length;
            
            if (sLength % 8 > 0)
            {
                int sPadLength = 8 - (int)(sLength % 8);
                sRowList["PAD"] = new byte[sPadLength];
                sLength += sPadLength;
            }

            sRowList["LEN"] = BitConverter.GetBytes(sLength);

            // append to dataList
            byte[] sAddRow = sRowList.Values.MakeArray();
            //byte[] sAddRow = sRowList.Values.SelectMany(a => (a != null) ? a : new byte[0]).ToArray();
            m_dataListLength += sAddRow.Length;
            m_dataList.Add(sAddRow);

            if (m_dataListLength > MAX_BINARY_LENGTH)
            {
                ExceedBuffer = true;
            }
        }

        internal void ClearData()
        {
            m_dataList.Clear();
            m_dataListLength = 0;
            ExceedBuffer = false;
            AppendAddCount = 0;
        }

        private byte[] ParseIPv4ForAppend(string aIp)
        {
            byte[] returnIP = new byte[5];
            int pos = 0;

            if (aIp != null && aIp.Length > 0)
            {
                try
                {
                    IPAddress address = IPAddress.Parse(aIp);
                    byte[] sBuf = address.GetAddressBytes();

                    returnIP[pos++] = 4;
                    Array.Copy(sBuf, 0, returnIP, 1, 4);

                    //returnIP[pos++] = 4;
                    //string[] st = aIp.Split('.');

                    //foreach (var ipNumber in st)
                    //{
                    //    returnIP[pos++] = Convert.ToByte(ipNumber);
                    //}
                }
                catch (FormatException)
                {
                    // BUGBUG throw e??
                    for (int i = 0; i < 17; i++)
                    {
                        returnIP[pos++] = 0;
                    }
                }
            }
            else //ipv4 null
            { 
                for (int i = 0; i < 5; i++)
                {
                    returnIP[pos++] = 0;
                }
            }

            return returnIP;
        }

        private byte[] ParseIPv6ForAppend(string aIp)
        {
            byte[] returnIP = new byte[17];
            int pos = 0;
            int i;

            if (aIp != null && aIp.Length > 0)
            {
                try
                {
                    IPAddress address = IPAddress.Parse(aIp);
                    byte[] sBuf = address.GetAddressBytes();
                    
                    returnIP[pos++] = 16;
                    Array.Copy(sBuf, 0, returnIP, 1, 16);

                    //if (address.IsIPv4MappedToIPv6() == true) // such as ::ffff:0.0.0.1
                    //{
                    //    for (i = 0; i < 12; i++)
                    //    {
                    //        returnIP[pos++] = 0;
                    //    }
                    //    for (i = 12; i < sBuf.Length; i++)
                    //    {
                    //        returnIP[pos++] = sBuf[i];
                    //    }
                    //}
                    //else
                    //{
                        //for (i = 0; i < sBuf.Length; i++)
                        //{
                        //    returnIP[pos++] = sBuf[i];
                        //}
                    //}
                }
                catch (FormatException)
                {
                    // BUGBUG throw e??
                    for (i = 0; i < 17; i++)
                    {
                        returnIP[pos++] = 0;
                    }
                }
            }
            else //ipv6 null
            { 
                for (i = 0; i < 17; i++)
                {
                    returnIP[pos++] = 0;
                }
            }

            return returnIP;
        }

        private void SetNullBit(int ordinal, byte[] aNullArray)
        {
            int bytePos = ordinal / 8;
            int bitPos = ordinal % 8;

            aNullArray[bytePos] = (byte)(aNullArray[bytePos] | (1 << (7 - bitPos)));
        }

        internal MachCommand Command { get; private set; }
        internal List<byte[]> DataList { get => m_dataList; }
        internal int AppendAddCount { get; set; }

        internal AppendMeta Meta { get; }
        internal int ErrorCheckCount { get; set; }
        internal string TableName { get; set; }
        public long SuccessCount { get; internal set; }
        public long FailureCount { get; internal set; }
        public bool ExceedBuffer { get; private set; }
        internal ErrorDelegateFuncType ErrorDelegateFunc { get; private set; }
        public MachAppendOption Option { get; internal set; }

        private List<byte[]> m_dataList;
        private long m_dataListLength;

        public const int MAX_BINARY_LENGTH = 64 * 1024 * 1024;

        private string[] m_dateFormat =
        {
            "yyyy-MM-dd HH:mm:ss ffffff",
            "yyyy-MM-dd HH:mm:ss fff",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm",
            "yyyy-MM-dd HH",
            "yyyy-MM-dd",
            "yyyy-MM",
            "yyyy"
        };
    }

    internal class MachAppendNullValue
    {
        public const ushort SHORT_NULL = 0x8000;
        public const ushort USHORT_NULL = 0xFFFF;
        public const uint INTEGER_NULL = 0x80000000;
        public const uint UINTEGER_NULL = 0xFFFFFFFF;
        public const ulong LONG_NULL = 0x8000000000000000L;
        public const ulong ULONG_NULL = 0xFFFFFFFFFFFFFFFFUL;
        public const float FLOAT_NULL = 3.402823466e+38F;
        public const double DOUBLE_NULL = 1.7976931348623158e+308;

        public const int IP_NULL = 0;
        public const ulong DATETIME_NULL = ULONG_NULL;

        public const int VARCHAR_NULL = 0;
        public const int TEXT_NULL = 0;
        public const int CLOB_NULL = 0;
        public const int BLOB_NULL = 0;
        public const int BINARY_NULL = 0;
    }
}
