using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mach.Comm
{
    public enum ProtocolType
    {
        INIT_PROTOCOL = 1000, // unused
        CONNECT_PROTOCOL = 0,
        DISCONNECT_PROTOCOL,
        INVALID_PROTOCOL,
        LARGE_PROTOCOL,
        TIMEOUT_PROTOCOL,
        ADMIN_PROTOCOL,
        PREPARE_PROTOCOL,
        EXECUTE_PROTOCOL,
        EXECDIRECT_PROTOCOL,
        FETCH_PROTOCOL,
        FREE_PROTOCOL,
        APPEND_OPEN_PROTOCOL,
        APPEND_DATA_PROTOCOL,
        APPEND_CLOSE_PROTOCOL,
        ERROR_PROTOCOL = 1163022162
    }

    public enum PacketType
    {
        UNKNOWN_TYPE = 0,

        // CONNECT
        CONNECT_VERSION_ID = 0x00000001,
        CONNECT_CLIENT_ID = 0x00000002,
        CONNECT_LANG_ID = 0x00000003,
        CONNECT_DATABASE_ID = 0x00000004,
        CONNECT_ENDIAN_ID = 0x00000005,
        CONNECT_USER_ID = 0x00000006,
        CONNECT_PASSWORD_ID = 0x00000007,
        CONNECT_TIMEOUT_ID = 0x00000008,
        CONNECT_SID_ID = 0x00000040,  // Session ID from Server
        CONNECT_SHC_ID = 0x00000041,
        CONNECT_IP_ID = 0x00000042,
        CONNECT_SLANG_ID = 0x00000009,

        RETURN_RESULT_ID = 0x00000010,
        RETURN_MESSAGE_ID = 0x00000011,
        RETURN_EMESSAGE_ID = 0x00000012,
        RETURN_APPEND_SUCCESS_CNT = 0x00000061,
        RETURN_APPEND_FAILURE_CNT = 0x00000062,

        // PREPARE
        PREP_STATEMENT_ID = 0x00000020,
        PREP_BINDS_ID = 0x00000021,
        PREP_ID_ID = 0x00000022,
        PREP_ROWS_ID = 0x00000023,
        PREP_COLUMNS_ID = 0x00000024,
        PREP_TABLE_ID = 0x00000025,
        PREP_COLNAME_ID = 0x00000026,
        PREP_COLTYPE_ID = 0x00000027,
        PREP_DEFAULT_DATE_FMT_ID = 0x00000028,
        PREP_ERROR_ROWS_ID = 0x00000063,

        // EXECUTE_PROTOCOL - client side
        EXEC_BIND_ID = 0x00000030,
        EXEC_PARAM_ID = 0x00000031,
        EXEC_OUTPARAM_ID = 0x00000032,
        EXEC_STATEMENT_ID = PREP_STATEMENT_ID,

        // EXECUTE_PROTOCOL - server side
        EXEC_ID_ID = PREP_ID_ID,
        EXEC_ROWS_ID = PREP_ROWS_ID,
        EXEC_ERROR_ROWS_ID = PREP_ERROR_ROWS_ID,
        EXEC_COLUMNS_ID = PREP_COLUMNS_ID,
        EXEC_TABLE_ID = PREP_TABLE_ID,
        EXEC_COLNAME_ID = PREP_COLNAME_ID,
        EXEC_COLTYPE_ID = PREP_COLTYPE_ID,
        EXEC_ENDIAN_ID = 0x00000034,

        // EXECDIRECT_PROTOCOL
        DIRECT_ID_ID = PREP_ID_ID,
        DIRECT_STATEMENT_ID = 0x00000040,
        DIRECT_ROWS_ID = PREP_ROWS_ID,
        DIRECT_ERROR_ROWS_ID = PREP_ERROR_ROWS_ID,
        DIRECT_COLUMNS_ID = PREP_COLUMNS_ID,
        DIRECT_TABLE_ID = PREP_TABLE_ID,
        DIRECT_COLNAME_ID = PREP_COLNAME_ID,
        DIRECT_COLTYPE_ID = PREP_COLTYPE_ID,

        // FETCH_PROTOCOL
        FETCH_ID_ID = 0x00000050,
        FETCH_ROWS_ID = 0x00000051,
        FETCH_VALUE_ID = 0x00000052,

        // FREE_PROTOCOL
        FREE_ID_ID = 0x00000060
    }

    public enum DataType
    {
        DATA_PROTOCOL_TYPE = 0x00000001,
        DATA_STRING_TYPE = 0x00000002,
        DATA_BINARY_TYPE = 0x00000003,
        DATA_SCHAR_TYPE = 0x00000004,
        DATA_UCHAR_TYPE = 0x00000005,
        DATA_SSHORT_TYPE = 0x00000006,
        DATA_USHORT_TYPE = 0x00000007,
        DATA_SINT_TYPE = 0x00000008,
        DATA_UINT_TYPE = 0x00000009,
        DATA_SLONG_TYPE = 0x0000000A,
        DATA_ULONG_TYPE = 0x0000000B,
        DATA_DATE_TYPE = 0x0000000C,
        DATA_ROWS_TYPE = 0x0000000D,

        DATA_TNUMERIC_TYPE = 0x000000F1,
        DATA_NUMBER_TYPE = 0x00000DF2,
    }

    public enum ModeType
    {
        MODE_COMPLETE = 0x00,
        MODE_BEGIN = 0x01,
        MODE_MIDDLE = 0x02,
        MODE_END = 0x03,
    }

    internal enum Result
    {
        TIMEOUT = 1918126413,
        LARGE = 1917604423,
        INVALID = 1917406806,
        NONE = 1917733196,
        OK = 1917799263,
        LAST = 1917604692,
        ERROR = 1917013343,
    }

    internal class NullType
    {
        public const ushort SHORT_NULL    = 0x8000;
        public const ushort USHORT_NULL   = 0xFFFF;
        public const uint   INT_NULL      = 0x80000000;
        public const uint   UINT_NULL     = 0xFFFFFFFF;
        public const ulong  LONG_NULL     = 0x8000000000000000L;
        public const ulong  ULONG_NULL    = 0xFFFFFFFFFFFFFFFFL;
        public const float  FLOAT_NULL    = 3.402823466e+38F;
        public const double DOUBLE_NULL   = 1.7976931348623158e+308;
        public const ulong  DATETIME_NULL = 0xFFFFFFFFFFFFFFFFL;
    }

    internal static class TypeConverter
    {
        public static int ToValue(this PacketType aValue)
        {
            return (int)aValue;
        }
        public static int ToValue(this DataType aValue)
        {
            return (int)aValue;
        }
        public static int ToValue(this ModeType aValue)
        {
            return (int)aValue;
        }
    }
}
