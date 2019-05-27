namespace Mach.Data.MachClient
{
    public class MachErrorMsg
    {
        public static string INVALID_INIT_PROTOCOL = "invalid protocol while reading init packet({0})";
        public static string INVALID_NEXT_PROTOCOL = "invalid protocol while reading next packet({0})";
        public static string INVALID_CONNECT_PROTOCOL = "invalid protocol while connecting";
        public static string INVALID_DISCONNECT_PROTOCOL = "invalid protocol while disconnecting";
        public static string INVALID_FETCH_PROTOCOL = "invalid protocol while fetching";
        public static string INVALID_EXECDIRECT_PROTOCOL = "invalid protocol while execdirect";
        public static string INVALID_FREE_PROTOCOL = "invalid protocol while closing";
        public static string INVALID_PROTOCOL = "invalid protocol while {0}";
        public static string INVALID_RETURN_PROTOCOL = "invalid protocol after executing({0} != {1})\n";
        public static string CONNECT_BROKER_FAILURE = "Connect broker failure";
        public static string REJECT_CONNECTION = "server rejected the connection";
        public static string INVALID_CONNECTION = "MachConnection is not connected. (Connection state : {0})";
        public static string UNKNOWN_TYPE = "unknown type : index={0}, type={1}\n";
        public static string UNKNOWN_SQL_TYPE = "unknown SQL type : ";
        public static string INVALID_DATE_FORMAT = "Invalid date format.";
        public static string INVALID_TIME_FORMAT = "Invalid time format.";
        public static string INVALID_TIMESTAMP_FORMAT = "Invalid timestamp format.";
        public static string MISMATCH_DATA_TYPE = "mismatched data type.";
        public static string NO_DATA = "data does not exist.";

        public static string APPEND_COLUMN_COUNT_DIFFER = "column counts differ.({0} != {1})\n";
        public static string APPEND_CLOSE_TIMEOUT = "return timeout protocol while append close";
        public static string APPEND_CLOSE_REMAINDATA = "return append data protocol while append close";
        public static string APPEND_CLOSE_INVALID_PROTOCOL = "return invalid protocol while append close";

        public static string NOT_SUPPORTED_DATA_TYPE = "data type not supported.";
        public static string NOT_SUPPORTED_METHOD = "this method not supported.";
        public static string NOT_SUPPORTED_OPERATION = "operation not yet supported.";
        public static string NOT_SUPPORTED_PROCEDURE = "stored procedures not supported.";
        public static string OVER_MAX_STMT_NUM = "exceeding maximum statement number.";
        public static string INVALID_MACHBASE_URL = "Invalid Machbase URL: ";
        public static string INVALID_MACHBASE_PORT = "Invalid Machbase port number: ";
        public static string NOT_SELECT_QUERY = "this is not select query";
        public static string INVALID_EXECUTE = "invalid execution";
        public static string INVALID_EXEC_UPDATE = "invalid UpdateCount";
        public static string INVALID_EXEC_BATCH = "invalid execute batch";
        public static string INVALID_RESULTSET = "invalid resultset";
        public static string INVALID_BINARY_OBJECT = "invalid binary object type";
        public static string INVALID_APPEND_OPEN = "invalid append open result";
        public static string INVALID_APPEND_CALLBACK = "invalid append set error callback";
        public static string INVALID_APPEND_FLUSH = "invalid append flush";
        public static string INVALID_APPEND_DATA = "invalid append data";
        public static string INVALID_APPEND_DATA_TYPE = "invalid append data type on column {0} (expected {1}, but {2})";
        public static string INVALID_APPEND_DATATIME_TYPE = "invalid append datatime type({0})";
        public static string INVALID_APPEND_DATATIME_FORMAT = "invalid append datatime format({0})";
        public static string INVALID_APPEND_CLOSE = "invalid append close";
        public static string NOT_SUPPORTED_UDT = "UDTs are not supported.";
        public static string MISSING_CHECK = "missing in parameter at index";
        public static string LESS_INDEX_VALIDATE = "cannot address an index less than 1.";
        public static string MORE_INDEX_VALIDATE = "attempted to assign a value to parameter {0} when there are only {1} parameters";
        public static string NO_COLUMN_NAME = "column({0}) does not exist";
        public static string OUT_OF_INDEX = "out of index";
        public static string INVALID_COLUMN_INDEX = "invalid column index";

        public static string FAIL_TO_CONNECT_SERVER = "cannot connect to server on {0}:{1}";
        public static string NO_TABLENAME = "specify tablename.";
        public static string UNSUPPORTED_ENCODING = "unsupported charset";
        public static string TABLE_NOT_EXIST = "table does not exist.";

        public static string CONCURRENT_STATEMENT_EXECUTE = "Failed to execute this statement under {0} connection state.";
        public static string APPEND_DOUBLE_OPEN = "Failed to execute APPEND-OPEN because it is already opened.";
        public static string APPEND_ALREADY_CLOSED = "Failed to execute {0} because it is not opened.";
    }
}
