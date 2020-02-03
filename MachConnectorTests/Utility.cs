using System;
using System.Collections.Generic;
using System.Text;
using Mach.Data.MachClient;

namespace MachConnectorTests
{
    enum ErrorCheckType
    {
        ERROR_CHECK_YES = 0,
        ERROR_CHECK_NO
    }

    internal static class Utility
    {
        internal const string SERVER_HOST = "127.0.0.1";
        internal const int SERVER_PORT = 5656;
        internal static string SERVER_STRING
        {
            get
            {
                return String.Format("Server={0};Database=mydb;Uid=sys;pwd=manager;Port={1}", SERVER_HOST, SERVER_PORT);
            }
        }

        internal static void ExecuteQuery(MachConnection aConn, string aQueryString, ErrorCheckType aCheckType)
        {
            using (MachCommand sCommand = new MachCommand(aQueryString, aConn))
            {
                try
                {
                    sCommand.ExecuteNonQuery();
                }
                catch (Exception me)
                {
                    switch (aCheckType)
                    {
                        case ErrorCheckType.ERROR_CHECK_YES:
                            throw me;
                        case ErrorCheckType.ERROR_CHECK_NO:
                        default:
                            break;
                    }
                }
            }
        }

        internal static void ExecuteQuery(MachConnection aConn, string aQueryString)
        {
            ExecuteQuery(aConn, aQueryString, ErrorCheckType.ERROR_CHECK_YES);
        }
    }
}
