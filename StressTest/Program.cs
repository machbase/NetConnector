using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Mach.Data.MachClient;

namespace TestBed
{
    enum ErrorCheckType
    {
        ERROR_CHECK_YES = 0,
        ERROR_CHECK_WARNING,
        ERROR_CHECK_NO
    }

    class Program
    {
        //----------------------------
        // configurable parameters
        internal const string SERVER_HOST = "192.168.0.31";
        internal const int SERVER_PORT = 23000;
        static String tableName = "VOL_TABLE";
        //static String sCreateQuery = @"CREATE TABLE VOL_TABLE (TAGID    varchar(100),
        //                                                       SENSORID varchar(100),
        //                                                       REGTIME datetime,
        //                                                       VALUE11_RMS double,
        //                                                       VALUE12_RMS double,
        //                                                       VALUE13_RMS double,
        //                                                       VALUE21_RMS double,
        //                                                       VALUE22_RMS double,
        //                                                       VALUE23_RMS double);";
        static String sCreateQuery = @"CREATE LOOKUP TABLE VOL_TABLE (TAGID varchar(100) PRIMARY KEY);";
        static int sSleepSec = 12000;
        static bool isStop = false;
        //----------------------------

        private static MachConnection gConn;

        public static bool DoSelect(MachConnection connection, string iConditionQuery, ErrorCheckType aCheckType, ref string query)
        {
            MachDataReader rs = null;
            Random r = new Random();

            string field = "*";
            string sql = "";

            if (iConditionQuery.Trim() == string.Empty)
                sql = "select " + field + " from " + tableName;
            else
                sql = "select " + field + " from " + tableName + " where " + iConditionQuery;

            // TODO hard-copy?
            query = sql;

            try
            {
                if (connection.State == System.Data.ConnectionState.Broken ||
                    connection.State == System.Data.ConnectionState.Closed)
                {
                    connection.Open();
                }

                MachCommand command = new MachCommand(query, connection);
                command.ParameterCollection.AddWithValue("id", String.Format("TAG-{0}", r.Next(0, 29).ToString("00")));

                try
                {
                    rs = command.ExecuteReader();
                }
                catch (Exception me)
                {
                    switch (aCheckType)
                    {
                        case ErrorCheckType.ERROR_CHECK_YES:
                            Console.WriteLine("Error!" + me.ToString());
                            //throw me;
                            break;
                        case ErrorCheckType.ERROR_CHECK_WARNING:
                            Console.WriteLine("[WARNING!]");
                            Console.WriteLine("{0}", me.ToString());
                            break;
                        case ErrorCheckType.ERROR_CHECK_NO:
                        default:
                            break;
                    }
                    return false;
                }

                // FIXME: 이거 왜 초기화를 안 해주고 record 마다 추가시키지? 한 건이라고 무시한 듯..
                Dictionary<string, int> dicFields = new Dictionary<string, int>();
                bool isSetFields = false;

                //TblSENSOR_RMS oTblSENSOR_RMS = null;

                if (rs.Read())
                {
                    //if (!isSetFields)
                    //{
                    //    isSetFields = true;
                    //    for (int i = 0; i < rs.FieldCount; i++)
                    //    {
                    //        dicFields.Add(rs.GetName(i), i);
                    //    }
                    //}

                    for (int i = 0; i < rs.FieldCount; i++)
                    {
                        Console.Write(String.Format("SELECT : {0} : {1}, ", rs.GetName(i), rs.GetValue(i)));
                    }
                    Console.WriteLine();

                    //oTblSENSOR_RMS = new TblSENSOR_RMS();
                    //if (!System.DBNull.Value.Equals(rs[dicFields["TAGID"]])) { oTblSENSOR_RMS.TAGID = (string)rs[dicFields["TAGID"]]; }
                    //if (!System.DBNull.Value.Equals(rs[dicFields["SENSORID"]])) { oTblSENSOR_RMS.SENSORID = (string)rs[dicFields["SENSORID"]]; }
                    //if (!System.DBNull.Value.Equals(rs[dicFields["REGTIME"]])) { oTblSENSOR_RMS.REGTIME = (System.DateTime)rs[dicFields["REGTIME"]]; }
                    //if (!System.DBNull.Value.Equals(rs[dicFields["VALUE11_RMS"]])) { oTblSENSOR_RMS.VALUE11_RMS = (double)rs[dicFields["VALUE11_RMS"]]; }
                    //if (!System.DBNull.Value.Equals(rs[dicFields["VALUE12_RMS"]])) { oTblSENSOR_RMS.VALUE12_RMS = (double)rs[dicFields["VALUE12_RMS"]]; }
                    //if (!System.DBNull.Value.Equals(rs[dicFields["VALUE13_RMS"]])) { oTblSENSOR_RMS.VALUE13_RMS = (double)rs[dicFields["VALUE13_RMS"]]; }
                    //if (!System.DBNull.Value.Equals(rs[dicFields["VALUE21_RMS"]])) { oTblSENSOR_RMS.VALUE21_RMS = (double)rs[dicFields["VALUE21_RMS"]]; }
                    //if (!System.DBNull.Value.Equals(rs[dicFields["VALUE22_RMS"]])) { oTblSENSOR_RMS.VALUE22_RMS = (double)rs[dicFields["VALUE22_RMS"]]; }
                    //if (!System.DBNull.Value.Equals(rs[dicFields["VALUE23_RMS"]])) { oTblSENSOR_RMS.VALUE23_RMS = (double)rs[dicFields["VALUE23_RMS"]]; }

                    //AddItem(oTblSENSOR_RMS);
                }
                else
                {
                    Console.WriteLine("SELECT : nothing to selected");
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception!" + ex.ToString());
                return false;
            }
            finally
            {
                if (rs != null)
                    rs.Close();
            }
            return true;
        }

        private static void ExecuteQuery(MachConnection aConn, string aQueryString, ErrorCheckType aCheckType)
        {
            try
            {
                Console.WriteLine("== Entering ExecuteQuery " + aConn.State);

                // 쿼리 수행 전 연결 확인
                if (!aConn.IsConnected())
                {
                    Console.WriteLine("Retrying to connect...");
                    aConn.Open();
                }

                // 쿼리 수행
                using (MachCommand sCommand = new MachCommand(aQueryString, aConn))
                {
                    try
                    {
                        Console.WriteLine("Ready to execute...");
                        sCommand.ExecuteNonQuery();
                    }
                    catch (SocketException se)
                    {
                        throw se;
                    }
                    catch (Exception me)
                    {
                        switch (aCheckType)
                        {
                            case ErrorCheckType.ERROR_CHECK_YES:
                                throw me;
                            case ErrorCheckType.ERROR_CHECK_WARNING:
                                Console.WriteLine("[WARNING!]");
                                Console.WriteLine("{0}", me.ToString());
                                break;
                            case ErrorCheckType.ERROR_CHECK_NO:
                            default:
                                break;
                        }
                    }
                }
                Console.WriteLine("== Exiting ExecuteQuery() " + aQueryString + " is succceeded. (Its state : " + aConn.State + ")");
            }
            catch (Exception e)
            {
                Console.WriteLine("== Exiting ExecuteQuery() " + aQueryString + " is failed. (Its state : " + aConn.State + " and exception type : " + e.GetType() + ")");
                throw e;
            }
        }

        static void ExecuteQueryWithRetry(MachConnection aConn, string aQueryString, ErrorCheckType aCheckType, int aRetryCount)
        {
            for (int attempts = 0; attempts < aRetryCount; attempts++)
            {
                try
                {
                    ExecuteQuery(aConn, aQueryString, aCheckType);
                    break;
                }
                catch (SocketException)
                {
                    // Nothing to do but retry...
                }
                catch (Exception e)
                {
                    throw e;
                }
            }
        }

        static void SelectThread()
        {
            MachConnection sConn = new MachConnection(String.Format("DSN={0};PORT_NO={1};UID=SYS;PWD=MANAGER", SERVER_HOST, SERVER_PORT));

            int i = 0;

            while (!isStop)
            {
                // TODO 
                // sCondition 은 랜덤하게 해 줘야 함..

                String sCondition = "";
                String sQuery = "SELECT";
                //if (!DB_SELECT(sConn, sCondition, ErrorCheckType.ERROR_CHECK_YES, ref sQuery))
                //{
                //    Console.WriteLine("What is this?? error??");
                //    break;
                //}
                //else
                //{
                //    // Nothing to do but continue
                //}
                i++;
                Thread.Sleep(100); // 송부받은 Spec 대로 100ms sleep
            }
        }

        static void UpsertThread()
        {
            int i = 0;
            MachConnection sConn = new MachConnection(String.Format("DSN={0};PORT_NO={1};UID=SYS;PWD=MANAGER", SERVER_HOST, SERVER_PORT));

            //String sQuery = "INSERT INTO VOL_TABLE VALUES (@id, @sensorid, @regtime, @value1, @value2, @value3, @value4, @value5, @value6)";
            String sQuery = "INSERT INTO VOL_TABLE VALUES (@id) ON DUPLICATE KEY UPDATE";
            MachCommand sCommand = new MachCommand(sQuery, sConn);

            while (!isStop)
            {
                sCommand.ParameterCollection.AddWithValue("id", String.Format("TAG-{0}", (i % 30).ToString("00")));                 
                // TODO 최소한 sensorid, regtime, value 1개는 넣어라.
                try
                {
                    sCommand.ExecuteNonQuery();
                }
                catch (Exception me)
                {
                    throw me;
                }

                Thread.Sleep(100);
                i++;
            }
        }

        static void Main(string[] args)
        {
            String sResultQuery = "";

            gConn = new MachConnection(String.Format("DSN={0};PORT_NO={1};UID=SYS;PWD=MANAGER", SERVER_HOST, SERVER_PORT));
            // TODO UPSERT 는 다른 Connection 을 만들어서 써야 한다.

            try
            {
                // DROP TABLE & CREATE TABLE
                ExecuteQueryWithRetry(gConn, "DROP TABLE " + tableName + ";", ErrorCheckType.ERROR_CHECK_NO, 10);
                ExecuteQueryWithRetry(gConn, sCreateQuery, ErrorCheckType.ERROR_CHECK_YES, 10);

                // SELECT
                //DoSelect(gConn, "", ErrorCheckType.ERROR_CHECK_YES, ref sResultQuery);
            }
            catch (MachException me)
            {
                if (me.MachErrorCode == 2024) // table already exists
                {
                    Console.WriteLine(">> Warning. CREATE_TABLE is succeeded before something is wrong... but that's okay.");
                }
                else
                {
                    throw me;
                }
            }
            catch (Exception e)
            {
                throw e;
            }

            //Thread t1 = new Thread(new ThreadStart(SelectThread));
            //Thread t2 = new Thread(new ThreadStart(UpsertThread));

            //Console.WriteLine("== threads are starting up...");
            //t1.Start();
            //t2.Start();

            //Console.WriteLine("== threads are running until " + sSleepSec + " miliseconds are elapsed.");
            //Thread.Sleep(sSleepSec);
            //Console.WriteLine("== threads are shutting down...");

            //isStop = true;
            //t1.Join();
            //t2.Join();

            Console.WriteLine("Press any key to exit.");
            Console.Read();
        }
    }
}
