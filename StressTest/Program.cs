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
        static readonly String tableName = "VOL_TABLE";
        //static String sCreateQuery = @"CREATE TABLE VOL_TABLE (TAGID    varchar(100),
        //                                                       SENSORID varchar(100),
        //                                                       REGTIME datetime,
        //                                                       VALUE11_RMS double,
        //                                                       VALUE12_RMS double,
        //                                                       VALUE13_RMS double,
        //                                                       VALUE21_RMS double,
        //                                                       VALUE22_RMS double,
        //                                                       VALUE23_RMS double);";
        static readonly String sCreateQuery = @"CREATE LOOKUP TABLE VOL_TABLE (TAGID varchar(100) PRIMARY KEY);";
        static readonly int sSleepMilliSec = 12000;
        static bool isStop = false;
        //----------------------------

        private static MachConnection gConn;
        private static MachConnection gConn2;

        public static void DoSelect(MachConnection aConn, ErrorCheckType aCheckType)
        {
            MachDataReader rs = null;
            Random r = new Random();
            string sQueryString = "";
            string sTagID = "";

            sQueryString = "select * from " + tableName + " where TAGID = @id";

            try
            {
                Console.WriteLine("== Entering DoSelect... " + aConn.State);

                // 쿼리 수행 전 연결 확인
                if (!aConn.IsConnected())
                {
                    Console.WriteLine("== Retrying to connect...");
                    aConn.Open();
                }

                // 쿼리 수행 & 페치
                using (MachCommand sCommand = new MachCommand(sQueryString, aConn))
                {
                    // 랜덤 태그 선택
                    sTagID = String.Format("TAG-{0}", r.Next(0, 29).ToString("00"));
                    sCommand.ParameterCollection.AddWithValue("id", sTagID);

                    rs = sCommand.ExecuteReader();
                    bool isFetched = false;

                    while (rs.Read())
                    {
                        isFetched = true;
                        for (int i = 0; i < rs.FieldCount; i++)
                        {
                            Console.Write(String.Format("SELECT : {0} : {1}, ", rs.GetName(i), rs.GetValue(i)));
                        }
                        Console.WriteLine();
                    }

                    if (!isFetched)
                    {
                        Console.WriteLine("== DoSelect() : nothing to selected with tagID '{0}'", sTagID);
                    }
                    else
                    {
                        Console.WriteLine("== DoSelect() completed to scan with tagID '{0}'", sTagID);
                    }
                }
            }
            catch (SocketException se)
            {
                Console.WriteLine("== Exiting DoSelect() " + sQueryString + " is failed. (Its state : " + aConn.State + " and exception type : " + se.GetType() + ")");
                throw se;
            }
            catch (Exception me)
            {
                switch (aCheckType)
                {
                    case ErrorCheckType.ERROR_CHECK_YES:
                        Console.WriteLine("== Exiting DoSelect() " + sQueryString + " is failed. (Its state : " + aConn.State + " and exception type : " + me.GetType() + ")");
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
            finally
            {
                if (rs != null)
                    rs.Close();
            }
        }

        public static void DoUpsert(MachConnection aConn, ErrorCheckType aCheckType)
        {
            Random r = new Random();
            string sQueryString = "INSERT INTO VOL_TABLE VALUES (@id) ON DUPLICATE KEY UPDATE";
            string sTagID = "";

            try
            {
                Console.WriteLine("== Entering DoUpsert... " + aConn.State);

                // 쿼리 수행 전 연결 확인
                if (!aConn.IsConnected())
                {
                    Console.WriteLine("== Retrying to connect...");
                    aConn.Open();
                }

                // 쿼리 수행
                using (MachCommand sCommand = new MachCommand(sQueryString, aConn))
                {
                    // 랜덤 태그 선택
                    sTagID = String.Format("TAG-{0}", r.Next(0, 29).ToString("00"));
                    sCommand.ParameterCollection.AddWithValue("id", sTagID);
                    sCommand.ExecuteNonQuery();
                    Console.WriteLine("== Exiting DoUpsert() : '{0}' is inserted", sTagID);
                }
            }
            catch (SocketException se)
            {
                Console.WriteLine("== Exiting DoUpsert() " + sQueryString + " is failed. (Its state : " + aConn.State + " and exception type : " + se.GetType() + ")");
                throw se;
            }
            catch (Exception me)
            {
                switch (aCheckType)
                {
                    case ErrorCheckType.ERROR_CHECK_YES:
                        Console.WriteLine("== Exiting DoUpsert() " + sQueryString + " is failed. (Its state : " + aConn.State + " and exception type : " + me.GetType() + ")");
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
                    Console.WriteLine("Ready to execute...");
                    sCommand.ExecuteNonQuery();
                }
                Console.WriteLine("== Exiting ExecuteQuery() " + aQueryString + " is succceeded. (Its state : " + aConn.State + ")");
            }
            catch (SocketException se)
            {
                Console.WriteLine("== Exiting ExecuteQuery() " + aQueryString + " is failed. (Its state : " + aConn.State + " and exception type : " + se.GetType() + ")");
                throw se;
            }
            catch (Exception me)
            {
                switch (aCheckType)
                {
                    case ErrorCheckType.ERROR_CHECK_YES:
                        Console.WriteLine("== Exiting ExecuteQuery() " + aQueryString + " is failed. (Its state : " + aConn.State + " and exception type : " + me.GetType() + ")");
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
            int i = 0;

            while (!isStop)
            {
                Thread.Sleep(100); // 송부받은 Spec 대로 100ms sleep

                try
                {
                    DoSelect(gConn, ErrorCheckType.ERROR_CHECK_YES);
                }
                catch (SocketException)
                {
                    // Nothing to do but retry...
                }
                catch (Exception e)
                {
                    throw e;
                }
                
                i++;
            }
        }

        static void UpsertThread()
        {
            int i = 0;

            while (!isStop)
            {
                Thread.Sleep(100); // 송부받은 Spec 대로 100ms sleep

                try
                {
                    DoUpsert(gConn2, ErrorCheckType.ERROR_CHECK_YES);
                }
                catch (SocketException)
                {
                    // Nothing to do but retry...
                }
                catch (Exception e)
                {
                    throw e;
                }

                i++;
            }
        }

        static void Main(string[] args)
        {
            gConn = new MachConnection(String.Format("DSN={0};PORT_NO={1};UID=SYS;PWD=MANAGER", SERVER_HOST, SERVER_PORT));
            gConn2 = new MachConnection(String.Format("DSN={0};PORT_NO={1};UID=SYS;PWD=MANAGER", SERVER_HOST, SERVER_PORT));

            try
            {
                // DROP TABLE & CREATE TABLE
                ExecuteQueryWithRetry(gConn, "DROP TABLE " + tableName + ";", ErrorCheckType.ERROR_CHECK_NO, 10);
                ExecuteQueryWithRetry(gConn, sCreateQuery, ErrorCheckType.ERROR_CHECK_YES, 10);

                
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

            // SELECT & UPSERT
            Console.WriteLine("====================================================");
            Thread t1 = new Thread(new ThreadStart(SelectThread));
            Thread t2 = new Thread(new ThreadStart(UpsertThread));

            Console.WriteLine("== threads are starting up...");
            t1.Start();
            t2.Start();

            Console.WriteLine("== threads are running until " + sSleepMilliSec + " miliseconds are elapsed.");
            Thread.Sleep(sSleepMilliSec);
            Console.WriteLine("== threads are shutting down...");

            isStop = true;
            t1.Join();
            t2.Join();

            Console.WriteLine("Press any key to exit.");
            Console.Read();
        }
    }
}
