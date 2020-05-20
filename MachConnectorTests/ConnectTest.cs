using System;
using Xunit;
using Mach.Core;
using Mach.Data.MachClient;
using System.Data.Common;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Mach.Comm;
using Xunit.Abstractions;
using Dapper;

namespace MachConnectorTests
{
    public class ConnectTest
    {
        private readonly ITestOutputHelper output;

        public ConnectTest(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public void ConnectWithMach()
        {
            MachConnection sConn = new MachConnection(Utility.SERVER_STRING);
            sConn.Open();
            Assert.True(sConn.State == ConnectionState.Open, "Not connected");
            sConn.Close();
        }

        [Fact]
        public void ConnectAndExecDirect()
        {
            using (MachConnection sConn = new MachConnection(Utility.SERVER_STRING))
            {
                sConn.Open();

                Utility.ExecuteQuery(sConn, "drop table t1;", ErrorCheckType.ERROR_CHECK_NO);
                Utility.ExecuteQuery(sConn, "create table t1 (i1 int, i2 int);");
                Utility.ExecuteQuery(sConn, "insert into t1 values (1, 2);");
                Utility.ExecuteQuery(sConn, "insert into t1(i1) values (2);");

                using (MachCommand sCommand = new MachCommand("select * from t1;", sConn))
                {
                    MachDataReader sDataReader = sCommand.ExecuteReader();

                    while (sDataReader.Read())
                    {
                        output.WriteLine("==========");
                        for (int i = 0; i < sDataReader.FieldCount; i++)
                        {
                            output.WriteLine(String.Format("{0} : {1} ({2}/{3})",
                                                           sDataReader.GetName(i),
                                                           sDataReader.GetValue(i),
                                                           sDataReader.GetDataTypeName(i),
                                                           sDataReader.GetFieldType(i)));
                        }

                        if ((int)(sDataReader.GetValue(0)) == 2)
                        {
                            Assert.True(sDataReader.IsDBNull(1) == true);
                        }
                        else
                        {
                            Assert.True(sDataReader.IsDBNull(1) == false);
                        }
                    }
                }

                Utility.ExecuteQuery(sConn, "drop table t1;");
            }
        }

        [Fact]
        public void SelectWithParam()
        {
            using (MachConnection sConn = new MachConnection(Utility.SERVER_STRING))
            {
                sConn.Open();

                Utility.ExecuteQuery(sConn, "drop table t1;", ErrorCheckType.ERROR_CHECK_NO);
                Utility.ExecuteQuery(sConn, "create table t1 (id integer, start_date datetime);");
                Utility.ExecuteQuery(sConn, "insert into t1 values (1, '2017-12-21');");
                Utility.ExecuteQuery(sConn, "insert into t1 values (2, '2017-12-22');");
                Utility.ExecuteQuery(sConn, "insert into t1 values (3, '2017-12-23');");
                Utility.ExecuteQuery(sConn, "insert into t1 values (4, '2017-12-24');");

                using (MachCommand sCommand = new MachCommand("select * from t1 where start_date > @Date;", sConn, null))
                {
                    sCommand.ParameterCollection.Add(new MachParameter { ParameterName = "@Date", Value = new DateTime(2017, 12, 22, 0, 0, 0) });

                    MachDataReader sDataReader = sCommand.ExecuteReader();
                    while (sDataReader.Read())
                    {
                        output.WriteLine("==========");
                        for (int i = 0; i < sDataReader.FieldCount; i++)
                        {
                            output.WriteLine(String.Format("{0} : {1}",
                                                           sDataReader.GetName(i),
                                                           sDataReader.GetValue(i)));
                        }

                        Assert.True((int)(sDataReader.GetValue(0)) > 2);
                    }

                    sCommand.ParameterCollection.Clear();

                    sCommand.ParameterCollection.Add(new MachParameter { ParameterName = "@Date", Value = new DateTime(2017, 12, 23, 0, 0, 0) });

                    sDataReader = sCommand.ExecuteReader();
                    while (sDataReader.Read())
                    {
                        output.WriteLine("==========");
                        for (int i = 0; i < sDataReader.FieldCount; i++)
                        {
                            output.WriteLine(String.Format("{0} : {1}",
                                                           sDataReader.GetName(i),
                                                           sDataReader.GetValue(i)));
                        }

                        Assert.True((int)(sDataReader.GetValue(0)) > 3);
                    }
                }

                Utility.ExecuteQuery(sConn, "drop table t1;");
            }
        }

        [Fact]
        public void AppendOpenCloseTest()
        {
            int sResult = 0;

            using (MachConnection sConn = new MachConnection(Utility.SERVER_STRING))
            {
                sConn.Open();

                Utility.ExecuteQuery(sConn, "drop table t1;", ErrorCheckType.ERROR_CHECK_NO);
                Utility.ExecuteQuery(sConn, "create table t1 (i1 int);");

                using (MachCommand sCommand = new MachCommand(sConn, null))
                {
                    MachAppendWriter sWriter = sCommand.AppendOpen("t1");

                    var sList = new List<object>();
                    for (int i = 1; i <= 10; i++)
                    {
                        sList.Add(i);
                        sCommand.AppendData(sWriter, sList);
                        sList.Clear();
                    }


                    sCommand.AppendClose(sWriter);
                }

                using (MachCommand sCommand = new MachCommand("select * from t1;", sConn, null))
                {
                    MachDataReader sDataReader = sCommand.ExecuteReader();
                    while (sDataReader.Read())
                    {
                        output.WriteLine("==========");
                        for (int i = 0; i < sDataReader.FieldCount; i++)
                        {
                            output.WriteLine(String.Format("{0} : {1}",
                                                           sDataReader.GetName(i),
                                                           sDataReader.GetValue(i)));
                        }

                        sResult += (int)(sDataReader.GetValue(0));


                    }
                    Assert.True(sResult == 55);
                }

                Utility.ExecuteQuery(sConn, "drop table t1;");
            }
        }

        [Fact]
        public void AppendNullTest()
        {
            int sIndex = 0;

            using (MachConnection sConn = new MachConnection(Utility.SERVER_STRING))
            {
                sConn.Open();

                Utility.ExecuteQuery(sConn, "drop table t1;", ErrorCheckType.ERROR_CHECK_NO);
                Utility.ExecuteQuery(sConn, "create table t1 (id integer, i1 short, i2 uinteger, i3 long, i4 datetime);");

                using (MachCommand sCommand = new MachCommand(sConn, null))
                {
                    MachAppendWriter sWriter = sCommand.AppendOpen("t1");

                    var sList = new List<object>();

                    for (int i = 1; i <= 15; i++)
                    {
                        sList.Add(i);

                        if (i % 5 != 0)
                            sList.Add(DBNull.Value);
                        else
                            sList.Add((short)(i - 100));

                        if (i % 3 != 0)
                            sList.Add(DBNull.Value);
                        else
                            sList.Add((uint)(i * 2));

                        if (i % 7 != 0)
                            sList.Add(DBNull.Value);
                        else
                            sList.Add((long)(i * 10));

                        if (i % 2 != 0)
                            sList.Add(DBNull.Value);
                        else
                            sList.Add(DateTime.UtcNow);

                        sCommand.AppendData(sWriter, sList);
                        sList.Clear();
                    }

                    sCommand.AppendClose(sWriter);
                }

                using (MachCommand sCommand = new MachCommand("select * from t1;", sConn))
                {
                    MachDataReader sDataReader = sCommand.ExecuteReader();

                    while (sDataReader.Read())
                    {
                        output.WriteLine("==========");
                        for (int i = 0; i < sDataReader.FieldCount; i++)
                        {
                            output.WriteLine(String.Format("{0} : {1} ({2}/{3})",
                                                           sDataReader.GetName(i),
                                                           sDataReader.GetValue(i),
                                                           sDataReader.GetDataTypeName(i),
                                                           sDataReader.GetFieldType(i)));
                        }

                        sIndex = (int)(sDataReader.GetValue(0));

                        if (sIndex % 5 != 0)
                            Assert.True(sDataReader.IsDBNull(1) == true);

                        if (sIndex % 3 != 0)
                            Assert.True(sDataReader.IsDBNull(2) == true);

                        if (sIndex % 7 != 0)
                            Assert.True(sDataReader.IsDBNull(3) == true);

                        if (sIndex % 2 != 0)
                            Assert.True(sDataReader.IsDBNull(4) == true);
                    }
                }

                Utility.ExecuteQuery(sConn, "drop table t1;");
            }
        }

        [Fact]
        public void AppendIPTest()
        {
            using (MachConnection sConn = new MachConnection(Utility.SERVER_STRING))
            {
                sConn.Open();

                Utility.ExecuteQuery(sConn, "drop table t1;", ErrorCheckType.ERROR_CHECK_NO);
                Utility.ExecuteQuery(sConn, "create table t1 (i1 ipv4);");

                using (MachCommand sCommand = new MachCommand(sConn, null))
                {
                    MachAppendWriter sWriter = sCommand.AppendOpen("t1");

                    var sList = new List<object>();
                    for (int i = 1; i <= 2; i++)
                    {
                        sList.Add(String.Format("192.168.0.{0}", i));

                        sCommand.AppendData(sWriter, sList);
                        sList.Clear();
                    }

                    sCommand.AppendClose(sWriter);
                }

                using (MachCommand sCommand = new MachCommand("select * from t1 order by 1;", sConn, null))
                {
                    MachDataReader sDataReader = sCommand.ExecuteReader();

                    while (sDataReader.Read())
                    {
                        output.WriteLine("==========");
                        for (int i = 0; i < sDataReader.FieldCount; i++)
                        {
                            output.WriteLine(String.Format("{0} : {1}",
                                                           sDataReader.GetName(i),
                                                           sDataReader.GetValue(i)));
                        }
                    }
                }

                Utility.ExecuteQuery(sConn, "drop table t1;");
            }
        }

        [Fact]
        public void AppendWithTimeTest()
        {
            using (MachConnection sConn = new MachConnection(Utility.SERVER_STRING))
            {
                sConn.Open();

                Utility.ExecuteQuery(sConn, "drop table t1;", ErrorCheckType.ERROR_CHECK_NO);
                sConn.Execute("create table t1 (dt datetime);");
                DateTime sNow = DateTime.Now;
                DateTime sRead = sNow.AddSeconds(-1);

                using (MachCommand sCommand = new MachCommand(sConn, null))
                {
                    MachAppendWriter sWriter = sCommand.AppendOpen("t1");
                    var sList = new List<object>();

                    for (int i = 0; i < 12; i++)
                    {
                        sList.Add(sNow.AddSeconds(10+i));

                        sCommand.AppendDataWithTime(sWriter, sList, sNow.AddSeconds(i), "");
                        sList.Clear();
                    }

                    sCommand.AppendClose(sWriter);
                }

                output.WriteLine("========== (select _arrival_time, dt from t1)");

                var sData = sConn.Query("select _arrival_time, dt from t1").ToList();
                foreach (var sItem in sData)
                {
                    output.WriteLine(String.Format("{0} : {1}", sItem._arrival_time, sItem.dt));
                    sRead = sItem._arrival_time;
                }

                sConn.Execute("drop table t1;");
                sConn.Close();

                output.WriteLine("========== Check first _arrival_time");

                output.WriteLine(String.Format("append : {0} / select : {1}", sNow.ToString("yyyy-MM-dd HH:mm:ss"), sRead.ToString("yyyy-MM-dd HH:mm:ss")));
                Assert.True(sNow.ToString("yyyy-MM-dd HH:mm:ss").CompareTo(sRead.ToString("yyyy-MM-dd HH:mm:ss")) == 0);
            }
        }

        [Fact]
        public void DatetimeTest()
        {
            string[] m_dateFormat =
            {
                "yyyy-MM-dd HH:mm:ss fffffff",
                "yyyy-MM-dd HH:mm:ss ffffff",
                "yyyy-MM-dd HH:mm:ss fff",
                "yyyy-MM-dd HH:mm",
                "yyyy-MM-dd HH",
                "yyyy-MM-dd",
                "yyyy-MM",
                "yyyy",
                "MM-yyyy",
                "dd-MM-yyyy",
                "HH MM-dd-yyyy",
                "ss:mm:HH yyyy-MM-dd"
            };
            using (MachConnection sConn = new MachConnection(Utility.SERVER_STRING))
            {
                sConn.Open();

                Utility.ExecuteQuery(sConn, "drop table t1;", ErrorCheckType.ERROR_CHECK_NO);
                Utility.ExecuteQuery(sConn, "create table t1 (d1 datetime);");

                using (MachCommand sCommand = new MachCommand(sConn, null))
                {
                    MachAppendWriter sWriter = sCommand.AppendOpen("t1");
                    string dt;
                    var sList = new List<object>();
                    List<string> sDateFormatList = new List<string>();

                    for (int i = 0; i < 12; i++)
                    {
                        sDateFormatList.Add(m_dateFormat[i]);
                        dt = DateTime.Now.ToString(m_dateFormat[i]);            // DateFormatList
                        sList.Add(dt);

                        sCommand.AppendData(sWriter, sList, sDateFormatList);
                        sList.Clear();
                    }
                    dt = DateTime.Now.ToString("yyyy-dd-MM HH:mm:ss fff");      // Specified DateFormat
                    sList.Add(dt);

                    sCommand.AppendData(sWriter, sList, "yyyy-dd-MM HH:mm:ss fff");
                    sList.Clear();

                    sCommand.AppendClose(sWriter);
                }

                using (MachCommand sCommand = new MachCommand("select * from t1 order by 1;", sConn, null))
                {
                    MachDataReader sDataReader = sCommand.ExecuteReader();

                    while (sDataReader.Read())
                    {
                        output.WriteLine("==========");
                        for (int i = 0; i < sDataReader.FieldCount; i++)
                        {
                            output.WriteLine(String.Format("{0} : {1}",
                                                           sDataReader.GetName(i),
                                                           sDataReader.GetValue(i)));
                        }
                    }
                }

                Utility.ExecuteQuery(sConn, "drop table t1;");
            }
        }

        [Fact]
        public void DatetimeBindInsertTest()
        {
            using (MachConnection sConn = new MachConnection(Utility.SERVER_STRING))
            {
                sConn.Open();

                Utility.ExecuteQuery(sConn, "drop table t1;", ErrorCheckType.ERROR_CHECK_NO);
                sConn.Execute("create table t1 (id integer, dt datetime);");
                DateTime sNow = DateTime.Now;
                DateTime sRead;
                sConn.Execute("insert into t1 values (@id, @dt)", new[] { new { id = 0, dt = sNow }, new { id = 1, dt = sNow.AddSeconds(1) }, new { id = 2, dt = sNow.AddSeconds(2) } });

                output.WriteLine("========== (select * from t1)");

                var sData = sConn.Query("select * from t1").ToList();
                foreach (var sItem in sData)
                {
                    output.WriteLine(String.Format("{0} : {1}", sItem.ID, sItem.DT));
                }

                output.WriteLine("========== (select * from t1 where id=@id)");

                sData = sConn.Query("select * from t1 where id=@id", new { id = 0 }).ToList();
                sRead = sData[0].DT;
                output.WriteLine(String.Format("{0} : {1}", sData[0].ID, sRead));
                sConn.Execute("drop table t1;");
                sConn.Close();

                output.WriteLine("========== Check Result");

                output.WriteLine(String.Format("Insert : {0} / Select : {1}", sNow.ToString("yyyy-MM-dd HH:mm:ss"), sRead.ToString("yyyy-MM-dd HH:mm:ss")));
                Assert.True( sNow.ToString("yyyy-MM-dd HH:mm:ss").CompareTo(sRead.ToString("yyyy-MM-dd HH:mm:ss")) == 0 );
            }
        }

        /*****************************************************************
         * For testing of storing protocol payload buffer
         *****************************************************************/

        [Fact]
        public void StringTest()
        {
            var m_dict = new Dictionary<string, byte[]>
            {
                { "empty", new byte[8] },
                { "H_Query", BitConverter.GetBytes(10) },
                { "QueryString", null },
                { "H_ID", BitConverter.GetBytes(20) },
                { "StatementID", null },
            };

            m_dict["QueryString"] = Encoding.UTF8.GetBytes("select * from t1");
            var m_bytes = m_dict.Values.SelectMany(a => a ?? new byte[1]).ToArray();

            Assert.True(m_bytes[8] == 10);
            Assert.True(m_bytes[12] == 's');
        }

        [Fact]
        public void PayloadUtilityTest()
        {
            byte[] m_byte = PacketConverter.WriteStringWithLength("Hello World!");
            Assert.True(m_byte[0] == 12); // aspect to little-endian
            Assert.True(m_byte[8] == 'H');
        }

        [Fact]
        public void FlatternTest()
        {
            List<ArraySegment<byte>> sRecvList = new List<ArraySegment<byte>>();
            sRecvList.Add(new ArraySegment<byte>(BitConverter.GetBytes(10)));
            sRecvList.Add(new ArraySegment<byte>(Encoding.UTF8.GetBytes("Hello")));
            byte[] sFlattern = sRecvList.SelectMany(a => a.Array).ToArray();
            Assert.True(sFlattern[4] == 'H');

            ArraySegment<byte> ss = new ArraySegment<byte>(Encoding.UTF8.GetBytes("Hello"), 1, 3);
            //Console.WriteLine(sss);
        }

        [Fact]
        public void BitConverterTest()
        {
            byte[] sBytes = new byte[10] { 100, 0, 0, 0, 0, 0, 0, 0, 255, 100 };
            Array.Reverse(sBytes, 1, 8);
            Assert.True(sBytes[1] == 255);
            //Assert.True(BitConverter.ToInt64(sBytes.Reverse().ToArray(), 0) == 255);
        }

        /*****************************************************************
         * For testing Dapper
         *****************************************************************/

        [Fact]
        public void DapperTest()
        {
            using (MachConnection sConn = new MachConnection(Utility.SERVER_STRING))
            {
                sConn.Open();
                sConn.Execute("create table t1 (id varchar(20), i1 integer, i2 long);");
                //sConn.Execute("insert into t1 values ('ID1', 11, 12);");
                //sConn.Execute("insert into t1 values ('ID2', 21, 22);");
                //sConn.Execute("insert into t1 values ('ID3', 31, 32);");
                sConn.Execute("insert into t1 values (@a, @b, @c)", new[] { new { a = "ID1", b = 11, c = 12 }, new { a = "ID2", b = 21, c = 22 }, new { a = "ID3", b = 31, c = 32 } });

                output.WriteLine("========== (select * from t1)");

                var sData = sConn.Query<sTest>("select * from t1").ToList();
                foreach (var sItem in sData)
                {
                    output.WriteLine(String.Format("{0} : {1}, {2}", sItem.id, sItem.i1, sItem.i2));
                }

                output.WriteLine("========== (select * from t1 where id=@id)");

                sData = sConn.Query<sTest>("select * from t1 where id=@id", new {id="ID1"}).ToList();
                foreach (var sItem in sData)
                {
                    output.WriteLine(String.Format("{0} : {1}, {2}", sItem.id, sItem.i1, sItem.i2));
                }
                sConn.Execute("drop table t1;");
                sConn.Close();
            }
        }

        /*****************************************************************
         * For testing SetConnectAppendFlush() & SetAppendInterval()
         *****************************************************************/

        [Fact]
        public void TimedFlushTest()
        {
            int sCheck3, sCheck5 = 0;
            using (MachConnection sConn = new MachConnection(Utility.SERVER_STRING), sConn1 = new MachConnection(Utility.SERVER_STRING), sConn2 = new MachConnection(Utility.SERVER_STRING), sConn3 = new MachConnection(Utility.SERVER_STRING))
            {
                sConn.Open();
                sConn1.Open();
                sConn2.Open();
                sConn3.Open();

                Utility.ExecuteQuery(sConn, "drop table t1;", ErrorCheckType.ERROR_CHECK_NO);
                Utility.ExecuteQuery(sConn, "drop table t2;", ErrorCheckType.ERROR_CHECK_NO);
                Utility.ExecuteQuery(sConn, "drop table t3;", ErrorCheckType.ERROR_CHECK_NO);
                Utility.ExecuteQuery(sConn, "create table t1 (s1 varchar(20));");
                Utility.ExecuteQuery(sConn, "create table t2 (s1 varchar(20));");
                Utility.ExecuteQuery(sConn, "create table t3 (s1 varchar(20));");

                output.WriteLine("Set Auto Append Flush on T1 & T2 (interval = 1 sec)");
                sConn1.SetConnectAppendFlush(true);
                sConn2.SetConnectAppendFlush(true);

                using (MachCommand sCommand = new MachCommand(sConn), sCommand1 = new MachCommand(sConn1), sCommand2 = new MachCommand(sConn2, null), sCommand3 = new MachCommand(sConn3))
                {
                    MachAppendWriter sWriter1 = sCommand1.AppendOpen("t1");
                    MachAppendWriter sWriter2 = sCommand2.AppendOpen("t2");
                    MachAppendWriter sWriter3 = sCommand3.AppendOpen("t3");
                    output.WriteLine("Set Append Flush interval on T2 to 5 sec.");
                    sCommand2.SetAppendInterval(5000);

                    output.WriteLine("Append 100 rows to all tables.");
                    var sList = new List<object>();
                    for (int i = 0; i < 100; i++)
                    {
                        sList.Add(i.ToString());
                        sCommand1.AppendData(sWriter1, sList);
                        sCommand2.AppendData(sWriter2, sList);
                        sCommand3.AppendData(sWriter3, sList);
                        sList.Clear();
                    }

                    output.WriteLine("==========");
                    var sData = sConn.Query<sCount>("select count(*) cnt from t1;").ToList();
                    output.WriteLine(String.Format("T1 : {0}", sData[0].cnt));
                    sData = sConn.Query<sCount>("select count(*) cnt from t2;").ToList();
                    output.WriteLine(String.Format("T2 : {0}", sData[0].cnt));
                    sData = sConn.Query<sCount>("select count(*) cnt from t3;").ToList();
                    output.WriteLine(String.Format("T3 : {0}", sData[0].cnt));

                    Thread.Sleep(3000);
                    output.WriteLine("========== (sleep 3 sec, T1 -> 100)");
                    sData = sConn.Query<sCount>("select count(*) cnt from t1;").ToList();
                    sCheck3 = sData[0].cnt;
                    output.WriteLine(String.Format("T1 : {0}", sData[0].cnt));
                    sData = sConn.Query<sCount>("select count(*) cnt from t2;").ToList();
                    output.WriteLine(String.Format("T2 : {0}", sData[0].cnt));
                    sData = sConn.Query<sCount>("select count(*) cnt from t3;").ToList();
                    output.WriteLine(String.Format("T3 : {0}", sData[0].cnt));

                    Thread.Sleep(5000);
                    output.WriteLine("========== (sleep 5 sec, T1 & T2 -> 100)");
                    sData = sConn.Query<sCount>("select count(*) cnt from t1;").ToList();
                    output.WriteLine(String.Format("T1 : {0}", sData[0].cnt));
                    sData = sConn.Query<sCount>("select count(*) cnt from t2;").ToList();
                    sCheck5 = sData[0].cnt;
                    output.WriteLine(String.Format("T2 : {0}", sData[0].cnt));
                    sData = sConn.Query<sCount>("select count(*) cnt from t3;").ToList();
                    output.WriteLine(String.Format("T3 : {0}", sData[0].cnt));

                    sCommand1.AppendClose(sWriter1);
                    sCommand2.AppendClose(sWriter2);
                    sCommand3.AppendClose(sWriter3);

                    sConn1.SetConnectAppendFlush(false);
                    sConn2.SetConnectAppendFlush(false);

                    output.WriteLine("========== (append close, all -> 100)");
                    sData = sConn.Query<sCount>("select count(*) cnt from t1;").ToList();
                    output.WriteLine(String.Format("T1 : {0}", sData[0].cnt));
                    sData = sConn.Query<sCount>("select count(*) cnt from t2;").ToList();
                    output.WriteLine(String.Format("T2 : {0}", sData[0].cnt));
                    sData = sConn.Query<sCount>("select count(*) cnt from t3;").ToList();
                    output.WriteLine(String.Format("T3 : {0}", sData[0].cnt));
                }

                Utility.ExecuteQuery(sConn, "drop table t1;", ErrorCheckType.ERROR_CHECK_NO);
                Utility.ExecuteQuery(sConn, "drop table t2;", ErrorCheckType.ERROR_CHECK_NO);
                Utility.ExecuteQuery(sConn, "drop table t3;", ErrorCheckType.ERROR_CHECK_NO);

                sConn.Close();
                sConn1.Close();
                sConn2.Close();
                sConn3.Close();

                Assert.True(sCheck3 == 100 && sCheck5 == 100);
            }
        }
    }
    class sTest
    {
        public string id { get; set; }
        public int i1 { get; set; }
        public long i2 { get; set; }
    }
    class sCount
    {
        public int cnt { get; set; }
    }
}
