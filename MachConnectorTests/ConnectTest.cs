using System;
using Xunit;
using Mach.Core;
using Mach.Data.MachClient;
using System.Data.Common;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mach.Comm;
using Xunit.Abstractions;

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
    }
}
