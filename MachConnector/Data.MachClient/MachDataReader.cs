using Mach.Comm;
using Mach.Core;
using Mach.Core.Result;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Net.Sockets;

namespace Mach.Data.MachClient
{
    public sealed class MachDataReader : DbDataReader
    {
        internal static MachDataReader Create(MachCommand aCommand, CommandBehavior aBehavior)
        {
            var sDataReader = new MachDataReader(aCommand, aBehavior);

            sDataReader.FetchSize = aCommand.FetchSize;

            //try
            //{
            //    dataReader.ReadFirstResultSet();
            //}
            //catch (Exception)
            //{
            //    dataReader.Dispose();
            //    throw;
            //}

            return sDataReader;
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if (disposing)
                    DoClose();
            }
            finally
            {
                base.Dispose(disposing);
            }
        }

        internal void ReadFirstResultSet()
        {
            //m_resultSet = new ResultSet(this).ReadResultSetHeader();
            //ActivateResultSet(m_resultSet);
            //m_resultSetBuffered = m_resultSet;
        }

        private MachDataReader(MachCommand command, CommandBehavior behavior)
        {
            Command = command;
            m_behavior = behavior;
            ResultSet = new ResultSet();
        }

        Session m_session;

        internal MachCommand Command { get; private set; }
        internal Session Session { get => m_session; set => m_session = value; }
        internal ResultSet ResultSet { get; set; }
        internal FetchProtocol FetchDataProtocol { get; private set; }
        public long FetchSize { get; internal set; }

        private CommandBehavior m_behavior;

        public override void Close()
        {
            DoClose();
        }

        public override string GetName(int ordinal) => GetResultSet().GetName(ordinal);

        public override int GetValues(object[] values) => GetResultSet().GetCurrentRow().GetValues(values);

        public override bool IsDBNull(int ordinal) => GetResultSet().GetCurrentRow().IsDBNull(ordinal);

        public override int FieldCount => GetResultSet().ColumnCount;

        public override object this[int ordinal] => GetResultSet().GetCurrentRow()[ordinal];

        public override object this[string name] => GetResultSet().GetCurrentRow()[name];

        public override bool HasRows => GetResultSet().HasRows;

        public override bool IsClosed => Command == null;

        public override int RecordsAffected => GetResultSet().RecordsAffected;

        public override int GetOrdinal(string name) => GetResultSet().GetOrdinal(name);

        public override bool GetBoolean(int ordinal) => GetResultSet().GetCurrentRow().GetBoolean(ordinal);

        public override byte GetByte(int ordinal) => GetResultSet().GetCurrentRow().GetByte(ordinal);

        public sbyte GetSByte(int ordinal) => GetResultSet().GetCurrentRow().GetSByte(ordinal);

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
            => GetResultSet().GetCurrentRow().GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);

        public override char GetChar(int ordinal) => GetResultSet().GetCurrentRow().GetChar(ordinal);

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
            => GetResultSet().GetCurrentRow().GetChars(ordinal, dataOffset, buffer, bufferOffset, length);

        public override short GetInt16(int ordinal) => GetResultSet().GetCurrentRow().GetInt16(ordinal);

        public override int GetInt32(int ordinal) => GetResultSet().GetCurrentRow().GetInt32(ordinal);

        public override long GetInt64(int ordinal) => GetResultSet().GetCurrentRow().GetInt64(ordinal);

        public override string GetDataTypeName(int ordinal) => GetResultSet().GetDataTypeName(ordinal);

        public override Type GetFieldType(int ordinal) => GetResultSet().GetFieldType(ordinal);

        public override object GetValue(int ordinal) => GetResultSet().GetCurrentRow().GetValue(ordinal);

        public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

        public override int Depth => throw new NotSupportedException();

        protected override DbDataReader GetDbDataReader(int ordinal) => throw new NotSupportedException();

        public override DateTime GetDateTime(int ordinal) => GetResultSet().GetCurrentRow().GetDateTime(ordinal);

        public DateTimeOffset GetDateTimeOffset(int ordinal) => GetResultSet().GetCurrentRow().GetDateTimeOffset(ordinal);

        public override string GetString(int ordinal) => GetResultSet().GetCurrentRow().GetString(ordinal);

        public override decimal GetDecimal(int ordinal) => GetResultSet().GetCurrentRow().GetDecimal(ordinal);

        public override double GetDouble(int ordinal) => GetResultSet().GetCurrentRow().GetDouble(ordinal);

        public override float GetFloat(int ordinal) => GetResultSet().GetCurrentRow().GetFloat(ordinal);
        
        public override Guid GetGuid(int ordinal)
        {
            throw new NotSupportedException();
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotSupportedException();
            /*
            if (m_schemaTable == null)
            {
                m_schemaTable = BuildSchemaTable();
            }

            return m_schemaTable;
            */
        }

        public override bool NextResult()
        {
            // if command has more than one query,
            // get the next query's result..
            //throw new NotSupportedException();
            return false;
        }

        public override bool Read()
        {
            // move record in current result set's row queue
            // true : there is dequeued row (set it as currentRow to read by GetXXX())
            // false : no dequeued row exists.

            try
            {
                bool sExists = ResultSet.Read(); // set current row from dequeued row
                bool sRealExists = true;

                if (sExists == false)
                {
                    if (ResultSet.IsFetchEnd == false)
                    {
                        if (FetchDataProtocol == null)
                        {
                            FetchDataProtocol = new FetchProtocol();
                            FetchDataProtocol.Generate(this);
                        }
                        Command.Connection.Session.Transmit(FetchDataProtocol, Command.Connection.DefaultCommandTimeout);
                        sRealExists = ResultSet.Read();
                    }
                    else
                    {
                        sRealExists = false;
                    }

                    if (!sRealExists)
                    {
                        Command.Cancel();
                        Command.Connection.SetState(ConnectionState.Open);
                    }
                    return sRealExists;
                }
                else
                {
                    return true;
                }
            }
            catch (SocketException se)
            {
                Command.Connection.Close(ConnectionState.Broken);
                throw se;
            }
        }

        private void VerifyNotDisposed()
        {
            if (Command == null)
                throw new ObjectDisposedException(GetType().Name);
        }

        private ResultSet GetResultSet()
        {
            VerifyNotDisposed();
            return ResultSet ?? throw new InvalidOperationException("There is no current result set.");
        }

        private void DoClose()
        {
            if (Command != null)
            {
                if (ResultSet != null)
                {
                    // Command.Connection.Session.SetTimeout(Constants.InfiniteTimeout);
                    ResultSet = null;
                }

                //m_resultSetBuffered = null;
                //m_nextResultSetBuffer.Clear();

                var connection = Command.Connection;
                //connection.FinishQuerying();

                //Command.ReaderClosed();

                // if Command wants to close session after execution...
                if ((m_behavior & CommandBehavior.CloseConnection) != 0)
                {
                    Command.Dispose();
                    connection.Close();
                }
                Command = null;
            }
        }

        internal DataTable BuildSchemaTable()
        {
            /*
            var colDefinitions = GetResultSet().ColumnDefinitions;
            DataTable schemaTable = new DataTable("SchemaTable");
            schemaTable.Locale = CultureInfo.InvariantCulture;
            schemaTable.MinimumCapacity = colDefinitions.Length;

            var columnName = new DataColumn(SchemaTableColumn.ColumnName, typeof(string));
            var ordinal = new DataColumn(SchemaTableColumn.ColumnOrdinal, typeof(int));
            var size = new DataColumn(SchemaTableColumn.ColumnSize, typeof(int));
            var precision = new DataColumn(SchemaTableColumn.NumericPrecision, typeof(int));
            var scale = new DataColumn(SchemaTableColumn.NumericScale, typeof(int));
            var dataType = new DataColumn(SchemaTableColumn.DataType, typeof(System.Type));
            var providerType = new DataColumn(SchemaTableColumn.ProviderType, typeof(int));
            var isLong = new DataColumn(SchemaTableColumn.IsLong, typeof(bool));
            var allowDBNull = new DataColumn(SchemaTableColumn.AllowDBNull, typeof(bool));
            var isReadOnly = new DataColumn(SchemaTableOptionalColumn.IsReadOnly, typeof(bool));
            var isRowVersion = new DataColumn(SchemaTableOptionalColumn.IsRowVersion, typeof(bool));
            var isUnique = new DataColumn(SchemaTableColumn.IsUnique, typeof(bool));
            var isKey = new DataColumn(SchemaTableColumn.IsKey, typeof(bool));
            var isAutoIncrement = new DataColumn(SchemaTableOptionalColumn.IsAutoIncrement, typeof(bool));
            var isHidden = new DataColumn(SchemaTableOptionalColumn.IsHidden, typeof(bool));
            var baseCatalogName = new DataColumn(SchemaTableOptionalColumn.BaseCatalogName, typeof(string));
            var baseSchemaName = new DataColumn(SchemaTableColumn.BaseSchemaName, typeof(string));
            var baseTableName = new DataColumn(SchemaTableColumn.BaseTableName, typeof(string));
            var baseColumnName = new DataColumn(SchemaTableColumn.BaseColumnName, typeof(string));
            var isAliased = new DataColumn(SchemaTableColumn.IsAliased, typeof(bool));
            var isExpression = new DataColumn(SchemaTableColumn.IsExpression, typeof(bool));
            var isIdentity = new DataColumn("IsIdentity", typeof(bool));
            ordinal.DefaultValue = 0;
            precision.DefaultValue = 0;
            scale.DefaultValue = 0;
            isLong.DefaultValue = false;

            // must maintain order for backward compatibility
            var columns = schemaTable.Columns;
            columns.Add(columnName);
            columns.Add(ordinal);
            columns.Add(size);
            columns.Add(precision);
            columns.Add(scale);
            columns.Add(isUnique);
            columns.Add(isKey);
            columns.Add(baseCatalogName);
            columns.Add(baseColumnName);
            columns.Add(baseSchemaName);
            columns.Add(baseTableName);
            columns.Add(dataType);
            columns.Add(allowDBNull);
            columns.Add(providerType);
            columns.Add(isAliased);
            columns.Add(isExpression);
            columns.Add(isIdentity);
            columns.Add(isAutoIncrement);
            columns.Add(isRowVersion);
            columns.Add(isHidden);
            columns.Add(isLong);
            columns.Add(isReadOnly);

            foreach (MySqlDbColumn column in GetColumnSchema())
            {
                var schemaRow = schemaTable.NewRow();
                schemaRow[columnName] = column.ColumnName;
                schemaRow[ordinal] = column.ColumnOrdinal;
                schemaRow[dataType] = column.DataType;
                schemaRow[size] = column.ColumnSize;
                schemaRow[providerType] = column.ProviderType;
                schemaRow[isLong] = column.IsLong;
                schemaRow[isUnique] = false;
                schemaRow[isKey] = column.IsKey;
                schemaRow[allowDBNull] = column.AllowDBNull;
                schemaRow[scale] = column.NumericScale;
                schemaRow[precision] = column.NumericPrecision.GetValueOrDefault();

                schemaRow[baseCatalogName] = column.BaseCatalogName;
                schemaRow[baseColumnName] = column.BaseColumnName;
                schemaRow[baseSchemaName] = column.BaseSchemaName;
                schemaRow[baseTableName] = column.BaseTableName;
                schemaRow[isAutoIncrement] = column.IsAutoIncrement;
                schemaRow[isRowVersion] = false;
                schemaRow[isReadOnly] = column.IsReadOnly;

                schemaTable.Rows.Add(schemaRow);
                schemaRow.AcceptChanges();
            }

            return schemaTable;
            */
            return null;
        }
    }
}
