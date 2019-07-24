using Mach.Comm;
using Mach.Core;
using Mach.Utility;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Net.Sockets;

namespace Mach.Data.MachClient
{
    public sealed class MachCommand : DbCommand
    {

        public MachCommand(string aQueryString, MachConnection aConn, MachTransaction aTrans)
        {
            CommandText = aQueryString;
            Connection = aConn;
            Transaction = aTrans;
            m_commandExecutor = new CommandExecutor(this);
            ParameterCollection = new MachParameterCollection();
            m_isAppendOpened = false;
            m_queryUsed = false;
            if (Connection != null)
            {
                Connection.addMachCommand(this);
            }
        }

        public MachCommand(string aQueryString, MachConnection aConn)
            : this(aQueryString, aConn, null)
        {
        }

        public MachCommand(MachConnection aConn, MachTransaction aTrans)
            : this(null, aConn, aTrans)
        {
        }

        public MachCommand(MachConnection aConn)
            : this(null, aConn, null)
        {
        }

        // Dispose
        private bool disposed = false;
        public new void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected override void Dispose(bool disposing)
        {
            // Check to see if Dispose has already been called.
            if (!this.disposed)
            {
                // If disposing equals true, dispose all managed
                // and unmanaged resources.
                if (disposing)
                {
                    m_commandExecutor = null;
                    m_parameterCollection = null;
                    m_isAppendOpened = false;
                    Cancel();
                    if (Connection != null)
                    {
                        if (Connection.IsConnected())
                        {
                            Connection.SetState(ConnectionState.Open);
                        }
                        else
                        {
                            // Nothing to do but leave as it is
                        }
                    }
                }

                // Note disposing has been done.
                disposed = true;
            }
        }

        public bool IsDisposed()
        {
            return disposed;
        }

        ~MachCommand()
        {
            Dispose(false);
        }

        public new MachConnection Connection
        {
            get => m_connection;
            internal set
            {
                // if m_connection is not null and HasActiveReader is not null
                if (m_connection?.HasActiveReader ?? false)
                    throw new MachException("Cannot set MachCommand.Connection when there is an open DataReader for this command; it must be closed first.");
                m_connection?.removeMachCommand();
                m_connection = value;
                m_connection?.addMachCommand(this);
            }
        }
        public new MachTransaction Transaction { get; internal set; }
        internal CommandExecutor CommandExecutor { get => m_commandExecutor; }
        public MachParameterCollection ParameterCollection {
            get => m_parameterCollection;
            set => m_parameterCollection = value;
        }
        public override string CommandText { get; set; }
        public override int CommandTimeout
        {
            get => Math.Min(m_commandTimeout ?? Connection?.DefaultCommandTimeout ?? 0, int.MaxValue / 1000);
            set => m_commandTimeout = value >= 0 ? value : throw new ArgumentOutOfRangeException(nameof(value), "CommandTimeout must be greater than or equal to zero.");
        }
        public override CommandType CommandType
        {
            get => m_commandType;
            set
            {
                // we cannot handle neither stored procedure and table direct scan yet.
                if (value != CommandType.Text)
                    throw new ArgumentException("CommandType must be Text.", nameof(value));
                m_commandType = value;
            }
        }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource
        {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }
        protected override DbConnection DbConnection
        {
            get => Connection;
            set
            {
                if (value == null)
                {
                    throw new ArgumentException("Connection must not be null.", nameof(value));
                }
                Connection = (MachConnection)value;
            }
        }
        protected override DbTransaction DbTransaction
        {
            get => Transaction;
            set => Transaction = (MachTransaction)value;
        }

        protected override DbParameterCollection DbParameterCollection => m_parameterCollection;

        public new MachParameter CreateParameter() => (MachParameter)base.CreateParameter();

        public int LastInsertedId { get; internal set; }
        public int FetchSize
        {
            get
            {
                if (m_fetchSize == 0)
                    return Constants.DefaultFetchSize;
                else
                    return m_fetchSize;
            }
            set => m_fetchSize = value;
        }

        public bool IsAppendOpened { get => m_isAppendOpened; internal set => m_isAppendOpened = value; }
        public bool QueryUsed { get => m_queryUsed; internal set => m_queryUsed = value; }

        public override void Cancel()
        {
            if (m_queryUsed)
            {
                m_queryUsed = false;
                FreeProtocol sFreeProtocol = new FreeProtocol();
                sFreeProtocol.Generate();
                Connection.Session.Transmit(sFreeProtocol, Connection.DefaultCommandTimeout);
            }
        }

        /** APPEND **/

        public MachAppendWriter AppendOpen(string aTableName, int aErrorCheckCount, MachAppendOption aOption)
        {
            if (aTableName == null)
                throw new ArgumentNullException("aTableName", "AppendOpen() should get a target table.");

            if ((Connection.State == ConnectionState.Broken) ||
                (Connection.State == ConnectionState.Closed) ||
                (Connection.State == ConnectionState.Connecting))
                throw new MachException(String.Format(MachErrorMsg.INVALID_CONNECTION, Connection.State.ToString("g")));

            if (m_isAppendOpened == true)
                throw new MachException(MachErrorMsg.APPEND_DOUBLE_OPEN);

            if (m_connection.TryLock() == false)
                throw new MachException(String.Format(MachErrorMsg.CONCURRENT_STATEMENT_EXECUTE, m_connection.StatusString));

            m_connection.StatusString = "APPEND_OPEN";

            try
            {
                var sWriter = MachAppendWriter.Create(this, aTableName, aOption, aErrorCheckCount);

                AppendOpenProtocol sAppendOpenProtocol = new AppendOpenProtocol();
                sAppendOpenProtocol.Generate(aTableName, sWriter);
                Connection.Session.Transmit(sAppendOpenProtocol, Connection.DefaultCommandTimeout); // AppendMeta of sWriter will be filled.
                m_isAppendOpened = true;

                return sWriter;
            }
            catch (SocketException se)
            {
                m_connection.Close(ConnectionState.Broken);
                m_isAppendOpened = false;

                throw se;
            }
            finally
            {
                m_connection.Unlock();
            }
        }

        public MachAppendWriter AppendOpen(string aTableName)
        {
            return AppendOpen(aTableName, 0, MachAppendOption.None);
        }

        public MachAppendWriter AppendOpen(string aTableName, int aErrorCheckCount)
        {
            return AppendOpen(aTableName, aErrorCheckCount, MachAppendOption.None);
        }

        public void AppendData(MachAppendWriter aWriter, List<object> aDataList)
        {
            AppendDataWithTime(aWriter, aDataList, 0);
        }

        public void AppendDataWithTime(MachAppendWriter aWriter, List<object> aDataList, DateTime aArrivalTime)
        {
            // NOTE : DateTime expresses since 0001/01/01 00:00:00.000
            long sTicks = aArrivalTime.Ticks - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
            if (sTicks < 0)
                throw new MachException("_arrival_time to append is less than 1970-01-01.");

            // Resolution of Ticks = 100 nanosecond
            AppendDataWithTime(aWriter, aDataList, (ulong)(sTicks * 100));
        }

        public void AppendDataWithTime(MachAppendWriter aWriter, List<object> aDataList, ulong aArrivalTimeLong)
        {
            bool sFlushed = false;

            if (m_isAppendOpened == false)
                throw new MachException(String.Format(MachErrorMsg.APPEND_ALREADY_CLOSED, "APPEND_DATA"));

            aWriter.writeData(aDataList, aArrivalTimeLong);
            aWriter.AppendAddCount++;

            if (aWriter.ExceedBuffer == true)
            {
                AppendFlush(aWriter);
                sFlushed = true;
            }

            if ((sFlushed == false) && (aWriter.ErrorCheckCount > 0))
            {
                if ((aWriter.AppendAddCount % aWriter.ErrorCheckCount) == 0)
                {
                    AppendFlush(aWriter);
                    sFlushed = true;
                }
            }
        }

        private void AppendCheckError(MachAppendWriter aWriter, Protocol aProtocol)
        {
            while (this.Connection.Session.Socket.Available > 0)
            {
                try
                {
                    this.Connection.Session.Recv(aProtocol, Connection.DefaultCommandTimeout);
                }
                catch (SocketException se)
                {
                    m_connection.Close(ConnectionState.Broken);
                    m_isAppendOpened = false;

                    throw se;
                }
                catch (MachAppendException e)
                {
                    aWriter.CallErrorDelegator(e);
                }
            }
        }

        public void AppendFlush(MachAppendWriter aWriter)
        {
            string sStatusStringToBe = "APPEND_FLUSH";

            if ((Connection.State == ConnectionState.Broken) ||
                (Connection.State == ConnectionState.Closed) ||
                (Connection.State == ConnectionState.Connecting))
                throw new MachException(String.Format(MachErrorMsg.INVALID_CONNECTION, Connection.State.ToString("g")));

            if (m_isAppendOpened == false)
                throw new MachException(String.Format(MachErrorMsg.APPEND_ALREADY_CLOSED, sStatusStringToBe));

            if (m_connection.TryLock() == false)
                throw new MachException(String.Format(MachErrorMsg.CONCURRENT_STATEMENT_EXECUTE, m_connection.StatusString));

            m_connection.StatusString = sStatusStringToBe;

            try
            {
                AppendDataProtocol sAppendDataProtocol = new AppendDataProtocol();

                if (aWriter.AppendAddCount > 0) // regardless of ExceedBuffer
                {
                    sAppendDataProtocol.Generate(aWriter);
                    this.Connection.Session.Send(sAppendDataProtocol);
                }

                AppendCheckError(aWriter, sAppendDataProtocol);
            }
            catch (SocketException se)
            {
                m_connection.Close(ConnectionState.Broken);
                m_isAppendOpened = false;

                throw se;
            }
            catch (MachException e)
            {
                throw e;
            }
            finally
            {
                aWriter.ClearData();
                m_connection.Unlock();
            }
        }

        public void AppendClose(MachAppendWriter aWriter)
        {
            string sStatusStringToBe = "APPEND_CLOSE";

            AppendCloseProtocol sAppendCloseProtocol = new AppendCloseProtocol();
            AppendDataProtocol sAppendDataProtocol = new AppendDataProtocol();
            bool sIsSent = false;

            if (m_isAppendOpened == false)
                throw new MachException(String.Format(MachErrorMsg.APPEND_ALREADY_CLOSED, sStatusStringToBe));

            // first, flush remaning records
            AppendFlush(aWriter);

            if (m_connection.TryLock() == false)
                throw new MachException(String.Format(MachErrorMsg.CONCURRENT_STATEMENT_EXECUTE, m_connection.StatusString));

            m_connection.StatusString = sStatusStringToBe;

        Retry:
            try
            {
                if (sIsSent == false)
                { 
                    // make and send APPEND_CLOSE
                    sAppendCloseProtocol.Generate(aWriter);
                    Connection.Session.Send(sAppendCloseProtocol);
                    sIsSent = true;
                }

                // recv APPEND_DATA (exception occurs), or APPEND_CLOSE (normal)
                Connection.Session.RecvBranch(sAppendDataProtocol, sAppendCloseProtocol, Connection.DefaultCommandTimeout);
            }
            catch (SocketException se)
            {
                m_connection.Close(ConnectionState.Broken);
                m_isAppendOpened = false;

                throw se;
            }
            catch (MachAppendException e)
            {
                aWriter.CallErrorDelegator(e);
                goto Retry;
            }
            finally
            {
                m_isAppendOpened = false;
                m_connection.Unlock();
            }
        }

        /** END of APPEND **/

        public override int ExecuteNonQuery()
        {
            if (CommandText == null)
                throw new ArgumentNullException("CommandText", "MachCommand doesn't have any query string text.");

            if ((Connection.State == ConnectionState.Broken) ||
                (Connection.State == ConnectionState.Closed) ||
                (Connection.State == ConnectionState.Connecting))
                throw new MachException(String.Format(MachErrorMsg.INVALID_CONNECTION, Connection.State.ToString("g")));

            if (m_connection.TryLock() == false)
                throw new MachException(String.Format(MachErrorMsg.CONCURRENT_STATEMENT_EXECUTE, m_connection.StatusString));

            m_connection.StatusString = "EXECUTE_DIRECT";
            m_connection.SetState(ConnectionState.Executing);

            try
            { 
                var sObj = CommandExecutor.ExecuteNonQuery(CommandText, ParameterCollection);
                m_connection.SetState(ConnectionState.Open);
                Cancel();
                return sObj;
            }
            catch (SocketException se)
            {
                m_connection.Close(ConnectionState.Broken);
                m_isAppendOpened = false;

                throw se;
            }
            finally
            {
                m_connection.Unlock();
            }
        }

        public override object ExecuteScalar()
        {
            if (CommandText == null)
                throw new ArgumentNullException("CommandText", "MachCommand doesn't have any query string text.");

            if ((Connection.State == ConnectionState.Broken) ||
                (Connection.State == ConnectionState.Closed) ||
                (Connection.State == ConnectionState.Connecting))
                throw new MachException(String.Format(MachErrorMsg.INVALID_CONNECTION, Connection.State.ToString("g")));

            if (m_connection.TryLock() == false)
                throw new MachException(String.Format(MachErrorMsg.CONCURRENT_STATEMENT_EXECUTE, m_connection.StatusString));

            m_connection.StatusString = "EXECUTE_DIRECT";
            m_connection.SetState(ConnectionState.Executing);

            try
            {
                var sObj = CommandExecutor.ExecuteScalar(CommandText, ParameterCollection);
                m_connection.SetState(ConnectionState.Open);
                Cancel();
                return sObj;
            }
            catch (SocketException se)
            {
                m_connection.Close(ConnectionState.Broken);
                m_isAppendOpened = false;

                throw se;
            }
            finally
            {
                m_connection.Unlock();
            }
        }

        public override void Prepare()
        {
            // NOTE: Prepared statements in Mach are not currently supported.
            throw new NotSupportedException();
        }

        protected override DbParameter CreateDbParameter()
        {
            VerifyNotDisposed();
            return new MachParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior aBehavior)
        {
            Cancel();

            if (CommandText == null)
                throw new ArgumentNullException("CommandText", "MachCommand doesn't have any query string text.");

            if ((Connection.State == ConnectionState.Broken) || 
                (Connection.State == ConnectionState.Closed) ||
                (Connection.State == ConnectionState.Connecting))
                throw new MachException(String.Format(MachErrorMsg.INVALID_CONNECTION, Connection.State.ToString("g")));

            if (m_connection.TryLock() == false)
                throw new MachException(String.Format(MachErrorMsg.CONCURRENT_STATEMENT_EXECUTE, m_connection.StatusString));

            m_connection.StatusString = "FETCH";
            m_connection.SetState(ConnectionState.Fetching);

            try
            {
                m_queryUsed = true;
                return CommandExecutor.ExecuteRead(CommandText, ParameterCollection, aBehavior);
            }
            catch (SocketException se)
            {
                m_connection.Close(ConnectionState.Broken);
                m_isAppendOpened = false;

                throw se;
            }
            finally
            {
                m_connection.Unlock();
            }
        }

        public new MachDataReader ExecuteReader() => (MachDataReader)base.ExecuteReader(); // link to ExecuteDbDataReader()

        public new MachDataReader ExecuteReader(CommandBehavior commandBehavior) => (MachDataReader)base.ExecuteReader(commandBehavior);

        private void VerifyNotDisposed()
        {
            if (m_parameterCollection == null)
                throw new ObjectDisposedException(GetType().Name);
        }

        private bool m_isAppendOpened;
        private CommandExecutor m_commandExecutor;
        private MachConnection m_connection;
        private MachParameterCollection m_parameterCollection;
        private CommandType m_commandType;
        private int? m_commandTimeout;
        private int m_fetchSize;
        private bool m_queryUsed;
    }
}
