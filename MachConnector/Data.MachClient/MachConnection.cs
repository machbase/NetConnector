using Mach.Core;
using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;

namespace Mach.Data.MachClient
{
    public sealed class MachConnection : DbConnection
    {
        public override string ConnectionString
        {
            get => m_connectionString;
            set
            {
                if (m_hasBeenOpened)
                    throw new MachException("Cannot change connection string on a connection that has already been opened.");

                m_connectionString = value;
            }
        }

        public override string Database => throw new NotImplementedException();

        public override string DataSource => throw new NotImplementedException();

        public override string ServerVersion => throw new NotImplementedException();

        internal Session Session { get => m_session; set => m_session = value; }

        public override void ChangeDatabase(string databaseName)
        {
            // TODO don't need to change
            throw new NotImplementedException();
        }

        public override void Open()
        {
            if (m_hasBeenOpened == true)
            {
                throw new MachException("Already connected");
            }

            // regardless of connection success
            if (m_command != null)
            {
                if (!m_command.IsDisposed())
                {
                    m_command.IsAppendOpened = false;
                    m_command.QueryUsed = false;
                }
                else
                {
                    m_command = null;
                }
            }

            try
            {
                SetState(ConnectionState.Connecting);

                Session.Connect(m_connectionSettings);
                m_hasBeenOpened = true;

                Session.PrepareSocket(DefaultConnectionTimeout); // == aConnSetting.ConnectionTimeout
                Session.ConnectUser(m_connectionSettings, DefaultConnectionTimeout); // == aConnSetting.ConnectionTimeout
                SetState(ConnectionState.Open);

                
            }
            catch (Exception e)
            {
                DoClose(ConnectionState.Broken);
                throw e;
            }
        }

        public bool IsConnected()
        {
            if ((m_state == ConnectionState.Broken) ||
                (m_state == ConnectionState.Closed) ||
                (m_state == ConnectionState.Connecting))
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        public override void Close() => DoClose(ConnectionState.Closed);

        internal void Close(ConnectionState aState) => DoClose(aState);

        private void DoClose(ConnectionState aState)
        {
            if (m_command != null)
            {
                if (!m_command.IsDisposed())
                { 
                    m_command.Cancel();
                }
            }

            SetState(aState); // regardless of open status
            if (m_hasBeenOpened == true)
            {
                m_hasBeenOpened = false;

                Session.Disconnect();
            }
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
                    DoClose(ConnectionState.Closed);
                }

                // Note disposing has been done.
                disposed = true;
            }
        }

        ~MachConnection()
        {
            Dispose(false);
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotImplementedException();
        }

        protected override DbCommand CreateDbCommand()
        {
            throw new NotImplementedException();
        }

        private bool m_hasBeenOpened;
        private Session m_session;

        public MachConnection(string aConnectionString)
        {
            ConnectionString = aConnectionString;
            m_connectionStringBuilder = new ConnectionStringBuilder(aConnectionString);
            m_connectionSettings = new ConnectionSettings(m_connectionStringBuilder);
            Session = new Session();
            m_hasBeenOpened = false;
            SetState(ConnectionState.Closed);
        }

        internal void Lock()
        {
            bool lockTaken = false;
            m_lockObject.Enter(ref lockTaken);
            Debug.Assert(lockTaken == true);
        }

        internal void Unlock()
        {
            m_lockObject.Exit(true); // use memory barrier
        }

        internal bool TryLock()
        {
            bool lockTaken = false;
            try
            {
                m_lockObject.TryEnter(ref lockTaken);
                return lockTaken;
            }
            catch(LockRecursionException)
            {
                return false;
            }
        }

        internal int DefaultCommandTimeout => m_connectionSettings.CommandTimeout;
        internal int DefaultConnectionTimeout => m_connectionSettings.ConnectionTimeout;

        public bool HasActiveReader { get; internal set; } // TODO need to couple with ActiveReader
        public string StatusString { get; internal set; }
        public override ConnectionState State { get => m_state; }
        public MachCommand Command { get => m_command; }

        internal void addMachCommand(MachCommand aComm)
        {
            m_command = aComm;
        }

        internal void removeMachCommand() // TODO MachCommand aComm! with ID
        {
            m_command = null;
        }

        internal void SetState(ConnectionState aState)
        {
            m_state = aState;
        }

        private ConnectionStringBuilder m_connectionStringBuilder;
        private ConnectionSettings m_connectionSettings;
        private string m_connectionString;

        private SpinLock m_lockObject = new SpinLock();
        private ConnectionState m_state;

        private MachCommand m_command;

    }
}
