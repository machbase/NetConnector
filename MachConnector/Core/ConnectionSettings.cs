using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mach.Data.MachClient;

namespace Mach.Core
{
    public sealed class ConnectionSettings
    {
        public ConnectionSettings(ConnectionStringBuilder csb)
        {
            ConnectionString = csb.ConnectionString;

            // Base Options
            HostNames = csb.Server.Split(',');
            Port = (int)csb.Port;
            UserID = csb.UserID;
            Password = csb.Password;
            Database = csb.Database;

            // Connection Pooling Options
            Pooling = csb.Pooling;
            ConnectionLifeTime = (int)csb.ConnectionLifeTime;
            ConnectionReset = csb.ConnectionReset;
            ConnectionIdleTimeout = (int)csb.ConnectionIdleTimeout;
            if (csb.MinimumPoolSize > csb.MaximumPoolSize)
                throw new MachException("MaximumPoolSize must be greater than or equal to MinimumPoolSize");
            MinimumPoolSize = (int)csb.MinimumPoolSize;
            MaximumPoolSize = (int)csb.MaximumPoolSize;

            // Other Options
            ConnectionTimeout = (int)csb.ConnectionTimeout;
            CommandTimeout = (int)csb.DefaultCommandTimeout;
            ShowHiddenColumns = (int)csb.ShowHiddenColumns;
            /*
            AllowPublicKeyRetrieval = csb.AllowPublicKeyRetrieval;
            AllowUserVariables = csb.AllowUserVariables;
            AutoEnlist = csb.AutoEnlist;
            
            ConvertZeroDateTime = csb.ConvertZeroDateTime;
            
            ForceSynchronous = csb.ForceSynchronous;
            Keepalive = csb.Keepalive;
            OldGuids = csb.OldGuids;
            PersistSecurityInfo = csb.PersistSecurityInfo;
            ServerRsaPublicKeyFile = csb.ServerRsaPublicKeyFile;
            TreatTinyAsBoolean = csb.TreatTinyAsBoolean;
            UseAffectedRows = csb.UseAffectedRows;
            UseCompression = csb.UseCompression;
            */
        }

        public ConnectionSettings(object p)
        {
            this.p = p;
        }

        // Base Options
        public string ConnectionString { get; }
        public IEnumerable<string> HostNames { get; }
        public int Port { get; }
        public string UnixSocket { get; }
        public string UserID { get; }
        public string Password { get; }
        public string Database { get; }

        // Connection Pooling Options
        public bool Pooling { get; }
        public int ConnectionLifeTime { get; }
        public bool ConnectionReset { get; }
        public int ConnectionIdleTimeout { get; }
        public int MinimumPoolSize { get; }
        public int MaximumPoolSize { get; }

        // Other Options
        public int ConnectionTimeout { get; }
        public int CommandTimeout { get; }
        public int ShowHiddenColumns { get; }
        /*
        public bool AllowPublicKeyRetrieval { get; }
        public bool AllowUserVariables { get; }
        public bool AutoEnlist { get; }
        
        public bool ConvertZeroDateTime { get; }
        
        public bool ForceSynchronous { get; }
        public uint Keepalive { get; }
        public bool OldGuids { get; }
        public bool PersistSecurityInfo { get; }
        public string ServerRsaPublicKeyFile { get; }
        public bool TreatTinyAsBoolean { get; }
        public bool UseAffectedRows { get; }
        public bool UseCompression { get; }
        */

        // Helper Functions
        private object p;
    }

}
