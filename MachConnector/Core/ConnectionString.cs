using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Globalization;
using Mach.Core;

namespace Mach.Core
{
    public sealed class ConnectionStringBuilder : DbConnectionStringBuilder
    {
        public ConnectionStringBuilder()
        {
        }

        public ConnectionStringBuilder(string connectionString)
        {
            ConnectionString = connectionString;
        }

        // Base Options
        public string Server
        {
            get => ConnectionStringOption.Server.GetValue(this);
            set => ConnectionStringOption.Server.SetValue(this, value);
        }

        public uint Port
        {
            get => ConnectionStringOption.Port.GetValue(this);
            set => ConnectionStringOption.Port.SetValue(this, value);
        }

        public string UserID
        {
            get => ConnectionStringOption.UserID.GetValue(this).ToUpper();
            set => ConnectionStringOption.UserID.SetValue(this, value);
        }

        public string Password
        {
            get => ConnectionStringOption.Password.GetValue(this).ToUpper();
            set => ConnectionStringOption.Password.SetValue(this, value);
        }

        public string Database
        {
            get => ConnectionStringOption.Database.GetValue(this);
            set => ConnectionStringOption.Database.SetValue(this, value);
        }

        // Connection Pooling Options
        public bool Pooling
        {
            get => ConnectionStringOption.Pooling.GetValue(this);
            set => ConnectionStringOption.Pooling.SetValue(this, value);
        }

        public uint ConnectionLifeTime
        {
            get => ConnectionStringOption.ConnectionLifeTime.GetValue(this);
            set => ConnectionStringOption.ConnectionLifeTime.SetValue(this, value);
        }

        public bool ConnectionReset
        {
            get => ConnectionStringOption.ConnectionReset.GetValue(this);
            set => ConnectionStringOption.ConnectionReset.SetValue(this, value);
        }

        public uint ConnectionIdleTimeout
        {
            get => ConnectionStringOption.ConnectionIdleTimeout.GetValue(this);
            set => ConnectionStringOption.ConnectionIdleTimeout.SetValue(this, value);
        }

        public uint MinimumPoolSize
        {
            get => ConnectionStringOption.MinimumPoolSize.GetValue(this);
            set => ConnectionStringOption.MinimumPoolSize.SetValue(this, value);
        }

        public uint MaximumPoolSize
        {
            get => ConnectionStringOption.MaximumPoolSize.GetValue(this);
            set => ConnectionStringOption.MaximumPoolSize.SetValue(this, value);
        }

        // Other Options
        public uint ConnectionTimeout
        {
            get => ConnectionStringOption.ConnectionTimeout.GetValue(this);
            set => ConnectionStringOption.ConnectionTimeout.SetValue(this, value);
        }

        public uint DefaultCommandTimeout
        {
            get => ConnectionStringOption.DefaultCommandTimeout.GetValue(this);
            set => ConnectionStringOption.DefaultCommandTimeout.SetValue(this, value);
        }

        public uint ShowHiddenColumns
        {
            get => ConnectionStringOption.ShowHiddenColumns.GetValue(this);
            set => ConnectionStringOption.ShowHiddenColumns.SetValue(this, value);
        }

        /*
        public bool AllowPublicKeyRetrieval
        {
            get => ConnectionStringOption.AllowPublicKeyRetrieval.GetValue(this);
            set => ConnectionStringOption.AllowPublicKeyRetrieval.SetValue(this, value);
        }

        public bool AllowUserVariables
        {
            get => ConnectionStringOption.AllowUserVariables.GetValue(this);
            set => ConnectionStringOption.AllowUserVariables.SetValue(this, value);
        }

        public bool AutoEnlist
        {
            get => ConnectionStringOption.AutoEnlist.GetValue(this);
            set => ConnectionStringOption.AutoEnlist.SetValue(this, value);
        }

        public string CharacterSet
        {
            get => ConnectionStringOption.CharacterSet.GetValue(this);
            set => ConnectionStringOption.CharacterSet.SetValue(this, value);
        }

        public bool ConvertZeroDateTime
        {
            get => ConnectionStringOption.ConvertZeroDateTime.GetValue(this);
            set => ConnectionStringOption.ConvertZeroDateTime.SetValue(this, value);
        }

        public bool ForceSynchronous
        {
            get => ConnectionStringOption.ForceSynchronous.GetValue(this);
            set => ConnectionStringOption.ForceSynchronous.SetValue(this, value);
        }

        public uint Keepalive
        {
            get => ConnectionStringOption.Keepalive.GetValue(this);
            set => ConnectionStringOption.Keepalive.SetValue(this, value);
        }

        public bool OldGuids
        {
            get => ConnectionStringOption.OldGuids.GetValue(this);
            set => ConnectionStringOption.OldGuids.SetValue(this, value);
        }

        public bool PersistSecurityInfo
        {
            get => ConnectionStringOption.PersistSecurityInfo.GetValue(this);
            set => ConnectionStringOption.PersistSecurityInfo.SetValue(this, value);
        }

        public string ServerRsaPublicKeyFile
        {
            get => ConnectionStringOption.ServerRsaPublicKeyFile.GetValue(this);
            set => ConnectionStringOption.ServerRsaPublicKeyFile.SetValue(this, value);
        }

        public bool TreatTinyAsBoolean
        {
            get => ConnectionStringOption.TreatTinyAsBoolean.GetValue(this);
            set => ConnectionStringOption.TreatTinyAsBoolean.SetValue(this, value);
        }

        public bool UseAffectedRows
        {
            get => ConnectionStringOption.UseAffectedRows.GetValue(this);
            set => ConnectionStringOption.UseAffectedRows.SetValue(this, value);
        }

        public bool UseCompression
        {
            get => ConnectionStringOption.UseCompression.GetValue(this);
            set => ConnectionStringOption.UseCompression.SetValue(this, value);
        }
        */

        // Other Methods
        public override bool ContainsKey(string key)
        {
            var option = ConnectionStringOption.TryGetOptionForKey(key);
            return option != null && base.ContainsKey(option.Key);
        }

        public override bool Remove(string key)
        {
            var option = ConnectionStringOption.TryGetOptionForKey(key);
            return option != null && base.Remove(option.Key);
        }

        public override object this[string key]
        {
            get => ConnectionStringOption.GetOptionForKey(key).GetObject(this);
            set => base[ConnectionStringOption.GetOptionForKey(key).Key] = Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        internal string GetConnectionString(bool includePassword)
        {
            var connectionString = ConnectionString;
            if (includePassword)
                return connectionString;

            if (m_cachedConnectionString != connectionString)
            {
                var csb = new ConnectionStringBuilder(connectionString);
                foreach (string key in Keys)
                    foreach (var passwordKey in ConnectionStringOption.Password.Keys)
                        if (string.Equals(key, passwordKey, StringComparison.OrdinalIgnoreCase))
                            csb.Remove(key);
                m_cachedConnectionStringWithoutPassword = csb.ConnectionString;
                m_cachedConnectionString = connectionString;
            }

            return m_cachedConnectionStringWithoutPassword;
        }

        string m_cachedConnectionString;
        string m_cachedConnectionStringWithoutPassword;
    }

    internal abstract class ConnectionStringOption
    {
        // Base Options
        public static readonly ConnectionStringOption<string> Server;
        public static readonly ConnectionStringOption<uint> Port;
        public static readonly ConnectionStringOption<string> UserID;
        public static readonly ConnectionStringOption<string> Password;
        public static readonly ConnectionStringOption<string> Database;

        // Connection Pooling Options
        public static readonly ConnectionStringOption<bool> Pooling;
        public static readonly ConnectionStringOption<uint> ConnectionLifeTime;
        public static readonly ConnectionStringOption<bool> ConnectionReset;
        public static readonly ConnectionStringOption<uint> ConnectionIdleTimeout;
        public static readonly ConnectionStringOption<uint> MinimumPoolSize;
        public static readonly ConnectionStringOption<uint> MaximumPoolSize;

        // Other Options
        public static readonly ConnectionStringOption<uint> ConnectionTimeout;
        public static readonly ConnectionStringOption<uint> DefaultCommandTimeout;
        public static readonly ConnectionStringOption<uint> ShowHiddenColumns;
        /*
        public static readonly ConnectionStringOption<bool> AllowPublicKeyRetrieval;
        public static readonly ConnectionStringOption<bool> AllowUserVariables;
        public static readonly ConnectionStringOption<bool> AutoEnlist;
        public static readonly ConnectionStringOption<string> CharacterSet;
        
        public static readonly ConnectionStringOption<bool> ConvertZeroDateTime;
        
        public static readonly ConnectionStringOption<bool> ForceSynchronous;
        public static readonly ConnectionStringOption<uint> Keepalive;
        public static readonly ConnectionStringOption<bool> OldGuids;
        public static readonly ConnectionStringOption<bool> PersistSecurityInfo;
        public static readonly ConnectionStringOption<string> ServerRsaPublicKeyFile;
        public static readonly ConnectionStringOption<bool> TreatTinyAsBoolean;
        public static readonly ConnectionStringOption<bool> UseAffectedRows;
        public static readonly ConnectionStringOption<bool> UseCompression;
        */

        public static ConnectionStringOption TryGetOptionForKey(string key) =>
            s_options.TryGetValue(key, out var option) ? option : null;

        public static ConnectionStringOption GetOptionForKey(string key) =>
            TryGetOptionForKey(key) ?? throw new InvalidOperationException(String.Format("Option '{0}' not supported.", key));

        public string Key => m_keys.ToArray()[0];
        public IEnumerable<string> Keys => m_keys;

        public abstract object GetObject(ConnectionStringBuilder builder);

        protected ConnectionStringOption(IEnumerable<string> keys)
        {
            m_keys = keys;
        }

        private static void AddOption(ConnectionStringOption option)
        {
            foreach (string key in option.m_keys)
                s_options.Add(key, option);
        }

        static ConnectionStringOption()
        {
            s_options = new Dictionary<string, ConnectionStringOption>(StringComparer.OrdinalIgnoreCase);

            // Base Options
            AddOption(Server = new ConnectionStringOption<string>(
                keys: new[] { "SERVER", "HOST", "DATA_SOURCE", "DATASOURCE", "ADDRESS", "ADDR", "DSN" },
                defaultValue: ""));

            AddOption(Port = new ConnectionStringOption<uint>(
                keys: new[] { "PORT", "PORTNO", "PORT_NO" },
                defaultValue: 5656u));

            AddOption(UserID = new ConnectionStringOption<string>(
                keys: new[] { "USERID", "USERNAME", "UID", "USER" },
                defaultValue: "SYS"));

            AddOption(Password = new ConnectionStringOption<string>(
                keys: new[] { "PASSWORD", "PWD" },
                defaultValue: ""));

            AddOption(Database = new ConnectionStringOption<string>(
                keys: new[] { "DATABASE", "DB_NAME" },
                defaultValue: ""));

            // Connection Pooling Options
            AddOption(Pooling = new ConnectionStringOption<bool>(
                keys: new[] { "Pooling" },
                defaultValue: true));

            AddOption(ConnectionLifeTime = new ConnectionStringOption<uint>(
                keys: new[] { "Connection Lifetime", "ConnectionLifeTime" },
                defaultValue: 0));

            AddOption(ConnectionReset = new ConnectionStringOption<bool>(
                keys: new[] { "Connection Reset", "ConnectionReset" },
                defaultValue: true));

            AddOption(ConnectionIdleTimeout = new ConnectionStringOption<uint>(
                keys: new[] { "Connection Idle Timeout", "ConnectionIdleTimeout" },
                defaultValue: 180));

            AddOption(MinimumPoolSize = new ConnectionStringOption<uint>(
                keys: new[] { "Minimum Pool Size", "Min Pool Size", "MinimumPoolSize", "minpoolsize" },
                defaultValue: 0));

            AddOption(MaximumPoolSize = new ConnectionStringOption<uint>(
                keys: new[] { "Maximum Pool Size", "Max Pool Size", "MaximumPoolSize", "maxpoolsize" },
                defaultValue: 100));

            // Other Options
            AddOption(ConnectionTimeout = new ConnectionStringOption<uint>(
                keys: new[] { "Connection Timeout", "ConnectionTimeout", "Connect Timeout", "connectTimeout", "CONNECT_TIMEOUT" },
                defaultValue: 60000)); // msec

            AddOption(DefaultCommandTimeout = new ConnectionStringOption<uint>(
                keys: new[] { "Default Command Timeout", "DefaultCommandTimeout", "Command Timeout", "commandTimeout", "COMMAND_TIMEOUT" },
                defaultValue: 60000)); // msec

            AddOption(ShowHiddenColumns = new ConnectionStringOption<uint>(
                keys: new[] { "SHOW_HIDDEN_COLS" },
                defaultValue: 0));

            /*
            AddOption(AllowPublicKeyRetrieval = new ConnectionStringOption<bool>(
                keys: new[] { "AllowPublicKeyRetrieval", "Allow Public Key Retrieval" },
                defaultValue: false));

            AddOption(AllowUserVariables = new ConnectionStringOption<bool>(
                keys: new[] { "AllowUserVariables", "Allow User Variables" },
                defaultValue: false));

            AddOption(AutoEnlist = new ConnectionStringOption<bool>(
                keys: new[] { "AutoEnlist", "Auto Enlist" },
                defaultValue: true));

            AddOption(CharacterSet = new ConnectionStringOption<string>(
                keys: new[] { "CharSet", "Character Set", "CharacterSet" },
                defaultValue: ""));

            AddOption(ConvertZeroDateTime = new ConnectionStringOption<bool>(
                keys: new[] { "Convert Zero Datetime", "ConvertZeroDateTime" },
                defaultValue: false));

            AddOption(ForceSynchronous = new ConnectionStringOption<bool>(
                keys: new[] { "ForceSynchronous" },
                defaultValue: false));

            AddOption(Keepalive = new ConnectionStringOption<uint>(
                keys: new[] { "Keep Alive", "Keepalive" },
                defaultValue: 0u));

            AddOption(OldGuids = new ConnectionStringOption<bool>(
                keys: new[] { "Old Guids", "OldGuids" },
                defaultValue: false));

            AddOption(PersistSecurityInfo = new ConnectionStringOption<bool>(
                keys: new[] { "Persist Security Info", "PersistSecurityInfo" },
                defaultValue: false));

            AddOption(ServerRsaPublicKeyFile = new ConnectionStringOption<string>(
                keys: new[] { "ServerRSAPublicKeyFile", "Server RSA Public Key File" },
                defaultValue: null));

            AddOption(TreatTinyAsBoolean = new ConnectionStringOption<bool>(
                keys: new[] { "Treat Tiny As Boolean", "TreatTinyAsBoolean" },
                defaultValue: true));

            AddOption(UseAffectedRows = new ConnectionStringOption<bool>(
                keys: new[] { "Use Affected Rows", "UseAffectedRows" },
                defaultValue: true));

            AddOption(UseCompression = new ConnectionStringOption<bool>(
                keys: new[] { "Compress", "Use Compression", "UseCompression" },
                defaultValue: false));
            */
        }

        static readonly Dictionary<string, ConnectionStringOption> s_options;

        readonly IEnumerable<string> m_keys;
    }

    internal sealed class ConnectionStringOption<T> : ConnectionStringOption
    {
        public ConnectionStringOption(IEnumerable<string> keys, T defaultValue, Func<T, T> coerce = null)
            : base(keys)
        {
            m_defaultValue = defaultValue;
            m_coerce = coerce;
        }

        public T GetValue(ConnectionStringBuilder builder) =>
            builder.TryGetValue(Key, out var objectValue) ? ChangeType(objectValue) : m_defaultValue;

        public void SetValue(ConnectionStringBuilder builder, T value)
        {
            builder[Key] = m_coerce != null ? m_coerce(value) : value;
        }

        public override object GetObject(ConnectionStringBuilder builder)
        {
            return GetValue(builder);
        }

        private static T ChangeType(object objectValue)
        {
            if (typeof(T) == typeof(bool) && objectValue is string booleanString)
            {
                if (string.Equals(booleanString, "yes", StringComparison.OrdinalIgnoreCase))
                    return (T)(object)true;
                if (string.Equals(booleanString, "no", StringComparison.OrdinalIgnoreCase))
                    return (T)(object)false;
            }

            /*
            if (typeof(T) == typeof(MySqlLoadBalance) && objectValue is string loadBalanceString)
            {
                foreach (var val in Enum.GetValues(typeof(T)))
                {
                    if (string.Equals(loadBalanceString, val.ToString(), StringComparison.OrdinalIgnoreCase))
                        return (T)val;
                }
                throw new InvalidOperationException("Value '{0}' not supported for option '{1}'.".FormatInvariant(objectValue, typeof(T).Name));
            }

            if (typeof(T) == typeof(MySqlSslMode) && objectValue is string sslModeString)
            {
                foreach (var val in Enum.GetValues(typeof(T)))
                {
                    if (string.Equals(sslModeString, val.ToString(), StringComparison.OrdinalIgnoreCase))
                        return (T)val;
                }
                throw new InvalidOperationException("Value '{0}' not supported for option '{1}'.".FormatInvariant(objectValue, typeof(T).Name));
            }
            */

            return (T)Convert.ChangeType(objectValue, typeof(T), CultureInfo.InvariantCulture);
        }

        readonly T m_defaultValue;
        readonly Func<T, T> m_coerce;
    }
}
