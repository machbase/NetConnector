using Mach.Comm;
using Mach.Core.Statement;
using Mach.Data.MachClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;

namespace Mach.Core
{
    internal class CommandExecutor
    {
        internal CommandExecutor(MachCommand command)
        {
            m_command = command;
        }

        public int ExecuteNonQuery(string commandText, MachParameterCollection parameterCollection)
        {
            using (var reader = (MachDataReader)ExecuteRead(commandText, parameterCollection, CommandBehavior.Default))
            {
                return reader.RecordsAffected;
            }
        }

        public object ExecuteScalar(string commandText, MachParameterCollection parameterCollection)
        {
            object result = null;

            using (var reader = (MachDataReader)ExecuteRead(commandText, parameterCollection, CommandBehavior.SingleResult | CommandBehavior.SingleRow))
            {
                if (reader.Read())
                    result = reader.GetValue(0);

                return result;
            }
        }

        public DbDataReader ExecuteRead(string commandText, MachParameterCollection parameterCollection, CommandBehavior behavior)
        {
            var bytes = CreateQueryPayload(commandText, parameterCollection);
           
            m_command.LastInsertedId = -1;
            try
            {
                MachDataReader sReader = MachDataReader.Create(m_command, behavior);
                ExecDirectProtocol sEDPayload = new ExecDirectProtocol();
                sEDPayload.Generate(bytes, sReader);
                m_command.Connection.Session.Transmit(sEDPayload, m_command.Connection.DefaultCommandTimeout); // ResultSet of sReader will be filled.

                return sReader;
            }
            catch (Exception ex) when (bytes.Count > 4_194_304 && (ex is SocketException || ex is IOException))
            {
                /** should be inside of ExecDirectPayload.. (??) **/

                // base on MySQL settings.. https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_allowed_packet
                int megabytes = bytes.Count / 1_000_000;
                throw new MachException(String.Format("Error submitting {0}MB packet; ensure 'max_allowed_packet' is greater than {0}MB.", megabytes), ex);
            }
        }

        // ?? TODO need to move to QueryPayload
        private ArraySegment<byte> CreateQueryPayload(string commandText, MachParameterCollection parameterCollection)
        {
            // bind
            var preparer = new StatementPreparer(commandText, parameterCollection);
            return preparer.ParseAndBindParameters();
        }

        readonly MachCommand m_command;
    }
}
