using Mach.Data.MachClient;
using System;
using System.Data;
using System.IO;
using System.Text;
using Mach.Utility;

namespace Mach.Core.Statement
{
    internal sealed class StatementPreparer
    {
        public StatementPreparer(string commandText, MachParameterCollection parameters)
        {
            m_commandText = commandText;
            m_parameters = parameters;
        }

        public ArraySegment<byte> ParseAndBindParameters()
        {
            using (var stream = new MemoryStream(m_commandText.Length + 1))
            using (var writer = new BinaryWriter(stream, Encoding.UTF8))
            {
                if (!string.IsNullOrWhiteSpace(m_commandText))
                {
                    var parser = new ParameterSqlParser(this, writer);
                    parser.Parse(m_commandText);
                }

    			var array = stream.ToArray();
                return new ArraySegment<byte>(array, 0, checked((int)stream.Length));
            }
        }

        private sealed class ParameterSqlParser : SqlParser
        {
            public ParameterSqlParser(StatementPreparer preparer, BinaryWriter writer)
            {
                m_preparer = preparer;
                m_writer = writer;
            }

            protected override void OnBeforeParse(string sql)
            {
            }

            protected override void OnNamedParameter(int index, int length)
            {
                var parameterName = m_preparer.m_commandText.Substring(index, length);
                var parameterIndex = m_preparer.m_parameters.NormalizedIndexOf(parameterName);
                if (parameterIndex != -1)
                    DoAppendParameter(parameterIndex, index, length);
            }

            protected override void OnPositionalParameter(int index)
            {
                DoAppendParameter(m_currentParameterIndex, index, 1);
                m_currentParameterIndex++;
            }

            private void DoAppendParameter(int parameterIndex, int textIndex, int textLength)
            {
                AppendString(m_preparer.m_commandText, m_lastIndex, textIndex - m_lastIndex);
                var parameter = m_preparer.m_parameters[parameterIndex];
                if (parameter.Direction != ParameterDirection.Input)
                    throw new MachException(String.Format("Only ParameterDirection.Input is supported when CommandType is Text (parameter name: {0})", parameter.ParameterName));
                m_preparer.m_parameters[parameterIndex].AppendSqlString(m_writer);
                m_lastIndex = textIndex + textLength;
            }

            protected override void OnParsed()
            {
                AppendString(m_preparer.m_commandText, m_lastIndex, m_preparer.m_commandText.Length - m_lastIndex);
            }

            private void AppendString(string value, int offset, int length)
            {
                m_writer.WriteUtf8(value, offset, length);
            }

            readonly StatementPreparer m_preparer;
            readonly BinaryWriter m_writer;
            int m_currentParameterIndex;
            int m_lastIndex;
        }

        readonly string m_commandText;
        readonly MachParameterCollection m_parameters;
    }
}
