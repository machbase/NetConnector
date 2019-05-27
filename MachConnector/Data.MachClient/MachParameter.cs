using System;
using System.Data;
using System.Data.Common;
using System.IO;
using Mach.Utility;
using Mach.Core;
using Mach.Core.Types;

namespace Mach.Data.MachClient
{
    public sealed class MachParameter : DbParameter
    {
        /* Non-overrieded properties */
        internal string NormalizedParameterName { get; private set; }

        /* Overrieded properties */
        public override DbType DbType
        {
            get => m_dbType;
            set
            {
                m_dbType = value;
                //m_MachDBType = TypeMapper.Instance.GetMachDBTypeForDbType(value);
                HasSetDbType = true;
            }
        }

        public MachDBType MachDBType
        {
            get => m_MachDBType;
            set
            {
                m_dbType = TypeMapper.Instance.GetDbTypeForMachDBType(value);
                m_MachDBType = value;
                HasSetDbType = true;
            }
        }

        public override ParameterDirection Direction
        {
            get => m_direction.GetValueOrDefault(ParameterDirection.Input);
            set
            {
                if (value != ParameterDirection.Input && value != ParameterDirection.Output &&
                    value != ParameterDirection.InputOutput && value != ParameterDirection.ReturnValue)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "{0} is not a supported value for ParameterDirection".FormatInvariant(value));
                }
                m_direction = value;
            }
        }

        public override bool IsNullable { get; set; }

        public override string ParameterName
        {
            get
            {
                return m_name;
            }
            set
            {
                m_name = value;
                NormalizedParameterName = NormalizeParameterName(m_name);
            }
        }
        public override int Size { get; set; }

        public override string SourceColumn { get; set; }

        public override bool SourceColumnNullMapping { get; set; }

        public override DataRowVersion SourceVersion { get; set; }

        public override object Value { get; set; }

        public override void ResetDbType()
        {
            DbType = default(DbType);
        }

        internal void AppendSqlString(BinaryWriter writer)
        {
            if (Value == null || Value == DBNull.Value)
            {
                writer.WriteUtf8("NULL");
            }
            else if (Value is string stringValue)
            {
                writer.Write((byte)'\'');
                writer.WriteUtf8(stringValue.Replace("\\", "\\\\").Replace("'", "\\'"));
                writer.Write((byte)'\'');
            }
            else if (Value is byte || Value is sbyte || Value is short || Value is int || Value is long || Value is ushort || Value is uint || Value is ulong || Value is decimal)
            {
                writer.WriteUtf8("{0}".FormatInvariant(Value));
            }
            else if (Value is byte[] byteArrayValue)
            {
                // determine the number of bytes to be written
                const string c_prefix = "_binary'";
                var length = byteArrayValue.Length + c_prefix.Length + 1;
                foreach (var by in byteArrayValue)
                {
                    if (by == 0x27 || by == 0x5C)
                        length++;
                }

                ((MemoryStream)writer.BaseStream).Capacity = (int)writer.BaseStream.Length + length;

                writer.WriteUtf8(c_prefix);
                foreach (var by in byteArrayValue)
                {
                    if (by == 0x27 || by == 0x5C)
                        writer.Write((byte)0x5C);
                    writer.Write(by);
                }
                writer.Write((byte)'\'');
            }
            else if (Value is bool boolValue)
            {
                writer.WriteUtf8(boolValue ? "true" : "false");
            }
            else if (Value is float || Value is double)
            {
                writer.WriteUtf8("{0:R}".FormatInvariant(Value));
            }
            else if (Value is DateTime sTimeValue)
            {
                long sTicks = sTimeValue.Ticks - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
                writer.WriteUtf8("{0}".FormatInvariant(sTicks * 100));
                // writer.WriteUtf8("to_date('{0:yyyy'-'MM'-'dd' 'HH':'mm':'ss}')".FormatInvariant(Value));
            }
            else if (Value is DateTimeOffset dateTimeOffsetValue)
            {
                // store as UTC as it will be read as such when deserialized from a timespan column
                writer.WriteUtf8("to_date('{0:yyyy'-'MM'-'dd' 'HH':'mm':'ss}')".FormatInvariant(dateTimeOffsetValue.UtcDateTime));
            }
            else if (Value is TimeSpan ts)
            {
                writer.WriteUtf8("time '");
                if (ts.Ticks < 0)
                {
                    writer.Write((byte)'-');
                    ts = TimeSpan.FromTicks(-ts.Ticks);
                }
                writer.WriteUtf8("{0}:{1:mm':'ss'.'ffffff}'".FormatInvariant(ts.Days * 24 + ts.Hours, ts));
            }
            else if (Value is Guid guidValue)
            {
                writer.WriteUtf8("'{0:D}'".FormatInvariant(guidValue));
            }
            else if (MachDBType == MachDBType.INT16)
            {
                writer.WriteUtf8("{0}".FormatInvariant((short)Value));
            }
            else if (MachDBType == MachDBType.UINT16)
            {
                writer.WriteUtf8("{0}".FormatInvariant((ushort)Value));
            }
            else if (MachDBType == MachDBType.INT32)
            {
                writer.WriteUtf8("{0}".FormatInvariant((int)Value));
            }
            else if (MachDBType == MachDBType.UINT32)
            {
                writer.WriteUtf8("{0}".FormatInvariant((uint)Value));
            }
            else if (MachDBType == MachDBType.INT64)
            {
                writer.WriteUtf8("{0}".FormatInvariant((long)Value));
            }
            else if (MachDBType == MachDBType.UINT64)
            {
                writer.WriteUtf8("{0}".FormatInvariant((ulong)Value));
            }
            else if (Value is Enum)
            {
                writer.WriteUtf8("{0:d}".FormatInvariant(Value));
            }
            else
            {
                throw new NotSupportedException("Parameter type {0} (DbType: {1}) not currently supported. Value: {2}".FormatInvariant(Value.GetType().Name, DbType, Value));
            }
        }

        internal static string NormalizeParameterName(string name)
        {
            name = name.Trim();

            if ((name.StartsWith("@`", StringComparison.Ordinal) || name.StartsWith("?`", StringComparison.Ordinal)) && name.EndsWith("`", StringComparison.Ordinal))
                return name.Substring(2, name.Length - 3).Replace("``", "`");
            if ((name.StartsWith("@'", StringComparison.Ordinal) || name.StartsWith("?'", StringComparison.Ordinal)) && name.EndsWith("'", StringComparison.Ordinal))
                return name.Substring(2, name.Length - 3).Replace("''", "'");
            if ((name.StartsWith("@\"", StringComparison.Ordinal) || name.StartsWith("?\"", StringComparison.Ordinal)) && name.EndsWith("\"", StringComparison.Ordinal))
                return name.Substring(2, name.Length - 3).Replace("\"\"", "\"");

            return name.StartsWith("@", StringComparison.Ordinal) || name.StartsWith("?", StringComparison.Ordinal) ? name.Substring(1) : name;
        }

        private DbType m_dbType;
        private MachDBType m_MachDBType;

        public bool HasSetDbType { get; private set; }

        /* Private fields */
        string m_name;
        ParameterDirection? m_direction;
    }
}