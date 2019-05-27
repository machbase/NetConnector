using Mach.Data.MachClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mach.Core.Types
{
    internal sealed class TypeMetadata
    {
        public static string CreateLookupKey(string columnTypeName, bool isUnsigned, int length) => $"{columnTypeName}|{(isUnsigned ? "u" : "s")}|{length}";

        public TypeMetadata(string dataTypeName, DbTypeMapping dbTypeMapping, MachDBType machDbType, bool isUnsigned = false, bool binary = false, int length = 0, string simpleDataTypeName = null, string createFormat = null, long columnSize = 0)
        {
            DataTypeName = dataTypeName;
            SimpleDataTypeName = simpleDataTypeName ?? dataTypeName;
            CreateFormat = createFormat ?? (dataTypeName + (isUnsigned ? " UNSIGNED" : ""));
            DbTypeMapping = dbTypeMapping;
            MachDBType = machDbType;
            ColumnSize = columnSize;
            IsUnsigned = isUnsigned;
            Binary = binary;
            Length = length;
        }

        public string DataTypeName { get; }
        public string SimpleDataTypeName { get; }
        public string CreateFormat { get; }
        public DbTypeMapping DbTypeMapping { get; }
        public MachDBType MachDBType { get; }
        public bool Binary { get; }
        public long ColumnSize { get; }
        public bool IsUnsigned { get; }
        public int Length { get; }

        public string CreateLookupKey() => CreateLookupKey(DataTypeName, IsUnsigned, Length);
    }
}
