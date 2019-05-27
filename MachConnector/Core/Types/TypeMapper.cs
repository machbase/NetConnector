using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Mach.Data.MachClient;
using Mach.Utility;

namespace Mach.Core.Types
{
    internal sealed class TypeMapper
    {
        public static TypeMapper Instance = new TypeMapper();

        private TypeMapper()
        {
            m_typeMetadata = new List<TypeMetadata>();
            m_dbTypeMappingsByClrType = new Dictionary<Type, DbTypeMapping>();
            m_dbTypeMappingsByDbType = new Dictionary<DbType, DbTypeMapping>();
            m_columnTypeMetadataLookup = new Dictionary<string, TypeMetadata>(StringComparer.OrdinalIgnoreCase);
            m_MachDBTypeToColumnTypeMetadata = new Dictionary<MachDBType, TypeMetadata>();

            // boolean
            var typeBoolean = AddDbTypeMapping(new DbTypeMapping(typeof(bool), new[] { DbType.Boolean }, convert: o => Convert.ToBoolean(o)));
            AddTypeMetadata(new TypeMetadata("TINYINT", typeBoolean, MachDBType.BOOL, isUnsigned: false, length: 1, columnSize: 1, simpleDataTypeName: "BOOL", createFormat: "BOOL"));
            AddTypeMetadata(new TypeMetadata("TINYINT", typeBoolean, MachDBType.BOOL, isUnsigned: true, length: 1));

            // integers
            var typeShort = AddDbTypeMapping(new DbTypeMapping(typeof(short), new[] { DbType.Int16 }, convert: o => Convert.ToInt16(o)));
            var typeUshort = AddDbTypeMapping(new DbTypeMapping(typeof(ushort), new[] { DbType.UInt16 }, convert: o => Convert.ToUInt16(o)));
            var typeInt = AddDbTypeMapping(new DbTypeMapping(typeof(int), new[] { DbType.Int32 }, convert: o => Convert.ToInt32(o)));
            var typeUint = AddDbTypeMapping(new DbTypeMapping(typeof(uint), new[] { DbType.UInt32 }, convert: o => Convert.ToUInt32(o)));
            var typeLong = AddDbTypeMapping(new DbTypeMapping(typeof(long), new[] { DbType.Int64 }, convert: o => Convert.ToInt64(o)));
            var typeUlong = AddDbTypeMapping(new DbTypeMapping(typeof(ulong), new[] { DbType.UInt64 }, convert: o => Convert.ToUInt64(o)));
            AddTypeMetadata(new TypeMetadata("SHORT", typeShort, MachDBType.INT16, isUnsigned: false));
            AddTypeMetadata(new TypeMetadata("USHORT", typeUshort, MachDBType.UINT16, isUnsigned: true));
            AddTypeMetadata(new TypeMetadata("INT", typeInt, MachDBType.INT32, isUnsigned: false));
            AddTypeMetadata(new TypeMetadata("UINT", typeUint, MachDBType.UINT32, isUnsigned: true));
            AddTypeMetadata(new TypeMetadata("LONG", typeLong, MachDBType.INT64, isUnsigned: false));
            AddTypeMetadata(new TypeMetadata("ULONG", typeUlong, MachDBType.UINT64, isUnsigned: true));

            // decimals
            var typeDouble = AddDbTypeMapping(new DbTypeMapping(typeof(double), new[] { DbType.Double }, convert: o => Convert.ToDouble(o)));
            var typeFloat = AddDbTypeMapping(new DbTypeMapping(typeof(float), new[] { DbType.Single }, convert: o => Convert.ToSingle(o)));
            AddTypeMetadata(new TypeMetadata("FLOAT", typeFloat, MachDBType.FLT32));
            AddTypeMetadata(new TypeMetadata("DOUBLE", typeDouble, MachDBType.FLT64));

            // string
            var typeFixedString = AddDbTypeMapping(new DbTypeMapping(typeof(string), new[] { DbType.StringFixedLength, DbType.AnsiStringFixedLength }, convert: Convert.ToString));
            var typeString = AddDbTypeMapping(new DbTypeMapping(typeof(string), new[] { DbType.String, DbType.AnsiString, DbType.Xml }, convert: Convert.ToString));
            AddTypeMetadata(new TypeMetadata("VARCHAR", typeString, MachDBType.VARCHAR, createFormat: "VARCHAR({0});size"));
            AddTypeMetadata(new TypeMetadata("TEXT", typeString, MachDBType.TEXT, columnSize: ushort.MaxValue, simpleDataTypeName: "VARCHAR"));

            // binary
            var typeBinary = AddDbTypeMapping(new DbTypeMapping(typeof(byte[]), new[] { DbType.Binary }));
            AddTypeMetadata(new TypeMetadata("BINARY", typeBinary, MachDBType.BINARY, binary: true, simpleDataTypeName: "BLOB", createFormat: "BINARY({0});length"));

            // date/time
            var typeDateTime = AddDbTypeMapping(new DbTypeMapping(typeof(DateTime), new[] { DbType.DateTime, DbType.DateTime2, DbType.DateTimeOffset }));
            var typeDateLongTime = AddDbTypeMapping(new DbTypeMapping(typeof(ulong), new[] { DbType.DateTime, DbType.DateTime2, DbType.DateTimeOffset }));
            var typeDateStringTime = AddDbTypeMapping(new DbTypeMapping(typeof(string), new[] { DbType.DateTime, DbType.DateTime2, DbType.DateTimeOffset }));
            AddTypeMetadata(new TypeMetadata("DATETIME", typeDateTime, MachDBType.DATE));
            AddTypeMetadata(new TypeMetadata("DATETIME", typeDateLongTime, MachDBType.DATE));
            AddTypeMetadata(new TypeMetadata("DATETIME", typeDateStringTime, MachDBType.DATE));

            // ip (as string..)
            AddTypeMetadata(new TypeMetadata("IPv4", typeString, MachDBType.IPV4, columnSize: 4));
            AddTypeMetadata(new TypeMetadata("IPv6", typeString, MachDBType.IPV6, columnSize: 8));

            // null
            var typeNull = AddDbTypeMapping(new DbTypeMapping(typeof(object), new[] { DbType.Object }, convert: o => null));
            AddTypeMetadata(new TypeMetadata("NULL", typeNull, MachDBType.NULL));
        }

        public IEnumerable<TypeMetadata> GetColumnTypeMetadata() => m_typeMetadata.AsReadOnly();

        public TypeMetadata GetColumnTypeMetadata(MachDBType MachDBType) => m_MachDBTypeToColumnTypeMetadata[MachDBType];

        public DbType GetDbTypeForMachDBType(MachDBType MachDBType) => m_MachDBTypeToColumnTypeMetadata[MachDBType].DbTypeMapping.DbTypes[0];

        public MachDBType GetMachDBTypeForDbType(DbType dbType)
        {
            foreach (var pair in m_MachDBTypeToColumnTypeMetadata)
            {
                if (pair.Value.DbTypeMapping.DbTypes.Contains(dbType))
                    return pair.Key;
            }
            return MachDBType.VARCHAR;
        }

        private DbTypeMapping AddDbTypeMapping(DbTypeMapping dbTypeMapping)
        {
            m_dbTypeMappingsByClrType[dbTypeMapping.ClrType] = dbTypeMapping;

            if (dbTypeMapping.DbTypes != null)
                foreach (var dbType in dbTypeMapping.DbTypes)
                    m_dbTypeMappingsByDbType[dbType] = dbTypeMapping;

            return dbTypeMapping;
        }

        private void AddTypeMetadata(TypeMetadata columnTypeMetadata)
        {
            m_typeMetadata.Add(columnTypeMetadata);
            var lookupKey = columnTypeMetadata.CreateLookupKey();
            if (!m_columnTypeMetadataLookup.ContainsKey(lookupKey))
                m_columnTypeMetadataLookup.Add(lookupKey, columnTypeMetadata);
            if (!m_MachDBTypeToColumnTypeMetadata.ContainsKey(columnTypeMetadata.MachDBType))
                m_MachDBTypeToColumnTypeMetadata.Add(columnTypeMetadata.MachDBType, columnTypeMetadata);
        }

        internal DbTypeMapping GetDbTypeMapping(Type clrType)
        {
            m_dbTypeMappingsByClrType.TryGetValue(clrType, out var dbTypeMapping);
            return dbTypeMapping;
        }

        internal DbTypeMapping GetDbTypeMapping(DbType dbType)
        {
            m_dbTypeMappingsByDbType.TryGetValue(dbType, out var dbTypeMapping);
            return dbTypeMapping;
        }

        public DbTypeMapping GetDbTypeMapping(string columnTypeName, bool unsigned = false, int length = 0)
        {
            return GetColumnTypeMetadata(columnTypeName, unsigned, length)?.DbTypeMapping;
        }

        private TypeMetadata GetColumnTypeMetadata(string columnTypeName, bool unsigned, int length)
        {
            if (!m_columnTypeMetadataLookup.TryGetValue(TypeMetadata.CreateLookupKey(columnTypeName, unsigned, length), out var columnTypeMetadata) && length != 0)
                m_columnTypeMetadataLookup.TryGetValue(TypeMetadata.CreateLookupKey(columnTypeName, unsigned, 0), out columnTypeMetadata);
            return columnTypeMetadata;
        }

        internal IEnumerable<TypeMetadata> GetColumnMappings()
        {
            return m_columnTypeMetadataLookup.Values.AsEnumerable();
        }

        readonly List<TypeMetadata> m_typeMetadata;
        readonly Dictionary<Type, DbTypeMapping> m_dbTypeMappingsByClrType;
        readonly Dictionary<DbType, DbTypeMapping> m_dbTypeMappingsByDbType;
        readonly Dictionary<string, TypeMetadata> m_columnTypeMetadataLookup;
        readonly Dictionary<MachDBType, TypeMetadata> m_MachDBTypeToColumnTypeMetadata;
    }

    internal static class TypeConverter
    {
        public static string GetDbTypeName(this MachDBType aType)
        {
            return TypeMapper.Instance.GetColumnTypeMetadata(aType).SimpleDataTypeName;
        }

        public static Type GetCompatibleType(this MachDBType aType)
        {
            return TypeMapper.Instance.GetColumnTypeMetadata(aType).DbTypeMapping.ClrType;
        }
    }
}
