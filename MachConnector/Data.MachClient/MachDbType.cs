using System;
using System.Data;
using System.Data.Common;
using System.IO;

namespace Mach.Data.MachClient
{
    internal enum MachDBTypeMask
    {
        FIX_FLAG = (0x0000),   /* FIX  datatype flag : for storage flag special case!*/
        VAR_FLAG = (0x0001),   /* VAR  datatype flag : for storage flag special case!*/
        TIME_FLAG = (0x0002),  /* TIME datatype flag : for storage flag special case!*/
    }

	public enum MachDBType
	{
        /* VAR flag type */
        VARCHAR = ((0x0001 << 2) | MachDBTypeMask.VAR_FLAG),

        /* TIME flag type */
        DATE = ((0x0001 << 2) | MachDBTypeMask.TIME_FLAG),

        /* FIXED flag type */
        INT16 = ((0x0001 << 2) | MachDBTypeMask.FIX_FLAG),
        INT32 = ((0x0002 << 2) | MachDBTypeMask.FIX_FLAG),
        INT64 = ((0x0003 << 2) | MachDBTypeMask.FIX_FLAG),

        FLT32 = ((0x0004 << 2) | MachDBTypeMask.FIX_FLAG),
        FLT64 = ((0x0005 << 2) | MachDBTypeMask.FIX_FLAG),

        NULL = ((0x0006 << 2) | MachDBTypeMask.FIX_FLAG),
        AVG = ((0x0007 << 2) | MachDBTypeMask.FIX_FLAG),

        IPV4 = ((0x0008 << 2) | MachDBTypeMask.FIX_FLAG),
        IPV6 = ((0x0009 << 2) | MachDBTypeMask.FIX_FLAG),
        BOOL = ((0x000A << 2) | MachDBTypeMask.FIX_FLAG),

        CHAR = ((0x000B) << 2 | MachDBTypeMask.VAR_FLAG),
        TEXT = ((0x000C) << 2 | MachDBTypeMask.VAR_FLAG),
        CLOB = ((0x000D) << 2 | MachDBTypeMask.VAR_FLAG),
        BLOB = ((0x000E) << 2 | MachDBTypeMask.VAR_FLAG),
        BINARY = ((0x0018) << 2 | MachDBTypeMask.VAR_FLAG),
        IPNET = ((0x0019) << 2 | MachDBTypeMask.VAR_FLAG),

        /* unsigned type*/
        UINT16 = ((0x001A) << 2 | MachDBTypeMask.FIX_FLAG),
        UINT32 = ((0x001B) << 2 | MachDBTypeMask.FIX_FLAG),
        UINT64 = ((0x001C) << 2 | MachDBTypeMask.FIX_FLAG),

        MAX = (0x003F << 2),
    }

    internal static class MachDBTypeConverter
    {
        public static bool IsVariableType(this MachDBType aType)
        {
            return ((aType == MachDBType.TEXT) ||
                    (aType == MachDBType.BINARY) ||
                    (aType == MachDBType.VARCHAR));
        }
    }
}
