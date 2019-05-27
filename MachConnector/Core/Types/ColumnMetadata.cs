using Mach.Data.MachClient;

namespace Mach.Core.Types
{
	internal sealed class ColumnMetadata
	{
		public ColumnMetadata(string aColName, ulong aType)
		{
            ColumnName = aColName;
            ColumnType = (MachDBType)((aType >> 56) & 0xFF);
            Precision = (int)((aType >> 28) & 0xFFFFFFF);
            Scale = (int)(aType & 0xFFFFFFF);
        }

        public string ColumnName { get; private set; }
		public MachDBType ColumnType { get; private set; }
        public int Precision { get; private set; }
        public int Scale { get; private set; }
    }
}