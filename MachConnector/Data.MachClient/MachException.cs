using System;
using System.Data.Common;

namespace Mach.Data.MachClient
{
    public class MachException : DbException
    {
        internal MachException(string message)
            : this(0, message, null)
        {
        }

        internal MachException(string message, Exception innerException)
            : this(0, message, innerException)
        {
        }

        internal MachException(int errorcode, string message, Exception innerException)
            : base(message, innerException)
        {
            MachErrorCode = errorcode;
        }

        /// <summary>
        /// Error code from MACHBASE (5 decimal digits)
        /// </summary>
        public int MachErrorCode { get; }
    }
}