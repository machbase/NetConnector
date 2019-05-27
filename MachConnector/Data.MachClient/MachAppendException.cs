using System;
using System.Data.Common;

namespace Mach.Data.MachClient
{
    public sealed class MachAppendException : MachException
    {
        internal MachAppendException(string message, string aRowBuffer)
            : this(message, null, aRowBuffer)
        {
        }

        internal MachAppendException(string message, Exception innerException, string aRowBuffer)
            : base(message, innerException)
        {
            this.RowBuffer = aRowBuffer;
        }

        /// <summary>
        /// Get row buffer of failed append data.
        /// </summary>
        /// <returns></returns>
        public string GetRowBuffer()
        {
            return RowBuffer;
        }

        private string rowBuffer;

        internal string RowBuffer { get => rowBuffer; set => rowBuffer = value; }
    }
}