using Mach.Data.MachClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mach.Comm
{
    class InitProtocol : Protocol
    {
        public InitProtocol() 
            : base()
        {
            base.ProtocolType = ProtocolType.INIT_PROTOCOL;
        }
        
        /* ------------------------- */

        public void Generate(string aPayloadString)
        {
            // aString looks "CMI_INET", "CMI_JDBC" or "CMI_CNET"
            string sConcatedString = null;
            if (BitConverter.IsLittleEndian == true)
            {
                sConcatedString = String.Concat(aPayloadString, "0");
            }
            else
            {
                sConcatedString = String.Concat(aPayloadString, "1");
            }
            var length = Encoding.UTF8.GetByteCount(sConcatedString);
            var payload = new byte[length];
            Encoding.UTF8.GetBytes(sConcatedString, 0, length, payload, 0);

            base.SendData = payload;
        }

        public void Generate(ArraySegment<byte> aPayloadData)
        {
            Generate(aPayloadData.ToString());
        }

        public override void Interpret()
        {
            throw new NotSupportedException();
        }

        public void Interpret(ArraySegment<byte> aMsg)
        {
            UTF8Encoding enc = new UTF8Encoding();
            string sReturnStr = enc.GetString(aMsg.Array, aMsg.Offset, aMsg.Count);
            if (sReturnStr.Length != 9)
            {
                throw new MachException(MachErrorMsg.REJECT_CONNECTION);
            }

            if (sReturnStr.CompareTo("CMI_READY") != 0)
            {
                throw new MachException(MachErrorMsg.REJECT_CONNECTION);
            }
        }

        /* ------------------------- */

        public const int CONNECT_PAYLOAD_RESPONSE_LEN = 9;
    }
}
