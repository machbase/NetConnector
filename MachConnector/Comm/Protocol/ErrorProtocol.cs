using Mach.Core;
using Mach.Core.Statement;
using Mach.Data.MachClient;
using Mach.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mach.Comm
{
    class ErrorProtocol : Protocol
    {
        public ErrorProtocol() 
            : base()
        {
            base.ProtocolType = ProtocolType.ERROR_PROTOCOL;
        }

        /* ------------------------- */

        public void Generate()
        {
            throw new NotImplementedException();
        }

        public override void Interpret()
        {
            Packet sNext = this.ReadNext();

            while (sNext != null)
            {
                switch (sNext.PacketType)
                {
                    case PacketType.RETURN_RESULT_ID:
                        // TODO
                        break;
                    case PacketType.RETURN_MESSAGE_ID:
                        throw new MachException(String.Format("[ERR-%05d : %s]", 0, sNext.GetString()));
                }
                sNext = this.ReadNext();
            }
        }
    }
}
