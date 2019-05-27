using System;
using System.Data;
using System.Data.Common;

namespace Mach.Data.MachClient
{
    public sealed class MachTransaction : DbTransaction
    {
        public override IsolationLevel IsolationLevel => throw new NotImplementedException();

        protected override DbConnection DbConnection => throw new NotImplementedException();

        public override void Commit()
        {
            // Run commit; statement directly (even if it has no effect..)
            throw new NotImplementedException();
        }

        public override void Rollback()
        {
            // Run rollback; statement directly (even if it has no effect..)
            throw new NotImplementedException();
        }
    }
}