using System;
using Xunit;
using Mach.Core;
using Mach.Data.MachClient;
using System.Data.Common;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mach.Comm;
using Xunit.Abstractions;

namespace MachConnectorTests
{
    public class FunctionTest
    {
        private readonly ITestOutputHelper output;

        public FunctionTest(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public void AppendMicroSecTest()
        {
            // APPEND using MachAppendOption.MicroSecTruncated option
        }

        [Fact]
        public void InvalidConnStateTest()
        {
            // Using command with invalid connection object?
        }
    }
}
