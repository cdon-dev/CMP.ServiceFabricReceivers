using System;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace CMP.ServiceFabricReceiver.Common.Tests
{
    using Appender = Func<string, Task>;

    public class CompositionTests
    {
        [Fact]
        public async Task tAsync()
        {
            var sb = new StringBuilder();

            Appender a(Appender f) => async s =>
             {
                 sb.Append("#");
                 sb.Append(s);
                 await f(s);
                 sb.Append("#");
             };

            Appender a2(Appender f) => async s =>
             {
                 sb.Append("€");
                 sb.Append(s);
                 await f(s);
                 sb.Append("€");
             };

            await Composition.Combine<string>(a, a2)("hej");

            Assert.Equal("#hej€hej€#", sb.ToString());
        
        }


    }
}
