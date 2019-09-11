using System;
using System.Linq;

namespace CMP.ServiceFabricReceiver.Common
{
    public static class Logging
    {
        public static Action<string, object[]> Combine(params Action<string, object[]>[] f)
            => f.Aggregate((l, r) => l);

        public static Action<Exception, string, object[]> Combine(params Action<Exception, string, object[]>[] f)
            => f.Aggregate((l, r) => l);
    }
}
