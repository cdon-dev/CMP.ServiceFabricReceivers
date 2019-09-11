using CMP.ServiceFabricReceiver.Common.Testing;
using CMP.ServiceFabricReceiver.Common;
using Microsoft.Azure.EventHubs;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Serilog;

namespace Publisher
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var cts = new CancellationTokenSource();
            var cs = args.First();
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();

            Log.Logger.Information($"Publishing to :  {cs}");

            await PublishAsync(cts.Token, cs, m => Log.Logger.Information(m));

            Log.Logger.Information($"Done");
        }


        public static async Task PublishAsync(CancellationToken token, string connectionString, Action<string> log)
        {
            var c = EventHubClient
            .CreateFromConnectionString(connectionString);

            while (!token.IsCancellationRequested)
            {
                log("Sending events...");

                await c.SendAsync(Enumerable.Range(0, 100)
                    .Select(x => new TestEvent { SourceId = Guid.NewGuid().ToString() })
                    );

                log("Done.");
                log("Waiting...");

                await Task.Delay(TimeSpan.FromSeconds(5), token);
            }

        }

    }
}
