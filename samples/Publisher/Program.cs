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
            var cts = new CancellationTokenSource(5000);
            var cs = args.First();
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();

            Log.Logger.Information($"Publishing to :  {cs}");

            await PublishAsync(cts.Token, cs, m => Log.Logger.Information(m), 1000);

            Log.Logger.Information($"Done");
        }


        public static async Task PublishAsync(CancellationToken token, string connectionString, Action<string> log, int events = 100)
        {
            var c = EventHubClient
            .CreateFromConnectionString(connectionString);

            while (!token.IsCancellationRequested)
            {
                log($"Sending {events} events...");

                await c.SendAsync(Enumerable.Range(0, events)
                    .Select(x => new TestEvent { SourceId = Guid.NewGuid().ToString() })
                    );

                await Task.Delay(5000);

                log("Done.");
                log("Waiting...");
            }
        }
    }
}
