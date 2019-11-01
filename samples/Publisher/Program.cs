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
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cs = args.First();
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();

            Log.Logger.Information($"Publishing to :  {cs}");

            await PublishAsync(cts.Token, cs, m => Log.Logger.Information(m), args.Skip(1).FirstOrDefault());

            Log.Logger.Information($"Done");
        }


        public static async Task PublishAsync(CancellationToken token, string connectionString, Action<string> log, string name, int events = 1000)
        {
            var c = EventHubClient
            .CreateFromConnectionString(connectionString);

            var r = new Random();
            int batch = 1;
            while (!token.IsCancellationRequested)
            {
                log($"Sending {events} events...");

                await c.SendAsync(Enumerable.Range(0, events)
                    .Select((x) => new TestEvent
                    {
                        SourceId = r.Next(1, 3).ToString(),
                        Message = $"{name ?? "unnamed"}-{x + 1}-{Guid.NewGuid()}",
                        Count = (x + 1) * batch
                    }));

                batch++;
                log("Done.");
                log("Waiting...");
            }
        }
    }
}
