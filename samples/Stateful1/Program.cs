using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.ServiceFabric.Services.Runtime;
using Serilog;
using Serilog.Extensions.Logging;
using ServiceFabricReceiver;

namespace Stateful1
{
    internal static class Program
    {
        /// <summary>
        /// This is the entry point of the service host process.
        /// </summary>
        private static void Main()
        {
            Log.Logger = new LoggerConfiguration()
                .CreateLogger();

            var logger = new SerilogLoggerProvider(Log.Logger, true)
                .CreateLogger("Stateful-Sample");

            try
            {
                ServiceRuntime.RegisterServiceAsync(
                    "ReceiverServiceType",
                    context =>
                        new SampleService(
                         context,
                         logger,
                         new TelemetryClient(TelemetryConfiguration.Active),
                         new ReceiverOptions()
                         {
                             ConnectionString = "",
                             ConsumerGroup = "sf"
                         },
                         ServiceEventSource.Current.Message,
                         (events, ct) => EventHandler.Handle(events.ToArray()),
                            ct => Task.CompletedTask
                   )).GetAwaiter().GetResult();


                ServiceEventSource.Current.ServiceTypeRegistered(Process.GetCurrentProcess().Id, typeof(ReceiverService).Name);

                // Prevents this host process from terminating so services keep running.
                Thread.Sleep(Timeout.Infinite);
            }
            catch (Exception e)
            {
                ServiceEventSource.Current.ServiceHostInitializationFailed(e.ToString());
                throw;
            }
        }
    }
}
