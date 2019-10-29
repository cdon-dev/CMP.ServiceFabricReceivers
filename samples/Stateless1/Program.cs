using System;
using System.Diagnostics;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CMP.ServiceFabricRecevier.Stateless;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.ServiceFabric.Services.Runtime;
using Serilog;
using Serilog.Extensions.Logging;

namespace Stateless1
{
    internal static class Program
    {
        /// <summary>
        /// This is the entry point of the service host process.
        /// </summary>
        private static void Main()
        {

            Log.Logger = new LoggerConfiguration()
               .WriteTo.ApplicationInsights(TelemetryConfiguration.Active, TelemetryConverter.Traces, Serilog.Events.LogEventLevel.Debug)
               .MinimumLevel.Debug()
               .CreateLogger();

            var logger = new SerilogLoggerProvider(Log.Logger, true)
                .CreateLogger("Stateless-Sample");

            try
            {
                // The ServiceManifest.XML file defines one or more service type names.
                // Registering a service maps a service type name to a .NET type.
                // When Service Fabric creates an instance of this service type,
                // an instance of the class is created in this host process.

                ServiceRuntime.RegisterServiceAsync(
                    "ReceiverServiceType2",
                    context =>
                        new SampleService(
                         context,
                         logger,
                         new TelemetryClient(TelemetryConfiguration.Active),
                         new ReceiverSettings()
                         {
                             EventHubConnectionString = "",
                             EventHubPath = "sample",
                             StorageConnectionString = "UseDevelopmentStorage=true",
                             ConsumerGroup = "sf",
                             LeaseContainerName = "leases"
                         },
                         ServiceEventSource.Current.Message,
                         async (events, ct) => {
                             ServiceEventSource.Current.Message($"Handle events got {events.Count()} events.");
                             await EventHandler.Handle(events.ToArray());
                         },
                         ct => Task.CompletedTask,
                         new Microsoft.Azure.EventHubs.Processor.EventProcessorOptions()
                   )).GetAwaiter().GetResult();

                ServiceEventSource.Current.ServiceTypeRegistered(Process.GetCurrentProcess().Id, $"{typeof(ReceiverService).Name}2");

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