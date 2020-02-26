using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CMP.ServiceFabricReceiver.Common;
using CMP.ServiceFabricRecevier.Stateless;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.PlatformAbstractions;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.WindowsAzure.Storage;
using Serilog;
using Serilog.Extensions.Logging;
namespace Stateless1
{
    internal static class Program
    {
        /// <summary>
        /// This is the entry point of the service host process.
        /// </summary>
        private static void Main(string[] args)
        {
            var telemetryClient = new TelemetryClient(TelemetryConfiguration.CreateDefault());

            Log.Logger = new LoggerConfiguration()
               .WriteTo.ApplicationInsights(telemetryClient, TelemetryConverter.Traces, Serilog.Events.LogEventLevel.Debug)
               //.WriteTo.AzureTableStorage(CloudStorageAccount.DevelopmentStorageAccount, Serilog.Events.LogEventLevel.Warning)
               .WriteTo.ColoredConsole(Serilog.Events.LogEventLevel.Debug, outputTemplate:
                    "[{Timestamp:HH:mm:ss} {Level:u3}] {PartitionId} {Scope:lj} {Message:lj}{NewLine}{Exception}")
               .MinimumLevel.Debug()
               .Enrich.FromLogContext()
               .CreateLogger();

            var logger = new SerilogLoggerProvider(Log.Logger, true)
                .CreateLogger("Stateless-Sample");

            var storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
            var table = storageAccount.CreateCloudTableClient().GetTableReference("receiversample");
            table.CreateIfNotExistsAsync().GetAwaiter().GetResult();

            var settings = new ReceiverSettings()
            {
                EventHubConnectionString = args.First(),
                EventHubPath = "sample",
                StorageConnectionString = "UseDevelopmentStorage=true",
                ConsumerGroup = "sf",
                LeaseContainerName = "leases"
            };

            var options = new EventProcessorOptions
            {
                InitialOffsetProvider = partition =>
                {
                    logger.LogWarning("InitialOffsetProvider called for {partition}", partition);
                    return EventPosition.FromStart();
                }
            };

            var pipeline = Composition.Combine(
                                 Features.PartitionLogging(),
                                 Features.OperationLogging(telemetryClient),
                                 Features.Logging(),
                                 Features.Retry(),
                                 Features.Handling(x => EventHandler.Handle("Sample", table, x.Events)),
                                 Features.Checkpointing()
                                 );

            var isInCluster = PlatformServices.Default.Application.ApplicationBasePath.Contains(".Code.");

            if (!isInCluster)
            {
                ReceiverService
                    .RunAsync(settings.ToHost(), logger, options, CancellationToken.None, (s, o) => { }, "none" , partitionId => ctx => pipeline(ctx))
                    .GetAwaiter()
                    .GetResult();
                
                Thread.Sleep(Timeout.Infinite);
            }

            try
            {
                ServiceRuntime.RegisterServiceAsync(
                    "ReceiverServiceType2",
                    context =>
                        new SampleService(
                         context,
                         logger,
                         settings,
                         ServiceEventSource.Current.Message,
                         ct => Task.CompletedTask,
                         partitionId => ctx => pipeline(ctx),
                         options)
                        ).GetAwaiter().GetResult();

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
