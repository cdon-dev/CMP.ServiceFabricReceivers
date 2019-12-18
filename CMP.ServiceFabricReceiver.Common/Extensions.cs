using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.EventHubs;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace CMP.ServiceFabricReceiver.Common
{
    public static class Extensions
    {
        public static async Task ProcessAsync(
            this IEnumerable<EventData> messages,
            CancellationToken cancellationToken,
            Func<IReadOnlyCollection<EventData>, Func<Task>, Task> operationLogger,
            string partitionId,
            Func<EventData, Task> checkpoint,
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents,
            Action<string, object[]> logDebug,
            Action<string, object[]> logInfo,
            Action<Exception, string, object[]> logError,
            int exceptionDelaySeconds = 1
            )
        {
            var processed = false;
            var faulted = false;
            var events = messages.ToList();

            while (!processed)
            {
                try
                {
                    await operationLogger(events, async () =>
                    {
                        const string name = "EventProcessor";
                        logDebug($"{name}.ProcessEventsAsync for partition {partitionId} got {events.Count()} events",
                            new object[] { partitionId, events.Count });

                        if (!events.Any()) logDebug("Empty event list", Array.Empty<object>());

                        cancellationToken.ThrowIfCancellationRequested();
                        await handleEvents(events, cancellationToken);
                        cancellationToken.ThrowIfCancellationRequested();
                    });

                    faulted = false;
                    processed = true;
                }
                catch (Exception ex) when (faulted)
                {
                    logError(ex, $"Failed to process events- Faulted : {faulted}. Cancelled : {cancellationToken.IsCancellationRequested}", new object[] { cancellationToken.IsCancellationRequested });
                    cancellationToken.ThrowIfCancellationRequested();
                    throw;
                }
                catch (Exception ex)
                {
                    logError(ex, $"Failed to process events. Cancelled : {cancellationToken.IsCancellationRequested}", new object[] { cancellationToken.IsCancellationRequested });
                    faulted = true;
                    cancellationToken.ThrowIfCancellationRequested();
                    if (exceptionDelaySeconds > 0)
                        await Task.Delay(TimeSpan.FromSeconds(exceptionDelaySeconds), cancellationToken);
                }
                if (processed && events.Any())
                {
                    await checkpoint(events.Last());
                    logDebug("Checkpoint saved with offset : {offset}. Partition : {partitionId}", new object[] { events.Last().SystemProperties.Offset, partitionId });
                }
            }
        }

        public static Task SendAsync(this EventHubClient client, IEnumerable<IEvent> events)
            => Task.WhenAll(events.ToBatches().Select(x => client.SendAsync(x)));

        public static string GetVersion(this Type type)
            => type?.GetTypeInfo().Assembly.GetName().Version.ToString() ?? string.Empty;

        public static void ForEach<T>(this IEnumerable<T> self, Action<T> f)
        {
            foreach (var item in self)
            {
                f(item);
            }
        }

        public static T Tap<T>(this T self, Action<T> f)
        {
            f(self);
            return self;
        }

        public static void AppendToRequestLog(this IOperationHolder<RequestTelemetry> requestLog,
          IReadOnlyCollection<EventData> events, string partitionId)
        {
            Append("FirstEvent", events.First());
            Append("LastEvent", events.Last());
            requestLog.Telemetry.Properties["EventHubPartitionId"] = partitionId;
            requestLog.Telemetry.Properties["EventCount"] = events.Count.ToString(CultureInfo.InvariantCulture);

            void Append(string prefix, EventData @event)
            {
                requestLog.Telemetry.Properties[prefix + "Offset"] = @event.SystemProperties.Offset;
                requestLog.Telemetry.Properties[prefix + "EnqueueTimeUtc"] =
                    @event.SystemProperties.EnqueuedTimeUtc.ToString(CultureInfo.InvariantCulture);
            }
        }

        public static Func<IReadOnlyCollection<EventData>, string, Func<Task>, Task> UseOperationLogging(this TelemetryClient telemetryClient, bool enabled = false)
         => async (events, partitionId, f) =>
         {
             if (enabled)
             {
                 using (var operation = telemetryClient.StartOperation<RequestTelemetry>("ProcessEvents"))
                 {
                     operation.AppendToRequestLog(events, partitionId);
                     await f();
                 }
             }
             else
             {
                 await f();
             }
         };
    }
}
