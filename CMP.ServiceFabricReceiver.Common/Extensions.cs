using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.Azure.EventHubs;
using System;
using System.Collections.Generic;
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
            TelemetryClient telemetryClient,
            string partitionId,
            Func<EventData, Task> checkpoint,
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents,
            Action<string, object[]> logDebug,
            Action<string, object[]> logInfo,
            Action<Exception, string, object[]> logError
            )
        {
            var processed = false;
            var events = messages.ToList();

            while (!processed)
            {
                try
                {
                    using (telemetryClient.StartOperation<RequestTelemetry>("ProcessEvents"))
                    {
                        const string name = "EventProcessor";
                        logDebug($"{name}.ProcessEventsAsync for {partitionId} got {events.Count()} events",
                            new object[] { partitionId, events.Count });

                        if (!events.Any()) logDebug("Empty event list", Array.Empty<object>());

                        cancellationToken.ThrowIfCancellationRequested();
                        await handleEvents(events, cancellationToken);
                        cancellationToken.ThrowIfCancellationRequested();

                        if (events.Any())
                        {
                            await checkpoint(events.Last());
                            logDebug("Checkpoint saved", Array.Empty<object>());
                        }
                    }

                    processed = true;
                }
                catch (Exception ex)
                {
                    logError(ex, $"Failed to process events. Canceled : {cancellationToken.IsCancellationRequested}", new object[] { cancellationToken.IsCancellationRequested });
                    cancellationToken.ThrowIfCancellationRequested();
                    throw;
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
    }
}
