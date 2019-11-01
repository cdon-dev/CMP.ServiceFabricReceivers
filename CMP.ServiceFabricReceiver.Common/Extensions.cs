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
            Func<IDisposable> operationLogger,
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
                    using (operationLogger())
                    {
                        const string name = "EventProcessor";
                        logDebug($"{name}.ProcessEventsAsync for partition {partitionId} got {events.Count()} events",
                            new object[] { partitionId, events.Count });

                        if (!events.Any()) logDebug("Empty event list", Array.Empty<object>());

                        cancellationToken.ThrowIfCancellationRequested();
                        await handleEvents(events, cancellationToken);
                        cancellationToken.ThrowIfCancellationRequested();
                    }

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
                    logError(ex, $"Failed to process events. Canceled : {cancellationToken.IsCancellationRequested}", new object[] { cancellationToken.IsCancellationRequested });
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
    }
}
