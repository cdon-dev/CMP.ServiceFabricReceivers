using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CMP.ServiceFabricReceiver.Common
{
    public class EventContext
    {
        public string PartitionId { get; set; }
        public EventData[] Events { get; set; }
        public CancellationToken CancellationToken { get; set; }
        public Func<EventData, Task> Checkpoint { get; set; }
        public ILogger Logger { get; set; }
    }

    public static class Composition
    {
        public static Func<T, T> Combine<T>(params Func<Func<T, T>, Func<T, T>>[] funcs)
            => input => funcs.Aggregate((l, r) => f => l(r(f)))(c => c)(input);

        public static Func<T, Task> Combine<T>(params Func<Func<T, Task>, Func<T, Task>>[] funcs)
            => input => funcs.Aggregate((l, r) => f => l(r(f)))(c => Task.CompletedTask)(input);
    }

    public static class Features
    {
        public static Func<Func<EventContext, Task>, Func<EventContext, Task>> Handling(Func<EventContext, Task> handle)
            => f => async ctx =>
            {
                using (ctx.Logger.BeginScope("{FeatureName}", nameof(Handling)))
                {
                    ctx.CancellationToken.ThrowIfCancellationRequested();
                    await handle(ctx);
                    ctx.CancellationToken.ThrowIfCancellationRequested();
                    await f(ctx);
                    ctx.CancellationToken.ThrowIfCancellationRequested();
                }
            };

        public static Func<Func<EventContext, Task>, Func<EventContext, Task>> PartitionLogging()
         => f => async ctx =>
         {
             using (ctx.Logger.BeginScope("{FeatureName} - {EventHubPartitionId}", nameof(PartitionLogging), ctx.PartitionId))
             {
                 await f(ctx);
             }
         };

        public static Func<Func<EventContext, Task>, Func<EventContext, Task>> Logging()
         => f => async ctx =>
         {
             using (ctx.Logger.BeginScope("{FeatureName} - Events ({EventCount}) - Cancelled : {IsCancellationRequested}", nameof(Logging), ctx.Events.Length, ctx.CancellationToken.IsCancellationRequested))
             {
                 const string name = "EventProcessor";
                 ctx.Logger.LogDebug($"{name}.ProcessEventsAsync for partition {ctx.PartitionId} got {ctx.Events.Count()} events",
                     new object[] { ctx.PartitionId, ctx.Events.Count() });

                 if (!ctx.Events.Any()) ctx.Logger.LogDebug("Empty event list", Array.Empty<object>());

                 await f(ctx);
             }
         };

        public static Func<Func<EventContext, Task>, Func<EventContext, Task>> OperationLogging(TelemetryClient telemetryClient)
            => f => async ctx =>
            {
                using (ctx.Logger.BeginScope("{FeatureName}", nameof(OperationLogging)))
                {
                    using (var operation = telemetryClient.StartOperation<RequestTelemetry>("ProcessEvents"))
                    {
                        operation.Telemetry.Success = false;
                        operation.AppendToRequestLog(ctx.Events, ctx.PartitionId);
                        await f(ctx);
                        operation.Telemetry.Success = true;
                    }
                }
            };

        public static Func<Func<EventContext, Task>, Func<EventContext, Task>> Retry(int exceptionDelaySeconds = 1)
            => f => ctx => Retry(f, ctx);

        public static async Task Retry(Func<EventContext, Task> f, EventContext ctx, bool faulted = false, int exceptionDelaySeconds = 1)
        {
            using (ctx.Logger.BeginScope("{FeatureName} - Retry : {IsRetry}", nameof(Retry), faulted))
            {
                try
                {
                    await f(ctx);
                }
                catch (Exception ex) when (faulted)
                {
                    ctx.Logger.LogError(ex, "Failed to process events - IsRetry : {IsRetry}. Cancelled : {IsCancellationRequested}", ctx.CancellationToken.IsCancellationRequested);
                    ctx.CancellationToken.ThrowIfCancellationRequested();
                    throw;
                }
                catch (Exception ex)
                {
                    ctx.Logger.LogError(ex, "Failed to process events. Cancelled : {IsCancellationRequested}", ctx.CancellationToken.IsCancellationRequested);
                    ctx.CancellationToken.ThrowIfCancellationRequested();
                    if (exceptionDelaySeconds > 0)
                        await Task.Delay(TimeSpan.FromSeconds(exceptionDelaySeconds), ctx.CancellationToken);

                    await Retry(f, ctx, true, exceptionDelaySeconds);
                }
            }
        }

        public static Func<Func<EventContext, Task>, Func<EventContext, Task>> Checkpointing()
            => f => async ctx =>
            {
                using (ctx.Logger.BeginScope("{FeatureName}", nameof(Checkpointing)))
                {
                    if (ctx.Events.Any())
                    {
                        ctx.CancellationToken.ThrowIfCancellationRequested();
                        await ctx.Checkpoint(ctx.Events.Last());
                    }
                    await f(ctx);
                }
            };
    }
}
