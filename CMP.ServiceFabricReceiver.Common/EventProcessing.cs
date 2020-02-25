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
                ctx.Logger.LogDebug($"{nameof(Handling)} start");

                await handle(ctx);
                await f(ctx);
                ctx.Logger.LogDebug($"{nameof(Handling)} end");
            };

        public static Func<Func<EventContext, Task>, Func<EventContext, Task>> PartitionLogging()
         => f => async ctx =>
         {
             ctx.Logger.LogDebug($"{nameof(PartitionLogging)} start");

             using (ctx.Logger.BeginScope("Event hub partition : {PartitionId}", ctx.PartitionId))
             {
                 await f(ctx);
             }
             ctx.Logger.LogDebug($"{nameof(PartitionLogging)} end");
         };

        public static Func<Func<EventContext, Task>, Func<EventContext, Task>> Logging()
         => f => async ctx =>
         {
             ctx.Logger.LogDebug($"{nameof(Logging)} start");

             const string name = "EventProcessor";
             ctx.Logger.LogDebug($"{name}.ProcessEventsAsync for partition {ctx.PartitionId} got {ctx.Events.Count()} events",
                 new object[] { ctx.PartitionId, ctx.Events.Count() });

             if (!ctx.Events.Any()) ctx.Logger.LogDebug("Empty event list", Array.Empty<object>());

             ctx.CancellationToken.ThrowIfCancellationRequested();
             await f(ctx);
             ctx.CancellationToken.ThrowIfCancellationRequested();

             ctx.Logger.LogDebug($"{nameof(Logging)} end");
         };

        public static Func<Func<EventContext, Task>, Func<EventContext, Task>> OperationLogging(TelemetryClient telemetryClient)
            => f => async ctx =>
            {
                ctx.Logger.LogDebug($"{nameof(OperationLogging)} start");

                using (var operation = telemetryClient.StartOperation<RequestTelemetry>("ProcessEvents"))
                {
                    operation.Telemetry.Success = false;
                    operation.AppendToRequestLog(ctx.Events, ctx.PartitionId);
                    await f(ctx);
                    operation.Telemetry.Success = true;
                }
                ctx.Logger.LogDebug($"{nameof(OperationLogging)} end");
            };

        public static Func<Func<EventContext, Task>, Func<EventContext, Task>> Retry(int exceptionDelaySeconds = 1)
            => f => ctx => Retry(f, ctx);

        public static async Task Retry(Func<EventContext, Task> f, EventContext ctx, bool faulted = false, int exceptionDelaySeconds = 1)
        {
            
            ctx.Logger.LogDebug($"{nameof(Retry)} start");

            try
            {
                await f(ctx);
            }
            catch (Exception ex) when (faulted)
            {
                ctx.Logger.LogError(ex, $"Failed to process events- Faulted : {faulted}. Cancelled : {ctx.CancellationToken.IsCancellationRequested}", new object[] { ctx.CancellationToken.IsCancellationRequested });
                ctx.CancellationToken.ThrowIfCancellationRequested();
                throw;
            }
            catch (Exception ex)
            {
                ctx.Logger.LogError(ex, $"Failed to process events. Cancelled : {ctx.CancellationToken.IsCancellationRequested}", new object[] { ctx.CancellationToken.IsCancellationRequested });
                ctx.CancellationToken.ThrowIfCancellationRequested();
                if (exceptionDelaySeconds > 0)
                    await Task.Delay(TimeSpan.FromSeconds(exceptionDelaySeconds), ctx.CancellationToken);

                await Retry(f, ctx, true, exceptionDelaySeconds);
            }
            ctx.Logger.LogDebug($"{nameof(Retry)} end");
        }

        public static Func<Func<EventContext, Task>, Func<EventContext, Task>> Checkpointing()
            => f => async ctx =>
            {
                ctx.Logger.LogDebug($"{nameof(Checkpointing)} start");
                if (ctx.Events.Any())
                {

                    ctx.CancellationToken.ThrowIfCancellationRequested();
                    await ctx.Checkpoint(ctx.Events.Last());
                }

                ctx.Logger.LogDebug($"{nameof(Checkpointing)} end");
            };

    }
}
