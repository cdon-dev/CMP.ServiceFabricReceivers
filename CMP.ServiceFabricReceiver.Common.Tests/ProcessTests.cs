using Microsoft.ApplicationInsights;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace CMP.ServiceFabricReceiver.Common.Tests
{
    public class ProcessTests
    {
        [Fact]
        public async Task ProccessCancels()
        {
            var eventList = Enumerable.Range(0, 1).Select(x => new EventData(new byte[100]));
            var cts = new CancellationTokenSource();
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents = (events, token) => Task.CompletedTask;
            Func<EventData, Task> checkpoint = ed => Task.CompletedTask;
            var tc = new TelemetryClient();

            cts.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await Execution.ExecuteAsync(cts.Token, new TestLogger(), (s, o) => { }, "test", "0",
                    ct => eventList.ProcessAsync(ct,
                   (events, f) => tc.UseOperationLogging()(events, "test", f),
                   Guid.NewGuid().ToString(),
                   checkpoint,
                   handleEvents,
                   (s, o) => { },
                   (s, o) => { },
                   (e, s, o) => { }
                   ));
            });
        }

        [Fact]
        public async Task ProccessHandleEventsThrows()
        {
            var eventList = Enumerable.Range(0, 1).Select(x => new EventData(new byte[100]));
            var cts = new CancellationTokenSource();
            var tc = new TelemetryClient();

            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents = (events, token)
                =>
            {
                throw new Exception("test");
            };

            Func<EventData, Task> checkpoint = ed => Task.CompletedTask;

            cts.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await Execution.ExecuteAsync(cts.Token, new TestLogger(), (s, o) => { }, "test", "0",
                ct => eventList.ProcessAsync(ct,
                   (events, f) => tc.UseOperationLogging()(events, "test", f),
                   Guid.NewGuid().ToString(),
                   checkpoint,
                   handleEvents,
                   (s, o) => { },
                   (s, o) => { },
                   (e, s, o) => { },
                   0
                   ));
            });
        }

        [Fact]
        public async Task ProccessCancelsHandleEventsThrows()
        {
            var eventList = Enumerable.Range(0, 1).Select((x, i) => new EventData(new byte[100]).Tap(e => {
                e.SystemProperties = new EventData.SystemPropertiesCollection(i, DateTime.UtcNow, "45000", "test");
            }));
            var cts = new CancellationTokenSource();
            var tc = new TelemetryClient();

            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents = (events, token)
                =>
            {
                throw new Exception("test");
            };

            Func<EventData, Task> checkpoint = ed => Task.CompletedTask;

            await Assert.ThrowsAsync<Exception>(async () =>
            {
                await Execution.ExecuteAsync(cts.Token, new TestLogger(), (s, o) => { }, "test", "0",
                ct => eventList.ProcessAsync(ct,
                   (events, f) => tc.UseOperationLogging()(events, "test", f),
                   Guid.NewGuid().ToString(),
                   checkpoint,
                   handleEvents,
                   (s, o) => { },
                   (s, o) => { },
                   (e, s, o) => { },
                   0
                   ));
            });
        }

        [Fact]
        public async Task ProccessCancelsWhenHandleEventsDelays()
        {
            var eventList = Enumerable.Range(0, 1).Select(x => new EventData(new byte[100]));
            var cts = new CancellationTokenSource();
            var tc = new TelemetryClient();

            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents = (events, token)
                => Task.Delay(1000, cts.Token);
            Func<EventData, Task> checkpoint = ed => Task.CompletedTask;

            cts.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await Execution.ExecuteAsync(cts.Token, new TestLogger(), (s, o) => { }, "test", "0",
                ct => eventList.ProcessAsync(ct,
                   (events, f) => tc.UseOperationLogging()(events, "test", f),
                   Guid.NewGuid().ToString(),
                   checkpoint,
                   handleEvents,
                   (s, o) => { },
                   (s, o) => { },
                   (e, s, o) => { }
                   ));
            });
        }

        [Fact]
        public async Task ProccessCancelsCheckpointThrows()
        {
            var eventList = Enumerable.Range(0, 1).Select(x => new EventData(new byte[100]));
            var cts = new CancellationTokenSource();
            var tc = new TelemetryClient();

            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents = (events, token) => Task.CompletedTask;
            Func<EventData, Task> checkpoint = ed =>
            {
                throw new FabricNotPrimaryException();
            };

            cts.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await Execution.ExecuteAsync(cts.Token, new TestLogger(), (s, o) => { }, "test", "0",
                 ct => eventList.ProcessAsync(ct,
                   (events, f) => tc.UseOperationLogging()(events, "test", f),
                   Guid.NewGuid().ToString(),
                   checkpoint,
                   handleEvents,
                   (s, o) => { },
                   (s, o) => { },
                   (e, s, o) => { }
                   ));
            });
        }

        [Fact]
        public async Task ProccessCheckpointThrows()
        {
            var eventList = Enumerable.Range(0, 1).Select(x => new EventData(new byte[100]));
            var cts = new CancellationTokenSource();
            var tc = new TelemetryClient();

            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents = (events, token) => Task.CompletedTask;
            Func<EventData, Task> checkpoint = ed =>
            {
                throw new FabricNotPrimaryException();
            };

            await Assert.ThrowsAsync<FabricNotPrimaryException>(async () =>
            {
                await Execution.ExecuteAsync(cts.Token, new TestLogger(), (s, o) => { }, "test", "0",
                 ct => eventList.ProcessAsync(ct,
                   (events, f) => tc.UseOperationLogging()(events, "test", f),
                   Guid.NewGuid().ToString(),
                   checkpoint,
                   handleEvents,
                   (s, o) => { },
                   (s, o) => { },
                   (e, s, o) => { }
                   ));
            });
        }

    }

    public class TestLogger : ILogger
    {
        public IDisposable BeginScope<TState>(TState state) => DisposableAction.Empty;

        public bool IsEnabled(LogLevel logLevel) => false;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        { }
    }
}
