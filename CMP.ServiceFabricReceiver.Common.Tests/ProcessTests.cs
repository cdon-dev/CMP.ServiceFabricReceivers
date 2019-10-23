using Microsoft.Azure.EventHubs;
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

            cts.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await eventList.ProcessAsync(cts.Token,
                   () => DisposableAction.Empty,
                   Guid.NewGuid().ToString(),
                   checkpoint,
                   handleEvents,
                   (s, o) => { },
                   (s, o) => { },
                   (e, s, o) => { }
                   );
            });
        }

        [Fact]
        public async Task ProccessHandleEventsThrows()
        {
            var eventList = Enumerable.Range(0, 1).Select(x => new EventData(new byte[100]));
            var cts = new CancellationTokenSource();
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents = (events, token)
                =>
            {
                throw new Exception("test");
            };

            Func<EventData, Task> checkpoint = ed => Task.CompletedTask;

            cts.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await eventList.ProcessAsync(cts.Token,
                   () => DisposableAction.Empty,
                   Guid.NewGuid().ToString(),
                   checkpoint,
                   handleEvents,
                   (s, o) => { },
                   (s, o) => { },
                   (e, s, o) => { },
                   0
                   );
            });
        }

        [Fact]
        public async Task ProccessCancelsHandleEventsThrows()
        {
            var eventList = Enumerable.Range(0, 1).Select(x => new EventData(new byte[100]));
            var cts = new CancellationTokenSource();
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents = (events, token)
                =>
            {
                throw new Exception("test");
            };

            Func<EventData, Task> checkpoint = ed => Task.CompletedTask;

            await Assert.ThrowsAsync<Exception>(async () =>
            {
                await eventList.ProcessAsync(cts.Token,
                   () => DisposableAction.Empty,
                   Guid.NewGuid().ToString(),
                   checkpoint,
                   handleEvents,
                   (s, o) => { },
                   (s, o) => { },
                   (e, s, o) => { },
                   0
                   );
            });
        }

        [Fact]
        public async Task ProccessCancelsWhenHandleEventsDelays()
        {
            var eventList = Enumerable.Range(0, 1).Select(x => new EventData(new byte[100]));
            var cts = new CancellationTokenSource();
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents = (events, token)
                => Task.Delay(1000, cts.Token);
            Func<EventData, Task> checkpoint = ed => Task.CompletedTask;

            cts.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await eventList.ProcessAsync(cts.Token,
                   () => DisposableAction.Empty,
                   Guid.NewGuid().ToString(),
                   checkpoint,
                   handleEvents,
                   (s, o) => { },
                   (s, o) => { },
                   (e, s, o) => { }
                   );
            });
        }

        [Fact]
        public async Task ProccessCancelsCheckpointThrows()
        {
            var eventList = Enumerable.Range(0, 1).Select(x => new EventData(new byte[100]));
            var cts = new CancellationTokenSource();
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents = (events, token) => Task.CompletedTask;
            Func<EventData, Task> checkpoint = ed =>
            {
                throw new FabricNotPrimaryException();
            };

            cts.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await eventList.ProcessAsync(cts.Token,
                   () => DisposableAction.Empty,
                   Guid.NewGuid().ToString(),
                   checkpoint,
                   handleEvents,
                   (s, o) => { },
                   (s, o) => { },
                   (e, s, o) => { }
                   );
            });
        }

        [Fact]
        public async Task ProccessCheckpointThrows()
        {
            var eventList = Enumerable.Range(0, 1).Select(x => new EventData(new byte[100]));
            var cts = new CancellationTokenSource();
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents = (events, token) => Task.CompletedTask;
            Func<EventData, Task> checkpoint = ed =>
            {
                throw new FabricNotPrimaryException();
            };

            await Assert.ThrowsAsync<FabricNotPrimaryException>(async () =>
            {
                await eventList.ProcessAsync(cts.Token,
                   () => DisposableAction.Empty,
                   Guid.NewGuid().ToString(),
                   checkpoint,
                   handleEvents,
                   (s, o) => { },
                   (s, o) => { },
                   (e, s, o) => { }
                   );
            });
        }

    }
}
