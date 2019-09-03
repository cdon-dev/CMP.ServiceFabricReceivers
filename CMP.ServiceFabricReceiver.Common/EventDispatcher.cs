using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CMP.ServiceFabricReceiver.Common
{
    public class EventDispatcher
    {
        private const int BatchSize = 200;
        private readonly List<Handler> _consumers = new List<Handler>();

        public void Register<T>(Func<T, CancellationToken, Task> f)
            where T : IEvent
        {
            _consumers.Add(new Handler
            {
                Type = typeof(T),
                Action = (e, ct) => f((T)e, ct)
            });
        }

        public async Task PublishAsync(CancellationToken ct, IEnumerable<IEvent> events)
        {
            var eventHandlerPairs = (
                from e in events
                let consumers = GetConsumers(e.GetType())
                from consumer in consumers
                select new { Event = e, Consumer = consumer.Action }).ToList();

            foreach (var batch in eventHandlerPairs.AsBatches(BatchSize))
            {
                var tasks = batch.Select(x => x.Consumer(x.Event, ct));
                await Task.WhenAll(tasks);
            }
        }

        private IEnumerable<Handler> GetConsumers(Type type)
        {
            return _consumers.Where(x => x.Type.IsAssignableFrom(type));
        }

        private class Handler
        {
            public Type Type;
            public Func<IEvent, CancellationToken, Task> Action;
        }
    }

    public static class Extensions
    {
        public static IEnumerable<IReadOnlyCollection<T>> AsBatches<T>(this IEnumerable<T> source, int batchSize)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize));

            var batch = new List<T>();

            foreach (var item in source)
            {
                if (batch.Count == batchSize)
                {
                    yield return batch;
                    batch = new List<T>();
                }

                batch.Add(item);
            }

            if (batch.Any())
            {
                yield return batch;
            }
        }
    }
}