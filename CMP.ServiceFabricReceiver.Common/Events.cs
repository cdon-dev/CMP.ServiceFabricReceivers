using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace CMP.ServiceFabricReceiver.Common
{
    public static class Events
    {
        public static Dictionary<string, Type> GetEventsInNamespace<T>()
            => GetEventsInAssembly<T>()
                .Where(x => x.Value.Namespace == typeof(T).Namespace)
                .ToDictionary(kv => kv.Key, kv => kv.Value);

        public static Dictionary<string, Type> GetEventsInAssembly<T>()
            => typeof(T).GetTypeInfo().Assembly.GetTypes()
                .Where(x => x.GetInterfaces().Any(i => i == typeof(IEvent)))
                .ToDictionary(type => type.Name, type => type);

        public static Dictionary<string, Type> GetEventsFromList(params IEvent[] events)
            => GetEventsFromTypes(events.Select(x => x.GetType()).ToArray());

        public static Dictionary<string, Type> GetEventsFromTypes(params Type[] types)
            => types
                .ToDictionary(x => x.Name, x => x);

        public static IEnumerable<IEvent> FilteredOnEvents(this IEnumerable<EventData> events,
            IReadOnlyDictionary<string, Type> knownEvents, string typePropertyName = "eventname")
            => events
                .Where(x => x.Properties.ContainsKey(typePropertyName))
                .Where(x => knownEvents.ContainsKey(x.Properties[typePropertyName].ToString()))
                .Select(x => x.ToEvent(knownEvents[x.Properties[typePropertyName].ToString()]));

        public static IEvent ToEvent(this EventData eventData, Type type)
         => DeserializeJsonAs<IEvent>(eventData, type)
            .Tap(x => eventData.SystemProperties.ForEach(sp =>
            {
                if (!x.Meta.ContainsKey(sp.Key.ToLower()))
                    x.Meta.Add(sp.Key.ToLower(), sp.Value.ToString());
            }));

        public static T DeserializeJsonAs<T>(this EventData eventData, Type type)
            where T : class
            => JsonConvert.DeserializeObject(
                Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count),
                type) as T;

        public static EventData ToEventData(this IEvent @event)
            => ToEventData(@event, e =>
            {
                var json = JsonConvert.SerializeObject(e);
                return Encoding.UTF8.GetBytes(json);
            });

        public static EventData ToEventData(this IEvent @event, Func<IEvent, byte[]> serializer)
        {
            if (@event.Meta == null)
            {
                @event.Meta = new Dictionary<string, string>();
            }

            var t = @event.GetType();
            var meta = new Dictionary<string, string>
            {
                {"sourceid", @event.SourceId},
                {"eventname", t.Name},
                {"type.eventtypename", t.Name},
                {"type.eventtypefullname", t.FullName},
                {"type.assemblyqualifiedname", t.AssemblyQualifiedName},
                {"type.fullname", t.FullName},
                {"type.version", t.GetVersion()}
            };

            meta.ForEach(p =>
            {
                if (!@event.Meta.ContainsKey(p.Key))
                {
                    @event.Meta.Add(p.Key, p.Value);
                }
            });

            return new EventData(serializer(@event))
                .Tap(
                    x => @event.Meta.ForEach(
                        kv =>
                        {
                            if (!x.Properties.ContainsKey(kv.Key.ToLower()))
                            {
                                x.Properties.Add(kv.Key.ToLower(), kv.Value);
                            }
                        }));
        }

        private const long MaxSize = 1046528;

        public static IEnumerable<EventDataBatch> ToBatches(this IEnumerable<IEvent> events)
        {
            if (events == null) throw new ArgumentNullException(nameof(events));

            return events
                .GroupBy(x => x.SourceId)
                .SelectMany(g => ToPartitionBatches(g.Key, g));
        }

        private static IEnumerable<EventDataBatch> ToPartitionBatches(string partitionKey, IEnumerable<IEvent> events)
        {
            if (partitionKey == null) throw new ArgumentNullException(nameof(partitionKey));
            if (events == null) throw new ArgumentNullException(nameof(events));

            var eventDatas = events
                .Select(e => e.ToEventData());

            var batch = new EventDataBatch(MaxSize, partitionKey);
            foreach (var eventData in eventDatas)
            {
                if (batch.TryAdd(eventData)) continue;

                yield return batch;
                batch = new EventDataBatch(MaxSize, partitionKey);
                var added = batch.TryAdd(eventData);

                if (!added) throw new Exception($"Couldn't add event to new batch. PartitionKey : {partitionKey}");
            }

            yield return batch;
        }

    }
}
