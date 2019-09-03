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
         => DeserializeJsonAs<IEvent>(eventData, type);

        public static T DeserializeJsonAs<T>(this EventData eventData, Type type)
            where T : class
            => JsonConvert.DeserializeObject(
                Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count),
                type) as T;
    }
}
