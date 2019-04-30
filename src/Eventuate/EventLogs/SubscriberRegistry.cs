using Akka.Actor;
using Eventuate.EventsourcingProtocol;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;

namespace Eventuate.EventLogs
{
    internal sealed class SubscriberRegistry
    {
        public SubscriberRegistry(AggregateRegistry aggregateRegistry = null, ImmutableHashSet<IActorRef> defaultRegistry = null)
        {
            AggregateRegistry = aggregateRegistry ?? new AggregateRegistry();
            DefaultRegistry = defaultRegistry ?? ImmutableHashSet<IActorRef>.Empty;
        }

        public AggregateRegistry AggregateRegistry { get; }
        public ImmutableHashSet<IActorRef> DefaultRegistry { get; }

        public SubscriberRegistry RegisterDefaultSubscriber(IActorRef subscriber) =>
            new SubscriberRegistry(AggregateRegistry, DefaultRegistry.Add(subscriber));

        public SubscriberRegistry RegisterAggregateSubscriber(IActorRef subscriber, string aggregateId) =>
            new SubscriberRegistry(AggregateRegistry.Add(subscriber, aggregateId), DefaultRegistry);

        public SubscriberRegistry UnregisterSubscriber(IActorRef subscriber)
        {
            if (AggregateRegistry.TryGetAggregateId(subscriber, out var aggregateId))
            {
                return new SubscriberRegistry(AggregateRegistry.Remove(subscriber, aggregateId), DefaultRegistry.Remove(subscriber));
            }
            else return new SubscriberRegistry(AggregateRegistry, DefaultRegistry.Remove(subscriber));
        }

        public void NotifySubscribers(IEnumerable<DurableEvent> events, Func<IActorRef, bool> predicate = null)
        {
            foreach (var e in events)
            {
                var written = new Written(e);

                // in any case, notify all default subscribers
                // for which condition evaluates to true
                foreach (var subscriber in DefaultRegistry)
                {
                    if (predicate is null || predicate(subscriber))
                        subscriber.Tell(written);
                }

                // notify subscribers with matching aggregate id
                foreach (var aggregateId in e.DestinationAggregateIds)
                {
                    foreach (var aggregate in AggregateRegistry[aggregateId])
                    {
                        if (predicate is null || predicate(aggregate))
                            aggregate.Tell(written);
                    }
                }
            }
        }
    }

    internal sealed class AggregateRegistry
    {
        public AggregateRegistry(
            ImmutableDictionary<string, ImmutableHashSet<IActorRef>> aggregateRegistry = null,
            ImmutableDictionary<IActorRef, string> aggregateRegistryIndex = null)
        {
            Registry = aggregateRegistry ?? ImmutableDictionary<string, ImmutableHashSet<IActorRef>>.Empty;
            RegistryIndex = aggregateRegistryIndex ?? ImmutableDictionary<IActorRef, string>.Empty;
        }

        public ImmutableDictionary<string, ImmutableHashSet<IActorRef>> Registry { get; }
        public ImmutableDictionary<IActorRef, string> RegistryIndex { get; }

        public ImmutableHashSet<IActorRef> this[string aggregateId] => this.Registry.GetValueOrDefault(aggregateId, ImmutableHashSet<IActorRef>.Empty);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetAggregateId(IActorRef @ref, out string aggregateId) => this.RegistryIndex.TryGetValue(@ref, out aggregateId);

        public AggregateRegistry Add(IActorRef aggregate, string aggregateId)
        {
            if (Registry.TryGetValue(aggregateId, out var aggregates))
            {
                return new AggregateRegistry(
                    Registry.SetItem(aggregateId, aggregates.Add(aggregate)), 
                    RegistryIndex.SetItem(aggregate, aggregateId));
            }
            else return new AggregateRegistry(
                Registry.SetItem(aggregateId, ImmutableHashSet<IActorRef>.Empty.Add(aggregate)), 
                RegistryIndex.SetItem(aggregate, aggregateId));
        }

        public AggregateRegistry Remove(IActorRef aggregate, string aggregateId)
        {
            if (Registry.TryGetValue(aggregateId, out var aggregates))
            {
                var newAggregates = aggregates.Remove(aggregate);
                var newRegistry = newAggregates.IsEmpty ? Registry.Remove(aggregateId) : Registry.SetItem(aggregateId, newAggregates);

                return new AggregateRegistry(newRegistry, RegistryIndex.Remove(aggregate));
            }
            else return this;
        }
    }
}
