using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text;
using static Eventuate.VersionedAggregate;

namespace Eventuate
{
    public static class VersionedAggregate
    {
        public readonly struct Resolved
        {
            public Resolved(string id, VectorTime selected, string origin = null)
            {
                Id = id;
                Selected = selected;
                Origin = origin ?? string.Empty;
            }

            public string Id { get; }
            public VectorTime Selected { get; }
            public string Origin { get; }
        }

        public readonly struct Resolve
        {
            public Resolve(string id, VectorTime selected, string origin = null)
            {
                Id = id;
                Selected = selected;
                Origin = origin ?? string.Empty;
            }

            public string Id { get; }
            public VectorTime Selected { get; }
            public string Origin { get; }

            public Resolve WithOrigin(string origin) => new Resolve(this.Id, this.Selected, origin);
        }

        public static string Priority(string creator1, string creator2) => 
            (string.CompareOrdinal(creator1, creator2) < 0) ? creator1 : creator2;
    }

    public abstract class AbstractAggregateException : Exception
    {
        public AbstractAggregateException(string message) : base(message) { }
        protected AbstractAggregateException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }

    public class AggregateNotLoadedException : AbstractAggregateException
    {
        public AggregateNotLoadedException(string id) : base($"Aggregate {id} not loaded.") { }
        public AggregateNotLoadedException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    public class AggregateAlreadyExistsException : AbstractAggregateException
    {
        public AggregateAlreadyExistsException(string id) : base($"Aggregate {id} already exists.") { }
        public AggregateAlreadyExistsException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    public class AggregateDoesNotExistException : AbstractAggregateException
    {
        public AggregateDoesNotExistException(string id) : base($"Aggregate {id} does not exist.") { }
        public AggregateDoesNotExistException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    public class ConflictResolutionRejectedException : AbstractAggregateException
    {
        public ConflictResolutionRejectedException(string id, string origin1, string origin2) : base($"Conflict for aggregate {id} can only be resolved by {origin1} but {origin2} has attempted") { }
        public ConflictResolutionRejectedException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    public class ConflictNotDetectedException : AbstractAggregateException
    {
        public ConflictNotDetectedException(string id) : base($"Conflict for aggregate {id} not detected.") { }
        public ConflictNotDetectedException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    public class ConflictDetectedException<T> : AbstractAggregateException
    {
        public ConflictDetectedException(string id, IEnumerable<Versioned<T>> versions) : base($"Conflict for aggregate {id} not detected.")
        {
            Versions = versions;
        }
        public ConflictDetectedException(SerializationInfo info, StreamingContext context) : base(info, context) { }
        public IEnumerable<Versioned<T>> Versions { get; }
    }

    public interface IDomainCommand
    {
        string Origin { get; }
    }

    public interface IDomainEvent
    {
        string Origin { get; }
    }

    /// <summary>
    /// Manages concurrent versions of an event-sourced aggregate.
    /// </summary>
    /// <typeparam name="TState">Aggregate type.</typeparam>
    /// <typeparam name="TCommand">Command type.</typeparam>
    /// <typeparam name="TEvent">Event type.</typeparam>
    public class VersionedAggregate<TState, TCommand, TEvent>
        where TState : new ()
        where TCommand : IDomainCommand
        where TEvent : IDomainEvent
    {
        public VersionedAggregate(
            string id,
            Func<TState, TCommand, TEvent> commandHandler,
            Func<TState, TEvent, TState> eventHandler,
            IConcurrentVersions<TState, TEvent> aggregate = null)
        {
            Id = id;
            CommandHandler = commandHandler;
            EventHandler = eventHandler;
            Aggregate = aggregate;
        }

        public string Id { get; }
        public Func<TState, TCommand, TEvent> CommandHandler { get; }
        public Func<TState, TEvent, TState> EventHandler { get; }
        public IConcurrentVersions<TState, TEvent> Aggregate { get; }

        public IEnumerable<Versioned<TState>> Versions => Aggregate.All ?? Enumerable.Empty<Versioned<TState>>();

        public VersionedAggregate<TState, TCommand, TEvent> WithAggregate(IConcurrentVersions<TState, TEvent> aggregate) =>
            new VersionedAggregate<TState, TCommand, TEvent>(this.Id, this.CommandHandler, this.EventHandler, aggregate);

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="AggregateDoesNotExistException">Thrown, when <see cref="Aggregate"/> is not set.</exception>
        /// <exception cref="ConflictDetectedException{TState}">Thrown, when <see cref="Aggregate"/> contains conflicting versions.</exception>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public TEvent ValidateCreate(TCommand command)
        {
            if (this.Aggregate is null) throw new AggregateDoesNotExistException(this.Id);
            if (Aggregate.HasConflict)  throw new ConflictDetectedException<TState>(this.Id, this.Versions);

            return this.CommandHandler(this.Versions.First().Value, command);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="AggregateDoesNotExistException">Thrown, when <see cref="Aggregate"/> is not set.</exception>
        /// <exception cref="ConflictNotDetectedException">Throwm, when <see cref="Aggregate"/> has not conflicts to resolve.</exception>
        /// <exception cref="ConflictResolutionRejectedException">Thrown, when <see cref="Aggregate"/> has owner different than provided <paramref name="origin"/>.</exception>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public Resolved ValidateResolve(int selected, string origin)
        {
            if (this.Aggregate is null) throw new AggregateDoesNotExistException(this.Id);
            if (!this.Aggregate.HasConflict) throw new ConflictNotDetectedException(this.Id);
            if (this.Aggregate.Owner != origin) throw new ConflictResolutionRejectedException(this.Id, this.Aggregate.Owner, origin);

            return new Resolved(this.Id, this.Versions.ElementAt(selected).VectorTimestamp, origin);
        }

        public virtual VersionedAggregate<TState, TCommand, TEvent> HandleCreated(TEvent e, VectorTime vectorTimestamp, long sequenceNr)
        {
            if (this.Aggregate is null)
            {
                var versions = ConcurrentVersionsTree.Create(this.EventHandler)
                    .WithOwner(e.Origin)
                    .Update(e, vectorTimestamp);
                return new VersionedAggregate<TState, TCommand, TEvent>(this.Id, this.CommandHandler, this.EventHandler, versions);
            }
            else
            {
                var versions = this.Aggregate
                    .WithOwner(Priority(this.Aggregate.Owner, e.Origin))
                    .Update(e, vectorTimestamp);
                return new VersionedAggregate<TState, TCommand, TEvent>(this.Id, this.CommandHandler, this.EventHandler, versions);
            }
        }

        public virtual VersionedAggregate<TState, TCommand, TEvent> HandleUpdated(TEvent e, VectorTime vectorTimestamp, long sequenceNr) =>
            new VersionedAggregate<TState, TCommand, TEvent>(this.Id, this.CommandHandler, this.EventHandler, this.Aggregate?.Update(e, vectorTimestamp));

        public virtual VersionedAggregate<TState, TCommand, TEvent> HandleResolved(Resolved resolved, VectorTime updateTimestamp, long sequenceNr) =>
            new VersionedAggregate<TState, TCommand, TEvent>(this.Id, this.CommandHandler, this.EventHandler, this.Aggregate?.Resolve(resolved.Selected, updateTimestamp));
    }
}
