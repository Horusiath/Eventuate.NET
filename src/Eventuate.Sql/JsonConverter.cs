#region copyright

// -----------------------------------------------------------------------
//  <copyright file="SqlSnapshotStore.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------

#endregion


using System;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using Akka.Actor;
using Newtonsoft.Json;

namespace Eventuate.Sql
{
    public sealed class JsonEvent
    {
        public string Payload { get; set; }
        public string EmitterId { get; set; }
        public string EmitterAggregateId { get; set; }
        public string CustomDestinationAggregateIds { get; set; }
        public DateTime SystemTimestamp { get; set; }
        public string VectorTimestamp { get; set; }
        public string ProcessId { get; set; }
        public string LocalLogId { get; set; }
        public long SequenceNr { get; set; }
        public string DeliveryId { get; set; }
        public long? PersistOnSequenceNr { get; set; }
        public string PersistOnProcessId { get; set; }
    }

    public sealed class JsonSnapshot
    {
        public string Payload { get; set; }
        public string EmitterId { get; set; }
        public long SequenceNr { get; set; }
        public string CurrentTime { get; set; }
        public string DeliveryAttempts { get; set; }
        public string LastEvent { get; set; }
        public string PersistOnEventRequests { get; set; }
    }

    public sealed class DeliveryAttemptJson
    {
        public DeliveryAttemptJson(DeliveryAttempt attempt)
        {
            this.Message = attempt.Message;
            this.Destination = attempt.Destination.ToStringWithoutAddress();
            this.DeliveryId = attempt.DeliveryId;
        }
        
        public DeliveryAttempt ToDeliveryAttempt() => 
            new DeliveryAttempt(
                this.DeliveryId, 
                this.Message, 
                ActorPath.Parse(this.Destination));

        public object Message { get; set; }

        public string Destination { get; set; }

        public string DeliveryId { get; set; }
    }

    public sealed class JsonConverter : IEventConverter<JsonEvent>, ISnapshotConverter<JsonSnapshot>
    {
        private readonly JsonSerializerSettings settings;

        public JsonConverter() : this(new JsonSerializerSettings())
        {
        }

        public JsonConverter(JsonSerializerSettings settings)
        {
            this.settings = settings;
        }

        public DurableEvent ToEvent(JsonEvent row)
        {
            var eventId = row.PersistOnSequenceNr.HasValue && !(row.PersistOnProcessId is null)
                ? new EventId(row.PersistOnProcessId, row.PersistOnSequenceNr.Value)
                : default(EventId?);

            return new DurableEvent(
                payload: JsonConvert.DeserializeObject(row.Payload, settings),
                emitterId: row.EmitterId,
                emitterAggregateId: row.EmitterAggregateId,
                customDestinationAggregateIds: JsonConvert.DeserializeObject<ImmutableHashSet<string>>(row.CustomDestinationAggregateIds, settings),
                systemTimestamp: row.SystemTimestamp,
                vectorTimestamp: new VectorTime(JsonConvert.DeserializeObject<ImmutableDictionary<string, long>>(row.VectorTimestamp, settings)),
                processId: row.ProcessId,
                localLogId: row.LocalLogId,
                localSequenceNr: row.SequenceNr,
                deliveryId: row.DeliveryId,
                persistOnEventSequenceNr: row.PersistOnSequenceNr,
                persistOnEventId: eventId);
        }

        public JsonEvent FromEvent(DurableEvent e)
        {
            var row = new JsonEvent
            {
                Payload = JsonConvert.SerializeObject(e.Payload, settings),
                CustomDestinationAggregateIds = JsonConvert.SerializeObject(e.CustomDestinationAggregateIds, settings),
                VectorTimestamp = JsonConvert.SerializeObject(e.VectorTimestamp, settings),
                EmitterId = e.EmitterId,
                EmitterAggregateId = e.EmitterAggregateId,
                SystemTimestamp = e.SystemTimestamp,
                ProcessId = e.ProcessId,
                SequenceNr = e.LocalSequenceNr,
                DeliveryId = e.DeliveryId,
                PersistOnSequenceNr = e.PersistOnEventSequenceNr
            };

            if (e.PersistOnEventId.HasValue)
            {
                var eventId = e.PersistOnEventId.Value;
                row.PersistOnSequenceNr = eventId.SequenceNr;
                row.PersistOnProcessId = eventId.ProcessId;
            }

            return row;
        }

        public Snapshot ToSnapshot(JsonSnapshot row)
        {
            return new Snapshot(
                payload: JsonConvert.DeserializeObject(row.Payload, settings),
                emitterId: row.EmitterId,
                currentTime: new VectorTime(JsonConvert.DeserializeObject<ImmutableDictionary<string, long>>(row.CurrentTime)),
                sequenceNr: row.SequenceNr,
                lastEvent: JsonConvert.DeserializeObject<DurableEvent>(row.LastEvent, settings),
                deliveryAttempts: row.DeliveryAttempts is null ? default :
                    JsonConvert.DeserializeObject<DeliveryAttemptJson[]>(row.DeliveryAttempts, settings)
                        .Select(d => d.ToDeliveryAttempt())
                        .ToImmutableHashSet(),
                persistOnEventRequests: row.PersistOnEventRequests is null ? default :
                    JsonConvert.DeserializeObject<PersistOnEventRequest[]>(row.PersistOnEventRequests, settings)
                        .ToImmutableHashSet());
        }

        public JsonSnapshot FromSnapshot(Snapshot snapshot)
        {
            var row = new JsonSnapshot
            {
                Payload = JsonConvert.SerializeObject(snapshot.Payload, settings),
                EmitterId = snapshot.EmitterId,
                SequenceNr = snapshot.SequenceNr,
                CurrentTime = JsonConvert.SerializeObject(snapshot.CurrentTime, settings),
                LastEvent = JsonConvert.SerializeObject(snapshot.LastEvent, settings)
            };
            
            if (!snapshot.DeliveryAttempts.IsEmpty)
                row.DeliveryAttempts = JsonConvert.SerializeObject(snapshot.DeliveryAttempts, settings);
            
            if (!snapshot.PersistOnEventRequests.IsEmpty)
                row.PersistOnEventRequests = JsonConvert.SerializeObject(snapshot.PersistOnEventRequests, settings);
            
            return row;
        }

        public string SelectStatement { get; } = "SELECT * FROM snapshots WHERE emitterId = @emitterId";
        public string DeleteToStatement { get; } = "DELETE FROM snapshots WHERE sequenceNr < @sequenceNr";
        public string InsertStatement { get; } = "INSERT INTO snapshots(sequenceNr, emitterId, currentTime, payload, lastEvent, deliveryAttempts, persistOnEventRequests) VALUES(@SequenceNr, @EmitterId, @CurrentTime, @Payload, @LastEvent, @DeliveryAttempts, @PersistOnEventRequests)";
    }
}