#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ReplicationProtocolSerializer.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using Akka.Actor;
using Akka.Serialization;
using Eventuate.ReplicationProtocol;
using Eventuate.Serialization.Proto;
using Google.Protobuf;

namespace Eventuate.Serialization
{
    public sealed class ReplicationProtocolSerializer : SerializerWithStringManifest
    {
        private const string GetReplicationEndpointInfoManifest = "A";
        private const string GetReplicationEndpointInfoSuccessManifest = "B"; 
        private const string SynchronizeReplicationProgressManifest = "C";
        private const string SynchronizeReplicationProgressSuccessManifest = "D";
        private const string SynchronizeReplicationProgressFailureManifest = "E";
        private const string SynchronizeReplicationProgressSourceExceptionManifest = "F";
        private const string ReplicationReadEnvelopeManifest = "G";
        private const string ReplicationReadManifest = "H";
        private const string ReplicationReadSuccessManifest = "I";
        private const string ReplicationReadFailureManifest = "J";
        private const string ReplicationDueManifest = "K";
        private const string ReplicationReadSourceExceptionManifest = "L";
        private const string IncompatibleApplicationVersionExceptionManifest = "M";
            
        private readonly DurableEventSerializer eventSerializer;
        private readonly ReplicationFilterSerializer filterSerializer;
        private readonly DelegatingPayloadSerializer payloadSerializer;

        public ReplicationProtocolSerializer(ExtendedActorSystem system) : base(system)
        {
            this.payloadSerializer = new DelegatingPayloadSerializer(system);
            this.eventSerializer = (DurableEventSerializer)system.Serialization.FindSerializerForType(typeof(DurableEvent));
            this.filterSerializer = new ReplicationFilterSerializer(system);
        }

        public override int Identifier { get; } = 22565;

        public override string Manifest(object o)
        {
            switch (o)
            {
                case GetReplicationEndpointInfo _: return GetReplicationEndpointInfoManifest;
                case GetReplicationEndpointInfoSuccess _: return GetReplicationEndpointInfoSuccessManifest;
                case SynchronizeReplicationProgress _: return SynchronizeReplicationProgressManifest;
                case SynchronizeReplicationProgressSuccess _: return SynchronizeReplicationProgressSuccessManifest;
                case SynchronizeReplicationProgressFailure _: return SynchronizeReplicationProgressFailureManifest;
                case ReplicationReadEnvelope _: return ReplicationReadEnvelopeManifest;
                case ReplicationRead _: return ReplicationReadManifest;
                case ReplicationReadSuccess _: return ReplicationReadSuccessManifest;
                case ReplicationReadFailure _: return ReplicationReadFailureManifest;
                case ReplicationDue _: return ReplicationDueManifest;
                case ReplicationReadSourceException _: return ReplicationReadSourceExceptionManifest;
                case IncompatibleApplicationVersionException _: return IncompatibleApplicationVersionExceptionManifest;
                case SynchronizeReplicationProgressSourceException _: return SynchronizeReplicationProgressSourceExceptionManifest;
                default: throw new NotSupportedException($"{nameof(ReplicationProtocolSerializer)} cannot serialize '{o.GetType().FullName}'");
            }
        }
        
        public override byte[] ToBinary(object o)
        {
            switch (o)
            {
                case GetReplicationEndpointInfo _: return Array.Empty<byte>();
                case GetReplicationEndpointInfoSuccess b: return GetReplicationEndpointInfoSuccessToProto(b).ToByteArray();
                case SynchronizeReplicationProgress c: return SynchronizeReplicationProgressToProto(c).ToByteArray();
                case SynchronizeReplicationProgressSuccess d: return SynchronizeReplicationProgressSuccessToProto(d).ToByteArray();
                case SynchronizeReplicationProgressFailure e: return SynchronizeReplicationProgressFailureToProto(e).ToByteArray();
                case ReplicationReadEnvelope f: return ReplicationReadEnvelopeToProto(f).ToByteArray();
                case ReplicationRead g: return ReplicationReadToProto(g).ToByteArray();
                case ReplicationReadSuccess h: return ReplicationReadSuccessToProto(h).ToByteArray();
                case ReplicationReadFailure i: return ReplicationReadFailureToProto(i).ToByteArray();
                case ReplicationDue _: return Array.Empty<byte>();
                case ReplicationReadSourceException k: return ReplicationReadSourceExceptionToProto(k).ToByteArray();
                case IncompatibleApplicationVersionException l: return IncompatibleApplicationVersionExceptionToProto(l).ToByteArray();
                case SynchronizeReplicationProgressSourceException m: return SynchronizeReplicationProgressSourceExceptionToProto(m).ToByteArray();
                default: throw new NotSupportedException($"{nameof(ReplicationProtocolSerializer)} cannot serialize '{o.GetType().FullName}'");
            }
        }

        private SynchronizeReplicationProgressSourceExceptionFormat SynchronizeReplicationProgressSourceExceptionToProto(SynchronizeReplicationProgressSourceException x) => 
            new SynchronizeReplicationProgressSourceExceptionFormat { Message = x.Message };

        private IncompatibleApplicationVersionExceptionFormat IncompatibleApplicationVersionExceptionToProto(IncompatibleApplicationVersionException err) =>
            new IncompatibleApplicationVersionExceptionFormat
            {
                SourceEndpointId = err.SourceEndpointId,
                SourceApplicationVersion = ApplicationVersionFormatBuilder(err.SourceApplicationVersion),
                TargetApplicationVersion = ApplicationVersionFormatBuilder(err.TargetApplicationVersion)
            };

        private ApplicationVersionFormat ApplicationVersionFormatBuilder(ApplicationVersion ver) =>
            new ApplicationVersionFormat
            {
                Major = ver.Major,
                Minor = ver.Minor
            };

        private ReplicationReadSourceExceptionFormat ReplicationReadSourceExceptionToProto(ReplicationReadSourceException err) =>
            new ReplicationReadSourceExceptionFormat { Message = err.Message };

        private ReplicationReadFailureFormat ReplicationReadFailureToProto(ReplicationReadFailure x) =>
            new ReplicationReadFailureFormat
            {
                Cause = payloadSerializer.PayloadFormatBuilder(x.Cause),
                TargetLogId = x.TargetLogId
            };

        private ReplicationReadSuccessFormat ReplicationReadSuccessToProto(ReplicationReadSuccess x)
        {
            var proto = new ReplicationReadSuccessFormat
            {
                ReplicationProgress = x.ReplicationProgress,
                FromSequenceNr = x.FromSequenceNr,
                TargetLogId = x.TargetLogId,
                CurrentSourceVersionVector = eventSerializer.commonSerializer.VectorTimeFormatBuilder(x.CurrentSourceVersionVector)
            };

            foreach (var e in x.Events)
            {
                proto.Events.Add(eventSerializer.DurableEventFormatBuilder(e));
            }
            
            return proto;
        }

        private ReplicationReadFormat ReplicationReadToProto(ReplicationRead x) =>
            new ReplicationReadFormat
            {
                Filter = filterSerializer.FilterTreeFormatBuilder(x.Filter),
                Max = x.Max,
                ScanLimit = x.ScanLimit,
                FromSequenceNr = x.FromSequenceNr,
                TargetLogId = x.TargetLogId,
                CurrentTargetVersionVector = eventSerializer.commonSerializer.VectorTimeFormatBuilder(x.CurrentTargetVersionVector),
                Replicator = Akka.Serialization.Serialization.SerializedActorPath(x.Replicator)
            };

        private ReplicationReadEnvelopeFormat ReplicationReadEnvelopeToProto(ReplicationReadEnvelope x) =>
            new ReplicationReadEnvelopeFormat
            {
                Payload = ReplicationReadToProto(x.Payload),
                LogName = x.LogName,
                TargetApplicationName = x.TargetApplicationName,
                TargetApplicationVersion = ApplicationVersionFormatBuilder(x.TargetApplicationVersion)
            };

        private SynchronizeReplicationProgressFailureFormat SynchronizeReplicationProgressFailureToProto(in SynchronizeReplicationProgressFailure x) => 
            new SynchronizeReplicationProgressFailureFormat { Cause = payloadSerializer.PayloadFormatBuilder(x.Cause) };

        private SynchronizeReplicationProgressSuccessFormat SynchronizeReplicationProgressSuccessToProto(in SynchronizeReplicationProgressSuccess x) => 
            new SynchronizeReplicationProgressSuccessFormat { Info = ReplicationEndpointInfoToProto(x.Info) };

        private ReplicationEndpointInfoFormat ReplicationEndpointInfoToProto(in ReplicationEndpointInfo x)
        {
            var proto = new ReplicationEndpointInfoFormat
            {
                EndpointId = x.EndpointId
            };

            foreach (var entry in x.LogSequenceNumbers) 
                proto.LogInfos.Add(LogInfoFormatToProto(entry));

            return proto;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private LogInfoFormat LogInfoFormatToProto(in KeyValuePair<string,long> entry) =>
            new LogInfoFormat
            {
                LogName = entry.Key,
                SequenceNr = entry.Value
            };

        private SynchronizeReplicationProgressFormat SynchronizeReplicationProgressToProto(in SynchronizeReplicationProgress x) =>
            new SynchronizeReplicationProgressFormat { Info = ReplicationEndpointInfoToProto(x.Info) };

        private GetReplicationEndpointInfoSuccessFormat GetReplicationEndpointInfoSuccessToProto(in GetReplicationEndpointInfoSuccess x) => 
            new GetReplicationEndpointInfoSuccessFormat { Info = ReplicationEndpointInfoToProto(x.Info) };

        public override object FromBinary(byte[] bytes, string manifest)
        {
            switch (manifest)
            {
                case GetReplicationEndpointInfoManifest: return GetReplicationEndpointInfo.Default; 
                case GetReplicationEndpointInfoSuccessManifest: return GetReplicationEndpointInfoSuccessFromProto(GetReplicationEndpointInfoSuccessFormat.Parser.ParseFrom(bytes));
                case SynchronizeReplicationProgressManifest: return SynchronizeReplicationProgressFromProto(SynchronizeReplicationProgressFormat.Parser.ParseFrom(bytes));
                case SynchronizeReplicationProgressSuccessManifest: return SynchronizeReplicationProgressSuccessFromProto(SynchronizeReplicationProgressSuccessFormat.Parser.ParseFrom(bytes));
                case SynchronizeReplicationProgressFailureManifest: return SynchronizeReplicationProgressFailureFromProto(SynchronizeReplicationProgressFailureFormat.Parser.ParseFrom(bytes));
                case ReplicationReadEnvelopeManifest: return ReplicationReadEnvelopeFromProto(ReplicationReadEnvelopeFormat.Parser.ParseFrom(bytes));
                case ReplicationReadManifest: return ReplicationReadFromProto(ReplicationReadFormat.Parser.ParseFrom(bytes));
                case ReplicationReadSuccessManifest: return ReplicationReadSuccessFromProto(ReplicationReadSuccessFormat.Parser.ParseFrom(bytes));
                case ReplicationReadFailureManifest: return ReplicationReadFailureFromProto(ReplicationReadFailureFormat.Parser.ParseFrom(bytes));
                case ReplicationDueManifest: return ReplicationDue.Default;
                case ReplicationReadSourceExceptionManifest: return ReplicationReadSourceExceptionFromProto(ReplicationReadSourceExceptionFormat.Parser.ParseFrom(bytes));
                case IncompatibleApplicationVersionExceptionManifest: return IncompatibleApplicationVersionExceptionFromProto(IncompatibleApplicationVersionExceptionFormat.Parser.ParseFrom(bytes));
                case SynchronizeReplicationProgressSourceExceptionManifest: return SynchronizeReplicationProgressSourceExceptionFromProto(SynchronizeReplicationProgressSourceExceptionFormat.Parser.ParseFrom(bytes));
                default: throw new NotSupportedException($"{nameof(ReplicationProtocolSerializer)} cannot deserialize object from unknown manifest '{manifest}'");
            }
        }

        private SynchronizeReplicationProgressSourceException SynchronizeReplicationProgressSourceExceptionFromProto(SynchronizeReplicationProgressSourceExceptionFormat format) => 
            new SynchronizeReplicationProgressSourceException(format.Message);

        private IncompatibleApplicationVersionException IncompatibleApplicationVersionExceptionFromProto(IncompatibleApplicationVersionExceptionFormat format)
        {
            return new IncompatibleApplicationVersionException(
                sourceEndpointId: format.SourceEndpointId, 
                sourceApplicationVersion: ApplicationVersionFromProto(format.SourceApplicationVersion),
                targetApplicationVersion: ApplicationVersionFromProto(format.TargetApplicationVersion));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ApplicationVersion ApplicationVersionFromProto(ApplicationVersionFormat format) => 
            new ApplicationVersion(format.Major, format.Minor);

        private ReplicationReadSourceException ReplicationReadSourceExceptionFromProto(ReplicationReadSourceExceptionFormat format) => 
            new ReplicationReadSourceException(format.Message);

        private ReplicationReadSuccess ReplicationReadSuccessFromProto(ReplicationReadSuccessFormat format)
        {
            var events = new DurableEvent[format.Events.Count];
            for (int i = 0; i < format.Events.Count; i++)
            {
                events[i] = eventSerializer.ToDurableEvent(format.Events[i]);
            }
            
            return new ReplicationReadSuccess(
                events: events, 
                fromSequenceNr: format.FromSequenceNr,
                replicationProgress: format.ReplicationProgress,
                targetLogId: format.TargetLogId,
                currentSourceVersionVector: eventSerializer.commonSerializer.VectorTime(format.CurrentSourceVersionVector));
        }

        private ReplicationReadFailure ReplicationReadFailureFromProto(ReplicationReadFailureFormat format) =>
            new ReplicationReadFailure(
                targetLogId: format.TargetLogId,
                cause: (ReplicationReadException)payloadSerializer.Payload(format.Cause));

        private ReplicationRead ReplicationReadFromProto(ReplicationReadFormat format) =>
            new ReplicationRead(
                fromSequenceNr: format.FromSequenceNr,
                max: format.Max,
                scanLimit: format.ScanLimit,
                filter: filterSerializer.FilterTree(format.Filter),
                targetLogId: format.TargetLogId,
                replicator: system.Provider.ResolveActorRef(format.Replicator),
                currentTargetVersionVector: eventSerializer.commonSerializer.VectorTime(format.CurrentTargetVersionVector));

        private ReplicationReadEnvelope ReplicationReadEnvelopeFromProto(ReplicationReadEnvelopeFormat format) =>
            new ReplicationReadEnvelope(
                payload: ReplicationReadFromProto(format.Payload),
                logName: format.LogName,
                targetApplicationName: format.TargetApplicationName,
                targetApplicationVersion: ApplicationVersionFromProto(format.TargetApplicationVersion));

        private SynchronizeReplicationProgressFailure SynchronizeReplicationProgressFailureFromProto(SynchronizeReplicationProgressFailureFormat format) => 
            new SynchronizeReplicationProgressFailure((SynchronizeReplicationProgressException)payloadSerializer.Payload(format.Cause));

        private SynchronizeReplicationProgressSuccess SynchronizeReplicationProgressSuccessFromProto(SynchronizeReplicationProgressSuccessFormat format) => 
            new SynchronizeReplicationProgressSuccess(ReplicationEndpointInfo(format.Info));

        private ReplicationEndpointInfo ReplicationEndpointInfo(ReplicationEndpointInfoFormat format)
        {
            var builder = ImmutableDictionary.CreateBuilder<string, long>();
            foreach (var info in format.LogInfos)
            {
                builder[info.LogName] = info.SequenceNr;
            }
            
            return new ReplicationEndpointInfo(
                endpointId: format.EndpointId,
                logSequenceNumbers: builder.ToImmutable());
        }

        private SynchronizeReplicationProgress SynchronizeReplicationProgressFromProto(SynchronizeReplicationProgressFormat format) => 
            new SynchronizeReplicationProgress(ReplicationEndpointInfo(format.Info));

        private GetReplicationEndpointInfoSuccess GetReplicationEndpointInfoSuccessFromProto(GetReplicationEndpointInfoSuccessFormat format) => 
            new GetReplicationEndpointInfoSuccess(ReplicationEndpointInfo(format.Info));
    }
}