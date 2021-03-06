// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: CommonFormats.proto
#pragma warning disable 1591, 0612, 3021
#region Designer generated code

using pb = global::Google.Protobuf;
using pbc = global::Google.Protobuf.Collections;
using pbr = global::Google.Protobuf.Reflection;
using scg = global::System.Collections.Generic;
namespace Eventuate.Serialization.Proto {

  /// <summary>Holder for reflection information generated from CommonFormats.proto</summary>
  internal static partial class CommonFormatsReflection {

    #region Descriptor
    /// <summary>File descriptor for CommonFormats.proto</summary>
    public static pbr::FileDescriptor Descriptor {
      get { return descriptor; }
    }
    private static pbr::FileDescriptor descriptor;

    static CommonFormatsReflection() {
      byte[] descriptorData = global::System.Convert.FromBase64String(
          string.Concat(
            "ChNDb21tb25Gb3JtYXRzLnByb3RvEh1FdmVudHVhdGUuU2VyaWFsaXphdGlv",
            "bi5Qcm90byJpCg1QYXlsb2FkRm9ybWF0EhQKDHNlcmlhbGl6ZXJJZBgBIAEo",
            "BRIPCgdwYXlsb2FkGAIgASgMEhcKD3BheWxvYWRNYW5pZmVzdBgDIAEoCRIY",
            "ChBpc1N0cmluZ01hbmlmZXN0GAQgASgIIsQBCg9WZXJzaW9uZWRGb3JtYXQS",
            "PQoHcGF5bG9hZBgBIAEoCzIsLkV2ZW50dWF0ZS5TZXJpYWxpemF0aW9uLlBy",
            "b3RvLlBheWxvYWRGb3JtYXQSSAoPdmVjdG9yVGltZXN0YW1wGAIgASgLMi8u",
            "RXZlbnR1YXRlLlNlcmlhbGl6YXRpb24uUHJvdG8uVmVjdG9yVGltZUZvcm1h",
            "dBIXCg9zeXN0ZW1UaW1lc3RhbXAYAyABKAMSDwoHY3JlYXRvchgEIAEoCSI/",
            "ChVWZWN0b3JUaW1lRW50cnlGb3JtYXQSEQoJcHJvY2Vzc0lkGAEgASgJEhMK",
            "C2xvZ2ljYWxUaW1lGAIgASgDIlkKEFZlY3RvclRpbWVGb3JtYXQSRQoHZW50",
            "cmllcxgBIAMoCzI0LkV2ZW50dWF0ZS5TZXJpYWxpemF0aW9uLlByb3RvLlZl",
            "Y3RvclRpbWVFbnRyeUZvcm1hdGIGcHJvdG8z"));
      descriptor = pbr::FileDescriptor.FromGeneratedCode(descriptorData,
          new pbr::FileDescriptor[] { },
          new pbr::GeneratedClrTypeInfo(null, new pbr::GeneratedClrTypeInfo[] {
            new pbr::GeneratedClrTypeInfo(typeof(global::Eventuate.Serialization.Proto.PayloadFormat), global::Eventuate.Serialization.Proto.PayloadFormat.Parser, new[]{ "SerializerId", "Payload", "PayloadManifest", "IsStringManifest" }, null, null, null),
            new pbr::GeneratedClrTypeInfo(typeof(global::Eventuate.Serialization.Proto.VersionedFormat), global::Eventuate.Serialization.Proto.VersionedFormat.Parser, new[]{ "Payload", "VectorTimestamp", "SystemTimestamp", "Creator" }, null, null, null),
            new pbr::GeneratedClrTypeInfo(typeof(global::Eventuate.Serialization.Proto.VectorTimeEntryFormat), global::Eventuate.Serialization.Proto.VectorTimeEntryFormat.Parser, new[]{ "ProcessId", "LogicalTime" }, null, null, null),
            new pbr::GeneratedClrTypeInfo(typeof(global::Eventuate.Serialization.Proto.VectorTimeFormat), global::Eventuate.Serialization.Proto.VectorTimeFormat.Parser, new[]{ "Entries" }, null, null, null)
          }));
    }
    #endregion

  }
  #region Messages
  /// <summary>
  ///
  /// This message is used for (de-)serializing custom payloads, like
  /// custom events, snapshots or replication filters
  /// </summary>
  internal sealed partial class PayloadFormat : pb::IMessage<PayloadFormat> {
    private static readonly pb::MessageParser<PayloadFormat> _parser = new pb::MessageParser<PayloadFormat>(() => new PayloadFormat());
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pb::MessageParser<PayloadFormat> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::Eventuate.Serialization.Proto.CommonFormatsReflection.Descriptor.MessageTypes[0]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public PayloadFormat() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public PayloadFormat(PayloadFormat other) : this() {
      serializerId_ = other.serializerId_;
      payload_ = other.payload_;
      payloadManifest_ = other.payloadManifest_;
      isStringManifest_ = other.isStringManifest_;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public PayloadFormat Clone() {
      return new PayloadFormat(this);
    }

    /// <summary>Field number for the "serializerId" field.</summary>
    public const int SerializerIdFieldNumber = 1;
    private int serializerId_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int SerializerId {
      get { return serializerId_; }
      set {
        serializerId_ = value;
      }
    }

    /// <summary>Field number for the "payload" field.</summary>
    public const int PayloadFieldNumber = 2;
    private pb::ByteString payload_ = pb::ByteString.Empty;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public pb::ByteString Payload {
      get { return payload_; }
      set {
        payload_ = pb::ProtoPreconditions.CheckNotNull(value, "value");
      }
    }

    /// <summary>Field number for the "payloadManifest" field.</summary>
    public const int PayloadManifestFieldNumber = 3;
    private string payloadManifest_ = "";
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public string PayloadManifest {
      get { return payloadManifest_; }
      set {
        payloadManifest_ = pb::ProtoPreconditions.CheckNotNull(value, "value");
      }
    }

    /// <summary>Field number for the "isStringManifest" field.</summary>
    public const int IsStringManifestFieldNumber = 4;
    private bool isStringManifest_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool IsStringManifest {
      get { return isStringManifest_; }
      set {
        isStringManifest_ = value;
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override bool Equals(object other) {
      return Equals(other as PayloadFormat);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool Equals(PayloadFormat other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (SerializerId != other.SerializerId) return false;
      if (Payload != other.Payload) return false;
      if (PayloadManifest != other.PayloadManifest) return false;
      if (IsStringManifest != other.IsStringManifest) return false;
      return true;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override int GetHashCode() {
      int hash = 1;
      if (SerializerId != 0) hash ^= SerializerId.GetHashCode();
      if (Payload.Length != 0) hash ^= Payload.GetHashCode();
      if (PayloadManifest.Length != 0) hash ^= PayloadManifest.GetHashCode();
      if (IsStringManifest != false) hash ^= IsStringManifest.GetHashCode();
      return hash;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override string ToString() {
      return pb::JsonFormatter.ToDiagnosticString(this);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void WriteTo(pb::CodedOutputStream output) {
      if (SerializerId != 0) {
        output.WriteRawTag(8);
        output.WriteInt32(SerializerId);
      }
      if (Payload.Length != 0) {
        output.WriteRawTag(18);
        output.WriteBytes(Payload);
      }
      if (PayloadManifest.Length != 0) {
        output.WriteRawTag(26);
        output.WriteString(PayloadManifest);
      }
      if (IsStringManifest != false) {
        output.WriteRawTag(32);
        output.WriteBool(IsStringManifest);
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int CalculateSize() {
      int size = 0;
      if (SerializerId != 0) {
        size += 1 + pb::CodedOutputStream.ComputeInt32Size(SerializerId);
      }
      if (Payload.Length != 0) {
        size += 1 + pb::CodedOutputStream.ComputeBytesSize(Payload);
      }
      if (PayloadManifest.Length != 0) {
        size += 1 + pb::CodedOutputStream.ComputeStringSize(PayloadManifest);
      }
      if (IsStringManifest != false) {
        size += 1 + 1;
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(PayloadFormat other) {
      if (other == null) {
        return;
      }
      if (other.SerializerId != 0) {
        SerializerId = other.SerializerId;
      }
      if (other.Payload.Length != 0) {
        Payload = other.Payload;
      }
      if (other.PayloadManifest.Length != 0) {
        PayloadManifest = other.PayloadManifest;
      }
      if (other.IsStringManifest != false) {
        IsStringManifest = other.IsStringManifest;
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(pb::CodedInputStream input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            input.SkipLastField();
            break;
          case 8: {
            SerializerId = input.ReadInt32();
            break;
          }
          case 18: {
            Payload = input.ReadBytes();
            break;
          }
          case 26: {
            PayloadManifest = input.ReadString();
            break;
          }
          case 32: {
            IsStringManifest = input.ReadBool();
            break;
          }
        }
      }
    }

  }

  internal sealed partial class VersionedFormat : pb::IMessage<VersionedFormat> {
    private static readonly pb::MessageParser<VersionedFormat> _parser = new pb::MessageParser<VersionedFormat>(() => new VersionedFormat());
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pb::MessageParser<VersionedFormat> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::Eventuate.Serialization.Proto.CommonFormatsReflection.Descriptor.MessageTypes[1]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public VersionedFormat() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public VersionedFormat(VersionedFormat other) : this() {
      Payload = other.payload_ != null ? other.Payload.Clone() : null;
      VectorTimestamp = other.vectorTimestamp_ != null ? other.VectorTimestamp.Clone() : null;
      systemTimestamp_ = other.systemTimestamp_;
      creator_ = other.creator_;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public VersionedFormat Clone() {
      return new VersionedFormat(this);
    }

    /// <summary>Field number for the "payload" field.</summary>
    public const int PayloadFieldNumber = 1;
    private global::Eventuate.Serialization.Proto.PayloadFormat payload_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public global::Eventuate.Serialization.Proto.PayloadFormat Payload {
      get { return payload_; }
      set {
        payload_ = value;
      }
    }

    /// <summary>Field number for the "vectorTimestamp" field.</summary>
    public const int VectorTimestampFieldNumber = 2;
    private global::Eventuate.Serialization.Proto.VectorTimeFormat vectorTimestamp_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public global::Eventuate.Serialization.Proto.VectorTimeFormat VectorTimestamp {
      get { return vectorTimestamp_; }
      set {
        vectorTimestamp_ = value;
      }
    }

    /// <summary>Field number for the "systemTimestamp" field.</summary>
    public const int SystemTimestampFieldNumber = 3;
    private long systemTimestamp_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public long SystemTimestamp {
      get { return systemTimestamp_; }
      set {
        systemTimestamp_ = value;
      }
    }

    /// <summary>Field number for the "creator" field.</summary>
    public const int CreatorFieldNumber = 4;
    private string creator_ = "";
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public string Creator {
      get { return creator_; }
      set {
        creator_ = pb::ProtoPreconditions.CheckNotNull(value, "value");
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override bool Equals(object other) {
      return Equals(other as VersionedFormat);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool Equals(VersionedFormat other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (!object.Equals(Payload, other.Payload)) return false;
      if (!object.Equals(VectorTimestamp, other.VectorTimestamp)) return false;
      if (SystemTimestamp != other.SystemTimestamp) return false;
      if (Creator != other.Creator) return false;
      return true;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override int GetHashCode() {
      int hash = 1;
      if (payload_ != null) hash ^= Payload.GetHashCode();
      if (vectorTimestamp_ != null) hash ^= VectorTimestamp.GetHashCode();
      if (SystemTimestamp != 0L) hash ^= SystemTimestamp.GetHashCode();
      if (Creator.Length != 0) hash ^= Creator.GetHashCode();
      return hash;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override string ToString() {
      return pb::JsonFormatter.ToDiagnosticString(this);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void WriteTo(pb::CodedOutputStream output) {
      if (payload_ != null) {
        output.WriteRawTag(10);
        output.WriteMessage(Payload);
      }
      if (vectorTimestamp_ != null) {
        output.WriteRawTag(18);
        output.WriteMessage(VectorTimestamp);
      }
      if (SystemTimestamp != 0L) {
        output.WriteRawTag(24);
        output.WriteInt64(SystemTimestamp);
      }
      if (Creator.Length != 0) {
        output.WriteRawTag(34);
        output.WriteString(Creator);
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int CalculateSize() {
      int size = 0;
      if (payload_ != null) {
        size += 1 + pb::CodedOutputStream.ComputeMessageSize(Payload);
      }
      if (vectorTimestamp_ != null) {
        size += 1 + pb::CodedOutputStream.ComputeMessageSize(VectorTimestamp);
      }
      if (SystemTimestamp != 0L) {
        size += 1 + pb::CodedOutputStream.ComputeInt64Size(SystemTimestamp);
      }
      if (Creator.Length != 0) {
        size += 1 + pb::CodedOutputStream.ComputeStringSize(Creator);
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(VersionedFormat other) {
      if (other == null) {
        return;
      }
      if (other.payload_ != null) {
        if (payload_ == null) {
          payload_ = new global::Eventuate.Serialization.Proto.PayloadFormat();
        }
        Payload.MergeFrom(other.Payload);
      }
      if (other.vectorTimestamp_ != null) {
        if (vectorTimestamp_ == null) {
          vectorTimestamp_ = new global::Eventuate.Serialization.Proto.VectorTimeFormat();
        }
        VectorTimestamp.MergeFrom(other.VectorTimestamp);
      }
      if (other.SystemTimestamp != 0L) {
        SystemTimestamp = other.SystemTimestamp;
      }
      if (other.Creator.Length != 0) {
        Creator = other.Creator;
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(pb::CodedInputStream input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            input.SkipLastField();
            break;
          case 10: {
            if (payload_ == null) {
              payload_ = new global::Eventuate.Serialization.Proto.PayloadFormat();
            }
            input.ReadMessage(payload_);
            break;
          }
          case 18: {
            if (vectorTimestamp_ == null) {
              vectorTimestamp_ = new global::Eventuate.Serialization.Proto.VectorTimeFormat();
            }
            input.ReadMessage(vectorTimestamp_);
            break;
          }
          case 24: {
            SystemTimestamp = input.ReadInt64();
            break;
          }
          case 34: {
            Creator = input.ReadString();
            break;
          }
        }
      }
    }

  }

  internal sealed partial class VectorTimeEntryFormat : pb::IMessage<VectorTimeEntryFormat> {
    private static readonly pb::MessageParser<VectorTimeEntryFormat> _parser = new pb::MessageParser<VectorTimeEntryFormat>(() => new VectorTimeEntryFormat());
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pb::MessageParser<VectorTimeEntryFormat> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::Eventuate.Serialization.Proto.CommonFormatsReflection.Descriptor.MessageTypes[2]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public VectorTimeEntryFormat() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public VectorTimeEntryFormat(VectorTimeEntryFormat other) : this() {
      processId_ = other.processId_;
      logicalTime_ = other.logicalTime_;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public VectorTimeEntryFormat Clone() {
      return new VectorTimeEntryFormat(this);
    }

    /// <summary>Field number for the "processId" field.</summary>
    public const int ProcessIdFieldNumber = 1;
    private string processId_ = "";
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public string ProcessId {
      get { return processId_; }
      set {
        processId_ = pb::ProtoPreconditions.CheckNotNull(value, "value");
      }
    }

    /// <summary>Field number for the "logicalTime" field.</summary>
    public const int LogicalTimeFieldNumber = 2;
    private long logicalTime_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public long LogicalTime {
      get { return logicalTime_; }
      set {
        logicalTime_ = value;
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override bool Equals(object other) {
      return Equals(other as VectorTimeEntryFormat);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool Equals(VectorTimeEntryFormat other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (ProcessId != other.ProcessId) return false;
      if (LogicalTime != other.LogicalTime) return false;
      return true;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override int GetHashCode() {
      int hash = 1;
      if (ProcessId.Length != 0) hash ^= ProcessId.GetHashCode();
      if (LogicalTime != 0L) hash ^= LogicalTime.GetHashCode();
      return hash;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override string ToString() {
      return pb::JsonFormatter.ToDiagnosticString(this);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void WriteTo(pb::CodedOutputStream output) {
      if (ProcessId.Length != 0) {
        output.WriteRawTag(10);
        output.WriteString(ProcessId);
      }
      if (LogicalTime != 0L) {
        output.WriteRawTag(16);
        output.WriteInt64(LogicalTime);
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int CalculateSize() {
      int size = 0;
      if (ProcessId.Length != 0) {
        size += 1 + pb::CodedOutputStream.ComputeStringSize(ProcessId);
      }
      if (LogicalTime != 0L) {
        size += 1 + pb::CodedOutputStream.ComputeInt64Size(LogicalTime);
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(VectorTimeEntryFormat other) {
      if (other == null) {
        return;
      }
      if (other.ProcessId.Length != 0) {
        ProcessId = other.ProcessId;
      }
      if (other.LogicalTime != 0L) {
        LogicalTime = other.LogicalTime;
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(pb::CodedInputStream input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            input.SkipLastField();
            break;
          case 10: {
            ProcessId = input.ReadString();
            break;
          }
          case 16: {
            LogicalTime = input.ReadInt64();
            break;
          }
        }
      }
    }

  }

  internal sealed partial class VectorTimeFormat : pb::IMessage<VectorTimeFormat> {
    private static readonly pb::MessageParser<VectorTimeFormat> _parser = new pb::MessageParser<VectorTimeFormat>(() => new VectorTimeFormat());
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pb::MessageParser<VectorTimeFormat> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::Eventuate.Serialization.Proto.CommonFormatsReflection.Descriptor.MessageTypes[3]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public VectorTimeFormat() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public VectorTimeFormat(VectorTimeFormat other) : this() {
      entries_ = other.entries_.Clone();
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public VectorTimeFormat Clone() {
      return new VectorTimeFormat(this);
    }

    /// <summary>Field number for the "entries" field.</summary>
    public const int EntriesFieldNumber = 1;
    private static readonly pb::FieldCodec<global::Eventuate.Serialization.Proto.VectorTimeEntryFormat> _repeated_entries_codec
        = pb::FieldCodec.ForMessage(10, global::Eventuate.Serialization.Proto.VectorTimeEntryFormat.Parser);
    private readonly pbc::RepeatedField<global::Eventuate.Serialization.Proto.VectorTimeEntryFormat> entries_ = new pbc::RepeatedField<global::Eventuate.Serialization.Proto.VectorTimeEntryFormat>();
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public pbc::RepeatedField<global::Eventuate.Serialization.Proto.VectorTimeEntryFormat> Entries {
      get { return entries_; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override bool Equals(object other) {
      return Equals(other as VectorTimeFormat);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool Equals(VectorTimeFormat other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if(!entries_.Equals(other.entries_)) return false;
      return true;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override int GetHashCode() {
      int hash = 1;
      hash ^= entries_.GetHashCode();
      return hash;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override string ToString() {
      return pb::JsonFormatter.ToDiagnosticString(this);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void WriteTo(pb::CodedOutputStream output) {
      entries_.WriteTo(output, _repeated_entries_codec);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int CalculateSize() {
      int size = 0;
      size += entries_.CalculateSize(_repeated_entries_codec);
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(VectorTimeFormat other) {
      if (other == null) {
        return;
      }
      entries_.Add(other.entries_);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(pb::CodedInputStream input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            input.SkipLastField();
            break;
          case 10: {
            entries_.AddEntriesFrom(input, _repeated_entries_codec);
            break;
          }
        }
      }
    }

  }

  #endregion

}

#endregion Designer generated code
