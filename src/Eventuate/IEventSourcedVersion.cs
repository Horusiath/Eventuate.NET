namespace Eventuate
{
    /// <summary>
    /// Maintains the current version of an event-sourced component. The current version is updated by merging
    /// <see cref="DurableEvent.VectorTimestamp"/>'s of handled events.
    /// </summary>
    public interface IEventSourcedVersion
    {
        VectorTime CurrentVersion { get; }
    }
}