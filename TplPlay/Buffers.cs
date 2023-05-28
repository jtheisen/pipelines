using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Reflection;
using TplPlay.Pipes;

namespace TplPlay;

public static class Buffers
{
    static PropertyInfo pipeLengthProperty = typeof(Pipe).GetProperty("Length", BindingFlags.NonPublic | BindingFlags.Instance);
    static PropertyInfo resumeWriterThresholdProperty = typeof(Pipe).GetProperty("ResumeWriterThreshold", BindingFlags.NonPublic | BindingFlags.Instance);
    static PropertyInfo pauseWriterThresholdProperty = typeof(Pipe).GetProperty("PauseWriterThreshold", BindingFlags.NonPublic | BindingFlags.Instance);

    public static PipeReportPart GetReport(Object buffer)
    {
        var getReportMethod = typeof(Buffers).GetMethod(nameof(GetReport), BindingFlags.Public | BindingFlags.Static, new[] { buffer.GetType() });

        if (getReportMethod is null) throw new Exception($"Can't find GetReport method for buffer type {buffer.GetType()}");

        return (PipeReportPart)getReportMethod.Invoke(null, new[] { buffer });
    }

    public static PipeReportBufferPart GetReport<T>(this BlockingCollection<T> buffer)
        => new PipeReportBufferPart(
            GetBufferState(buffer.Count, buffer.BoundedCapacity >> 2, buffer.BoundedCapacity >> 1),
            buffer.Count,
            buffer.BoundedCapacity
        );

    public static PipeReportBufferPart GetReport(this Pipe buffer)
    {
        var count = (Int64)pipeLengthProperty.GetValue(buffer);
        var resumeThreshold = (Int64)resumeWriterThresholdProperty.GetValue(buffer);
        var pauseThreshold = (Int64)pauseWriterThresholdProperty.GetValue(buffer);

        return new PipeReportBufferPart(
            GetBufferState(count, resumeThreshold >> 2, resumeThreshold >> 1),
            count,
            pauseThreshold
        );
    }

    public static PipeReportBufferState GetBufferState(Int64 count, Int64 lower, Int64 upper)
    {
        if (count < lower) return PipeReportBufferState.Empty;
        if (count > upper) return PipeReportBufferState.Full;
        return PipeReportBufferState.Mixed;
    }
}
