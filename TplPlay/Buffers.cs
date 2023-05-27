using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Reflection;
using TplPlay.Pipes;

namespace TplPlay;

public static class Buffers
{
    static PropertyInfo pipeLengthProperty = typeof(Pipe).GetProperty("Length", BindingFlags.NonPublic | BindingFlags.Instance);
    static PropertyInfo pauseWriterThresholdProperty = typeof(Pipe).GetProperty("PauseWriterThreshold", BindingFlags.NonPublic | BindingFlags.Instance);

    public static PipeReportPart GetReport(Object buffer)
    {
        var getReportMethod = typeof(Buffers).GetMethod(nameof(GetReport), BindingFlags.Public | BindingFlags.Static, new[] { buffer.GetType() });

        if (getReportMethod is null) throw new Exception($"Can't find GetReport method for buffer type {buffer.GetType()}");

        return (PipeReportPart)getReportMethod.Invoke(null, new[] { buffer });
    }

    public static PipeReportItemBufferPart GetReport<T>(this BlockingCollection<T> buffer)
        => new PipeReportItemBufferPart(buffer.Count, buffer.BoundedCapacity);

    public static PipeReportBinaryBufferPart GetReport(this Pipe buffer)
        => new PipeReportBinaryBufferPart(
            (Int64)pipeLengthProperty.GetValue(buffer),
            (Int64)pauseWriterThresholdProperty.GetValue(buffer)
        );
}
