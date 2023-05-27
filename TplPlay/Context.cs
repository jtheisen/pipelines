using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Reflection;
using TplPlay.Pipes;

namespace TplPlay;

public static class Extensions
{
    static PropertyInfo pipeLengthProperty = typeof(Pipe).GetProperty("Length", BindingFlags.NonPublic | BindingFlags.Instance);

    public static PipeBufferPart ToPart(this Pipe pipe)
    {
        return new PipeBufferPart(() => (Int64)pipeLengthProperty.GetValue(pipe));
    }

    public static PipeBufferPart ToPart<T>(this BlockingCollection<T> collection)
    {
        return new PipeBufferPart(() => collection.Count);
    }
}