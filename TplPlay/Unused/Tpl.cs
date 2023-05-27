using System.Threading.Tasks.Dataflow;

namespace TplPlay;

public static class Tpl
{
    static readonly DataflowLinkOptions defaultLinkOptions = new DataflowLinkOptions { PropagateCompletion = true };

    public static BufferBlock<Int32> CreateBuffer()
    {
        var block = new BufferBlock<Int32>(new DataflowBlockOptions { BoundedCapacity = 5, MaxMessagesPerTask = 1 });

        return block;
    }

    public static TransformBlock<S, T> Select<S, T>(this ISourceBlock<S> source, Func<S, Task<T>> func)
    {
        var block = new TransformBlock<S, T>(func, new ExecutionDataflowBlockOptions { BoundedCapacity = 1, MaxMessagesPerTask = 1 });

        source.LinkTo(block, defaultLinkOptions);

        return block;
    }

    public static TransformManyBlock<S, T> SelectMany<S, T>(this ISourceBlock<S> source, Func<S, Task<IEnumerable<T>>> func)
    {
        var block = new TransformManyBlock<S, T>(func, new ExecutionDataflowBlockOptions { BoundedCapacity = 2, MaxMessagesPerTask = 1 });

        source.LinkTo(block, defaultLinkOptions);

        return block;
    }

    public static BatchBlock<T> Batch<S, T>(this ISourceBlock<T> source, Int32 batchSize)
    {
        var block = new BatchBlock<T>(batchSize);

        source.LinkTo(block, defaultLinkOptions);

        return block;
    }

    public static ActionBlock<T> Do<T>(this ISourceBlock<T> source, Func<T, Task> action)
    {
        var block = new ActionBlock<T>(action);

        source.LinkTo(block, defaultLinkOptions);

        return block;
    }


}
