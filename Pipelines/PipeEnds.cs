namespace Pipelines;

public interface IPipeEnd<B>
{
    void Run(B nextBuffer, IPipeContext context);
}

public interface IStreamPipeEnd : IPipeEnd<Pipe> { }

public interface IEnumerablePipeEnd<T> : IPipeEnd<BlockingCollection<T>> { }

public delegate void PipeEnd<B>(B pipe, IPipeContext context);

public delegate void TerminalPipeEndWorker<B>(B sourceOrSink, IPipeContext context);

public delegate void PipeWorker<SB, TB>(SB source, TB sink, IPipeContext context);

public delegate void StreamWorker(Stream source, Stream sink, IPipeContext context);

public delegate Stream StreamTransformation(Stream nested);

public class DelegateEnumerablePipeEnd<T> : IEnumerablePipeEnd<T>
{
    private readonly PipeEnd<BlockingCollection<T>> implementation;

    public DelegateEnumerablePipeEnd(PipeEnd<BlockingCollection<T>> implementation)
        => this.implementation = implementation;

    public void Run(BlockingCollection<T> nextBuffer, IPipeContext context)
        => implementation(nextBuffer, context);
}

public class DelegateNestedEnumerablePipeEnd<OB, T> : IEnumerablePipeEnd<T>
    where OB : class, new()
{
    private readonly IPipeEnd<OB> sourcePipeEnd;
    private readonly String name;
    private readonly PipeWorker<OB, BlockingCollection<T>> forward;
    private readonly PipeWorker<BlockingCollection<T>, OB> backward;

    public DelegateNestedEnumerablePipeEnd(IPipeEnd<OB> sourcePipeEnd, String name, PipeWorker<OB, BlockingCollection<T>> forward, PipeWorker<BlockingCollection<T>, OB> backward)
    {
        this.sourcePipeEnd = sourcePipeEnd;
        this.name = name;
        this.forward = forward;
        this.backward = backward;
    }

    public void Run(BlockingCollection<T> nextBuffer, IPipeContext context)
    {
        var buffer = Buffers.MakeBuffer<OB>();

        sourcePipeEnd.Run(buffer, context);

        context.AddBuffer(buffer);

        context.AddWorker(name);

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                if (forward is null) throw GetSuckingNotSupported(name);
                forward(buffer, nextBuffer, context);
                break;
            case PipeRunMode.Blow:
                if (backward is null) throw GetBlowingNotSupported(name);
                backward(nextBuffer, buffer, context);
                break;
        }
    }
}

public class DelegateStreamPipeEnd : IStreamPipeEnd
{
    private readonly PipeEnd<Pipe> implementation;

    public DelegateStreamPipeEnd(PipeEnd<Pipe> implementation)
        => this.implementation = implementation;

    public void Run(Pipe nextBuffer, IPipeContext context)
        => implementation(nextBuffer, context);
}

public class DelegateNestedStreamPipeEnd<SB> : IStreamPipeEnd
    where SB : class, new()
{
    private readonly IPipeEnd<SB> sourcePipeEnd;
    private readonly String name;
    private readonly PipeWorker<SB, Pipe> forward;
    private readonly PipeWorker<Pipe, SB> backward;

    public DelegateNestedStreamPipeEnd(IPipeEnd<SB> sourcePipeEnd, String name, PipeWorker<SB, Pipe> forward, PipeWorker<Pipe, SB> backward)
    {
        this.sourcePipeEnd = sourcePipeEnd;
        this.name = name;
        this.forward = forward;
        this.backward = backward;
    }

    public void Run(Pipe nextBuffer, IPipeContext context)
    {
        var buffer = Buffers.MakeBuffer<SB>();

        sourcePipeEnd.Run(buffer, context);

        context.AddBuffer(buffer);

        context.AddWorker(name);

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                if (forward is null) throw GetSuckingNotSupported(name);
                forward(buffer, nextBuffer, context);
                break;
            case PipeRunMode.Blow:
                if (backward is null) throw GetBlowingNotSupported(name);
                backward(nextBuffer, buffer, context);
                break;
            default:
                break;
        }
    }
}

public class EnumerableTransformEnumerablePipeEnd<S, T> : IEnumerablePipeEnd<T>
{
    private readonly IEnumerablePipeEnd<S> nestedPipeEnd;
    private readonly Func<IEnumerable<S>, IEnumerable<T>> map;

    public EnumerableTransformEnumerablePipeEnd(IEnumerablePipeEnd<S> nestedPipeEnd, Func<IEnumerable<S>, IEnumerable<T>> map)
    {
        this.nestedPipeEnd = nestedPipeEnd;
        this.map = map;
    }

    public void Run(BlockingCollection<T> nextBuffer, IPipeContext context)
    {
        var buffer = Buffers.MakeBuffer<BlockingCollection<S>>();

        nestedPipeEnd.Run(buffer, context);

        context.AddBuffer(buffer);

        context.AddWorker("transform");

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                context.Schedule("transforming", () => Transform(buffer, nextBuffer));
                break;
            case PipeRunMode.Blow:
                throw GetBlowingNotSupported("transform");
            default:
                break;
        }
    }

    void Transform(BlockingCollection<S> buffer, BlockingCollection<T> sink)
    {
        var items = map(buffer.GetConsumingEnumerable());

        foreach (var item in items)
        {
            sink.Add(item);
        }

        sink.CompleteAdding();
    }
}

public class AsyncEnumerableTransformEnumerablePipeEnd<S, T> : IEnumerablePipeEnd<T>
{
    private readonly IEnumerablePipeEnd<S> nestedPipeEnd;
    private readonly Func<IEnumerable<S>, IAsyncEnumerable<T>> map;

    public AsyncEnumerableTransformEnumerablePipeEnd(IEnumerablePipeEnd<S> nestedPipeEnd, Func<IEnumerable<S>, IAsyncEnumerable<T>> map)
    {
        this.nestedPipeEnd = nestedPipeEnd;
        this.map = map;
    }

    public void Run(BlockingCollection<T> nextBuffer, IPipeContext context)
    {
        var buffer = Buffers.MakeBuffer<BlockingCollection<S>>();

        nestedPipeEnd.Run(buffer, context);

        context.AddBuffer(buffer);

        context.AddWorker("transform");

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                context.ScheduleAsync("transforming", async ct =>
                {
                    var items = map(buffer.GetConsumingEnumerable(ct));

                    await foreach (var item in items)
                    {
                        nextBuffer.Add(item, ct);
                    }

                    nextBuffer.CompleteAdding();
                });
                break;
            case PipeRunMode.Blow:
                throw new Exception($"This worker does not support blowing");
            default:
                break;
        }
    }
}

public class TransformEnumerablePipeEnd<S, T> : IEnumerablePipeEnd<T>
{
    private readonly IEnumerablePipeEnd<S> nestedPipeEnd;
    private readonly Func<S, T> map;
    private readonly Func<T, S> reverseMap;

    public TransformEnumerablePipeEnd(IEnumerablePipeEnd<S> nestedPipeEnd, Func<S, T> map, Func<T, S> reverseMap = null)
    {
        this.nestedPipeEnd = nestedPipeEnd;
        this.map = map;
        this.reverseMap = reverseMap;
    }

    public void Run(BlockingCollection<T> nextBuffer, IPipeContext context)
    {
        var buffer = Buffers.MakeBuffer<BlockingCollection<S>>();

        nestedPipeEnd.Run(buffer, context);

        context.AddBuffer(buffer);

        context.AddWorker("transform");

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                context.Schedule("transforming", () => buffer.TransformTo(nextBuffer, map));
                break;
            case PipeRunMode.Blow:
                context.Schedule("transforming", () => nextBuffer.TransformTo(buffer, reverseMap));
                break;
            default:
                break;
        }
    }
}

public static partial class PipeEnds
{
    public static IEnumerablePipeEnd<T> AsSpecificPipeEnd<T>(IPipeEnd<BlockingCollection<T>> source)
        => new DelegateEnumerablePipeEnd<T>(source.Run);

    public static IStreamPipeEnd AsSpecificPipeEnd(IPipeEnd<Pipe> source)
        => new DelegateStreamPipeEnd(source.Run);

    public static IEnumerablePipeEnd<T> Enumerable<T>(PipeEnd<BlockingCollection<T>> implementation)
        => new DelegateEnumerablePipeEnd<T>(implementation);

    public static IEnumerablePipeEnd<T> Enumerable<OB, T>(IPipeEnd<OB> sourcePipeEnd, String name, PipeWorker<OB, BlockingCollection<T>> forward, PipeWorker<BlockingCollection<T>, OB> backward)
        where OB : class, new()
        => new DelegateNestedEnumerablePipeEnd<OB, T>(sourcePipeEnd, name, forward, backward);

    public static IEnumerablePipeEnd<T> Enumerable<T>(String name, TerminalPipeEndWorker<BlockingCollection<T>> producer, TerminalPipeEndWorker<BlockingCollection<T>> consumer)
        => new DelegateEnumerablePipeEnd<T>((nextBuffer, context) =>
    {
        context.AddWorker(name);

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                if (producer is null) throw new Exception($"Sucking from a {name} is not supported");

                producer(nextBuffer, context);
                break;
            case PipeRunMode.Blow:
                if (consumer is null) throw new Exception($"Blowing into a {name} is not supported");

                consumer(nextBuffer, context);
                break;
            default:
                break;
        }
    });

    public static IStreamPipeEnd Stream(PipeEnd<Pipe> implementation)
        => new DelegateStreamPipeEnd(implementation);

    public static IStreamPipeEnd Stream<OB>(IPipeEnd<OB> sourcePipeEnd, String name, PipeWorker<OB, Pipe> forward, PipeWorker<Pipe, OB> backward)
        where OB : class, new()
        => new DelegateNestedStreamPipeEnd<OB>(sourcePipeEnd, name, forward, backward);

    public static IStreamPipeEnd Stream(IStreamPipeEnd sourcePipeEnd, String name, StreamWorker forward, StreamWorker backward)
        => Stream(
            sourcePipeEnd,
            name,
            (source, sink, context) => forward(source.Reader.AsStream(), sink.Writer.AsStream(), context),
            (source, sink, context) => backward(source.Reader.AsStream(), sink.Writer.AsStream(), context));

    public static IStreamPipeEnd WrapStream(this IStreamPipeEnd sourcePipeEnd, String name, StreamTransformation forward, StreamTransformation backward)
        => Stream(sourcePipeEnd, name,
            (source, sink, context) => forward(source.Reader.AsStream()).CopyTo(sink.Writer.AsStream()),
            (source, sink, context) => backward(source.Reader.AsStream()).CopyTo(sink.Writer.AsStream()));

    public static StreamWorker ToWorker(this StreamTransformation transformation)
        => (source, sink, context) => transformation(source).CopyTo(sink);

    public static IStreamPipeEnd File(String fileName) => Stream((pipe, context) =>
    {
        var fileInfo = new FileInfo(fileName);

        context.AddWorker(nameof(File));

        // FIXME: have progress reporting

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                context.ScheduleAsync("reading", async (ct, progress) =>
                {
                    progress.ReportTotal(fileInfo.Length);

                    using var stream = fileInfo.OpenRead();

                    await stream.CopyToAsync(pipe.Writer, ct);

                    pipe.Writer.Complete();
                });

                break;
            case PipeRunMode.Blow:
                context.ScheduleAsync("writing", async ct =>
                {
                    using var stream = fileInfo.OpenWrite();

                    await pipe.Reader.CopyToAsync(stream, ct);
                });

                break;
            default:
                break;
        }
    });

    public static IEnumerablePipeEnd<T> FromQueryable<T>(this IQueryable<T> source)
        => Enumerable<T>(nameof(FromQueryable), (nextBuffer, context) =>
    {
        context.Schedule("querying", progress =>
        {
            var length = source.LongCount();

            progress.ReportTotal(length);

            var processed = 0L;

            foreach (var item in source)
            {
                nextBuffer.Add(item);

                progress.ReportProcessed(++processed);
            }

            nextBuffer.CompleteAdding();
        });
    }, null);

    public static IEnumerablePipeEnd<T> FromAction<T>(Action<T> action)
        => Enumerable<T>(nameof(FromAction), null, (nextBuffer, context) => context.Schedule("calling", () =>
        {
            foreach (var item in nextBuffer.GetConsumingEnumerable())
            {
                action(item);
            }
        })
    );

    public static IEnumerablePipeEnd<T> FromAsyncAction<T>(Func<T, CancellationToken, Task> action)
        => Enumerable<T>(nameof(FromAsyncAction), null, (nextBuffer, context) => context.ScheduleAsync("calling", async ct =>
        {
            foreach (var item in nextBuffer.GetConsumingEnumerable(ct))
            {
                await action(item, ct);
            }
        })
    );

    public static IEnumerablePipeEnd<T> Transform2<S, T>(this IEnumerablePipeEnd<S> source, Func<IEnumerable<S>, IEnumerable<T>> map)
        => Enumerable<T>(nameof(Transform), )

    public static IEnumerablePipeEnd<T> Transform<S, T>(this IEnumerablePipeEnd<S> source, Func<IEnumerable<S>, IEnumerable<T>> map)
        => new EnumerableTransformEnumerablePipeEnd<S, T>(source, map);

    public static IEnumerablePipeEnd<T> Map<S, T>(this IEnumerablePipeEnd<S> source, Func<S, T> map, Func<T, S> reverseMap = null)
        => new TransformEnumerablePipeEnd<S, T>(source, map, reverseMap);

    public static IEnumerablePipeEnd<T> Map<S, T>(this IEnumerablePipeEnd<S> source, Func<S, CancellationToken, Task<T>> map, Func<T, CancellationToken, Task<S>> reverseMap = null)
        => Enumerable<T>((nextBuffer, context) =>
    {
        var buffer = Buffers.MakeBuffer<BlockingCollection<S>>();

        source.Run(buffer, context);

        context.AddBuffer(buffer);

        context.AddWorker("transform");

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                context.ScheduleAsync("transforming", ct => buffer.TransformToAsync(nextBuffer, map, ct));
                break;
            case PipeRunMode.Blow:
                context.ScheduleAsync("transforming", ct => nextBuffer.TransformToAsync(buffer, reverseMap, ct));
                break;
            default:
                break;
        }
    });

    public static IEnumerablePipeEnd<T> Do<T>(this IEnumerablePipeEnd<T> source, Action<T> action)
    {
        Func<T, T> map = t =>
        {
            action(t);

            return t;
        };

        return new TransformEnumerablePipeEnd<T, T>(source, map, map);
    }

    public static IPipeline BuildCopyingPipeline<B>(this IPipeEnd<B> source, IPipeEnd<B> sink)
        where B : class, new()
    {
        return new Pipeline<B>(source, sink);
    }

    public static LivePipeline StartCopyingPipeline<B>(this IPipeEnd<B> source, IPipeEnd<B> sink)
        where B : class, new()
    {
        var pipeline = source.BuildCopyingPipeline(sink);

        return pipeline.Start();
    }

    public static Task CopyToAsync<B>(this IPipeEnd<B> source, IPipeEnd<B> sink)
        where B : class, new()
    {
        var livePipeline = source.StartCopyingPipeline(sink);

        return livePipeline.Task;
    }

    public static void CopyTo<B>(this IPipeEnd<B> source, IPipeEnd<B> sink)
        where B : class, new()
    {
        source.CopyToAsync(sink).Wait();
    }

    public static async Task<T[]> ReadAllAsync<T>(this IEnumerablePipeEnd<T> source)
    {
        var list = new List<T>();

        var target = FromAction<T>(list.Add);

        await source.CopyToAsync(target);

        return list.ToArray();
    }

    public static T[] ReadAll<T>(this IEnumerablePipeEnd<T> source)
        => source.ReadAllAsync().Result;

    public static LivePipeline Start(this IPipeline pipeline)
    {
        pipeline.Run(out var livePipeline);
        
        return livePipeline;
    }

    public static void Wait(this LivePipeline pipeline) => pipeline.Task.Wait();

    public static Task WaitAsync(this LivePipeline pipeline) => pipeline.Task;
}
