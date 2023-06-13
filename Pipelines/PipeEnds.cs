namespace Pipelines;

public interface IPipeEnd<B>
{
    void Run(B nextBuffer, IPipeContext context);
}

public interface IStreamPipeEnd : IPipeEnd<Pipe> { }

public interface IEnumerablePipeEnd<T> : IPipeEnd<BlockingCollection<T>> { }

public delegate void PipeEnd<B>(B pipe, IPipeContext context);

public delegate (Stream stream, Boolean leaveOpen) StreamOpener();

public delegate void TerminalPipeEndWorker<B>(B sourceOrSink, IPipeContext context);

public delegate void PipeWorker<SB, TB>(SB source, TB sink, IPipeContext context);

public delegate void StreamWorker(Stream source, Stream sink, IPipeContext context);

public delegate Stream StreamTransformation(Stream nested);

public class DelegatePipeEnd<B> : IPipeEnd<B>
{
    private readonly PipeEnd<B> implementation;

    public DelegatePipeEnd(PipeEnd<B> implementation)
        => this.implementation = implementation;

    public void Run(B buffer, IPipeContext context)
        => implementation(buffer, context);
}

public class DelegateEnumerablePipeEnd<T> : IEnumerablePipeEnd<T>
{
    private readonly PipeEnd<BlockingCollection<T>> implementation;

    public DelegateEnumerablePipeEnd(PipeEnd<BlockingCollection<T>> implementation)
        => this.implementation = implementation;

    public void Run(BlockingCollection<T> buffer, IPipeContext context)
        => implementation(buffer, context);
}

public class DelegateStreamPipeEnd : IStreamPipeEnd
{
    private readonly PipeEnd<Pipe> implementation;

    public DelegateStreamPipeEnd(PipeEnd<Pipe> implementation)
        => this.implementation = implementation;

    public void Run(Pipe buffer, IPipeContext context)
        => implementation(buffer, context);
}

public class DelegateNestedPipeEnd<NB, B> : IPipeEnd<B>
    where NB : class, new()
{
    private readonly IPipeEnd<NB> nestedPipeEnd;
    private readonly String name;
    private readonly PipeWorker<NB, B> forward;
    private readonly PipeWorker<B, NB> backward;

    public DelegateNestedPipeEnd(IPipeEnd<NB> nestedPipeEnd, String name, PipeWorker<NB, B> forward, PipeWorker<B, NB> backward)
    {
        this.nestedPipeEnd = nestedPipeEnd;
        this.name = name;
        this.forward = forward;
        this.backward = backward;
    }

    public void Run(B nextBuffer, IPipeContext context)
    {
        var buffer = Buffers.MakeBuffer<NB>();

        nestedPipeEnd.Run(buffer, context);

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

public static partial class PipeEnds
{
    public static IPipeEnd<B> Create<B>(String name, TerminalPipeEndWorker<B> producer, TerminalPipeEndWorker<B> consumer)
        => new DelegatePipeEnd<B>((nextBuffer, context) =>
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

    public static IEnumerablePipeEnd<T> CreateEnumerable<T>(PipeEnd<BlockingCollection<T>> implementation)
        => new DelegateEnumerablePipeEnd<T>(implementation);

    public static IEnumerablePipeEnd<T> CreateEnumerable<OB, T>(IPipeEnd<OB> sourcePipeEnd, String name, PipeWorker<OB, BlockingCollection<T>> forward, PipeWorker<BlockingCollection<T>, OB> backward)
        where OB : class, new()
        => new DelegateNestedPipeEnd<OB, BlockingCollection<T>>(sourcePipeEnd, name, forward, backward).AsSpecificPipeEnd();

    public static IEnumerablePipeEnd<T> CreateEnumerable<T>(String name, TerminalPipeEndWorker<BlockingCollection<T>> producer, TerminalPipeEndWorker<BlockingCollection<T>> consumer)
        => Create(name, producer, consumer).AsSpecificPipeEnd();

    public static IStreamPipeEnd CreateStream(PipeEnd<Pipe> implementation)
        => new DelegateStreamPipeEnd(implementation);

    public static IStreamPipeEnd CreateStream<OB>(IPipeEnd<OB> sourcePipeEnd, String name, PipeWorker<OB, Pipe> forward, PipeWorker<Pipe, OB> backward)
        where OB : class, new()
        => new DelegateNestedPipeEnd<OB, Pipe>(sourcePipeEnd, name, forward, backward).AsSpecificPipeEnd();

    public static IStreamPipeEnd CreateStream(IStreamPipeEnd sourcePipeEnd, String name, StreamWorker forward, StreamWorker backward)
        => CreateStream(
            sourcePipeEnd,
            name,
            (source, sink, context) => forward(source.Reader.AsStream(), sink.Writer.AsStream(), context),
            (source, sink, context) => backward(source.Reader.AsStream(), sink.Writer.AsStream(), context));

    public static IStreamPipeEnd CreateStream(String name, TerminalPipeEndWorker<Pipe> producer, TerminalPipeEndWorker<Pipe> consumer)
        => Create(name, producer, consumer).AsSpecificPipeEnd();

    static TerminalPipeEndWorker<Pipe> MakeStreamProducerWorker(this StreamOpener openSource)
        => (pipe, context) => context.ScheduleSync("reading", progress =>
        {
            var (source, leaveOpen) = openSource();

            if (source.Length > 0)
            {
                progress.ReportTotal(source.Length);
            }

            try
            {
                source.CopyTo(pipe.Writer.AsStream());

                pipe.Writer.Complete();
            }
            finally
            {
                if (!leaveOpen)
                {
                    source.Close();
                }
            }
        });

    static TerminalPipeEndWorker<Pipe> MakeStreamConsumerWorker(this StreamOpener openSink)
        => (pipe, context) => context.ScheduleSync("writing", () =>
        {
            var (sink, leaveOpen) = openSink();

            try
            {
                pipe.Reader.AsStream().CopyTo(sink);

                sink.Flush();
            }
            finally
            {
                if (!leaveOpen)
                {
                    sink.Close();
                }
            }
        });

    public static IStreamPipeEnd CreateStream(String name, StreamOpener producer, StreamOpener consumer)
        => CreateStream(
            name,
            producer?.Apply(MakeStreamProducerWorker),
            consumer?.Apply(MakeStreamConsumerWorker)
        );

    public enum ExistingFileWritingBehavior
    {
        Throw,
        Truncate,
        OverwriteIncrementally,
        Append
    }

    public static IStreamPipeEnd FromFile(String fileName, ExistingFileWritingBehavior existingFileWritingBehavior = ExistingFileWritingBehavior.Truncate)
    {
        var fileInfo = new FileInfo(fileName);

        (Stream, Boolean) OpenRead()
        {
            return (fileInfo.OpenRead(), false);
        }

        (Stream, Boolean) OpenWrite()
        {
            var stream = fileInfo.OpenWrite();

            if (stream.Length > 0)
            {
                switch (existingFileWritingBehavior)
                {
                    case ExistingFileWritingBehavior.Throw:
                        stream.Close();
                        throw new Exception($"File {fileName} already exists");
                    case ExistingFileWritingBehavior.Truncate:
                        stream.SetLength(0);
                        break;
                    case ExistingFileWritingBehavior.OverwriteIncrementally:
                        break;
                    case ExistingFileWritingBehavior.Append:
                        stream.Seek(0, SeekOrigin.End);
                        break;
                    default:
                        throw new Exception($"Unexpected existing file writing behavior {existingFileWritingBehavior}");
                }
            }

            return (stream, false);
        }

        return CreateStream(nameof(FromFile), OpenRead, OpenWrite);
    }

    public static IEnumerablePipeEnd<T> FromEnumerable<T>(IEnumerable<T> source)
        => CreateEnumerable<T>(nameof(FromQueryable), (nextBuffer, context) =>
    {
        context.ScheduleSync("enumerating", progress =>
        {
            if (source.TryGetNonEnumeratedCount(out var length))
            {
                progress.ReportTotal(length);
            }

            var processed = 0L;

            foreach (var item in source)
            {
                nextBuffer.Add(item);

                progress.ReportProcessed(++processed);
            }

            nextBuffer.CompleteAdding();
        });
    }, null);

    public static IEnumerablePipeEnd<T> FromQueryable<T>(IQueryable<T> source, Boolean dontQueryCount = false)
        => CreateEnumerable<T>(nameof(FromQueryable), (nextBuffer, context) =>
    {
        context.ScheduleSync("querying", progress =>
        {
            if (!dontQueryCount)
            {
                var length = source.LongCount();

                progress.ReportTotal(length);
            }

            var processed = 0L;

            foreach (var item in source)
            {
                nextBuffer.Add(item);

                progress.ReportProcessed(++processed);
            }

            nextBuffer.CompleteAdding();
        });
    }, null);

    public static IEnumerablePipeEnd<T> FromAction<T>(Action<T> action, String name = null)
        => CreateEnumerable<T>(name ?? nameof(FromAction), null, (nextBuffer, context) => context.ScheduleSync("calling", () =>
        {
            foreach (var item in nextBuffer.GetConsumingEnumerable())
            {
                action(item);
            }
        })
    );

    public static IEnumerablePipeEnd<T> FromAsyncAction<T>(Func<T, CancellationToken, Task> action, String name = null)
        => CreateEnumerable<T>(name ?? nameof(FromAsyncAction), null, (nextBuffer, context) => context.ScheduleAsync("calling", async ct =>
        {
            foreach (var item in nextBuffer.GetConsumingEnumerable(ct))
            {
                await action(item, ct);
            }
        })
    );

    public static IEnumerablePipeEnd<T> EnumerableBlackhole<T>()
        => FromAction<T>(_ => { }, nameof(EnumerableBlackhole));

    public static IPipeline BuildCopyingPipeline<B>(this IPipeEnd<B> source, IPipeEnd<B> sink)
        where B : class, new()
    {
        return new Pipeline<B>(source, sink);
    }

    public static LivePipeline StartCopyingPipeline<B>(this IPipeEnd<B> source, IPipeEnd<B> sink)
        where B : class, new()
    {
        var pipeline = source.BuildCopyingPipeline(sink);

        return pipeline.Run();
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

    public static async Task WriteAllAsync<T>(this IEnumerablePipeEnd<T> sink, IEnumerable<T> items)
    {
        var source = FromEnumerable(items);

        await source.CopyToAsync(sink);
    }

    public static void WriteAll<T>(this IEnumerablePipeEnd<T> sink, IEnumerable<T> items)
        => sink.WriteAllAsync(items).Wait();

    public static void Wait(this LivePipeline pipeline) => pipeline.Task.Wait();

    public static Task WaitAsync(this LivePipeline pipeline) => pipeline.Task;
}
