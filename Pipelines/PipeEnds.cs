using ICSharpCode.SharpZipLib.BZip2;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;
using System.Xml;
using System.Xml.Serialization;

namespace Pipelines;

public interface IPipeEnd<B>
{
    void Run(B nextBuffer, IPipeContext context);
}

public interface IStreamPipeEnd : IPipeEnd<Pipe> { }

public interface IEnumerablePipeEnd<T> : IPipeEnd<BlockingCollection<T>> { }

public delegate void PipeEnd<B>(B pipe, IPipeContext context);

public class DelegateEnumerablePipeEnd<T> : IEnumerablePipeEnd<T>
{
    private readonly PipeEnd<BlockingCollection<T>> implementation;

    public DelegateEnumerablePipeEnd(PipeEnd<BlockingCollection<T>> implementation)
        => this.implementation = implementation;

    public void Run(BlockingCollection<T> nextBuffer, IPipeContext context)
        => implementation(nextBuffer, context);
}

public class DelegateStreamPipeEnd : IStreamPipeEnd
{
    private readonly PipeEnd<Pipe> implementation;

    public DelegateStreamPipeEnd(PipeEnd<Pipe> implementation)
        => this.implementation = implementation;

    public void Run(Pipe nextBuffer, IPipeContext context)
        => implementation(nextBuffer, context);
}

public class ZipPipeEnd : IStreamPipeEnd
{
    private readonly IStreamPipeEnd nestedPipeEnd;

    public ZipPipeEnd(IStreamPipeEnd sourcePipeEnd)
    {
        this.nestedPipeEnd = sourcePipeEnd;
    }

    public void Run(Pipe nextPipe, IPipeContext context)
    {
        var pipe = Buffers.MakeBuffer<Pipe>();

        nestedPipeEnd.Run(pipe, context);

        context.AddBuffer(pipe);

        context.AddWorker("zip");

        switch (context.Mode)
        {
            case PipeRunMode.Probe:
                break;
            case PipeRunMode.Suck:
                {
                    var stream = new BZip2InputStream(pipe.Reader.AsStream());

                    context.ScheduleAsync("decrompressing", ct => stream.CopyToAsync(nextPipe.Writer, ct));
                }
                break;
            case PipeRunMode.Blow:
                {
                    var stream = new BZip2OutputStream(pipe.Writer.AsStream());

                    context.ScheduleAsync("compressing", ct => nextPipe.Reader.CopyToAsync(stream, ct));
                }
                break;
            default:
                break;
        }
    }
}

public class ParserPipeEnd<T> : IEnumerablePipeEnd<T>
{
    private readonly IStreamPipeEnd sourcePipeEnd;

    XmlSerializer serializer = new XmlSerializer(typeof(T));

    public ParserPipeEnd(IStreamPipeEnd sourcePipeEnd)
    {
        this.sourcePipeEnd = sourcePipeEnd;
    }

    public void Run(BlockingCollection<T> buffer, IPipeContext context)
    {
        var pipe = Buffers.MakeBuffer<Pipe>();

        sourcePipeEnd.Run(pipe, context);

        context.AddBuffer(pipe);

        context.AddWorker("xml");

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                context.Schedule("parsing", () => Parse(pipe.Reader, buffer));

                break;
            case PipeRunMode.Blow:
                throw new NotImplementedException();

            default:
                break;
        }
    }

    void Parse(PipeReader pipeReader, BlockingCollection<T> sink)
    {
        var reader = XmlReader.Create(pipeReader.AsStream());

        reader.MoveToContent();

        while (reader.Read())
        {
            switch (reader.NodeType)
            {
                case XmlNodeType.Element:
                    if (reader.LocalName == "page")
                    {
                        var page = (T)serializer.Deserialize(reader);

                        sink.Add(page);
                    }
                    break;
            }
        }

        sink.CompleteAdding();
    }
}

public class QueryablePipeEnd<T> : IEnumerablePipeEnd<T>
{
    private readonly IQueryable<T> source;

    public QueryablePipeEnd(IQueryable<T> source)
    {
        this.source = source;
    }

    public void Run(BlockingCollection<T> nextBuffer, IPipeContext context)
    {
        context.AddWorker("queryable");

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                context.Schedule("ingesting", progress =>
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
                break;
            case PipeRunMode.Blow:
                throw new NotImplementedException($"Blowing into a {nameof(QueryablePipeEnd<T>)} is not supported");
            default:
                break;
        }
    }
}

public class ActionPipeEnd<T> : IEnumerablePipeEnd<T>
{
    private readonly Action<T> action;

    public ActionPipeEnd(Action<T> action)
    {
        this.action = action;
    }

    public void Run(BlockingCollection<T> nextBuffer, IPipeContext context)
    {
        context.AddWorker("action");

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                throw new NotImplementedException($"Sucking from a {nameof(ActionPipeEnd<T>)} is not supported");
            case PipeRunMode.Blow:
                context.Schedule("calling", () =>
                {
                    foreach (var item in nextBuffer.GetConsumingEnumerable())
                    {
                        action(item);
                    }
                });
                break;
            default:
                break;
        }
    }
}

public class AsyncActionPipeEnd<T> : IEnumerablePipeEnd<T>
{
    private readonly Func<T, Task> action;

    public AsyncActionPipeEnd(Func<T, Task> action)
    {
        this.action = action;
    }

    public void Run(BlockingCollection<T> nextBuffer, IPipeContext context)
    {
        context.AddWorker("action");

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                throw new NotImplementedException($"Sucking from a {nameof(ActionPipeEnd<T>)} is not supported");
            case PipeRunMode.Blow:
                context.Schedule("calling", async () =>
                {
                    foreach (var item in nextBuffer.GetConsumingEnumerable())
                    {
                        await action(item);
                    }
                });
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
                throw new Exception($"This worker does not support blowing");
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

public static class PipeEnds
{
    public static IEnumerablePipeEnd<T> Enumerable<T>(PipeEnd<BlockingCollection<T>> implementation)
        => new DelegateEnumerablePipeEnd<T>(implementation);

    public static IStreamPipeEnd Stream(PipeEnd<Pipe> implementation)
        => new DelegateStreamPipeEnd(implementation);

    public static IStreamPipeEnd File(String fileName) => Stream((pipe, context) =>
    {
        var fileInfo = new FileInfo(fileName);

        context.AddWorker("file");

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                context.ScheduleAsync("reading", async ct =>
                {
                    using var stream = fileInfo.OpenRead();

                    await stream.CopyToAsync(pipe.Writer);

                    pipe.Writer.Complete();
                });

                break;
            case PipeRunMode.Blow:
                context.ScheduleAsync("writing", async ct =>
                {
                    using var stream = fileInfo.OpenWrite();

                    await pipe.Reader.CopyToAsync(stream);
                });

                break;
            default:
                break;
        }
    });

    public static IStreamPipeEnd Zip(this IStreamPipeEnd source) => new ZipPipeEnd(source);

    public static IEnumerablePipeEnd<T> FromQueryable<T>(this IQueryable<T> source)
        => new QueryablePipeEnd<T>(source);

    public static IEnumerablePipeEnd<T> FromAction<T>(Action<T> source)
        => new ActionPipeEnd<T>(source);

    public static IEnumerablePipeEnd<T> FromAsyncAction<T>(Func<T, Task> source)
        => new AsyncActionPipeEnd<T>(source);

    public static IEnumerablePipeEnd<T> ParseXml<T>(this IStreamPipeEnd source) => new ParserPipeEnd<T>(source);

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
