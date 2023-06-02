using ICSharpCode.SharpZipLib.BZip2;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Reflection.PortableExecutable;
using System.Xml;
using System.Xml.Serialization;

namespace TplPlay.Pipes;

public interface IPipeEnd<B>
{
    void Run(B nextBuffer, IPipeContext context);
}

public interface IWorker
{
    public String Name { get; }
}

public interface IStreamPipeEnd : IPipeEnd<Pipe> { }

public interface IEnumerablePipeEnd<T> : IPipeEnd<BlockingCollection<T>> { }

public class FilePipeEnd : IStreamPipeEnd, IWorker
{
    private readonly FileInfo fileInfo;

    public String Name => "file";

    public FilePipeEnd(String fileName)
    {
        fileInfo = new FileInfo(fileName);
    }

    public void Run(Pipe pipe, IPipeContext context)
    {
        context.AddWorker(this);

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                context.SetTask("reading", Suck(pipe));

                break;
            case PipeRunMode.Blow:
                context.SetTask("writing", Blow(pipe));

                break;
            default:
                break;
        }
    }

    async Task Suck(Pipe pipe)
    {
        using var stream = fileInfo.OpenRead();

        await stream.CopyToAsync(pipe.Writer);
    }

    async Task Blow(Pipe pipe)
    {
        using var stream = fileInfo.OpenWrite();

        await pipe.Reader.CopyToAsync(stream);
    }
}

public class ZipPipeEnd : IStreamPipeEnd, IWorker
{
    private readonly IStreamPipeEnd nestedPipeEnd;

    public ZipPipeEnd(IStreamPipeEnd sourcePipeEnd)
    {
        this.nestedPipeEnd = sourcePipeEnd;
    }

    public String Name => "zip";

    public void Run(Pipe nextPipe, IPipeContext context)
    {
        var pipe = new Pipe();

        nestedPipeEnd.Run(pipe, context);

        context.AddBuffer(pipe);

        context.AddWorker(this);

        switch (context.Mode)
        {
            case PipeRunMode.Probe:
                break;
            case PipeRunMode.Suck:
                {
                    var stream = new BZip2InputStream(pipe.Reader.AsStream());

                    context.SetTask("decrompressing", stream.CopyToAsync(nextPipe.Writer));
                }
                break;
            case PipeRunMode.Blow:
                {
                    var stream = new BZip2OutputStream(pipe.Writer.AsStream());

                    context.SetTask("compressing", nextPipe.Reader.CopyToAsync(stream));
                }
                break;
            default:
                break;
        }
    }
}

public class ParserPipeEnd<T> : IEnumerablePipeEnd<T>, IWorker
{
    private readonly IStreamPipeEnd sourcePipeEnd;

    XmlSerializer serializer = new XmlSerializer(typeof(T));

    public ParserPipeEnd(IStreamPipeEnd sourcePipeEnd)
    {
        this.sourcePipeEnd = sourcePipeEnd;
    }

    public String Name => "xml";

    public void Run(BlockingCollection<T> buffer, IPipeContext context)
    {
        var pipe = new Pipe();

        sourcePipeEnd.Run(pipe, context);

        context.AddBuffer(pipe);

        context.AddWorker(this);

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

public class TransformEnumerablePipeEnd<S, T> : IEnumerablePipeEnd<T>, IWorker
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

    public String Name => "transform";

    public void Run(BlockingCollection<T> nextBuffer, IPipeContext context)
    {
        var buffer = new BlockingCollection<S>();

        nestedPipeEnd.Run(buffer, context);

        context.AddBuffer(buffer);

        context.AddWorker(this);

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                context.Schedule("transforming", () => Transform(buffer, nextBuffer, map));
                break;
            case PipeRunMode.Blow:
                context.Schedule("transforming", () => Transform(nextBuffer, buffer, reverseMap));
                break;
            default:
                break;
        }
    }

    void Transform<S2, T2>(BlockingCollection<S2> buffer, BlockingCollection<T2> sink, Func<S2, T2> map)
    {
        while (!buffer.IsCompleted)
        {
            var item = buffer.Take();

            sink.Add(map(item));
        }

        sink.CompleteAdding();
    }
}

public class AsyncTransformEnumerablePipeEnd<S, T> : IEnumerablePipeEnd<T>, IWorker
{
    private readonly IEnumerablePipeEnd<S> nestedPipeEnd;
    private readonly Func<S, Task<T>> map;
    private readonly Func<T, Task<S>> reverseMap;

    public AsyncTransformEnumerablePipeEnd(IEnumerablePipeEnd<S> nestedPipeEnd, Func<S, Task<T>> map, Func<T, Task<S>> reverseMap = null)
    {
        this.nestedPipeEnd = nestedPipeEnd;
        this.map = map;
        this.reverseMap = reverseMap;
    }

    public String Name => "transform";

    public void Run(BlockingCollection<T> nextBuffer, IPipeContext context)
    {
        var buffer = new BlockingCollection<S>();

        nestedPipeEnd.Run(buffer, context);

        context.AddBuffer(buffer);

        context.AddWorker(this);

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                context.Schedule("transforming", () => Transform(buffer, nextBuffer, map));
                break;
            case PipeRunMode.Blow:
                context.Schedule("transforming", () => Transform(nextBuffer, buffer, reverseMap));
                break;
            default:
                break;
        }
    }

    async Task Transform<S2, T2>(BlockingCollection<S2> buffer, BlockingCollection<T2> sink, Func<S2, Task<T2>> map)
    {
        while (!buffer.IsCompleted)
        {
            var item = buffer.Take();

            var transformed = await map(item);

            sink.Add(transformed);
        }

        sink.CompleteAdding();
    }
}

public static class Pipes
{
    public static IStreamPipeEnd File(String fileName) => new FilePipeEnd(fileName);

    public static IStreamPipeEnd Zip(this IStreamPipeEnd source) => new ZipPipeEnd(source);

    public static IEnumerablePipeEnd<T> ParseXml<T>(this IStreamPipeEnd source) => new ParserPipeEnd<T>(source);

    public static IEnumerablePipeEnd<T> Transform<S, T>(this IEnumerablePipeEnd<S> source, Func<S, T> map, Func<T, S> reverseMap = null)
        => new TransformEnumerablePipeEnd<S, T>(source, map, reverseMap);

    public static IEnumerablePipeEnd<T> Transform<S, T>(this IEnumerablePipeEnd<S> source, Func<S, Task<T>> map, Func<T, Task<S>> reverseMap = null)
        => new AsyncTransformEnumerablePipeEnd<S, T>(source, map, reverseMap);

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

    //public static async Task ForeachAsync<T>(this IEnumerablePipeEnd<T> source, Action<T> action = null)
    //{
    //    if (action is not null)
    //    {
    //        source = source.Do(action);
    //    }

    //    var context = new PipeContext(PipeRunMode.Suck);

    //    var concurrentBlackhole = new ConcurrentBlackhole<T>();

    //    var blockingBlackhole = new BlockingCollection<T>(concurrentBlackhole);

    //    source.Run(blockingBlackhole, context);



    //    await Task.WhenAll(context.Tasks);
    //}
}
