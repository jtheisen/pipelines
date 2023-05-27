using ICSharpCode.SharpZipLib.BZip2;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Xml;
using System.Xml.Serialization;

namespace TplPlay.Pipes;


public record PipePart;

public record PipeBufferPart(Func<Int64> GetBufferSize) : PipePart;

public record PipeWorkerPart(String Name) : PipePart;

public interface IPipeContext
{
    void AddPart(PipePart part);
}

public class PipeContext : IPipeContext
{
    List<PipePart> parts;

    public IEnumerable<PipePart> Parts => parts;

    public void AddPart(PipePart part) => parts.Add(part);
}

public interface IStreamPipeEnd
{
    Task Suck(PipeWriter writer, IPipeContext context);
    Task Blow(PipeReader reader, IPipeContext context);
}

public interface IEnumerablePipeEnd<T>
{
    Task Suck(BlockingCollection<T> sink, IPipeContext context);
    Task Blow(BlockingCollection<T> source, IPipeContext context);
}

public class BlackholePipeEnd<T>
{
    private readonly IEnumerablePipeEnd<T> source;

    public BlackholePipeEnd(IEnumerablePipeEnd<T> source)
    {
        this.source = source;
    }

    public Task Blow() => throw new NotImplementedException();

    public Task Suck()
    {
        var concurrentBlackhole = new ConcurrentBlackhole<T>();

        var blockingBlackhole = new BlockingCollection<T>(concurrentBlackhole);

        return source.Suck(blockingBlackhole);
    }
}

public class FilePipeEnd : IStreamPipeEnd
{
    private readonly FileInfo fileInfo;

    public FilePipeEnd(String fileName)
    {
        fileInfo = new FileInfo(fileName);
    }

    public async Task Suck(PipeWriter writer, IPipeContext context)
    {
        using var stream = fileInfo.OpenRead();

        await stream.CopyToAsync(writer);
    }

    public async Task Blow(PipeReader reader, IPipeContext context)
    {
        using var stream = fileInfo.OpenWrite();

        await reader.CopyToAsync(stream);
    }
}

public class ZipPipeEnd : IStreamPipeEnd
{
    private readonly IStreamPipeEnd sourcePipeEnd;

    public ZipPipeEnd(IStreamPipeEnd sourcePipeEnd)
    {
        this.sourcePipeEnd = sourcePipeEnd;
    }

    public Task Suck(PipeWriter writer, IPipeContext context)
    {
        var pipe = new Pipe();

        var sourceTask = sourcePipeEnd.Suck(pipe.Writer, context);

        context.AddPart(pipe.ToPart());

        context.AddPart(new PipeWorkerPart("decompressing"));

        var stream = new BZip2InputStream(pipe.Reader.AsStream());

        return Task.WhenAll(sourceTask, stream.CopyToAsync(writer));
    }

    public Task Blow(PipeReader reader, IPipeContext context)
    {
        var pipe = new Pipe();

        var sourceTask = sourcePipeEnd.Blow(pipe.Reader, context);

        context.AddPart(pipe.ToPart());

        context.AddPart(new PipeWorkerPart("compressing"));

        var stream = new BZip2OutputStream(pipe.Writer.AsStream());

        return Task.WhenAll(sourceTask, reader.CopyToAsync(stream));
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

    public Task Suck(BlockingCollection<T> sink, IPipeContext context)
    {
        var pipe = new Pipe();

        var suckTask = sourcePipeEnd.Suck(pipe.Writer, context);

        var task = Task.Run(() => SuckSync(pipe.Reader, sink));

        return Task.WhenAll(suckTask, task);
    }

    void SuckSync(PipeReader pipeReader, BlockingCollection<T> sink)
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

    public Task Blow(BlockingCollection<T> source, IPipeContext context)
    {
        throw new NotImplementedException();
    }
}

public class TransformEnumerablePipeEnd<S, T> : IEnumerablePipeEnd<T>
{
    private readonly IEnumerablePipeEnd<S> sourcePipeEnd;
    private readonly Func<S, T> map;
    private readonly Func<T, S> reverseMap;

    public TransformEnumerablePipeEnd(IEnumerablePipeEnd<S> sourcePipeEnd, Func<S, T> map, Func<T, S> reverseMap = null)
    {
        this.sourcePipeEnd = sourcePipeEnd;
        this.map = map;
        this.reverseMap = reverseMap;
    }

    public Task Suck(BlockingCollection<T> sink, IPipeContext context)
    {
        var buffer = new BlockingCollection<S>();

        var sourceTask = sourcePipeEnd.Suck(buffer, context);

        context.AddPart(buffer.ToPart());

        var task = Task.Run(() => Transform(buffer, sink, map));

        return Task.WhenAll(sourceTask, task);
    }

    public Task Blow(BlockingCollection<T> source, IPipeContext context)
    {
        var buffer = new BlockingCollection<S>();

        var sourceTask = sourcePipeEnd.Blow(buffer, context);

        context.AddPart(buffer.ToPart());

        var task = Task.Run(() => Transform(source, buffer, reverseMap));

        return Task.WhenAll(sourceTask, task);
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

public static class Pipes
{
    public static IStreamPipeEnd File(String fileName) => new FilePipeEnd(fileName);

    public static IStreamPipeEnd Zip(this IStreamPipeEnd source) => new ZipPipeEnd(source);

    public static IEnumerablePipeEnd<T> ParseXml<T>(this IStreamPipeEnd source) => new ParserPipeEnd<T>(source);

    public static IEnumerablePipeEnd<T> Transform<S, T>(this IEnumerablePipeEnd<S> source, Func<S, T> map, Func<T, S> reverseMap = null)
        => new TransformEnumerablePipeEnd<S, T>(source, map, reverseMap);

    public static IEnumerablePipeEnd<T> Do<T>(this IEnumerablePipeEnd<T> source, Action<T> action)
    {
        Func<T, T> map = t =>
        {
            action(t);

            return t;
        };

        return new TransformEnumerablePipeEnd<T, T>(source, map, map);
    }

    public static async Task CopyTo(this IStreamPipeEnd source, IStreamPipeEnd sink)
    {
        var pipe = new Pipe();

        var suckingContext = new PipeContext();
        var blowingContext = new PipeContext();

        var suckingTask = source.Suck(pipe.Writer, suckingContext);
        var blowingTask = sink.Blow(pipe.Reader, blowingContext);

        await Task.WhenAll(suckingTask, blowingTask);
    }

    public static async Task CopyToAsync<T>(this IEnumerablePipeEnd<T> source, IEnumerablePipeEnd<T> sink)
    {
        var collection = new BlockingCollection<T>();

        var suckingContext = new PipeContext();
        var blowingContext = new PipeContext();

        var suckingTask = source.Suck(collection, suckingContext);
        var blowingTask = sink.Blow(collection, blowingContext);

        await Task.WhenAll(suckingTask, blowingTask);
    }

    public static async Task ForeachAsync<T>(this IEnumerablePipeEnd<T> source, Action<T> action = null)
    {
        if (action is not null)
        {
            source = source.Do(action);
        }

        var blackhole = new BlackholePipeEnd<T>(source);

        await blackhole.Suck();
    }

    static void SuckSync<T>(BlockingCollection<T> sink)
    {
        while (!sink.IsCompleted) sink.Take();
    }


}
