using ICSharpCode.SharpZipLib.BZip2;
using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Xml;
using System.Xml.Serialization;

namespace TplPlay.Pipes;

public interface IStreamPipeEnd
{
    Task Suck(PipeWriter writer);
    Task Blow(PipeReader reader);
}

public interface IEnumerablePipeEnd<T>
{
    Task Suck(BlockingCollection<T> sink);
    Task Blow(BlockingCollection<T> source);
}

public class ConcurrentBlackhole<T> : IProducerConsumerCollection<T>
{
    public Int32 Count => 0;

    public Boolean IsSynchronized => true;
    public Object SyncRoot => this;

    public void CopyTo(T[] array, Int32 index) { }
    public void CopyTo(Array array, Int32 index) { }

    public T[] ToArray() => new T[0];

    public Boolean TryAdd(T item) => true;
    public Boolean TryTake([MaybeNullWhen(false)] out T item)
    {
        item = default;
        return false;
    }

    public IEnumerator<T> GetEnumerator() => Enumerable.Empty<T>().GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
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

    public async Task Suck(PipeWriter writer)
    {
        using var stream = fileInfo.OpenRead();

        await stream.CopyToAsync(writer);
    }

    public async Task Blow(PipeReader reader)
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

    public Task Suck(PipeWriter writer)
    {
        var pipe = new Pipe();

        sourcePipeEnd.Suck(pipe.Writer);

        var stream = new BZip2InputStream(pipe.Reader.AsStream());

        return stream.CopyToAsync(writer);
    }

    public Task Blow(PipeReader reader)
    {
        var pipe = new Pipe();

        sourcePipeEnd.Blow(pipe.Reader);

        var stream = new BZip2OutputStream(pipe.Writer.AsStream());

        return reader.CopyToAsync(stream);
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

    public Task Suck(BlockingCollection<T> sink)
    {
        var pipe = new Pipe();

        var suckTask = sourcePipeEnd.Suck(pipe.Writer);

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

    public Task Blow(BlockingCollection<T> source)
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

    public Task Suck(BlockingCollection<T> sink)
    {
        var buffer = new BlockingCollection<S>();

        var sourceTask = sourcePipeEnd.Suck(buffer);

        var task = Task.Run(() => SuckSync(buffer, sink));

        return Task.WhenAll(sourceTask, task);
    }

    void SuckSync(BlockingCollection<S> buffer, BlockingCollection<T> sink)
    {
        while (!buffer.IsCompleted)
        {
            var item = buffer.Take();

            sink.Add(map(item));
        }

        sink.CompleteAdding();
    }

    public Task Blow(BlockingCollection<T> source)
    {
        throw new NotImplementedException();
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

        var suckingTask = source.Suck(pipe.Writer);
        var blowingTask = sink.Blow(pipe.Reader);

        await Task.WhenAll(suckingTask, blowingTask);
    }

    public static async Task CopyToAsync<T>(this IEnumerablePipeEnd<T> source, IEnumerablePipeEnd<T> sink)
    {
        var collection = new BlockingCollection<T>();

        var suckingTask = source.Suck(collection);
        var blowingTask = sink.Blow(collection);

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
