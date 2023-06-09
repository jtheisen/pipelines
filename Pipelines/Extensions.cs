namespace Pipelines;

public static class StaticHelpers
{
    public static Exception GetBlowingNotSupported(String name) => new Exception($"Blowing into {name} is not supported");
    public static Exception GetSuckingNotSupported(String name) => new Exception($"Sucking from {name} is not supported");
}

public static partial class Extensions
{
    public static IEnumerablePipeEnd<T> Itemize<T, C>(
        this IStreamPipeEnd sourcePipeEnd,
        String name,
        C config,
        Action<TextReader, Action<T>, C> parse,
        Action<TextWriter, IEnumerable<T>, C> serialize
    )
        => PipeEnds.Enumerable<T>((nextBuffer, context) =>
    {
        var pipe = Buffers.MakeBuffer<Pipe>();

        sourcePipeEnd.Run(pipe, context);

        context.AddBuffer(pipe);

        context.AddWorker(name);

        switch (context.Mode)
        {
            case PipeRunMode.Suck:
                context.Schedule("parsing", () =>
                {
                    var textReader = new StreamReader(pipe.Reader.AsStream());

                    parse(textReader, nextBuffer.Add, config);

                    nextBuffer.CompleteAdding();
                });

                break;
            case PipeRunMode.Blow:
                context.Schedule("serializing", () =>
                {
                    var textWriter = new StreamWriter(pipe.Writer.AsStream(), Encoding.UTF8);

                    serialize(textWriter, nextBuffer.GetConsumingEnumerable(), config);

                    textWriter.Flush();

                    pipe.Writer.Complete();
                });
                break;
            default:
                break;
        }
    });

    public static IEnumerablePipeEnd<T> Itemize<T, C>(
        this IStreamPipeEnd sourcePipeEnd,
        String name,
        C config,
        Action<Stream, Action<T>, C> parse,
        Action<Stream, IEnumerable<T>, C> serialize
    )
        => PipeEnds.Enumerable<T>((nextBuffer, context) =>
        {
            var pipe = Buffers.MakeBuffer<Pipe>();

            sourcePipeEnd.Run(pipe, context);

            context.AddBuffer(pipe);

            context.AddWorker(name);

            switch (context.Mode)
            {
                case PipeRunMode.Suck:
                    context.Schedule("parsing", () =>
                    {
                        var stream = pipe.Reader.AsStream();

                        parse(stream, nextBuffer.Add, config);

                        nextBuffer.CompleteAdding();
                    });

                    break;
                case PipeRunMode.Blow:
                    context.Schedule("serializing", () =>
                    {
                        var stream = pipe.Writer.AsStream();

                        serialize(stream, nextBuffer.GetConsumingEnumerable(), config);

                        stream.Flush();

                        pipe.Writer.Complete();
                    });
                    break;
                default:
                    break;
            }
        });
}
