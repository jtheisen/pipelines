using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;

namespace Pipelines
{
    internal static class StaticHelpers
    {
        public static Exception GetBlowingNotSupported(String name) => new Exception($"Blowing into {name} is not supported");
        public static Exception GetSuckingNotSupported(String name) => new Exception($"Sucking from {name} is not supported");
    }

    public static partial class Extensions
    {
        public static IStreamPipeEnd WrapStream(this IStreamPipeEnd sourcePipeEnd, String name, StreamTransformation forward, StreamTransformation backward)
            => PipeEnds.CreateStream(sourcePipeEnd, name,
                (source, sink, context) => context.ScheduleAsync("", ct => forward(source.Reader.AsStream()).CopyToAsync(sink.Writer.AsStream(), ct)),
                (source, sink, context) => context.ScheduleAsync("", ct => backward(source.Reader.AsStream()).CopyToAsync(sink.Writer.AsStream(), ct)));

        public static IEnumerablePipeEnd<T> AsSpecificPipeEnd<T>(this IPipeEnd<BlockingCollection<T>> source)
            => new DelegateEnumerablePipeEnd<T>(source.Run);

        public static IStreamPipeEnd AsSpecificPipeEnd(this IPipeEnd<Pipe> source)
            => new DelegateStreamPipeEnd(source.Run);

        public static IEnumerablePipeEnd<T> Itemize<T, C>(
            this IStreamPipeEnd sourcePipeEnd,
            String name,
            C config,
            Action<TextReader, Action<T>, C> parse,
            Action<TextWriter, IEnumerable<T>, C> serialize
        ) => PipeEnds.CreateEnumerable(
            sourcePipeEnd,
            name,
            parse?.Apply(p => CreateParser<T, C>((pipe, sink, c) => p(new StreamReader(pipe.Reader.AsStream()), sink, c), config)),
            serialize?.Apply(s => CreateSerializer<T, C>((pipe, source, c) => s(new StreamWriter(pipe.Writer.AsStream()), source, c), config))
        );

        public static IEnumerablePipeEnd<T> Itemize<T, C>(
            this IStreamPipeEnd sourcePipeEnd,
            String name,
            C config,
            Action<Stream, Action<T>, C> parse,
            Action<Stream, IEnumerable<T>, C> serialize
        ) => PipeEnds.CreateEnumerable(
            sourcePipeEnd,
            name,
            parse?.Apply(p => CreateParser<T, C>((pipe, sink, c) => p(pipe.Reader.AsStream(), sink, c), config)),
            serialize?.Apply(s => CreateSerializer<T, C>((pipe, source, c) => s(pipe.Writer.AsStream(), source, c), config))
        );

        static PipeWorker<Pipe, BlockingCollection<T>> CreateParser<T, C>(Action<Pipe, Action<T>, C> parse, C config)
        {
            void Run(Pipe source, BlockingCollection<T> sink, IPipeContext context)
            {
                void Parse()
                {
                    parse(source, sink.Add, config);

                    sink.CompleteAdding();
                }

                context.ScheduleSync("parse", Parse);
            }

            return Run;
        }

        static PipeWorker<BlockingCollection<T>, Pipe> CreateSerializer<T, C>(Action<Pipe, IEnumerable<T>, C> serialize, C config)
        {
            void Run(BlockingCollection<T> source, Pipe sink, IPipeContext context)
            {
                void Serialize()
                {
                    serialize(sink, source.GetConsumingEnumerable(), config);

                    sink.Writer.Complete();
                }

                context.ScheduleSync("parse", Serialize);
            }

            return Run;
        }

        public static void CopyTo(this Stream source, Stream sink, Action step, Int32 bufferSize = 81920)
        {
            var buffer = ArrayPool<Byte>.Shared.Rent(bufferSize);

            try
            {
                Int32 count;

                while ((count = source.Read(buffer, 0, buffer.Length)) != 0)
                {
                    sink.Write(buffer, 0, count);

                    step();
                }
            }
            finally
            {
                ArrayPool<Byte>.Shared.Return(buffer);
            }
        }
    }
}
