namespace Pipelines;

public static partial class Extensions
{
    static PipeWorker<BlockingCollection<S>, BlockingCollection<T>> MakeEnumerableMappingCopier<S, T>(String verb, Func<IEnumerable<S>, IEnumerable<T>> map)
    {
        void Run(BlockingCollection<S> source, BlockingCollection<T> sink, IPipeContext context)
        {
            void Copy()
            {
                var mapped = map(source.GetConsumingEnumerable());

                foreach (var item in mapped)
                {
                    sink.Add(item);
                }
            }

            context.Schedule(verb, Copy);
        }

        return Run;
    }

    public static IEnumerablePipeEnd<T> Transform<S, T>(this IEnumerablePipeEnd<S> sourcePipeEnd, Func<IEnumerable<S>, IEnumerable<T>> map, Func<IEnumerable<T>, IEnumerable<S>> mapReverse)
        => PipeEnds.CreateEnumerable(sourcePipeEnd, nameof(Transform), map?.Apply(m => MakeEnumerableMappingCopier("transforming", m)), mapReverse?.Apply(m => MakeEnumerableMappingCopier("transforming", m)));

    static PipeWorker<BlockingCollection<S>, BlockingCollection<T>> MakeItemMappingCopier<S, T>(String verb, Func<S, T> map)
    {
        void Run(BlockingCollection<S> source, BlockingCollection<T> sink, IPipeContext context)
        {
            void Copy()
            {
                foreach (var item in source.GetConsumingEnumerable())
                {
                    sink.Add(map(item));
                }
            }

            context.Schedule(verb, Copy);
        }

        return Run;
    }

    public static IEnumerablePipeEnd<T> Map<S, T>(this IEnumerablePipeEnd<S> source, Func<S, T> map, Func<T, S> reverseMap = null)
        => PipeEnds.CreateEnumerable(source, "Map", map?.Apply(m => MakeItemMappingCopier("transforming", m)), reverseMap?.Apply(m => MakeItemMappingCopier("transforming", m)));

    static PipeWorker<BlockingCollection<S>, BlockingCollection<T>> MakeItemMappingAsyncCopier<S, T>(String verb, Func<S, CancellationToken, Task<T>> map)
    {
        void Run(BlockingCollection<S> source, BlockingCollection<T> sink, IPipeContext context)
        {
            async Task Copy(CancellationToken ct)
            {
                foreach (var item in source.GetConsumingEnumerable(ct))
                {
                    var mappedItem = await map(item, ct);

                    sink.Add(mappedItem, ct);
                }
            }

            context.ScheduleAsync(verb, Copy);
        }

        return Run;
    }

    public static IEnumerablePipeEnd<T> Map<S, T>(this IEnumerablePipeEnd<S> source, Func<S, CancellationToken, Task<T>> map, Func<T, CancellationToken, Task<S>> reverseMap = null)
        => PipeEnds.CreateEnumerable(source, "Map", map?.Apply(m => MakeItemMappingAsyncCopier("transforming", m)), reverseMap?.Apply(m => MakeItemMappingAsyncCopier("transforming", m)));

    static Func<T, T> MakeDoer<T>(Action<T> action)
    {
        T Map(T value)
        {
            action(value);

            return value;
        }

        return Map;
    }

    public static IEnumerablePipeEnd<T> Do<T>(this IEnumerablePipeEnd<T> source, Action<T> action)
        => source.Map(action?.Apply(MakeDoer), action?.Apply(MakeDoer));
}
