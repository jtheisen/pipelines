namespace Pipelines;

public static partial class Extensions
{
    public static S Apply<S>(this S source, Action<S> func)
    {
        func(source);

        return source;
    }

    public static T Apply<S, T>(this S source, Func<S, T> func)
        => func(source);
}
