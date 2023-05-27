using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace TplPlay;

/*
 Unused, but may be interesting.
 */

public static class AsyncExtensions
{
    public static async IAsyncEnumerable<ICollection<T>> Batch<T>(this IAsyncEnumerable<T> source, Int32 batchSize)
    {
        var batch = new List<T>(batchSize);

        await using var e = source.GetAsyncEnumerator();

        while (true)
        {
            for (var i = 0; i < batchSize; i++)
            {
                if (!await e.MoveNextAsync()) yield break;

                batch.Add(e.Current);
            }

            yield return batch;
        }
    }

    public static async IAsyncEnumerable<T> ConsumeBuffered<T>(
        this IAsyncEnumerable<T> source, int capacity,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        Channel<T> channel = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
        {
            SingleWriter = true,
            SingleReader = true,
        });
        using CancellationTokenSource completionCts = new();

        Task producer = Task.Run(async () =>
        {
            try
            {
                await foreach (T item in source.WithCancellation(completionCts.Token)
                    .ConfigureAwait(false))
                {
                    await channel.Writer.WriteAsync(item).ConfigureAwait(false);
                }
            }
            catch (ChannelClosedException) { } // Ignore
            finally { channel.Writer.TryComplete(); }
        });

        try
        {
            await foreach (T item in channel.Reader.ReadAllAsync(cancellationToken)
                .ConfigureAwait(false))
            {
                yield return item;
                cancellationToken.ThrowIfCancellationRequested();
            }
            await producer.ConfigureAwait(false); // Propagate possible source error
        }
        finally
        {
            // Prevent fire-and-forget in case the enumeration is abandoned
            if (!producer.IsCompleted)
            {
                completionCts.Cancel();
                channel.Writer.TryComplete();
                await Task.WhenAny(producer).ConfigureAwait(false);
            }
        }
    }
}
