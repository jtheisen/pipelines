using System.Text.Json;

namespace Pipelines.Json;

public static partial class JsonExtensions
{
    public static IEnumerablePipeEnd<T> Json<T>(IStreamPipeEnd sourcePipeEnd, JsonSerializerOptions options)
        => PipeEnds.CreateEnumerable<T>((nextBuffer, context) =>
     {
         var pipe = Buffers.MakeBuffer<Pipe>();

         sourcePipeEnd.Run(pipe, context);

         context.AddBuffer(pipe);

         context.AddWorker("json");

         switch (context.Mode)
         {
             case PipeRunMode.Suck:
                 context.ScheduleAsync("parsing", ct => ParseJsonAsync(pipe.Reader.AsStream(), nextBuffer, options, ct));

                 break;
             case PipeRunMode.Blow:
                 context.ScheduleSync("serializing", ct => SerializeJson(pipe.Writer.AsStream(), nextBuffer.GetConsumingEnumerable(), options));

                 break;
             default:
                 break;
         }

     });

    static async Task ParseJsonAsync<T>(Stream inputStream, BlockingCollection<T> sink, JsonSerializerOptions options, CancellationToken ct)
    {
        var asyncEnumerable = JsonSerializer.DeserializeAsyncEnumerable<T>(inputStream, options);

        await foreach (var item in asyncEnumerable)
        {
            sink.Add(item, ct);
        }
    }

    static void SerializeJson<T>(Stream outputStream, IEnumerable<T> source, JsonSerializerOptions options)
    {
        JsonSerializer.Serialize(outputStream, source, options);
    }
}
