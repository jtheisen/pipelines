using System.Collections.Concurrent;

namespace TplPlay.Pipes;

public class PipePart { }

public class PipeBufferPart : PipePart
{
    public Object Buffer { get; init; }
}

public class PipeWorkerPart : PipePart
{
    public IWorker Worker { get; init; }
    public String Verb { get; set; }
    public Task Task { get; set; }
}

public enum PipeRunMode
{
    Probe,
    Suck,
    Blow
}

public interface IPipeContext
{
    PipeRunMode Mode { get; }

    void AddBuffer(Object buffer);

    void AddWorker<W>(W worker) where W : IWorker;

    void SetTask(String verb, Task task);

    void Schedule(String verb, Action task);
}

public class PipeContext : IPipeContext
{
    PipeRunMode mode;
    List<PipePart> parts;

    public PipeRunMode Mode => mode;

    public PipeContext(PipeRunMode mode)
    {
        this.mode = mode;
        this.parts = new List<PipePart>();
    }

    public IEnumerable<PipePart> Parts => parts;

    public void AddBuffer(Object buffer) => parts.Add(new PipeBufferPart { Buffer = buffer });

    public void AddWorker<W>(W worker) where W : IWorker => parts.Add(new PipeWorkerPart { Worker = worker });

    public void SetTask(String verb, Task task)
    {
        var lastPart = parts.LastOrDefault() as PipeWorkerPart;

        if (lastPart is null) throw new Exception($"Can't set task without a worker part");

        lastPart.Task = task;
        lastPart.Verb = verb;
    }

    public void Schedule(String verb, Action task) => SetTask(verb, Task.Run(task));
}

public interface IPipeline
{
    void Run(out LivePipeline livePipeline);
}

public class Pipeline<B> : IPipeline
    where B : class, new()
{
    private readonly IPipeEnd<B> source;
    private readonly IPipeEnd<B> sink;

    public Pipeline(IPipeEnd<B> source, IPipeEnd<B> sink)
    {
        this.source = source;
        this.sink = sink;
    }

    public void Run(out LivePipeline livePipeline)
    {
        livePipeline = Instantiate();
    }

    LivePipeline Instantiate()
    {
        var buffer = new B();

        var suckingContext = new PipeContext(PipeRunMode.Suck);
        var blowingContext = new PipeContext(PipeRunMode.Blow);

        source.Run(buffer, suckingContext);
        sink.Run(buffer, blowingContext);

        var parts = suckingContext.Parts
            .Concat(new[] { new PipeBufferPart { Buffer = buffer } })
            .Concat(blowingContext.Parts.Reverse());

        return new LivePipeline(parts);
    }
}

public class LivePipeline
{
    private PipePart[] parts;

    Task task;

    public Task Task => task;

    public LivePipeline(IEnumerable<PipePart> parts)
    {
        this.parts = parts.ToArray();

        var tasks = from p in parts.OfType<PipeWorkerPart>() let t = p.Task where t is not null select t;

        task = Task.WhenAll(tasks);
    }

    public PipeReport GetReport()
    {
        var parts = this.parts.Select(GetReportPart).ToArray();

        return new PipeReport(parts);
    }

    PipeReportPart GetReportPart(PipePart part) => part switch
    {
        PipeBufferPart bufferPart => Buffers.GetReport(bufferPart.Buffer),
        PipeWorkerPart workerPart => new PipeReportWorker(workerPart.Worker.Name),
        _ => null
    };
}

public record PipeReportPart;

public record PipeReportBinaryBufferPart(Int64 Content, Int64 Size) : PipeReportPart;

public record PipeReportItemBufferPart(Int32 Count, Int32 Size) : PipeReportPart;

public record PipeReportWorker(String Name) : PipeReportPart;

public record PipeReport(PipeReportPart[] Parts);

