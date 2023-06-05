namespace Pipelines;

public class PipePart { }

public class PipeBufferPart : PipePart
{
    public Object Buffer { get; init; }
}

public class PipeWorkerPart : PipePart
{
    public String Name { get; init; }
    public String Verb { get; set; }
    public WorkerInputProgress Progress { get; set; }
    public Task Task { get; set; }
}

public enum PipeRunMode
{
    Probe,
    Suck,
    Blow
}

public interface IWorkerInputProgress
{
    void ReportTotal(Int64 total);
    void ReportProcessed(Int64 processed);
}

public class WorkerInputProgress : IWorkerInputProgress
{
    Int64 total;
    Int64 processed;

    public void ReportTotal(Int64 total) => this.total = total;
    public void ReportProcessed(Int64 processed) => this.processed = processed;

    public Int64 Total => this.total;
    public Int64 Processed => this.processed;
}

public interface IPipeContext
{
    PipeRunMode Mode { get; }

    void AddBuffer(Object buffer);

    void AddWorker(String name);

    void Schedule(String verb, Action<IWorkerInputProgress> task);

    void Schedule(String verb, Action task);

    void ScheduleAsync(String verb, Func<CancellationToken, Task> task);

    void ScheduleAsync(String verb, Func<CancellationToken, IWorkerInputProgress, Task> task);
}

public class PipeContext : IPipeContext
{
    PipeRunMode mode;
    List<PipePart> parts;
    CancellationToken ct;

    public PipeRunMode Mode => mode;

    public PipeContext(PipeRunMode mode, CancellationToken ct)
    {
        this.mode = mode;
        this.parts = new List<PipePart>();
        this.ct = ct;
    }

    public IEnumerable<PipePart> Parts => parts;

    public void AddBuffer(Object buffer) => parts.Add(new PipeBufferPart { Buffer = buffer });

    public void AddWorker(String name) => parts.Add(new PipeWorkerPart { Name = name });

    void SetTask(String verb, Func<IWorkerInputProgress, Task> getTask)
    {
        var lastPart = parts.LastOrDefault() as PipeWorkerPart;

        if (lastPart is null) throw new Exception($"Can't set task without a worker part");

        var progress = new WorkerInputProgress();

        lastPart.Task = getTask(progress);
        lastPart.Verb = verb;
        lastPart.Progress = progress;
    }

    public void Schedule(String verb, Action task) => SetTask(verb, _ => Task.Run(task, ct));
    public void Schedule(String verb, Action<IWorkerInputProgress> task) => SetTask(verb, p => Task.Run(() => task(p), ct));

    public void ScheduleAsync(String verb, Func<CancellationToken, Task> task) => SetTask(verb, _ => task(ct));
    public void ScheduleAsync(String verb, Func<CancellationToken, IWorkerInputProgress, Task> task) => SetTask(verb, p => task(ct, p));
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
        var cts = new CancellationTokenSource();

        var buffer = new B();

        var suckingContext = new PipeContext(PipeRunMode.Suck, cts.Token);
        var blowingContext = new PipeContext(PipeRunMode.Blow, cts.Token);

        source.Run(buffer, suckingContext);
        sink.Run(buffer, blowingContext);

        var parts = suckingContext.Parts
            .Concat(new[] { new PipeBufferPart { Buffer = buffer } })
            .Concat(blowingContext.Parts.Reverse());

        return new LivePipeline(parts, cts);
    }
}

public class LivePipeline
{
    private readonly CancellationTokenSource cts;
    private PipePart[] parts;

    Task task;

    public Task Task => task;

    public LivePipeline(IEnumerable<PipePart> parts, CancellationTokenSource cts)
    {
        this.parts = parts.ToArray();
        this.cts = cts;

        var tasks = (
            from p in parts.OfType<PipeWorkerPart>()
            let t = p.Task
            where t is not null
            select t
        ).ToList();

        tasks.ForEach(t => t.ContinueWith(HandleFaultedTask, TaskContinuationOptions.OnlyOnFaulted));

        task = Task.WhenAll(tasks);
    }

    void HandleFaultedTask(Task _) => Cancel();

    public PipeReport GetReport()
    {
        var parts = this.parts.Select(GetReportPart).ToArray();

        return new PipeReport(parts);
    }

    public void Cancel()
    {
        cts.Cancel();
    }

    static PipeReportPart GetReportPart(PipePart part) => part switch
    {
        PipeBufferPart bufferPart => Buffers.GetAppropriateReport(bufferPart.Buffer),
        PipeWorkerPart workerPart => new PipeReportWorker(workerPart.Name, workerPart.Progress, GetState(workerPart.Task)),
        _ => null
    };

    static PipeReportWorkerState GetState(Task task)
    {
        if (task is null) return PipeReportWorkerState.Ready;

        if (task.IsCanceled) return PipeReportWorkerState.Cancelled;

        if (task.IsCompletedSuccessfully) return PipeReportWorkerState.Completed;

        if (task.IsFaulted) return PipeReportWorkerState.Failed;

        return PipeReportWorkerState.Running;
    }
}

public record PipeReportPart;

public enum PipeReportBufferState
{
    Empty,
    Mixed,
    Full
}

public record PipeReportBufferPart(PipeReportBufferState State, Int64 Content, Int64 Size) : PipeReportPart;

public enum PipeReportWorkerState
{
    Ready,
    Running,
    Completed,
    Cancelled,
    Failed
}

public record PipeReportWorker(String Name, WorkerInputProgress Progress, PipeReportWorkerState State) : PipeReportPart;

public record PipeReport(PipeReportPart[] Parts);

