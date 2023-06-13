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

    public PipeWorkerPartTask WorkerTask { get; set; }

    public Task Task => WorkerTask.Task;
}

public abstract class PipeWorkerPartTask
{
    public TaskFactory TaskFactory { get; protected set; }
    public Task Task { get; protected set; }
}


public class PipeAsyncWorkerPartTask : PipeWorkerPartTask
{
    private readonly Func<CancellationToken, Task> run;

    public PipeAsyncWorkerPartTask(Func<CancellationToken, Task> run)
    {
        this.run = run;
    }

    public void Start(CancellationToken ct)
    {
        Task = run(ct);
    }
}

public class PipeSyncWorkerPartTask : PipeWorkerPartTask
{
    private readonly Action<CancellationToken> run;

    public PipeSyncWorkerPartTask(Action<CancellationToken> run)
    {
        this.run = run;
    }

    public void Start(TaskFactory taskFactory, CancellationToken ct)
    {
        TaskFactory = taskFactory;
        Task = (taskFactory ?? Task.Factory).StartNew(() => run(ct), ct);
    }
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

    void ScheduleSync(String verb, Action<IWorkerInputProgress> task);

    void ScheduleSync(String verb, Action task);

    void ScheduleAsync(String verb, Func<CancellationToken, Task> task);

    void ScheduleAsync(String verb, Func<CancellationToken, IWorkerInputProgress, Task> task);
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

    public void AddWorker(String name) => parts.Add(new PipeWorkerPart { Name = name });

    void SetTask(String verb, Func<WorkerInputProgress, PipeWorkerPartTask> getWorkerTask)
    {
        var lastPart = parts.LastOrDefault() as PipeWorkerPart;

        if (lastPart is null) throw new Exception($"Can't set task without a worker part");

        var progress = new WorkerInputProgress();

        lastPart.Verb = verb;
        lastPart.Progress = progress;
        lastPart.WorkerTask = getWorkerTask(progress);
    }

    public void ScheduleSync(String verb, Action task)
        => SetTask(verb, _ => new PipeSyncWorkerPartTask(_ => task()));
    public void ScheduleSync(String verb, Action<IWorkerInputProgress> task)
        => SetTask(verb, p => new PipeSyncWorkerPartTask(_ => task(p)));

    public void ScheduleSync(String verb, Action<CancellationToken> task)
        => SetTask(verb, _ => new PipeSyncWorkerPartTask(task));
    public void ScheduleSync(String verb, Action<IWorkerInputProgress, CancellationToken> task)
        => SetTask(verb, p => new PipeSyncWorkerPartTask(ct => task(p, ct)));

    public void ScheduleAsync(String verb, Func<CancellationToken, Task> task)
        => SetTask(verb, _ => new PipeAsyncWorkerPartTask(ct => task(ct)));
    public void ScheduleAsync(String verb, Func<CancellationToken, IWorkerInputProgress, Task> task)
        => SetTask(verb, p => new PipeAsyncWorkerPartTask(ct => task(ct, p)));
}

public interface IPipeline
{
    LivePipeline Run(Func<CancellationToken, TaskFactory> createTaskFactory = null);
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

    public LivePipeline Run(Func<CancellationToken, TaskFactory> createTaskFactory = null)
    {
        return Instantiate(createTaskFactory);
    }

    LivePipeline Instantiate(Func<CancellationToken, TaskFactory> createTaskFactory)
    {
        var cts = new CancellationTokenSource();

        var ct = cts.Token;

        var buffer = Buffers.MakeBuffer<B>();

        var suckingContext = new PipeContext(PipeRunMode.Suck);
        var blowingContext = new PipeContext(PipeRunMode.Blow);

        source.Run(buffer, suckingContext);
        sink.Run(buffer, blowingContext);

        var workerParts = suckingContext.Parts.Concat(blowingContext.Parts).OfType<PipeWorkerPart>().ToArray();

        foreach (var part in workerParts)
        {
            var workerTask = part.WorkerTask;

            if (workerTask is PipeSyncWorkerPartTask syncWorkerTask)
            {
                var taskFactory = createTaskFactory?.Invoke(ct);

                syncWorkerTask.Start(taskFactory, ct);
            }
            else if (workerTask is PipeAsyncWorkerPartTask asyncWorkerTask)
            {
                asyncWorkerTask.Start(ct);
            }
            else
            {
                throw new Exception($"Unexpected worker task {workerTask}");
            }
        }

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

        task = Run();
    }

    async Task Run()
    {
        var workerTasks = parts.OfType<PipeWorkerPart>().Select(p => p.WorkerTask).ToList();

        workerTasks.ForEach(t => t.Task.ContinueWith(HandleFaultedTask, TaskContinuationOptions.OnlyOnFaulted));

        await Task.WhenAll(workerTasks.Select(p => p.Task));

        foreach (var workerTask in  workerTasks)
        {
            if (workerTask.TaskFactory?.Scheduler is not TaskScheduler taskScheduler) continue;

            if (taskScheduler is SingleThreadTaskScheduler singleThreadTaskScheduler)
            {
                singleThreadTaskScheduler.Join();
            }
            else
            {
                throw new Exception($"Unexpected task scheduler {taskScheduler} in factory");
            }
        }
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

