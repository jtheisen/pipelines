namespace Pipelines;

public class SingleThreadTaskScheduler : TaskScheduler
{
    private readonly BlockingCollection<Task> tasks;
    private readonly Thread thread;
    private readonly CancellationTokenSource cts;
    private readonly CancellationToken ct;
    private readonly Action<String> reportError;

    public SingleThreadTaskScheduler(CancellationToken ct, Action<String> reportError)
    {
        tasks = new BlockingCollection<Task>();

        thread = new Thread(Run);

        this.cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        this.ct = cts.Token;
        this.reportError = reportError;

        thread.Start();
    }

    public void Join()
    {
        cts.Cancel();

        thread.Join();
    }

    void Run()
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (tasks.TryTake(out var task, -1, ct))
                {
                    var success = TryExecuteTask(task);

                    if (!success)
                    {
                        reportError?.Invoke("Failed to execute task");
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            reportError($"Thread aborted with exception: " + ex.Message);
        }
    }

    protected override IEnumerable<Task> GetScheduledTasks() => throw new NotImplementedException();

    protected override void QueueTask(Task task) => tasks.Add(task);

    protected override Boolean TryExecuteTaskInline(Task task, Boolean taskWasPreviouslyQueued)
    {
        reportError?.Invoke("Refused to inline a previously queued task");

        if (taskWasPreviouslyQueued) return false;

        return base.TryExecuteTask(task);
    }
}
