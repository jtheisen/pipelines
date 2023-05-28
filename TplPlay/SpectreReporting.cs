namespace TplPlay.Pipes;

public class SpectreReporter
{
    public String GetLineForPart(PipeReportPart part) => part switch
    {
        PipeReportBufferPart b => $" │ {Bar((Int32)b.State, 2, "gray35", "gray15")} ({1.0 * b.Content / b.Size:p})",
        PipeReportWorker w => $"{w.Name}",
        _ => "?"
    };

    public String Bar(Int32 content, Int32 size, String foreground, String background)
        => $"{Bar(content, foreground)}{Bar(size - content, background)}";

    public String Bar(Int32 size, String color)
        => $"[default on {color}]{new String(' ', size)}[/]";
}
