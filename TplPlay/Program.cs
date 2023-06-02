using Pipelines;
using Spectre.Console;
using System.Reactive.Linq;

var fileName = @"c:\users\jens\downloads\dewiki-20230501-pages-articles.xml.bz2";
var sinkFileName = @"c:\users\jens\downloads\dewiki-20230501-pages-articles-copy.xml.bz2";


var input = Pipes.File(fileName)
    .Zip()
    ;

var output = Pipes.File(sinkFileName)
    //.Zip()
    ;

var pipeline = input.BuildCopyingPipeline(output);

pipeline.Run(out var livePipeline);

var table = new Table();

table.Border = TableBorder.None;

table.AddColumn("");

AnsiConsole.MarkupLine("Running pipeline");

var qwer2 = AnsiConsole.Live(table)
    .StartAsync(async ctx =>
    {
        var reporter = new SpectreReporter();

        while (!livePipeline.Task.IsCompleted)
        {
            await Task.Delay(250);

            {
                var report = livePipeline.GetReport();

                table.Rows.Clear();
                foreach (var part in report.Parts)
                {
                    table.AddRow(reporter.GetLineForPart(part));
                }
            }

            ctx.Refresh();
        }
    });

await livePipeline.Task;
