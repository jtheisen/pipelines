// See https://aka.ms/new-console-template for more information
using ICSharpCode.SharpZipLib.BZip2;
using Spectre.Console;
using System.IO.Pipelines;
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Xml.Linq;
using TplPlay;
using TplPlay.Pipes;

var fileName = @"c:\users\jens\downloads\dewiki-20230501-pages-articles.xml.bz2";
var sinkFileName = @"c:\users\jens\downloads\dewiki-20230501-pages-articles-copy.xml.bz2";

//using var zippedStream = new FileStream(fileName, FileMode.Open, FileAccess.Read);
//using var xmlStream = new BZip2InputStream(zippedStream);


//var pipeLengthProperty = typeof(Pipe).GetProperty("Length", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

//var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 1 << 20, resumeWriterThreshold: 1 << 19));

//var blackhole = new BlackholeStream(lagMillis: 10);

//var writingTask = xmlStream.CopyToAsync(pipe.Writer);
//var readingTask = pipe.Reader.CopyToAsync(blackhole);


//async void ShowProgress()
//{
//    while (true)
//    {
//        await Task.Delay(400);

//        Console.WriteLine($"Wrote {blackhole.Position} bytes; {pipeLengthProperty.GetValue(pipe)}");
//    }
//}

//ShowProgress();

//Task.WaitAll(readingTask, writingTask);

//Console.WriteLine($"Done writing {blackhole.Position} bytes");


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
