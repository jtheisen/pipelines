using Pipelines;
using Spectre.Console;
using System.Reactive.Linq;

var fileName = @"c:\users\jens\downloads\dewiki-20230501-pages-articles.xml.bz2";
var sinkFileName = @"c:\users\jens\downloads\dewiki-20230501-pages-articles-copy.xml.bz2";


var input = PipeEnds.FromFile(fileName)
    .GZip()
    ;

var output = PipeEnds.FromFile(sinkFileName)
    //.Zip()
    ;

AnsiConsole.MarkupLine("Running pipeline");

var pipeline = input.BuildCopyingPipeline(output);

pipeline.Start()
    .ReportSpectre()
    .Wait();
