// See https://aka.ms/new-console-template for more information
using System.Diagnostics;

Console.WriteLine("Hello, World!");

IEnumerable<Int32> CreateInts()
{
    var i = 0;

    while (true)
    {
        yield return i++;
    }
}

var pipeline = PipeEnds
    .FromEnumerable(CreateInts())
    .Map(i => i + 1)
    .BuildCopyingPipeline(PipeEnds.EnumerableBlackhole<Int32>())
    ;

pipeline.Run()
    .ReportSpectre()
    .Wait();
