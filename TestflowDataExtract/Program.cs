using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Pipelines;
using Pipelines.CsvHelper;
using System.Globalization;
using Testflow;

var configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .AddCommandLine(args)
    .Build();

var settings = new AppSettings();

configuration.Bind(settings);

Console.WriteLine($"Let's go with: {settings.TargetDbConnectionString}");


using var targetConnection = new SqlConnection(settings.TargetDbConnectionString);
targetConnection.Open();

var sourceEnumerable = targetConnection.Query($"select * from {settings.TargetTable} where result is not null and result <> '' and year(test_date) = 2022", buffered: false);

var sink = PipeEnds.FromFile("data.csv")
    .Csv<dynamic>(CultureInfo.InvariantCulture)
    ;

var source = PipeEnds.FromEnumerable(sourceEnumerable);

var pipeline = source.BuildCopyingPipeline(sink);

pipeline.Run()
    .ReportSpectre()
    .Wait();
