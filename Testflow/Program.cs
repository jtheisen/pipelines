using Azure.Storage.Blobs;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using MySqlConnector;
using System.Text;
using Testflow;

Console.WriteLine("Hello, World!");

var configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .AddCommandLine(args)
    .Build();

var settings = new AppSettings();

configuration.Bind(settings);

Console.WriteLine($"Let's go with: {settings.TargetDbConnectionString} and {settings.TestflowDbConnectionString}");


var sourceConnection = new MySqlConnection(settings.TestflowDbConnectionString);
sourceConnection.Open();

using var targetConnection = new SqlConnection(settings.TargetDbConnectionString);
targetConnection.Open();

var columns = new[] {
    "id",
    "company", "location", "event", "product", "testuser", "offer_id", "invoice_id", "laboratory_id",
    "timeslot", "timeslot_end", "checkin_at", "test_date", "result_date", "consent_at",
    "status", "created_at", "updated_at",
    "result"
};

var batchSize = 10000;

var columnsSql = String.Join(", ", columns);

var maxId = targetConnection.QuerySingle<Int64?>("select max(id) from testcases_archive") ?? 0;

Console.WriteLine($"Copying from id {maxId}");

var rowsWritten = 0;

while (true)
{
    var readingCommandSql = $"select {columnsSql} from testcases_archive where id > {maxId} limit {batchSize};";

    using var readingCommand = new MySqlCommand(readingCommandSql, sourceConnection);

    var reader = readingCommand.ExecuteReader();

    using var sqlBulkCopy = new SqlBulkCopy(targetConnection);
    sqlBulkCopy.DestinationTableName = "testcases_archive";
    sqlBulkCopy.WriteToServer(reader);

    if (sqlBulkCopy.RowsCopied == 0)
    {
        Console.WriteLine("Stopping after no more rows were copied");
        
        break;
    }

    rowsWritten += sqlBulkCopy.RowsCopied;

    maxId = targetConnection.QuerySingle<Int64>("select max(id) from testcases_archive");

    Console.WriteLine($"{sqlBulkCopy.RowsCopied} rows written, latest id is {maxId}");
}

Console.WriteLine("done");
