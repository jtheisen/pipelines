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

var columns = new[] {
    "id",
    "company", "location", "event", "product", "testuser", "offer_id", "invoice_id", "laboratory_id",
    "timeslot", "timeslot_end", "checkin_at", "test_date", "result_date", "consent_at",
    "status", "created_at", "updated_at"
};

var columnsSql = String.Join(", ", columns);

var readingCommandSql = $"select {columnsSql} from testcases_archive where id > 0 limit 10;";

//var batch = sourceConnection.Query(readingCommand);

var readingCommand = new MySqlCommand(readingCommandSql, sourceConnection);

var reader = readingCommand.ExecuteReader();

using var targetConnection = new SqlConnection(settings.TargetDbConnectionString);
targetConnection.Open();
using var sqlBulkCopy = new SqlBulkCopy(targetConnection);
sqlBulkCopy.DestinationTableName = "testcases_archive";
sqlBulkCopy.WriteToServer(reader);

//targetConnection.Execute($"inset into testcases_archive ({columnsSql}) values ()");


Console.WriteLine("done");
