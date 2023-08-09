using Azure.Storage.Blobs;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using MySqlConnector;
using System.Security.Cryptography;
using System.Text;
using Testflow;

var configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .AddCommandLine(args)
    .Build();

var settings = new AppSettings();

configuration.Bind(settings);

var options = new SqlRetryLogicOption()
{
    // Tries 5 times before throwing an exception
    NumberOfTries = 3,
    // Preferred gap time to delay before retry
    DeltaTime = TimeSpan.FromSeconds(1),
    // Maximum gap time for each delay time before retry
    MaxTimeInterval = TimeSpan.FromSeconds(10)
};

Console.WriteLine("Connecting to target...");

using var targetConnection = new SqlConnection(settings.TargetDbConnectionString);
targetConnection.Open();
targetConnection.RetryLogicProvider = SqlConfigurableRetryFactory.CreateExponentialRetryProvider(options);

Console.WriteLine("Connected.");

var columns = new[] {
    "id",
    "company", "location", "event", "product", "testuser", "offer_id", "invoice_id", "laboratory_id",
    "timeslot", "timeslot_end", "checkin_at", "test_date", "result_date", "consent_at",
    "status", "created_at", "updated_at",
    "result",
    "case when email like '%@%' then MD5(email) else null end as email_md5"
};

var sourceFilter = "result is not null and result <> ''";

var batchSize = 10000;

var columnsSql = String.Join(", ", columns);

var rowsWritten = 0L;
var totalCount = 0L;

void Copy(String sourceConnectionString, String sourceTable, String targetTable)
{
    Console.WriteLine("Opening source: " + sourceConnectionString);

    using var sourceConnection = new MySqlConnection(sourceConnectionString);
    sourceConnection.Open();

    Console.WriteLine("Connected, querying target");

    var maxId = targetConnection.QuerySingle<Int64?>($"select max(id) from {targetTable}") ?? 0;

    Console.WriteLine("2 worked");

    Console.WriteLine($"Copying starting after id {maxId:n0}, from {sourceTable} to {targetTable}");

    totalCount += sourceConnection.QuerySingle<Int64>($"select count(*) from {sourceTable} where {sourceFilter}");

    while (true)
    {
        var readingCommandSql = $"select {columnsSql} from {sourceTable} where id > {maxId} and {sourceFilter} order by id asc limit {batchSize};";

        using var readingCommand = new MySqlCommand(readingCommandSql, sourceConnection);

        using var reader = readingCommand.ExecuteReader();

        using var sqlBulkCopy = new SqlBulkCopy(targetConnection);
        sqlBulkCopy.DestinationTableName = settings.TargetTable;
        sqlBulkCopy.WriteToServer(reader);

        if (sqlBulkCopy.RowsCopied == 0)
        {
            Console.WriteLine("Stopping after no more rows were copied");

            break;
        }

        rowsWritten += sqlBulkCopy.RowsCopied;

        maxId = targetConnection.QuerySingle<Int64>($"select max(id) from {settings.TargetTable}");

        Console.WriteLine($"{rowsWritten:n0} rows written, latest id is {maxId:n0}");
    }
}

Copy(settings.ArchiveDbConnectionString, settings.ArchiveTable, settings.TargetTable);
Copy(settings.LatestDbConnectionString, settings.LatestTable, settings.TargetTable);

Console.WriteLine("done");
