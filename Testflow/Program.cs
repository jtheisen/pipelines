using Azure.Storage.Blobs;
using Dapper;
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

Console.WriteLine($"Let's go with: {settings.StorageConnectionString} and {settings.StorageConnectionString}");

var blobClient = new BlobClient(settings.StorageConnectionString, "testflow_main_db", "testcase");

blobClient.Upload(new MemoryStream(Encoding.UTF8.GetBytes("Hello, World!")));

Console.WriteLine("Blob written");

var connection = new MySqlConnection(settings.TestflowDbConnectionString);

var result = connection.QuerySingle("select count(*) from testcase;");

Console.WriteLine("Result from Mysql: " + result);
