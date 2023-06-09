using CsvHelper;
using CsvHelper.Configuration;

namespace Pipelines.CsvHelper;

public static partial class CsvExtensions
{
    public static IEnumerablePipeEnd<T> Csv<T>(IStreamPipeEnd sourcePipeEnd, CultureInfo cultureInfo)
        => sourcePipeEnd.Itemize<T, CsvConfiguration>(nameof(Csv), new CsvConfiguration(cultureInfo), ParseCsv, SerializeCsv);

    public static IEnumerablePipeEnd<T> Csv<T>(IStreamPipeEnd sourcePipeEnd, CsvConfiguration config)
        => sourcePipeEnd.Itemize<T, CsvConfiguration>(nameof(Csv), config, ParseCsv, SerializeCsv);

    static void ParseCsv<T>(TextReader reader, Action<T> sink, CsvConfiguration config)
    {
        var csvReader = new CsvReader(reader, CultureInfo.InvariantCulture);

        foreach (var record in csvReader.GetRecords<T>())
        {
            sink(record);
        }
    }

    static void SerializeCsv<T>(TextWriter writer, IEnumerable<T> source, CsvConfiguration config)
    {
        var csvWriter = new CsvWriter(writer, Iso8601Culture);

        csvWriter.WriteRecords(source);

        csvWriter.Flush();
    }

    public static CultureInfo Iso8601Culture = new CultureInfo("en-US")
    {
        DateTimeFormat =
        {
            LongDatePattern = "yyyy-MM-dd",
            ShortDatePattern = "yyyy-MM-dd",
            FullDateTimePattern = "yyyy-MM-dd HH:mm:ss.fff",
            LongTimePattern = "HH:mm:ss.fff",
            ShortTimePattern = "HH:mm:ss.fff"
        },
    };
}
