using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

namespace Pipelines.CsvHelper
{
    public static class Iso8601Cultures
    {
        public static CultureInfo Seconds = Create(0);
        public static CultureInfo Milliseconds = Create(3);

        public static CultureInfo Create(Int32 subSecondDigits = 0, String baseCultureName = "en-US")
        {
            var subSeconds = GetSubSecondDigitFormat(subSecondDigits);

            return new CultureInfo(baseCultureName)
            {
                DateTimeFormat =
            {
                LongDatePattern = "yyyy-MM-dd",
                ShortDatePattern = "yyyy-MM-dd",
                FullDateTimePattern = $"yyyy-MM-dd HH:mm:ss{subSeconds}",
                LongTimePattern = $"HH:mm:ss{subSeconds}",
                ShortTimePattern = "HH:mm:ss"
            },
            };
        }

        static String GetSubSecondDigitFormat(Int32 length) => length == 0 ? "" : "." + new String('f', length);
    }

    public static partial class CsvExtensions
    {
        public static IEnumerablePipeEnd<T> Csv<T>(IStreamPipeEnd sourcePipeEnd, CultureInfo cultureInfo)
            => sourcePipeEnd.Itemize<T, CsvConfiguration>(nameof(Csv), new CsvConfiguration(cultureInfo), ParseCsv, SerializeCsv);

        public static IEnumerablePipeEnd<T> Csv<T>(IStreamPipeEnd sourcePipeEnd, CsvConfiguration config)
            => sourcePipeEnd.Itemize<T, CsvConfiguration>(nameof(Csv), config, ParseCsv, SerializeCsv);

        static void ParseCsv<T>(TextReader reader, Action<T> sink, CsvConfiguration config)
        {
            var csvReader = new CsvReader(reader, config);

            foreach (var record in csvReader.GetRecords<T>())
            {
                sink(record);
            }
        }

        static void SerializeCsv<T>(TextWriter writer, IEnumerable<T> source, CsvConfiguration config)
        {
            var csvWriter = new CsvWriter(writer, config);

            csvWriter.WriteRecords(source);

            csvWriter.Flush();
        }
    }
}
