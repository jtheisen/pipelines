using ICSharpCode.SharpZipLib.BZip2;
using System;
using System.IO.Compression;

namespace Pipelines
{
    public static class ZipExtensions
    {
        public static IStreamPipeEnd GZip(this IStreamPipeEnd source, CompressionLevel level = CompressionLevel.Fastest)
            => source.WrapStream(nameof(GZip), s => new GZipStream(s, CompressionMode.Decompress, true), s => new GZipStream(s, level, true));

        public static IStreamPipeEnd BZip(this IStreamPipeEnd source, Int32 blockSize)
            => source.WrapStream(nameof(GZip), s => new BZip2InputStream(s), s => new BZip2OutputStream(s, blockSize));

        public static IStreamPipeEnd BZip(this IStreamPipeEnd source)
            => source.WrapStream(nameof(GZip), s => new BZip2InputStream(s), s => new BZip2OutputStream(s));
    }
}
