using System;
using System.IO;
using Azure.Storage.Blobs;

namespace Pipelines.Azure.Storage.Blobs
{
    public static class PipeEnds2
    {
        public static IStreamPipeEnd AsPipeEnd(this BlobClient blobClient)
        {
            (Stream, Boolean) OpenRead()
            {
                return (blobClient.OpenRead(), false);
            }

            (Stream, Boolean) OpenWrite()
            {
                var stream = blobClient.OpenWrite(true);

                return (stream, false);
            }

            return PipeEnds.CreateStream("BlobClient", OpenRead, OpenWrite);
        }
    }
}
