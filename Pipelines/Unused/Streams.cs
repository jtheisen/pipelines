namespace TplPlay;

public class BlackholeStream : Stream
{
    private readonly Int32 lagMillis;

    Int64 position;

    public BlackholeStream(Int32 lagMillis)
    {
        this.lagMillis = lagMillis;
    }

    public override Boolean CanRead => false;

    public override Boolean CanSeek => false;

    public override Boolean CanWrite => true;

    public override Int64 Length => position;

    public override Int64 Position { get => position; set => throw new NotImplementedException(); }

    public override void Flush()
    {
    }

    public override Int32 Read(Byte[] buffer, Int32 offset, Int32 count)
    {
        throw new NotImplementedException();
    }

    public override Int64 Seek(Int64 offset, SeekOrigin origin)
    {
        throw new NotImplementedException();
    }

    public override void SetLength(Int64 value)
    {
        throw new NotImplementedException();
    }

    public override void Write(Byte[] buffer, Int32 offset, Int32 count)
    {
        if (lagMillis > 0)
        {
            Thread.Sleep(lagMillis);
        }

        position += count;
    }
}
