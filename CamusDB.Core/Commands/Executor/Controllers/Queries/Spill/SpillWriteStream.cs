/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;

/// <summary>
/// The write side of one spill file. It reserves the bytes of each write in its
/// <see cref="SpillScope"/> before it passes them to the file, so a write that would pass a spill
/// limit throws <see cref="CamusDBException"/> and does not reach the disk.
///
/// <para>This is a wrapper and not a <see cref="FileStream"/> subclass on purpose. A subclass of
/// <see cref="FileStream"/> changes how the runtime routes its calls, and every write overload would
/// need an override to stay counted. Here every write goes through <see cref="Write(ReadOnlySpan{byte})"/>
/// or <see cref="WriteAsync(ReadOnlyMemory{byte}, CancellationToken)"/>, and the stream cannot read or
/// seek, so no path can skip the count.</para>
///
/// <para>Not thread-safe, like the <see cref="FileStream"/> it wraps: one operator writes it.</para>
/// </summary>
public sealed class SpillWriteStream : Stream
{
    private readonly FileStream inner;

    private readonly SpillScope scope;

    internal SpillWriteStream(FileStream inner, SpillScope scope)
    {
        this.inner = inner;
        this.scope = scope;
    }

    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => inner.CanWrite;

    public override long Length => inner.Length;

    public override long Position
    {
        get => inner.Position;
        set => throw new NotSupportedException("A spill file is written sequentially.");
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        scope.Reserve(buffer.Length);
        inner.Write(buffer);
    }

    public override void Write(byte[] buffer, int offset, int count)
        => Write(buffer.AsSpan(offset, count));

    public override void WriteByte(byte value)
    {
        scope.Reserve(1);
        inner.WriteByte(value);
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        scope.Reserve(buffer.Length);
        return inner.WriteAsync(buffer, cancellationToken);
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override void Flush() => inner.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);

    public override int Read(byte[] buffer, int offset, int count)
        => throw new NotSupportedException("A spill writer cannot read.");

    public override long Seek(long offset, SeekOrigin origin)
        => throw new NotSupportedException("A spill file is written sequentially.");

    public override void SetLength(long value)
        => throw new NotSupportedException("A spill file is written sequentially.");

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            inner.Dispose();

        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        await inner.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }
}
