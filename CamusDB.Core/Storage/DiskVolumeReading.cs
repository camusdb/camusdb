/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Storage;

/// <summary>
/// One <c>statfs</c>-class reading of a volume, in bytes. A field the probe could not determine is
/// <c>-1</c>, never <c>0</c>: zero free bytes is a real and alarming reading, so it must not double
/// as "unknown".
///
/// <para><see cref="AvailableBytes"/> is the space this process's user may still write (it honors
/// reserved blocks and per-user quotas); <see cref="FreeBytes"/> is the raw free space of the volume.
/// The write-admission watermark compares <see cref="AvailableBytes"/>, because that is the space
/// the storage engine can actually use.</para>
/// </summary>
public readonly record struct DiskVolumeReading(long TotalBytes, long FreeBytes, long AvailableBytes)
{
    /// <summary>A reading with every field unknown.</summary>
    public static DiskVolumeReading Unknown => new(-1, -1, -1);

    /// <summary>Bytes in use (<see cref="TotalBytes"/> minus <see cref="FreeBytes"/>), or -1 when either is unknown.</summary>
    public long UsedBytes => TotalBytes >= 0 && FreeBytes >= 0 ? TotalBytes - FreeBytes : -1;
}
