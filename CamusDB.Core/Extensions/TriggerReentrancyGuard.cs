/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Extensions;

/// <summary>
/// Stops a BEFORE trigger's SQL from changing a row that the statement which fired the trigger has
/// already read and is about to write.
///
/// <para><b>Why.</b> UPDATE and DELETE lock and read a whole chunk of rows, then run the BEFORE
/// triggers row by row, then write the chunk. The row locks keep other transactions out, but not
/// nested SQL in the same transaction. If a trigger on row 1 updates row 2 of the same chunk, the
/// outer statement still holds row 2's image from before that update: it would write row 2 back from
/// the stale image and compute its index changes against it, losing the nested write and leaving
/// stale or missing index entries.</para>
///
/// <para><b>How.</b> While a chunk's BEFORE triggers run, the chunk's row ids are protected for its
/// table in the current async flow (<see cref="Protect"/>). A nested UPDATE or DELETE checks the rows
/// it chose against every enclosing protected chunk before it locks them
/// (<see cref="ThrowIfProtected"/>), and a hit fails the statement with
/// <see cref="CamusDBErrorCodes.TriggerMutationConflict"/>. The check is conservative: it looks at the
/// rows the nested statement located, before its own re-check.</para>
///
/// <para><b>What it does not cover, on purpose.</b> Rows of earlier chunks were already written; a
/// nested change to one of them is applied to the stored row and is not lost. AFTER triggers run after
/// every write, when no chunk holds an image. An INSERT creates new rows and cannot hit a protected
/// one.</para>
///
/// <para>The frames ride an <see cref="AsyncLocal{T}"/>, set inside the async method that runs the
/// triggers: nested statements inherit them, and they end when that method returns.</para>
/// </summary>
internal static class TriggerReentrancyGuard
{
    private sealed record Frame(string TableId, IReadOnlySet<ObjectIdValue> Rows, Frame? Outer);

    private static readonly AsyncLocal<Frame?> current = new();

    /// <summary>
    /// Protects <paramref name="rows"/> of <paramref name="tableId"/> until <see cref="Release"/> gets
    /// the returned value back. Call it in the async method that runs the chunk's BEFORE triggers.
    /// </summary>
    public static object? Protect(string tableId, IReadOnlyList<ObjectIdValue> rows)
    {
        Frame? outer = current.Value;
        current.Value = new Frame(tableId, new HashSet<ObjectIdValue>(rows), outer);
        return outer;
    }

    /// <summary>Ends the protection that <see cref="Protect"/> started.</summary>
    public static void Release(object? token) => current.Value = (Frame?)token;

    /// <summary>
    /// Throws when any of <paramref name="rows"/> of <paramref name="tableId"/> is protected by an
    /// enclosing statement. Costs one null check when no trigger is running.
    /// </summary>
    public static void ThrowIfProtected(string tableId, IReadOnlyList<ObjectIdValue> rows)
    {
        for (Frame? frame = current.Value; frame is not null; frame = frame.Outer)
        {
            if (!string.Equals(frame.TableId, tableId, StringComparison.Ordinal))
                continue;

            foreach (ObjectIdValue row in rows)
            {
                if (frame.Rows.Contains(row))
                    throw new CamusDBException(
                        CamusDBErrorCodes.TriggerMutationConflict,
                        "A BEFORE trigger's SQL changed or deleted a row that the statement which fired the trigger " +
                        "already read and is still writing; the statement would write from a stale row image");
            }
        }
    }
}
