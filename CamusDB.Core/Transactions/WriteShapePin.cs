
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Transactions;

/// <summary>
/// A relation whose <b>write shape</b> a transaction can pin. The write shape is everything a write
/// to the relation must maintain or obey beyond the row itself: the indexes that take entries, the
/// foreign keys enforced on either side, the CHECK and NOT NULL constraints.
///
/// <para>The row layout is not part of it. A column change moves the table's layout version, and the
/// schema-version pin (<see cref="KvTransaction.PinSchemaVersion"/>) already watches that. An index
/// or a constraint leaves the layout version alone, so nothing else tells a writer that the work it
/// staged is now incomplete.</para>
///
/// <para>Kept as an interface so the transaction layer does not depend on the catalog model. The
/// table schema implements it.</para>
/// </summary>
public interface IWriteShapeSource
{
    /// <summary>
    /// The epoch of the last change that added a write obligation to this relation on this node, or
    /// zero when none happened since the relation was loaded. <c>long.MaxValue</c> marks an instance
    /// the node replaced: no pin on it can ever be valid again. Read lock-free at commit.
    /// </summary>
    long WriteShapeChangedAt { get; }

    /// <summary>The relation's name, for the error a refused commit reports.</summary>
    string? WriteShapeName { get; }
}

/// <summary>
/// One relation a transaction wrote, with the write-shape epoch the first such statement captured
/// <b>before</b> it resolved the relation. The commit is valid only while
/// <see cref="IWriteShapeSource.WriteShapeChangedAt"/> is not later than <see cref="Epoch"/>: a later
/// change means the statement planned its writes without an index or a constraint that now exists.
///
/// <para>The default value, whose <see cref="Table"/> is null, means "no pin".</para>
/// </summary>
internal readonly record struct WriteShapePin(IWriteShapeSource Table, long Epoch);
