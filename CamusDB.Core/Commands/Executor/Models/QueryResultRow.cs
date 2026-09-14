
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.ObjectModel;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// A row produced by the query pipeline: its KV row id plus its column values keyed by name.
/// <see cref="Row"/> is often a <see cref="Queries.QueryRow"/> (a lazy, layout-backed adapter),
/// so a consumer that never reads a cell never materializes it.
/// </summary>
public readonly struct QueryResultRow
{
    /// <summary>
    /// The single shared empty column set for records that carry a row id only. The DML locate
    /// buffers (DELETE, and UPDATE with plain values) retain no column values past the locate
    /// scan — the mutation phase re-reads every row under its lock — so their records all share
    /// this one instance instead of pinning the scanned row (and, for a borrowed-backed row,
    /// its full KV bytes) or allocating a dictionary per match.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, ColumnValue> EmptyRow =
        ReadOnlyDictionary<string, ColumnValue>.Empty;

    public ObjectIdValue RowId { get; }

    public IReadOnlyDictionary<string, ColumnValue> Row { get; }

    public QueryResultRow(ObjectIdValue rowId, IReadOnlyDictionary<string, ColumnValue> row)
	{
        RowId = rowId;
        Row = row;
	}
}
