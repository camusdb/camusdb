
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Represents a version of the table schema in the version history.
///
/// <para><b>The positions and types are the record; the names are labels.</b> A row written under
/// this version is decoded by position, and the decoder keys each cell by the name its column has
/// here. A rename changes the name of a column, not its id, so every layout must carry the current
/// name of each column for its rows to decode under that name. The persisted layouts are append-only
/// and keep the name a column had when the version was recorded; see
/// <see cref="TakeCurrentNames"/> for how a loaded layout is brought up to date.</para>
/// </summary>
public sealed class TableSchemaHistory
{
    public int Version { get; set; }

    public List<TableColumnSchema>? Columns { get; set; }

    /// <summary>
    /// Gives each column the name the column with the same id has in <paramref name="current"/>. A
    /// column that <paramref name="current"/> no longer has keeps its recorded name. Replaces
    /// <see cref="Columns"/> with a new list, never edits the list in place, because a decoder may be
    /// reading the old one; a layout whose names are already current is left as it is.
    ///
    /// <para>Called on every layout read back from storage. Without it a rename is lost on the first
    /// reopen: the rename relabels only the layouts held in memory, so after a reopen the rows written
    /// before the rename decode under the old name, and a read by the new name finds no such
    /// column.</para>
    /// </summary>
    internal void TakeCurrentNames(List<TableColumnSchema>? current)
    {
        List<TableColumnSchema>? columns = Columns;

        if (columns is null || current is null)
            return;

        List<TableColumnSchema>? renamed = null;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            for (int j = 0; j < current.Count; j++)
            {
                TableColumnSchema candidate = current[j];

                if (!string.Equals(candidate.Id, column.Id, StringComparison.Ordinal))
                    continue;

                if (!string.Equals(candidate.Name, column.Name, StringComparison.Ordinal))
                {
                    renamed ??= [.. columns];
                    renamed[i] = column.WithName(candidate.Name);
                }

                break;
            }
        }

        if (renamed is not null)
            Columns = renamed;
    }
}
