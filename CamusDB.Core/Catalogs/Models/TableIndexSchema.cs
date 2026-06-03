
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// In-memory view of an index as seen from a <c>TableDescriptor</c>. Carries an online
/// <see cref="State"/> so DML/reads can honor a half-built index during a backfill (DS9).
/// The durable form is <c>DatabaseIndexObject</c> in <c>SystemSchema</c>; this is the
/// name/column/type/state projection used at query and write time.
/// </summary>
public sealed class TableIndexSchema
{
    /// <summary>
    /// The name of the index (matches the key in TableDescriptor.Indexes).
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// The list of columns that make up the index
    /// </summary>
    public string[] Columns { get; }

    /// <summary>
    /// The type of index
    /// </summary>
    public IndexType Type { get; }

    /// <summary>
    /// Online schema-change state of the index.
    /// </summary>
    public SchemaElementState State { get; }

    public TableIndexSchema(string name, string[] columns, IndexType type, SchemaElementState state = SchemaElementState.Public)
    {
        Name = name;
        Columns = columns;
        Type = type;
        State = state;
    }
}
