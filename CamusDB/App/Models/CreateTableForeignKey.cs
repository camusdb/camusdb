/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// One foreign key in a <c>/create-table</c> request. The controller only copies these fields into a
/// <c>ForeignKeyInfo</c>; the validator and the engine check them, so the rules are the same as for
/// <c>CREATE TABLE ... REFERENCES</c> in SQL.
/// </summary>
public sealed class CreateTableForeignKey
{
    /// <summary>
    /// The constraint name. Required: unlike the SQL path, the request has no parse step that picks
    /// a default name.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>The referencing columns of the new table, in constraint order.</summary>
    public string[]? Columns { get; set; }

    /// <summary>The referenced table, in the same database. A qualified name is refused.</summary>
    public string? ReferencedTable { get; set; }

    /// <summary>
    /// The referenced columns, paired by position with <see cref="Columns"/>. Null or empty means the
    /// primary key of the referenced table.
    /// </summary>
    public string[]? ReferencedColumns { get; set; }

    /// <summary>
    /// <c>"no action"</c> (the default when null) or <c>"restrict"</c>. <c>"cascade"</c>,
    /// <c>"set null"</c> and <c>"set default"</c> are accepted words that the engine refuses as not
    /// supported. Case and an underscore in place of the space do not matter.
    /// </summary>
    public string? OnDelete { get; set; }

    /// <summary>The action on an update of a referenced key; the same words as <see cref="OnDelete"/>.</summary>
    public string? OnUpdate { get; set; }
}
