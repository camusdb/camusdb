/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// One expression key part of an expression index, in the persisted form that
/// <see cref="TableIndexSchema.KeyExpressions"/> holds (<c>CREATE INDEX i ON t ((lower(email)))</c>).
///
/// <para><b>Stored as SQL text with column ids.</b> <see cref="Sql"/> is the canonical rendering of
/// the expression in which every column reference is a <see cref="StoredColumnRef"/> token
/// (<c>lower(__camus_col_{id})</c>), the form view bodies use. A RENAME COLUMN therefore changes
/// nothing here. The text is used in place of the AST wire codec because that codec writes node
/// types as implicit enum numbers, which a new node type in the middle of the enum would shift.</para>
///
/// <para><b>The result type is stored.</b> <see cref="Type"/> is fixed when the index is created and
/// decides the key encoding of every entry. A later build that infers the type of a function in another
/// way must not change how an existing index encodes or decodes its keys.</para>
///
/// <para>At <see cref="Position"/>, <see cref="TableIndexSchema.ColumnIds"/> holds
/// <see cref="ColumnIdMarker"/>, which matches no column, so code that reads only
/// <c>ColumnIds</c> never mistakes the part for a plain column.</para>
/// </summary>
public sealed class IndexKeyExpression
{
    /// <summary>
    /// The value of <see cref="TableIndexSchema.ColumnIds"/> at an expression position. A column id is
    /// an ObjectId or a short base-62 string, so it can never be this value. A build that does not
    /// know expression indexes resolves it to no column name and fails on the first write to the
    /// index, which is louder than an index built over the wrong values.
    /// </summary>
    public const string ColumnIdMarker = "~expr";

    /// <summary>Key position of this part in the index, zero-based.</summary>
    public int Position { get; }

    /// <summary>Canonical SQL text with <see cref="StoredColumnRef"/> tokens for column references.</summary>
    public string Sql { get; }

    /// <summary>Result type of the expression: the key type at <see cref="Position"/>.</summary>
    public ColumnType Type { get; }

    /// <summary>
    /// Ids of the columns the expression reads, without duplicates. DROP COLUMN refuses each of them,
    /// and the backfill decodes only them.
    /// </summary>
    public string[] ColumnIds { get; }

    [JsonConstructor]
    public IndexKeyExpression(int position, string sql, ColumnType type, string[] columnIds)
    {
        Position = position;
        Sql = sql;
        Type = type;
        ColumnIds = columnIds ?? [];
    }
}
