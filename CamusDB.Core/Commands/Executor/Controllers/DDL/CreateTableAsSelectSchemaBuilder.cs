
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Derives the schema a <c>CREATE TABLE … AS SELECT</c> should create from the source query's output
/// columns.
///
/// <para>The column types come from the same derivation that produces a query's client-facing column
/// metadata, so a CTAS column always has the type a plain <c>SELECT</c> of that expression would
/// report. Nothing else is inherited: constraints, indexes, defaults, NOT NULL and comments belong to
/// the source table, not to the shape of its result, and standard CTAS does not copy them.</para>
///
/// <para>A derived query has no key of its own, and the engine requires every table to have a primary
/// key, so one is <b>synthesized</b> — see <see cref="ChooseSynthesizedKeyName"/>. Reusing a projected
/// column as the key instead would be unsafe: a projection is under no obligation to be unique (a join
/// or a non-distinct projection repeats values), and the copy would fail partway with a duplicate-key
/// error.</para>
/// </summary>
internal static class CreateTableAsSelectSchemaBuilder
{
    /// <summary>Preferred name for the synthesized primary key, when the query does not use it.</summary>
    private const string PreferredKeyName = "id";

    /// <summary>
    /// Builds the column and constraint definitions for the new table. <paramref name="derived"/> is
    /// the source query's output schema and drives the column list; <paramref name="projections"/> is
    /// the expression list that produced it, consulted only to reject projections that cannot become
    /// a column. The two must come from the same (post-rewrite) query or they can disagree.
    /// </summary>
    public static (ColumnInfo[] Columns, ConstraintInfo[] Constraints, string KeyName) Build(
        IReadOnlyList<NodeAst> projections,
        IReadOnlyList<DerivedColumnSchema> derived)
    {
        RejectUnusableProjections(projections);

        if (derived.Count == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "CREATE TABLE ... AS SELECT requires the source query to project at least one column");

        RejectQualifiedNames(derived);
        RejectDuplicateNames(derived);

        string keyName = ChooseSynthesizedKeyName(derived);

        ColumnInfo[] columns = new ColumnInfo[derived.Count + 1];

        // The synthesized key leads the table, and is generated per row rather than supplied by the
        // copy — the source query has no value to put there.
        columns[0] = new ColumnInfo(keyName, ColumnType.Id, notNull: true, defaultFunction: "gen_id");

        for (int i = 0; i < derived.Count; i++)
        {
            // Nullable with no default: a result column carries no such declaration, and inventing one
            // could reject rows the source query happily produced.
            //
            // String/Bytes columns are created unbounded (the engine's default cap) rather than
            // inheriting a source column's declared length. That cap is far larger than any declared
            // length, so a copy can never be truncated or rejected by it; the only cost is that the
            // new table is more permissive than the one it was copied from.
            columns[i + 1] = new ColumnInfo(derived[i].Name, derived[i].Type);
        }

        ConstraintInfo[] constraints =
        [
            new(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo(keyName, OrderType.Ascending)])
        ];

        return (columns, constraints, keyName);
    }

    /// <summary>
    /// Rejects projections whose result cannot be turned into a named, typed column: an expression
    /// with no alias (whose output name would be its ordinal position — a table with a column called
    /// "0" is not what anyone means) and a bare NULL literal (which has no type to declare).
    /// </summary>
    private static void RejectUnusableProjections(IReadOnlyList<NodeAst> projections)
    {
        for (int i = 0; i < projections.Count; i++)
        {
            NodeAst expression = projections[i];
            NodeAst target = QueryExpressionClassifier.UnwrapAlias(expression);

            if (target.nodeType == NodeType.Null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Cannot determine a column type for output column {i + 1} of CREATE TABLE ... AS SELECT " +
                    "because it is NULL; add an explicit CAST");

            // A plain identifier names itself and * expands to real column names; anything else needs
            // an alias to have a name at all.
            if (expression.nodeType is NodeType.ExprAlias or NodeType.Identifier
                || target.nodeType == NodeType.ExprAllFields)
                continue;

            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Output column {i + 1} of CREATE TABLE ... AS SELECT is an expression with no name; " +
                "add an alias (AS ...) so the created column can be named");
        }
    }

    /// <summary>
    /// A <c>SELECT *</c> over more than one source produces <c>{alias}.{column}</c> output names,
    /// which are not usable column names. The user has to list the columns and alias them.
    /// </summary>
    private static void RejectQualifiedNames(IReadOnlyList<DerivedColumnSchema> derived)
    {
        foreach (DerivedColumnSchema column in derived)
        {
            if (column.Name.Contains('.'))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "CREATE TABLE ... AS SELECT * is not supported over a join because the output " +
                    $"column '{column.Name}' is qualified; list the columns explicitly with aliases");
        }
    }

    /// <summary>
    /// Two output columns with one name is legal in a query result but not in a table. Compared
    /// case-insensitively because column lookup is case-insensitive, so <c>a</c> and <c>A</c> would
    /// collide once persisted.
    /// </summary>
    private static void RejectDuplicateNames(IReadOnlyList<DerivedColumnSchema> derived)
    {
        HashSet<string> seen = new(derived.Count, StringComparer.OrdinalIgnoreCase);

        foreach (DerivedColumnSchema column in derived)
        {
            if (!seen.Add(column.Name))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"CREATE TABLE ... AS SELECT produces more than one output column named " +
                    $"'{column.Name}'; alias them apart");
        }
    }

    /// <summary>
    /// Picks a name for the synthesized key that the query's own output does not already use, trying
    /// <c>id</c>, then <c>id2</c>, <c>id3</c>, … Comparison is case-insensitive to match column
    /// lookup, so a projected <c>ID</c> also pushes the key to the next candidate.
    ///
    /// <para>The candidates deliberately avoid the <c>_id</c> shape: <c>_id</c> is a reserved column
    /// name the create-table validator refuses, so a key named that way would fail every CTAS whose
    /// source happens to project an <c>id</c>.</para>
    /// </summary>
    private static string ChooseSynthesizedKeyName(IReadOnlyList<DerivedColumnSchema> derived)
    {
        HashSet<string> used = new(derived.Count, StringComparer.OrdinalIgnoreCase);
        foreach (DerivedColumnSchema column in derived)
            used.Add(column.Name);

        if (!used.Contains(PreferredKeyName))
            return PreferredKeyName;

        for (int suffix = 2; ; suffix++)
        {
            string candidate = PreferredKeyName + suffix;
            if (!used.Contains(candidate))
                return candidate;
        }
    }
}
