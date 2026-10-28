
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>What an <c>INSERT … ON CONFLICT</c> does with a proposed row whose arbiter key is taken.</summary>
internal enum OnConflictAction
{
    /// <summary><c>DO NOTHING</c>: the proposed row is skipped.</summary>
    DoNothing,

    /// <summary><c>DO UPDATE SET … [WHERE …]</c>: the stored row is updated, or skipped when the WHERE is false.</summary>
    DoUpdate,

    /// <summary>
    /// <c>UPSERT INTO t …</c>: <see cref="DoUpdate"/> on the primary key, with a SET list that assigns
    /// every listed column outside the key from <c>EXCLUDED</c>. The SET list is built against the table
    /// once it is open; see <see cref="RowUpserter"/>.
    /// </summary>
    Upsert,
}

/// <summary>
/// The <c>ON CONFLICT</c> clause of one INSERT statement, read from its <see cref="NodeType.OnConflict"/>
/// node. It holds names and expression trees only: the arbiter index, the column checks and the
/// privilege checks need the open table, and run in <see cref="RowUpserter"/> on every attempt.
///
/// <para>The SET expressions and the WHERE read two rows. A bare <c>c</c> or <c>t.c</c> is the stored
/// row, and <c>excluded.c</c> is the proposed row, after defaults, coercion and the BEFORE INSERT
/// triggers.</para>
/// </summary>
internal sealed class OnConflictClause
{
    /// <summary>The qualifier that names the proposed row in the SET list and the WHERE.</summary>
    internal const string ExcludedQualifier = "excluded";

    public OnConflictAction Action { get; }

    /// <summary>The <c>ON CONFLICT (cols)</c> target, or null.</summary>
    public IReadOnlyList<string>? TargetColumns { get; }

    /// <summary>The <c>ON CONFLICT ON CONSTRAINT name</c> target, or null.</summary>
    public string? ConstraintName { get; }

    /// <summary>The DO UPDATE SET list, keyed case-insensitively by column. Null for the other actions.</summary>
    public Dictionary<string, NodeAst>? SetValues { get; }

    /// <summary>The DO UPDATE WHERE condition, or null.</summary>
    public NodeAst? Where { get; }

    /// <summary>
    /// For <see cref="OnConflictAction.Upsert"/>: the statement's explicit column list, or null when it
    /// names none and so writes every column.
    /// </summary>
    public IReadOnlyList<string>? UpsertColumns { get; }

    private OnConflictClause(
        OnConflictAction action,
        IReadOnlyList<string>? targetColumns,
        string? constraintName,
        Dictionary<string, NodeAst>? setValues,
        NodeAst? where,
        IReadOnlyList<string>? upsertColumns)
    {
        Action = action;
        TargetColumns = targetColumns;
        ConstraintName = constraintName;
        SetValues = setValues;
        Where = where;
        UpsertColumns = upsertColumns;
    }

    /// <summary>True when a taken key leads to an update, so the statement also needs UPDATE.</summary>
    public bool Updates => Action is OnConflictAction.DoUpdate or OnConflictAction.Upsert;

    /// <summary>
    /// The clause of an <see cref="NodeType.Insert"/> or <see cref="NodeType.InsertSelect"/> node, or
    /// null when the statement has none.
    /// </summary>
    public static OnConflictClause? Of(NodeAst insertAst)
    {
        if (insertAst.extendedThree is not { nodeType: NodeType.OnConflict } node)
            return null;

        switch (node.yytext)
        {
            case sqlParser.OnConflictUpsert:
                {
                    List<string>? columns = null;

                    if (insertAst.rightAst is not null)
                    {
                        columns = new();
                        SQLExecutorBaseCreator.GetIdentifierList(insertAst.rightAst, columns);
                    }

                    return new(OnConflictAction.Upsert, null, null, null, null, columns);
                }

            case sqlParser.OnConflictNothing:
            case sqlParser.OnConflictUpdate:
                {
                    List<string>? targetColumns = null;
                    string? constraintName = null;

                    if (node.leftAst is { nodeType: NodeType.OnConflictConstraint } constraint)
                    {
                        constraintName = constraint.yytext;
                    }
                    else if (node.leftAst is not null)
                    {
                        targetColumns = new();
                        SQLExecutorBaseCreator.GetIdentifierList(node.leftAst, targetColumns);
                    }

                    if (node.yytext == sqlParser.OnConflictNothing)
                        return new(OnConflictAction.DoNothing, targetColumns, constraintName, null, null, null);

                    List<(string, NodeAst)> items = new();
                    SQLExecutorUpdateCreator.GetUpdateItemList(node.rightAst!, items);

                    Dictionary<string, NodeAst> setValues = new(items.Count, StringComparer.OrdinalIgnoreCase);
                    foreach ((string column, NodeAst value) in items)
                        setValues[column] = value;

                    return new(OnConflictAction.DoUpdate, targetColumns, constraintName, setValues, node.extendedOne, null);
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown ON CONFLICT action '" + node.yytext + "'");
        }
    }

    /// <summary>
    /// A copy whose SET list and WHERE are <paramref name="setValues"/> and <paramref name="where"/>:
    /// the same clause after its uncorrelated subqueries were replaced by their values.
    /// </summary>
    public OnConflictClause WithAssignments(Dictionary<string, NodeAst>? setValues, NodeAst? where) =>
        new(Action, TargetColumns, ConstraintName, setValues, where, UpsertColumns);
}
