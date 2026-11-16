/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The key columns of one side of a hash join where one or more keys are expressions of the row of
/// that side, as in <c>JOIN sidedef sd ON sd.id = CASE WHEN s.side = 0 THEN l.right_sidedef ELSE
/// l.left_sidedef END</c>.
///
/// <para>The list is the <see cref="Models.Plans.HashJoinNode.ProbeKeyColumns"/> (left side) or the
/// <see cref="Models.Plans.HashJoinNode.BuildKeyColumns"/> (right side) of the node, so every reader of
/// the key names (the cost estimate, the build and probe of the in-memory join, the partitioning and
/// the partition joins of the Grace join) keeps its signature.
/// <see cref="JoinKeyExtractor.TryExtractKeyInto"/> recognises the list and computes each expression
/// key from the row. A column key keeps its name; an expression key has a placeholder name
/// (<see cref="PlaceholderName"/>) that no row carries. A left row is qualified, so a left expression
/// reads it directly. A right row is unqualified, so a right expression reads it through a map from
/// the text of each identifier to the key of its cell (<see cref="RowKeyView"/>).</para>
///
/// <para>The rules that keep the key equal to what the <c>ON</c> predicate accepts:
/// <list type="bullet">
///   <item>The expression reads only the row of its side and has no parameter and no volatile call
///   (<see cref="JoinSideExpressions"/>): the plan is cached and reused with other parameter values,
///   and the list evaluates with no parameters.</item>
///   <item>The static type of the expression is the type of the other side, as for a column pair
///   (<see cref="JoinEquiJoinAnalyzer"/>). A computed value of another numeric type is rewritten into
///   the key type exactly (<see cref="NumericBoundNormalizer.NormalizeInListItem"/>), and a value that
///   no key of that type can equal (2.5 against an integer key) matches nothing.</item>
///   <item>A NULL value matches nothing, as for a column key.</item>
///   <item>An evaluation error is raised, as the <c>ON</c> predicate raises it in a nested loop. A
///   hash join computes the key for every left row, also for a left row that a nested loop pairs with
///   no right row, so an error can come from a row that the nested loop never tests.</item>
/// </list>
/// The <c>ON</c> predicate still tests each pair the hash table returns.</para>
///
/// <para>The expressions do not travel: a broadcast probe ships the key names to other nodes, so the
/// broadcast planner declines a node with this list, and a merge join is never planned with it.</para>
/// </summary>
internal sealed class JoinKeyExpressionList : IReadOnlyList<string>
{
    private readonly string[] names;

    private readonly NodeAst?[] expressions;

    private readonly ColumnType?[] keyTypes;

    private readonly IReadOnlyDictionary<string, string>?[] rowKeys;

    /// <param name="names">The key names; a placeholder for an expression key.</param>
    /// <param name="expressions">The expression of each key, or null for a column key.</param>
    /// <param name="keyTypes">The type each expression key is rewritten into, or null.</param>
    /// <param name="rowKeys">
    /// For each expression of an unqualified right row, the cell key of each identifier text; null for
    /// a column key and for an expression of the qualified left row.
    /// </param>
    public JoinKeyExpressionList(
        string[] names,
        NodeAst?[] expressions,
        ColumnType?[] keyTypes,
        IReadOnlyDictionary<string, string>?[] rowKeys)
    {
        this.names = names;
        this.expressions = expressions;
        this.keyTypes = keyTypes;
        this.rowKeys = rowKeys;
    }

    /// <summary>The name of the expression key at <paramref name="position"/>. No row has a cell with this name.</summary>
    public static string PlaceholderName(int position) => "$key" + position;

    /// <summary>The expression of key <paramref name="index"/>, or null for a column key.</summary>
    public NodeAst? ExpressionAt(int index) => expressions[index];

    public string this[int index] => names[index];

    public int Count => names.Length;

    public IEnumerator<string> GetEnumerator() => ((IEnumerable<string>)names).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => names.GetEnumerator();

    /// <summary>
    /// Fills <paramref name="destination"/> with the key of <paramref name="row"/>, a qualified left row
    /// or an unqualified right row as the list was built for. False when a component is NULL, is absent, or can equal no key of its
    /// type; the row then matches nothing.
    /// </summary>
    public bool TryExtractInto(IReadOnlyDictionary<string, ColumnValue> row, ColumnValue[] destination)
    {
        for (int i = 0; i < names.Length; i++)
        {
            NodeAst? expression = expressions[i];

            if (expression is null)
            {
                if (!row.TryGetValue(names[i], out ColumnValue? cell) || cell.Type == ColumnType.Null)
                    return false;

                destination[i] = cell;
                continue;
            }

            IReadOnlyDictionary<string, ColumnValue> source = rowKeys[i] is { } keys ? new RowKeyView(row, keys) : row;
            ColumnValue value = SqlExecutor.EvalExpr(expression, source, parameters: null);
            if (value.Type == ColumnType.Null)
                return false;

            if (keyTypes[i] is { } keyType && value.Type != keyType)
            {
                switch (NumericBoundNormalizer.NormalizeInListItem(value, keyType, out ColumnValue converted))
                {
                    case NumericBoundNormalizer.ItemOutcome.Converted:
                        value = converted;
                        break;

                    case NumericBoundNormalizer.ItemOutcome.Drop:
                        return false;

                    default:
                        // The planner takes the expression as a key only when its static type is the
                        // key type, so a value of a type with no exact rewrite means the two disagree.
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInternalOperation,
                            $"Join key expression returned {value.Type} for a key of type {keyType}");
                }
            }

            destination[i] = value;
        }

        return true;
    }

    /// <summary>
    /// An unqualified right row seen through the identifiers of a key expression: <c>p.u</c> reads the
    /// cell <c>u</c>. The evaluator reads a column only through <see cref="TryGetValue"/>.
    /// </summary>
    private sealed class RowKeyView(IReadOnlyDictionary<string, ColumnValue> row, IReadOnlyDictionary<string, string> keys)
        : IReadOnlyDictionary<string, ColumnValue>
    {
        public bool TryGetValue(string key, [System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out ColumnValue value)
        {
            if (keys.TryGetValue(key, out string? rowKey))
                return row.TryGetValue(rowKey, out value);

            value = null;
            return false;
        }

        public ColumnValue this[string key] =>
            TryGetValue(key, out ColumnValue? value) ? value : throw new KeyNotFoundException(key);

        public bool ContainsKey(string key) => TryGetValue(key, out _);

        public IEnumerable<string> Keys => keys.Keys;

        public IEnumerable<ColumnValue> Values => keys.Keys.Select(k => this[k]);

        public int Count => keys.Count;

        public IEnumerator<KeyValuePair<string, ColumnValue>> GetEnumerator() =>
            keys.Keys.Select(k => new KeyValuePair<string, ColumnValue>(k, this[k])).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
