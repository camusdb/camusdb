/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Builds the NULL-padded output row of a left outer join for a left row that matched nothing.
///
/// <para><b>Why the layout is eager.</b> Every join operator builds its output layout from the first
/// right row it sees. A left outer join may have to emit a padded row before any right row exists
/// (a zero-row right table, or a first left row with no match), so the right half of the layout is
/// computed up front from metadata instead. One instance is built per operator execution, and the
/// operator uses its layout for matched rows and padded rows alike, so a downstream stage sees one
/// key set whichever kind of row it reads.</para>
///
/// <para><b>Why the key set must equal the scanner's.</b> The right key list is derived from the same
/// inputs the leaf scanner decodes a right row from: the required-column set for the alias (null
/// means every readable column), the schema version the alias reads under, and for a derived table
/// its output row keys. A real right row therefore carries exactly these keys. If it did not, the
/// merge would throw "shape diverged" on the first matched row; that guard is kept on purpose, so a
/// drift between this list and the scanner is loud rather than a silent half-NULL row.</para>
///
/// <para>Not thread-safe; one instance belongs to one operator execution.</para>
/// </summary>
internal sealed class OuterJoinPadding
{
    private readonly string rightAlias;

    /// <summary>The right keys exactly as a right row carries them: bare for a table, the row key for a derived table.</summary>
    private readonly string[] rightKeys;

    private RowLayout? joinLayout;

    private Dictionary<string, int>? rightOrdinalMap;

    /// <summary>Ordinals of the right slots in <see cref="joinLayout"/>, filled with NULL by <see cref="Pad"/>.</summary>
    private int[]? rightOrdinals;

    private OuterJoinPadding(string rightAlias, string[] rightKeys)
    {
        this.rightAlias = rightAlias;
        this.rightKeys = rightKeys;
    }

    /// <summary>
    /// Computes the right key list for <paramref name="right"/> under <paramref name="plan"/>. A base
    /// table contributes every readable column of the schema version the alias reads under, kept to
    /// the alias's required-column set when the plan has one; a derived table contributes its output
    /// row keys. Awaits only when the alias reads under a schema version that is not the current one.
    /// </summary>
    internal static async ValueTask<OuterJoinPadding> CreateAsync(BoundJoinRightSource right, QueryPlan plan)
    {
        List<string> keys = new();

        if (right.Table is { } table)
        {
            IReadOnlySet<string>? required = JoinAliasMetadata.GetRequiredColumnsForAlias(plan, table.Alias);
            int version = JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, table.Alias);
            TableSchemaHistory history = await table.Table.Schema
                .GetSchemaHistoryAsync(plan.Ticket.TxnState.TransactionId, version)
                .ConfigureAwait(false);

            foreach (TableColumnSchema column in history.Columns ?? [])
            {
                if (!SchemaElementStateRules.IsReadable(column))
                    continue;

                if (required is null || required.Contains(column.Name))
                    keys.Add(column.Name);
            }
        }
        else
        {
            foreach (DerivedColumnSchema column in right.Derived!.Columns)
                keys.Add(column.RowKey);
        }

        return new OuterJoinPadding(right.Alias, keys.ToArray());
    }

    /// <summary>
    /// Returns the join layout and the right-key ordinal map, building both from the qualified keys
    /// of the first left row this instance sees. Every later call returns the same pair, so the
    /// operator can replace its lazy <c>??=</c> layout with this one for the whole execution.
    /// </summary>
    internal (RowLayout Layout, Dictionary<string, int> RightOrdinalMap) Bind(IReadOnlyDictionary<string, ColumnValue> leftQualified)
    {
        if (joinLayout is not null)
            return (joinLayout, rightOrdinalMap!);

        joinLayout = QueryRowMerger.BuildJoinLayout(leftQualified, rightKeys, rightAlias);
        rightOrdinalMap = QueryRowMerger.BuildRightKeyOrdinalMap(rightKeys, rightAlias, joinLayout);

        rightOrdinals = new int[rightKeys.Length];
        for (int i = 0; i < rightKeys.Length; i++)
            rightOrdinals[i] = rightOrdinalMap[rightKeys[i]];

        return (joinLayout, rightOrdinalMap);
    }

    /// <summary>
    /// The padded row for a left row with no accepted right row: the left values in their slots and
    /// the shared NULL value in every right slot. One array allocation per padded row, no dictionary.
    /// The left keys must be those <see cref="Bind"/> was given, else the layout cannot place them.
    /// </summary>
    internal QueryRow Pad(IReadOnlyDictionary<string, ColumnValue> leftQualified)
    {
        (RowLayout layout, _) = Bind(leftQualified);

        ColumnValue[] values = new ColumnValue[layout.Count];
        int placements = 0;

        foreach (KeyValuePair<string, ColumnValue> entry in leftQualified)
        {
            int ord = layout.IndexOf(entry.Key);
            if (ord < 0 || values[ord] is not null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Padded join row shape diverged from join layout at left key '{entry.Key}'");

            values[ord] = entry.Value;
            placements++;
        }

        int[] ordinals = rightOrdinals!;
        for (int i = 0; i < ordinals.Length; i++)
        {
            if (values[ordinals[i]] is not null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Padded join row shape diverged from join layout: right slot '{layout.NameAt(ordinals[i])}' already placed");

            values[ordinals[i]] = ColumnValue.Null;
            placements++;
        }

        if (placements != layout.Count)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Padded join row shape diverged from join layout: expected {layout.Count} columns, placed {placements}");

        return new QueryRow(default(ObjectIdValue), layout, values);
    }
}
