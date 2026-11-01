/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Jsonb;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// The KV keys of a GIN index (<see cref="IndexType.Inverted"/>). Each entry of a document
/// (<see cref="JsonbGinEntries"/>) is stored in the non-unique index shape, as if it were the value of a
/// one-column ordered index over a string: the key is <c>[entry text, row id]</c>, and
/// <see cref="KvKeyBuilder.BuildNonUniqueIndexKey"/> appends the row id hex. So the entries live in the
/// index's own <c>|i:{indexId}</c> key space, inside the table's placement group, and DROP INDEX, DROP
/// TABLE, TRUNCATE, branches and key-range registration treat them as any index. No storage revision
/// bump is needed: the key shape is the existing one.
///
/// <para>Every write path (insert, update, delete, backfill) builds its entries here, so the key form
/// is in one place. A row's entries are written and removed in the row's own transaction, through
/// <see cref="KvTransaction"/>, as for any index; a document with many leaves costs one mutation per
/// distinct entry and is refused by the transaction's mutation limit, never truncated.</para>
/// </summary>
internal static class JsonbGinIndexKeys
{
    /// <summary>The decode types of a GIN entry key: the entry text, then the row id.</summary>
    public static readonly ColumnType[] KeyTypes = [ColumnType.String, ColumnType.Id];

    /// <summary>The entry key of one entry of one row.</summary>
    public static CompositeColumnValue Key(string entry, ColumnValue rowIdValue) =>
        new([new ColumnValue(ColumnType.String, entry), rowIdValue]);

    /// <summary>A scan bound over the entry text alone (the row id part is left open).</summary>
    public static CompositeColumnValue Bound(string entryOrPrefix) =>
        new([new ColumnValue(ColumnType.String, entryOrPrefix)]);

    /// <summary>
    /// The distinct entries of the row's document in <paramref name="index"/>'s column, or null when the
    /// cell is NULL or absent (a NULL document has no entries).
    /// </summary>
    public static HashSet<string>? EntriesOf(TableIndexSchema index, IReadOnlyDictionary<string, ColumnValue> row) =>
        EntriesOf(index.Columns[0], row);

    /// <summary>The distinct entries of the document in <paramref name="columnName"/>, or null for a NULL cell.</summary>
    public static HashSet<string>? EntriesOf(string columnName, IReadOnlyDictionary<string, ColumnValue> row)
    {
        if (!row.TryGetValue(columnName, out ColumnValue? value) || value.Type != ColumnType.Jsonb)
            return null;

        return JsonbGinEntries.Entries(JsonbDocument.Item(value.BytesValue));
    }

    /// <summary>
    /// Refuses one row whose document needs more GIN entry writes than a transaction may make, so the
    /// error names the index and the count. Without it the transaction's own mutation check refuses the
    /// row with a message that names only the transaction. The row write itself is one more mutation,
    /// hence <c>&gt;=</c>. A <paramref name="mutationLimit"/> of zero or less means unlimited.
    /// </summary>
    public static void CheckEntryWrites(TableIndexSchema index, int entryWrites, int mutationLimit) =>
        CheckEntryWrites(index.Name, index.Columns[0], entryWrites, mutationLimit);

    /// <inheritdoc cref="CheckEntryWrites(TableIndexSchema, int, int)"/>
    public static void CheckEntryWrites(string indexName, string columnName, int entryWrites, int mutationLimit)
    {
        if (mutationLimit <= 0 || entryWrites < mutationLimit)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.TransactionMutationLimitExceeded,
            $"The document in column '{columnName}' needs {entryWrites} entries in GIN index '{indexName}', " +
            $"and a transaction allows at most {mutationLimit} mutations; store a smaller document or raise max_mutations_per_transaction");
    }

    /// <summary>The row id value every entry key of a row ends with.</summary>
    public static ColumnValue RowIdValue(ObjectIdValue rowId) => new(ColumnType.Id, rowId.ToString());
}
