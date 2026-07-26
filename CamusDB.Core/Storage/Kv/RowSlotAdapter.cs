
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Bridges the legacy name-keyed row representation (<see cref="IReadOnlyDictionary{TKey,TValue}"/> of
/// column name → <see cref="ColumnValue"/>) to the positional <see cref="ValueSlot"/> array the
/// <see cref="CompiledRowCodec"/> encodes. It exists only so DML can move to positional encoding while
/// its inputs are still name-keyed; it is a transitional seam, and should be retired once every write
/// path produces slots directly (at which point the name-keyed dictionary and its per-cell hashing
/// disappear from the write hot path).
///
/// <para>
/// The mapping reproduces the exact visibility contract of the old serializer's per-column loop: a
/// column that is not writable under its online <see cref="SchemaElementState"/>, and a column absent
/// from the supplied row, both encode as <see cref="ValueSlot.Null"/>. Defaults are assumed already
/// resolved into the row upstream (SQL insert/update creation), matching the legacy encoder which never
/// injected defaults itself.
/// </para>
/// </summary>
internal static class RowSlotAdapter
{
    /// <summary>
    /// Projects <paramref name="row"/> onto positional slots in <paramref name="columns"/> order. The
    /// returned array length equals <paramref name="columns"/>.Count and lines up 1:1 with the layout
    /// <see cref="CompiledRowCodec.Build"/> compiles from the same column list.
    /// </summary>
    public static ValueSlot[] FromRow(IReadOnlyList<TableColumnSchema> columns, IReadOnlyDictionary<string, ColumnValue> row)
    {
        ValueSlot[] slots = new ValueSlot[columns.Count];

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (!SchemaElementStateRules.IsWritable(column))
            {
                slots[i] = ValueSlot.Null;
                continue;
            }

            if (!row.TryGetValue(column.Name, out ColumnValue? value) || value.Type == ColumnType.Null)
            {
                slots[i] = ValueSlot.Null;
                continue;
            }

            // A present, non-null value must match the column's declared physical type. The positional
            // codec writes each cell using the column type, so a mismatch would otherwise be encoded as
            // silent garbage rather than rejected — preserve the old write-time contract here.
            if (value.Type != column.Type)
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownType,
                    $"Type {value.Type} cannot be assigned to {column.Name} ({column.Type})");

            slots[i] = ValueSlot.FromColumnValue(value);
        }

        return slots;
    }
}
