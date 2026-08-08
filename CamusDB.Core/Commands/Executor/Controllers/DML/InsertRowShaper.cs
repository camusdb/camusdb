
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// Turns one statement's positional target column list plus a row of values into the row dictionary
/// the insert path writes, applying declared-type coercion and column defaults.
///
/// <para>Both <c>INSERT … VALUES</c> and <c>INSERT … SELECT</c> shape their rows through this one
/// class so the two forms cannot drift: a value that would be coerced, defaulted, or rejected one
/// way in a VALUES list behaves identically when it arrives from a query.</para>
///
/// <para>The per-field schema lookup and the "has a default but was not listed" set are resolved
/// once in <see cref="Create"/> and reused for every row of the statement, because the target
/// columns are fixed for the whole statement while the row count is not.</para>
/// </summary>
internal sealed class InsertRowShaper
{
    private readonly IReadOnlyList<string> fields;

    /// <summary>Per-target-column schema entry (null when the name is not a column) and constant default.</summary>
    private readonly (TableColumnSchema? Schema, ColumnValue Default)[] fieldMeta;

    /// <summary>
    /// Columns carrying a default that the statement's target list does not mention. A function
    /// default has a null <see cref="TableColumnSchema.DefaultValue"/>, so both kinds are collected.
    /// </summary>
    private readonly List<TableColumnSchema> extraDefaults;

    private InsertRowShaper(
        IReadOnlyList<string> fields,
        (TableColumnSchema?, ColumnValue)[] fieldMeta,
        List<TableColumnSchema> extraDefaults)
    {
        this.fields = fields;
        this.fieldMeta = fieldMeta;
        this.extraDefaults = extraDefaults;
    }

    /// <summary>Number of values one row must supply — the arity every value row is checked against.</summary>
    public int FieldCount => fields.Count;

    /// <summary>The target column names, in the order values are supplied.</summary>
    public IReadOnlyList<string> Fields => fields;

    public static InsertRowShaper Create(TableSchema schema, IReadOnlyList<string> fields)
    {
        List<TableColumnSchema> schemaColumns = schema.Columns!;

        Dictionary<string, TableColumnSchema> colSchemaByName = new(schemaColumns.Count, StringComparer.Ordinal);
        foreach (TableColumnSchema col in schemaColumns)
            colSchemaByName[col.Name] = col;

        (TableColumnSchema? Schema, ColumnValue Default)[] fieldMeta = new (TableColumnSchema?, ColumnValue)[fields.Count];
        for (int i = 0; i < fields.Count; i++)
        {
            colSchemaByName.TryGetValue(fields[i], out TableColumnSchema? colDef);
            fieldMeta[i] = (colDef, colDef?.DefaultValue ?? ColumnValue.Null);
        }

        List<TableColumnSchema> extraDefaults = new();
        foreach (TableColumnSchema col in schemaColumns)
        {
            if ((col.DefaultValue is not null || col.DefaultFunction is not null) && !fields.Contains(col.Name))
                extraDefaults.Add(col);
        }

        return new InsertRowShaper(fields, fieldMeta, extraDefaults);
    }

    /// <summary>
    /// Builds one row. A null slot means "no value was supplied for this column" and takes the
    /// column's default; <paramref name="slots"/> must have exactly <see cref="FieldCount"/> entries.
    ///
    /// <para>The row is keyed case-insensitively on purpose: the statement may name a column in a
    /// different case than the schema does (<c>INSERT INTO t (UserName)</c> for a column stored as
    /// <c>username</c>), and the row is read back by the schema's own name during encoding — an
    /// ordinal dictionary would miss and silently write null.</para>
    /// </summary>
    public Dictionary<string, ColumnValue> ShapeRow(ColumnValue?[] slots)
    {
        Dictionary<string, ColumnValue> row = new(fields.Count + extraDefaults.Count, StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < fields.Count; i++)
        {
            ColumnValue val = slots[i] ?? fieldMeta[i].Default;
            if (fieldMeta[i].Schema is { } schema)
                val = CastScalarFunctions.CoerceToColumnType(val, schema);
            row[fields[i]] = val;
        }

        // Columns with a default that the target list omitted. A function default is evaluated here,
        // once per row, so each row gets a fresh value (e.g. a distinct gen_uuid_v7()); a constant
        // default is copied as-is.
        foreach (TableColumnSchema col in extraDefaults)
        {
            if (row.ContainsKey(col.Name))
                continue;

            row[col.Name] = col.DefaultFunction is not null
                ? ScalarFunctionEvaluator.EvaluateNullary(col.DefaultFunction)
                : col.DefaultValue!;
        }

        return row;
    }
}
