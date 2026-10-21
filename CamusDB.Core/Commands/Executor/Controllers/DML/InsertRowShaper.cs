
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
/// way in a VALUES list behaves identically when it arrives from a query. The typed row insert
/// (REST <c>/insert</c>, gRPC <c>InsertRow</c>) takes its omitted-column defaults from here too,
/// through <see cref="ApplyOmittedDefaults"/>.</para>
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
    /// Per-target-column flag: the statement supplies a value for this position but the value is
    /// discarded and the column takes its default instead. Set for identity columns under
    /// <c>OVERRIDING USER VALUE</c>; null when no position is ignored.
    /// </summary>
    private readonly bool[]? ignoredFields;

    /// <summary>
    /// Columns carrying a default that the statement's target list does not mention. A function
    /// default and a sequence default both have a null <see cref="TableColumnSchema.DefaultValue"/>,
    /// so all three kinds are collected.
    /// </summary>
    private readonly List<TableColumnSchema> extraDefaults;

    /// <summary>
    /// The sequence each omitted default draws from, <b>one entry per column</b> — with duplicates
    /// when two columns default from the same sequence, because shaping then consumes two values a
    /// row and a deduplicated list would buy one.
    ///
    /// <para>The insert path reserves against this before any row is shaped: a sequence default
    /// cannot be evaluated per row, because drawing a value is an asynchronous call and this class
    /// is synchronous by design.</para>
    /// </summary>
    public IReadOnlyList<string> DefaultSequenceIds { get; }

    /// <summary>
    /// True when an omitted default is a clock function (<c>DEFAULT now()</c>). The insert path then
    /// attaches the transaction and statement times to the parameters it shapes the rows with
    /// (<see cref="Functions.ClockScalarFunctions.AttachStatementClock(Models.Tickets.ExecuteSQLTicket)"/>),
    /// so every row of the statement takes the same default time.
    /// </summary>
    public bool DefaultsReadStatementClock { get; }

    private InsertRowShaper(
        IReadOnlyList<string> fields,
        (TableColumnSchema?, ColumnValue)[] fieldMeta,
        bool[]? ignoredFields,
        List<TableColumnSchema> extraDefaults,
        IReadOnlyList<string> defaultSequenceIds,
        bool defaultsReadStatementClock)
    {
        this.fields = fields;
        this.fieldMeta = fieldMeta;
        this.ignoredFields = ignoredFields;
        this.extraDefaults = extraDefaults;
        DefaultSequenceIds = defaultSequenceIds;
        DefaultsReadStatementClock = defaultsReadStatementClock;
    }

    /// <summary>Number of values one row must supply — the arity every value row is checked against.</summary>
    public int FieldCount => fields.Count;

    /// <summary>The target column names, in the order values are supplied.</summary>
    public IReadOnlyList<string> Fields => fields;

    /// <summary>
    /// Resolves the per-statement metadata for <paramref name="fields"/>.
    /// </summary>
    /// <param name="ignoredColumns">
    /// Target columns whose supplied values are discarded, so the column is defaulted as if the
    /// statement had omitted it — <c>OVERRIDING USER VALUE</c> on identity columns (see
    /// <see cref="SequenceDefaults.ApplyIdentityRules"/>). The row still has to supply a value at
    /// that position: only what happens to it changes. Null for none.
    /// </param>
    public static InsertRowShaper Create(TableSchema schema, IReadOnlyList<string> fields, IReadOnlySet<string>? ignoredColumns = null)
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

        bool[]? ignoredFields = null;

        if (ignoredColumns is { Count: > 0 })
        {
            for (int i = 0; i < fields.Count; i++)
            {
                if (ignoredColumns.Contains(fields[i]))
                    (ignoredFields ??= new bool[fields.Count])[i] = true;
            }
        }

        List<TableColumnSchema> extraDefaults = new();
        List<string> defaultSequenceIds = [];
        bool defaultsReadStatementClock = false;

        foreach (TableColumnSchema col in schemaColumns)
        {
            bool hasDefault = col.DefaultValue is not null
                || col.DefaultFunction is not null
                || col.DefaultSequenceId is not null;

            if (!hasDefault || (fields.Contains(col.Name) && !(ignoredColumns?.Contains(col.Name) ?? false)))
                continue;

            // A column still DeleteOnly, part-way through ADD COLUMN, may not be written yet: the insert
            // validation refuses a value for it, and the row stores a placeholder that the column's fill
            // replaces. Defaulting it here failed every INSERT into the table during that step, and
            // drew a sequence value nothing could keep.
            if (!SchemaElementStateRules.IsWritable(col))
                continue;

            extraDefaults.Add(col);

            if (col.DefaultSequenceId is { Length: > 0 } sequenceId)
                defaultSequenceIds.Add(sequenceId);

            if (col.DefaultFunction is { } function && ScalarFunctionEvaluator.IsStatementClockFunction(function))
                defaultsReadStatementClock = true;
        }

        return new InsertRowShaper(fields, fieldMeta, ignoredFields, extraDefaults, defaultSequenceIds, defaultsReadStatementClock);
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
    /// <param name="parameters">
    /// The statement's parameter dictionary, which carries the reserved sequence runs a sequence
    /// default draws from. May be null only for a statement whose table has no sequence default;
    /// one that has is refused rather than silently written as NULL.
    /// </param>
    public Dictionary<string, ColumnValue> ShapeRow(ColumnValue?[] slots, Dictionary<string, ColumnValue>? parameters = null)
    {
        Dictionary<string, ColumnValue> row = new(fields.Count + extraDefaults.Count, StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < fields.Count; i++)
        {
            // An ignored position leaves the column out of the row, so ApplyOmittedDefaults below
            // gives it its default.
            if (ignoredFields is not null && ignoredFields[i])
                continue;

            ColumnValue val = slots[i] ?? fieldMeta[i].Default;
            if (fieldMeta[i].Schema is { } schema)
                val = CastScalarFunctions.CoerceToColumnType(val, schema);
            row[fields[i]] = val;
        }

        ApplyOmittedDefaults(row, parameters);

        return row;
    }

    /// <summary>
    /// Adds to <paramref name="row"/> the default of every column the target list omitted: a
    /// constant default is copied as-is, a function default is evaluated here, once per row, so each
    /// row gets a fresh value (e.g. a distinct <c>gen_uuid_v7()</c>), and a sequence default takes
    /// the next value of the run reserved for the statement. A clock default (<c>now()</c>) reads the
    /// times in <paramref name="parameters"/>, so it is the same for every row of the statement. A
    /// column the row already holds is left alone.
    ///
    /// <para>Used by <see cref="ShapeRow"/> for SQL <c>INSERT</c>, and directly by the typed row
    /// insert, which receives a row that is already a dictionary. Sharing it is what makes a row
    /// written through either path take the same defaults.</para>
    ///
    /// <para><paramref name="row"/> must look keys up case-insensitively, as the dictionary
    /// <see cref="ShapeRow"/> builds does, or a column supplied under a different case is defaulted
    /// a second time.</para>
    /// </summary>
    /// <param name="parameters">
    /// The parameter dictionary carrying the reserved sequence runs, and the statement times when
    /// <see cref="DefaultsReadStatementClock"/> is set; see <see cref="ShapeRow"/>.
    /// </param>
    public void ApplyOmittedDefaults(Dictionary<string, ColumnValue> row, Dictionary<string, ColumnValue>? parameters)
    {
        foreach (TableColumnSchema col in extraDefaults)
        {
            if (row.ContainsKey(col.Name))
                continue;

            if (col.DefaultSequenceId is { Length: > 0 } sequenceId)
            {
                row[col.Name] = SequenceDefaults.TakeNextValue(sequenceId, col.Name, parameters);
                continue;
            }

            // The default goes through the same coercion as a supplied value. A function default is
            // evaluated here, so its type is known only now (CURRENT_DATE() on a datetime column),
            // and a constant stored by an older schema can predate a coercion rule.
            ColumnValue value = col.DefaultFunction is not null
                ? ScalarFunctionEvaluator.EvaluateNullary(col.DefaultFunction, parameters)
                : col.DefaultValue!;
            row[col.Name] = CastScalarFunctions.CoerceToColumnType(value, col);
        }
    }
}
