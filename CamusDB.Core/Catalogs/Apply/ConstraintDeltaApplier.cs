
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.Catalogs.Apply;

/// <summary>
/// Applies the committed deltas that add or drop a CHECK constraint, or set a column NOT NULL, and
/// rebuilds the parsed condition trees a constraint needs before it can be evaluated.
///
/// <para><b>A CHECK constraint is violated only when its condition evaluates to FALSE.</b> A NULL
/// result passes. That is SQL's three-valued logic and not an oversight: a row whose checked column
/// is NULL has an unknown, not a failing, condition. Treating NULL as a violation would reject rows
/// every other SQL engine accepts.</para>
///
/// <para><b>Only the expression text is persisted; the parsed tree is not.</b>
/// <see cref="ParseCheckConstraintAsts"/> rebuilds it after a load and after an apply, and skips any
/// constraint whose tree is already built, so it is safe to call on every path. A constraint whose
/// tree was never rebuilt would silently evaluate against nothing.</para>
/// </summary>
internal static class ConstraintDeltaApplier
{
    /// <summary>
    /// Applies an AddCheckConstraint delta. Idempotent: an existing CHECK with the same name is
    /// replaced. A name that a foreign key or a named NOT NULL uses is refused, in log order, so two
    /// kinds never share a name (<see cref="ConstraintNameRules"/>). Rebuilds the
    /// <c>ParsedCondition</c> AST cache so enforcement is immediately available on the applying node. Does not bump <c>TableSchema.Version</c> — check constraints
    /// do not affect row encoding.
    /// </summary>
    internal static TableSchema ApplyAddCheckConstraint(Schema schema, SchemaCheckConstraintPayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        ConstraintNameRules.RequireUnusedByOtherKinds(tableSchema, payload.ConstraintName);

        CheckConstraintSchema check = new()
        {
            Name = payload.ConstraintName,
            Expression = payload.Expression,
            ReferencedColumns = payload.ReferencedColumns
        };

        if (!string.IsNullOrEmpty(payload.Expression))
            check.ParsedCondition = SQLParserProcessor.ParseCondition(payload.Expression);

        tableSchema.CheckConstraints = [.. WithoutCheck(tableSchema.CheckConstraints, payload.ConstraintName), check];
        return tableSchema;
    }

    /// <summary>
    /// Applies a DropCheckConstraint delta. Idempotent: if the constraint is already absent the
    /// operation is a no-op. Returns the table even when absent so the schema version still advances.
    /// </summary>
    internal static TableSchema? ApplyDropCheckConstraint(Schema schema, SchemaCheckConstraintPayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            return null;

        if (tableSchema.CheckConstraints is not null)
            tableSchema.CheckConstraints = WithoutCheck(tableSchema.CheckConstraints, payload.ConstraintName);

        return tableSchema;
    }

    /// <summary>
    /// A new list with every constraint of <paramref name="checks"/> except the one named
    /// <paramref name="constraintName"/>. The published list is never changed in place: DML iterates
    /// <see cref="TableSchema.CheckConstraints"/> without the schema lock, and a change in place makes
    /// that iteration throw.
    /// </summary>
    internal static List<CheckConstraintSchema> WithoutCheck(List<CheckConstraintSchema>? checks, string constraintName)
    {
        if (checks is null)
            return [];

        List<CheckConstraintSchema> kept = new(checks.Count);
        foreach (CheckConstraintSchema check in checks)
        {
            if (!string.Equals(check.Name, constraintName, StringComparison.OrdinalIgnoreCase))
                kept.Add(check);
        }

        return kept;
    }

    /// <summary>
    /// Publishes a copy of the column list with the column at <paramref name="index"/> replaced. The
    /// published list is never changed in place: DML iterates <see cref="TableSchema.Columns"/> without
    /// the schema lock, and a write into the list makes that iteration throw
    /// <see cref="InvalidOperationException"/>. A swapped list also invalidates the current-version
    /// history that <see cref="TableSchema"/> caches by list identity.
    /// </summary>
    internal static void ReplaceColumn(TableSchema tableSchema, int index, TableColumnSchema column)
    {
        List<TableColumnSchema> columns = new(tableSchema.Columns!);
        columns[index] = column;
        tableSchema.Columns = columns;
    }

    /// <summary>
    /// Applies a <see cref="SchemaOp.SetColumnStorage"/> delta: replaces the target column with a copy
    /// carrying the new strategy. Idempotent. Does not bump <c>TableSchema.Version</c> and does not
    /// touch stored rows: the strategy decides only the form of future writes. Rejects a column type
    /// with no variable-length payload with <see cref="CamusDBErrorCodes.ColumnStorageNotApplicable"/>,
    /// so validation on the proposer and apply on every node reach the same answer.
    /// </summary>
    internal static TableSchema ApplySetColumnStorage(Schema schema, SchemaSetColumnStoragePayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        if (tableSchema.Columns is null)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Table '{payload.TableName}' has no columns");

        int idx = tableSchema.Columns.FindIndex(c => string.Equals(c.Name, payload.ColumnName, StringComparison.OrdinalIgnoreCase));
        if (idx < 0)
            throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"Column '{payload.ColumnName}' does not exist on table '{payload.TableName}'");

        TableColumnSchema old = tableSchema.Columns[idx];
        ReplaceColumn(tableSchema, idx, WithStorage(old, payload.Storage));
        return tableSchema;
    }

    /// <summary>
    /// Returns a copy of <paramref name="column"/> with <paramref name="storage"/>, after checking the
    /// column type can carry a strategy. Shared by the replicated apply and the single-node path.
    /// </summary>
    internal static TableColumnSchema WithStorage(TableColumnSchema column, ColumnStorageStrategy storage)
    {
        if (!TableColumnSchema.SupportsStorageStrategy(column.Type))
            throw new CamusDBException(
                CamusDBErrorCodes.ColumnStorageNotApplicable,
                $"Column '{column.Name}' of type {column.Type} has no variable-length value, so it cannot take a storage strategy");

        return new TableColumnSchema(
            id: column.Id,
            name: column.Name,
            type: column.Type,
            notNull: column.NotNull,
            defaultValue: column.DefaultValue,
            state: column.State,
            maxLength: column.MaxLength,
            arrayElementType: column.ArrayElementType,
            defaultFunction: column.DefaultFunction,
            notNullConstraintName: column.NotNullConstraintName,
            comment: column.Comment,
            storage: storage,
            defaultSequenceId: column.DefaultSequenceId,
            identityAlways: column.IdentityAlways);
    }

    /// <summary>
    /// Applies a SetColumnNotNull delta. Replaces the target column with an updated copy that has
    /// the new <c>NotNull</c> flag and <c>NotNullConstraintName</c>. Idempotent: setting the flag
    /// to its current value is a no-op. A constraint name that a CHECK, a foreign key or another
    /// column's NOT NULL uses is refused (<see cref="ConstraintNameRules"/>). Does not bump
    /// <c>TableSchema.Version</c> because the NOT NULL flag is not encoded in row bytes.
    /// </summary>
    internal static TableSchema ApplySetColumnNotNull(Schema schema, SchemaSetColumnNotNullPayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        if (tableSchema.Columns is null)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Table '{payload.TableName}' has no columns");

        int idx = tableSchema.Columns.FindIndex(c => string.Equals(c.Name, payload.ColumnName, StringComparison.OrdinalIgnoreCase));
        if (idx < 0)
            throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"Column '{payload.ColumnName}' does not exist on table '{payload.TableName}'");

        TableColumnSchema old = tableSchema.Columns[idx];

        if (payload.ConstraintName is { Length: > 0 } constraintName)
            ConstraintNameRules.RequireUnusedForNotNull(tableSchema, constraintName, old.Id);

        ReplaceColumn(tableSchema, idx, WithNotNull(old, payload.NotNull, payload.ConstraintName));
        return tableSchema;
    }

    /// <summary>A copy of <paramref name="old"/> with the NOT NULL flag and constraint name replaced.</summary>
    internal static TableColumnSchema WithNotNull(TableColumnSchema old, bool notNull, string? constraintName) => new(
        id: old.Id,
        name: old.Name,
        type: old.Type,
        notNull: notNull,
        defaultValue: old.DefaultValue,
        state: old.State,
        maxLength: old.MaxLength,
        arrayElementType: old.ArrayElementType,
        defaultFunction: old.DefaultFunction,
        notNullConstraintName: constraintName,
        comment: old.Comment,
        storage: old.Storage,
        defaultSequenceId: old.DefaultSequenceId,
        identityAlways: old.IdentityAlways
    );

    /// <summary>
    /// Rebuilds the transient <see cref="CheckConstraintSchema.ParsedCondition"/> AST cache for
    /// every check constraint on <paramref name="tableSchema"/> whose cache is null. Called after
    /// the JSON checkpoint is deserialized (the field is <c>[JsonIgnore]</c> and therefore absent
    /// in the persisted form) and after <c>TableDeltaApplier.ApplyCreateTable</c> copies constraints from the payload.
    /// </summary>
    internal static void ParseCheckConstraintAsts(TableSchema tableSchema)
    {
        if (tableSchema.CheckConstraints is null || tableSchema.CheckConstraints.Count == 0)
            return;

        foreach (CheckConstraintSchema check in tableSchema.CheckConstraints)
        {
            if (check.ParsedCondition is not null || string.IsNullOrEmpty(check.Expression))
                continue;

            check.ParsedCondition = SQLParserProcessor.ParseCondition(check.Expression);
        }
    }
}
