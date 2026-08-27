
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
    /// Applies an AddCheckConstraint delta. Idempotent: an existing constraint with the same name
    /// is replaced. Rebuilds the <c>ParsedCondition</c> AST cache so enforcement is immediately
    /// available on the applying node. Does not bump <c>TableSchema.Version</c> — check constraints
    /// do not affect row encoding.
    /// </summary>
    internal static TableSchema ApplyAddCheckConstraint(Schema schema, SchemaCheckConstraintPayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        CheckConstraintSchema check = new()
        {
            Name = payload.ConstraintName,
            Expression = payload.Expression,
            ReferencedColumns = payload.ReferencedColumns
        };

        if (!string.IsNullOrEmpty(payload.Expression))
            check.ParsedCondition = SQLParserProcessor.ParseCondition(payload.Expression);

        tableSchema.CheckConstraints ??= [];
        tableSchema.CheckConstraints.RemoveAll(c => string.Equals(c.Name, payload.ConstraintName, StringComparison.OrdinalIgnoreCase));
        tableSchema.CheckConstraints.Add(check);
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

        tableSchema.CheckConstraints?.RemoveAll(c => string.Equals(c.Name, payload.ConstraintName, StringComparison.OrdinalIgnoreCase));
        return tableSchema;
    }

    /// <summary>
    /// Applies a SetColumnNotNull delta. Replaces the target column with an updated copy that has
    /// the new <c>NotNull</c> flag and <c>NotNullConstraintName</c>. Idempotent: setting the flag
    /// to its current value is a no-op. Does not bump <c>TableSchema.Version</c> because the NOT
    /// NULL flag is not encoded in row bytes.
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
        tableSchema.Columns[idx] = new TableColumnSchema(
            id: old.Id,
            name: old.Name,
            type: old.Type,
            notNull: payload.NotNull,
            defaultValue: old.DefaultValue,
            state: old.State,
            maxLength: old.MaxLength,
            arrayElementType: old.ArrayElementType,
            defaultFunction: old.DefaultFunction,
            notNullConstraintName: payload.ConstraintName,
            comment: old.Comment
        );
        return tableSchema;
    }

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
