/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Apply;

/// <summary>
/// The DDL guards of foreign keys: what must not be removed while a constraint depends on it. Each rule
/// reads the published <see cref="ForeignKeyGraph"/>, which holds every constraint in any state, resolved
/// or not, so "is this referenced?" has one answer everywhere.
///
/// <para><b>Two callers per rule.</b> The DDL service calls a rule under the DDL semaphore, before the
/// statement changes anything, for a clear error and so that no local work is done and then undone. The
/// delta apply calls it again where the delta is proposed without a local change first (DROP TABLE,
/// TRUNCATE, DROP COLUMN, row-level TTL). That apply runs in log order on every node on top of the same
/// schema version, so it orders the change against concurrent DDL from other nodes, and every node
/// reaches the same answer.</para>
///
/// <para><b>DROP INDEX is guarded only by the service.</b> Its delta is proposed after the leader has
/// already removed the index from its in-memory schema, so a refusal at apply time would leave the
/// leader with no index while the followers keep it.</para>
/// </summary>
internal static class ForeignKeyDependencyRules
{
    /// <summary>
    /// Refuses to remove <paramref name="table"/> or its contents while a constraint of another table
    /// references it. A self-reference does not block: the referencing rows go with the table.
    /// </summary>
    /// <param name="operation">The statement, for the message: <c>DROP TABLE</c> or <c>TRUNCATE</c>.</param>
    internal static void RequireNotReferencedByOtherTables(Schema schema, TableSchema table, string operation)
    {
        if (table.Id is null)
            return;

        foreach (ForeignKeyPlan plan in schema.ForeignKeys.ParentPlansOf(table.Id))
        {
            if (plan.IsSelfReference)
                continue;

            throw new CamusDBException(
                CamusDBErrorCodes.DependentObjectsExist,
                $"Cannot {operation} '{table.Name}': foreign key '{plan.Constraint.Name}' on table '{TableName(schema, plan.ChildTableId)}' references it");
        }
    }

    /// <summary>
    /// Refuses to drop a column that a foreign key uses, as a referencing column of
    /// <paramref name="table"/> or as a column that a constraint references.
    /// </summary>
    internal static void RequireColumnNotInForeignKey(Schema schema, TableSchema table, string columnName)
    {
        TableColumnSchema? column = table.Columns?.Find(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
        if (column is null || table.Id is null)
            return;

        if (table.ForeignKeys is not null)
        {
            foreach (ForeignKeySchema foreignKey in table.ForeignKeys)
            {
                if (Array.IndexOf(foreignKey.ColumnIds, column.Id) >= 0)
                    throw new CamusDBException(
                        CamusDBErrorCodes.DependentObjectsExist,
                        $"Cannot drop column '{table.Name}.{column.Name}': foreign key '{foreignKey.Name}' uses it");
            }
        }

        foreach (ForeignKeyPlan plan in schema.ForeignKeys.ParentPlansOf(table.Id))
        {
            if (Array.IndexOf(plan.Constraint.ReferencedColumnIds, column.Id) >= 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.DependentObjectsExist,
                    $"Cannot drop column '{table.Name}.{column.Name}': foreign key '{plan.Constraint.Name}' on table '{TableName(schema, plan.ChildTableId)}' references it");
        }
    }

    /// <summary>
    /// Refuses to drop an index that a foreign key needs: the parent's referenced unique index (the
    /// rendezvous key lives there) or the child's backing index (the parent-side probe reads it). An
    /// index the engine created for a constraint goes only with that constraint.
    /// </summary>
    internal static void RequireIndexNotInForeignKey(Schema schema, TableSchema table, string indexName)
    {
        TableIndexSchema? index = table.Indexes?.Find(i => string.Equals(i.Name, indexName, StringComparison.OrdinalIgnoreCase));
        if (index is null || table.Id is null)
            return;

        if (table.ForeignKeys is not null)
        {
            foreach (ForeignKeySchema foreignKey in table.ForeignKeys)
            {
                if (string.Equals(foreignKey.BackingIndexId, index.KvId, StringComparison.Ordinal))
                    throw new CamusDBException(
                        CamusDBErrorCodes.DependentObjectsExist,
                        $"Cannot drop index '{index.Name}' of table '{table.Name}': foreign key '{foreignKey.Name}' uses it");
            }
        }

        foreach (ForeignKeyPlan plan in schema.ForeignKeys.ParentPlansOf(table.Id))
        {
            if (string.Equals(plan.Constraint.ReferencedIndexId, index.KvId, StringComparison.Ordinal))
                throw new CamusDBException(
                    CamusDBErrorCodes.DependentObjectsExist,
                    $"Cannot drop index '{index.Name}' of table '{table.Name}': foreign key '{plan.Constraint.Name}' on table '{TableName(schema, plan.ChildTableId)}' references it");
        }
    }

    /// <summary>
    /// Refuses row-level TTL on a table that a foreign key references, a self-reference included. The
    /// sweep deletes expired rows through its own path, which has no parent-side check, so it would
    /// remove rows that children still reference.
    /// </summary>
    internal static void RequireNoRowLevelTtlWhenReferenced(Schema schema, TableSchema table, IReadOnlyDictionary<string, string>? settingsAfter)
    {
        if (table.Id is null
            || settingsAfter is null
            || !settingsAfter.TryGetValue(TableSettings.TtlExpirationExpressionKey, out string? expression)
            || string.IsNullOrWhiteSpace(expression))
            return;

        ForeignKeyPlan[] plans = schema.ForeignKeys.ParentPlansOf(table.Id);
        if (plans.Length == 0)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.FeatureNotSupported,
            $"Cannot enable row-level TTL on '{table.Name}': foreign key '{plans[0].Constraint.Name}' on table '{TableName(schema, plans[0].ChildTableId)}' references it, " +
            "and expired rows are deleted without a foreign-key check");
    }

    private static string TableName(Schema schema, string tableId) =>
        SchemaDeltaApplier.FindRelationById(schema, tableId)?.Name ?? tableId;
}
