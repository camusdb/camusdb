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
/// Applies the committed delta that adds a foreign key to an existing table
/// (<see cref="SchemaOp.AddForeignKey"/>). The removal has no delta of its own: it is a
/// <see cref="SchemaOp.SetElementState"/> to <c>Absent</c> (see
/// <see cref="ElementStateApplier.ApplyForeignKeyElementState"/>).
///
/// <para><b>Every check runs before the first change.</b> The proposer dry-runs this apply against the
/// same base version, so the real apply on every node normally cannot fail. The checks still run here,
/// in log order, because that is what orders the constraint against a concurrent <c>DROP TABLE</c> of the
/// parent, a <c>DROP INDEX</c> of the referenced index or another <c>ADD CONSTRAINT</c> that would close
/// a cycle. If a check throws, nothing was changed.</para>
///
/// <para>The lists are replaced, never edited in place, so a lock-free reader that holds the old list
/// keeps a consistent one.</para>
/// </summary>
internal static class ForeignKeyDeltaApplier
{
    internal static TableSchema ApplyAddForeignKey(Schema schema, SchemaAddForeignKeyPayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? table)
            || !string.Equals(table.Id, payload.TableId, StringComparison.Ordinal))
            throw new CamusDBException(
                CamusDBErrorCodes.TableDoesntExist,
                $"Table '{payload.TableName}' does not exist");

        ForeignKeySchema foreignKey = payload.ForeignKey
            ?? throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"An AddForeignKey delta for table '{payload.TableName}' carries no constraint");

        // A re-delivered entry finds its own constraint already in place.
        if (table.ForeignKeys?.Exists(fk => string.Equals(fk.Id, foreignKey.Id, StringComparison.Ordinal)) == true)
            return table;

        if (foreignKey.State != SchemaElementState.WriteOnly)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Foreign key '{foreignKey.Name}' must be added in WriteOnly, not {foreignKey.State}");

        RequireUnusedConstraintName(table, foreignKey.Name);

        int claimedPosition = -1;

        if (payload.ClaimedIndexId is { } claimedIndexId)
        {
            claimedPosition = table.Indexes?.FindIndex(ix => string.Equals(ix.KvId, claimedIndexId, StringComparison.Ordinal)) ?? -1;

            if (claimedPosition < 0
                || !string.Equals(claimedIndexId, foreignKey.BackingIndexId, StringComparison.Ordinal)
                || table.Indexes![claimedPosition].OwnerConstraintId is not null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Foreign key '{foreignKey.Name}' on table '{table.Name}' claims index '{claimedIndexId}', which is not an unowned backing index of the table");
        }

        ForeignKeyDefinitionRules.Validate(schema, table, foreignKey);
        ForeignKeyDefinitionRules.RequireNoCycle(schema, table, foreignKey);

        if (claimedPosition >= 0)
        {
            TableIndexSchema claimed = table.Indexes![claimedPosition];
            List<TableIndexSchema> indexes = [.. table.Indexes];
            indexes[claimedPosition] = new TableIndexSchema(
                claimed.Id,
                claimed.Name,
                claimed.ColumnIds,
                claimed.Type,
                claimed.State,
                claimed.StartOffset,
                columnDirections: claimed.ColumnDirections,
                includeColumnIds: claimed.IncludeColumnIds,
                comment: claimed.Comment,
                ownerConstraintId: foreignKey.Id);
            table.Indexes = indexes;
        }

        table.ForeignKeys = table.ForeignKeys is null ? [foreignKey] : [.. table.ForeignKeys, foreignKey];
        return table;
    }

    /// <summary>
    /// <c>DROP CONSTRAINT name</c> resolves the name across CHECK, named NOT NULL and foreign-key
    /// constraints, so a name must be unique across all three; see <see cref="ConstraintNameRules"/>.
    /// </summary>
    internal static void RequireUnusedConstraintName(TableSchema table, string name) =>
        ConstraintNameRules.RequireUnused(table, name);
}
