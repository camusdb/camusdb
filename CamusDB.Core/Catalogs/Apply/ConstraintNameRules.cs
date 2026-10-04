/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.Catalogs.Apply;

/// <summary>
/// One name space for the named constraints of a table: CHECK, named NOT NULL and FOREIGN KEY.
///
/// <para><b>Why one name space.</b> <c>ALTER TABLE ... DROP CONSTRAINT name</c> finds the constraint
/// by name across the three kinds, and a CHECK or a NOT NULL wins over a foreign key. If two kinds
/// shared a name, the drop would remove the wrong one and keep the other, with no error. Every path
/// that creates a named constraint therefore refuses a name that any kind already uses.</para>
///
/// <para><b>Where it runs.</b> The executor checks under the schema lock, so a standalone ALTER is
/// refused before it changes anything. The replicated apply checks again, in log order, because two
/// nodes can propose the same name against the same base version. An apply that refuses changes
/// nothing. A re-delivered entry stays idempotent: each caller excludes the constraint it is about to
/// replace (the CHECK of the same name, the column it sets NOT NULL).</para>
/// </summary>
internal static class ConstraintNameRules
{
    /// <summary>True when a CHECK constraint of <paramref name="table"/> is named <paramref name="name"/>.</summary>
    internal static bool IsCheckName(TableSchema table, string name) =>
        table.CheckConstraints?.Exists(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase)) == true;

    /// <summary>True when a foreign key of <paramref name="table"/> is named <paramref name="name"/>.</summary>
    internal static bool IsForeignKeyName(TableSchema table, string name) =>
        table.ForeignKeys?.Exists(fk => string.Equals(fk.Name, name, StringComparison.OrdinalIgnoreCase)) == true;

    /// <summary>
    /// True when a column of <paramref name="table"/>, other than the one with id
    /// <paramref name="exceptColumnId"/>, has a NOT NULL constraint named <paramref name="name"/>.
    /// </summary>
    internal static bool IsNotNullName(TableSchema table, string name, string? exceptColumnId = null) =>
        table.Columns?.Exists(c =>
            string.Equals(c.NotNullConstraintName, name, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(c.Id, exceptColumnId, StringComparison.Ordinal)) == true;

    /// <summary>Refuses <paramref name="name"/> when any named constraint of <paramref name="table"/> uses it.</summary>
    internal static void RequireUnused(TableSchema table, string name)
    {
        if (IsCheckName(table, name) || IsForeignKeyName(table, name) || IsNotNullName(table, name))
            throw Taken(table, name);
    }

    /// <summary>
    /// For an ADD CHECK: refuses a name that a foreign key or a named NOT NULL uses. A CHECK of the same
    /// name is not refused here; the executor refuses it before the change, and the apply replaces it,
    /// because a re-delivered entry finds its own constraint already in place.
    /// </summary>
    internal static void RequireUnusedByOtherKinds(TableSchema table, string name)
    {
        if (IsForeignKeyName(table, name) || IsNotNullName(table, name))
            throw Taken(table, name);
    }

    /// <summary>
    /// For a SET NOT NULL: refuses a constraint name that a CHECK, a foreign key or the NOT NULL of
    /// another column uses. The column itself may already carry the name, from a re-delivered entry.
    /// </summary>
    internal static void RequireUnusedForNotNull(TableSchema table, string name, string? columnId)
    {
        if (IsCheckName(table, name) || IsForeignKeyName(table, name) || IsNotNullName(table, name, columnId))
            throw Taken(table, name);
    }

    private static CamusDBException Taken(TableSchema table, string name) =>
        new(CamusDBErrorCodes.InvalidInput, $"Constraint '{name}' already exists on table '{table.Name}'");
}
