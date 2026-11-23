
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.Catalogs.Transactional;

/// <summary>
/// The checks a DDL statement makes inside a transaction with a private schema, when the statement would
/// otherwise read every row of its table (an index build, a column fill, a constraint check).
///
/// <para>On a table the transaction created, such a statement runs inside the transaction: no other
/// transaction can see the table before the commit, so every row is one the transaction wrote, and the
/// statement reads and writes them through it. On any other table the statement runs its staged form:
/// the online steps run on the shared table under a coordinator job that the transaction owns, and the
/// step to <c>Public</c> joins the transaction's change set. A staged CHECK or NOT NULL has no such step:
/// it is enforced for every transaction from the statement on, and the commit makes it permanent.</para>
/// </summary>
internal static class TransactionalDdlRules
{
    /// <summary>
    /// True when <paramref name="tx"/> wrote or deleted a row of <paramref name="table"/>. Reads the
    /// transaction's own record of the keys it changed.
    /// </summary>
    internal static bool WroteRows(KvTransaction tx, TableDescriptor table)
    {
        string rowKeys = table.Store.RowKeySpace + "/";

        foreach ((string key, _) in tx.GetModifiedKeyPairs())
        {
            if (key.StartsWith(rowKeys, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Refuses the staged form of <paramref name="statement"/> on the shared table <paramref name="table"/>
    /// in the cases it cannot handle:
    /// <list type="bullet">
    /// <item>The transaction wrote rows to the table. The staged steps read the rows outside the
    /// transaction, where those writes are not visible, and an index build or a column fill would also wait
    /// for the transaction's own row locks.</item>
    /// <item>The transaction holds locks on the table's rows from a read, and <paramref name="locksRows"/> is
    /// true: an index build or a column fill locks each row before it reads it, so it would wait for the
    /// transaction while the transaction waits for the statement. A CHECK, NOT NULL or foreign key check
    /// reads with no lock and passes false. A foreign key that must build its backing index makes this check
    /// on its own, once it knows that it must (<see cref="RequireNoRowLocks"/>).</item>
    /// <item>The transaction's change set already changed the table. The staged steps run on the shared
    /// table, which does not have those changes before the commit.</item>
    /// </list>
    /// </summary>
    internal static void RequireStageableSharedTable(TransactionSchemaOverlay overlay, TableDescriptor table, string statement, bool locksRows = true)
    {
        if (WroteRows(overlay.Owner, table))
            throw new CamusDBException(
                CamusDBErrorCodes.StatementNotAllowedInTransaction,
                locksRows
                    ? $"{statement} on table '{table.Name}' cannot run in this transaction: the transaction already wrote rows to " +
                      "the table, and the statement fills the table's rows in batches that lock each row, so it would wait for " +
                      "the transaction's own locks. Run the statement before the writes, or in a transaction of its own"
                    : $"{statement} on table '{table.Name}' cannot run in this transaction: the transaction already wrote rows to " +
                      "the table, and the statement checks the table's rows outside the transaction, where those writes are not " +
                      "visible. Run the statement before the writes, or in a transaction of its own");

        if (locksRows)
            RequireNoRowLocks(overlay, table, statement);

        if (overlay.TouchedTableIds.Contains(table.Id))
            throw new CamusDBException(
                CamusDBErrorCodes.StatementNotAllowedInTransaction,
                $"{statement} on table '{table.Name}' cannot run in this transaction: the transaction already changed the " +
                "table's schema, and the statement runs on the shared table, which does not have that change before the " +
                "commit. Run the statement before the other changes to the table, or commit first");
    }

    /// <summary>
    /// Refuses <paramref name="statement"/>, which locks every row of <paramref name="table"/> in batches of
    /// its own, when the transaction holds locks on rows of the table. A Serializable read or a
    /// <c>SELECT ... FOR UPDATE</c> takes them, and the transaction cannot release them while it waits for
    /// the statement.
    /// </summary>
    internal static void RequireNoRowLocks(TransactionSchemaOverlay overlay, TableDescriptor table, string statement)
    {
        if (overlay.Owner.CountRangeLocksForBucket(table.Store.RowKeySpace) > 0)
            throw new CamusDBException(
                CamusDBErrorCodes.StatementNotAllowedInTransaction,
                $"{statement} on table '{table.Name}' cannot run in this transaction: the transaction holds locks on rows of " +
                "the table from an earlier read, and the statement fills the table's rows in batches that lock each row, so " +
                "it would wait for the transaction's own locks. Run the statement before the reads, or in a transaction of its own");
    }

    /// <summary>
    /// Refuses a foreign key staged from a shared child table when the transaction created, changed or wrote
    /// the parent table <paramref name="parent"/>. The constraint is added to the shared schema and the child
    /// rows are checked against the parent outside the transaction: a parent the transaction created does
    /// not exist there yet, a change to its schema is not there yet, and parent rows the transaction wrote
    /// or deleted are not visible there.
    /// </summary>
    internal static void RequireStageableParent(TransactionSchemaOverlay overlay, TableDescriptor parent, string statement)
    {
        if (overlay.CreatedTable(parent.Id))
            throw new CamusDBException(
                CamusDBErrorCodes.StatementNotAllowedInTransaction,
                $"{statement} cannot run in this transaction: it references table '{parent.Name}', which the transaction " +
                "created, from a table other transactions write. The constraint would be enforced on other transactions " +
                "before the referenced table exists for them. Commit first, then run it");

        if (overlay.TouchedTableIds.Contains(parent.Id))
            throw new CamusDBException(
                CamusDBErrorCodes.StatementNotAllowedInTransaction,
                $"{statement} cannot run in this transaction: the transaction already changed the schema of the referenced " +
                $"table '{parent.Name}', and the statement runs on the shared tables, which do not have that change before " +
                "the commit. Run the statement before the other changes, or commit first");

        if (WroteRows(overlay.Owner, parent))
            throw new CamusDBException(
                CamusDBErrorCodes.StatementNotAllowedInTransaction,
                $"{statement} cannot run in this transaction: the transaction already wrote rows to the referenced table " +
                $"'{parent.Name}', and the statement checks the child rows against it outside the transaction, where those " +
                "writes are not visible. Run the statement before the writes, or in a transaction of its own");
    }

    /// <summary>
    /// Refuses <paramref name="statement"/> unless <paramref name="table"/> is a table the transaction of
    /// <paramref name="overlay"/> created.
    /// </summary>
    internal static void RequireOwnTable(TransactionSchemaOverlay overlay, TableDescriptor table, string statement)
    {
        if (overlay.CreatedTable(table.Id))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.StatementNotAllowedInTransaction,
            $"{statement} on table '{table.Name}' cannot yet run inside an explicit transaction with transactional DDL on: " +
            "it must read the rows of the table, which is possible inside the transaction only for a table the transaction " +
            "created. Commit or roll back first, then run it");
    }
}
