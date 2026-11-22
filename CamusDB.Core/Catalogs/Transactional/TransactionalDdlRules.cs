
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
/// otherwise read every row of its table (an index build, a column fill).
///
/// <para>A table the transaction created is metadata-only for such a statement while the transaction
/// has written no row to it: no other transaction can see the table before the commit, so there is no
/// row to read. Every other case needs the staged form of the statement (an online backfill owned by the
/// transaction), which is not built yet, and is refused with
/// <see cref="CamusDBErrorCodes.StatementNotAllowedInTransaction"/>.</para>
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
    /// True when <paramref name="table"/> was created by the transaction of <paramref name="overlay"/> and
    /// that transaction wrote no row to it yet.
    /// </summary>
    internal static bool IsOwnEmptyTable(TransactionSchemaOverlay overlay, TableDescriptor table) =>
        overlay.CreatedTable(table.Id) && !WroteRows(overlay.Owner, table);

    /// <summary>
    /// Refuses <paramref name="statement"/> unless <paramref name="table"/> is a table the transaction
    /// created and wrote no row to.
    /// </summary>
    internal static void RequireOwnEmptyTable(TransactionSchemaOverlay overlay, TableDescriptor table, string statement)
    {
        if (IsOwnEmptyTable(overlay, table))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.StatementNotAllowedInTransaction,
            $"{statement} on table '{table.Name}' cannot yet run inside an explicit transaction with transactional DDL on: " +
            "it must read the rows of the table, which is possible inside the transaction only for a table the transaction " +
            "created and has not written to yet. Commit or roll back first, then run it");
    }
}
