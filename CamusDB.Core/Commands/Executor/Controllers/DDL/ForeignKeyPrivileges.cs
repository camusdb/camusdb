/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// The privilege a foreign key needs on the table it references: <c>SELECT</c>. CamusDB has no
/// <c>REFERENCES</c> privilege. A constraint lets its owner learn whether a parent key exists — every
/// child insert answers that question — so declaring one without the right to read the parent would be
/// a way to read around a missing grant.
///
/// <para><b>Checked on the node that received the statement, before any forwarding.</b> A forwarded
/// statement runs on the schema leader with no user principal, so a check placed after the forward
/// would never run for it.</para>
/// </summary>
internal static class ForeignKeyPrivileges
{
    /// <summary>
    /// Refuses <paramref name="ticket"/> when the caller lacks <c>SELECT</c> on a referenced table. A
    /// self-reference and a parent that does not exist are left to the DDL, which reports them with
    /// their own codes. Does nothing when authentication is off or the request carries no principal.
    /// </summary>
    internal static void RequireReferencePrivileges(DatabaseDescriptor database, CreateTableTicket ticket)
    {
        if (ticket.ForeignKeys.Length == 0 || !database.Options.AuthenticationEnabled)
            return;

        if (AuthorizationContext.Current.Principal is not { } principal)
            return;

        foreach (ForeignKeyInfo foreignKey in ticket.ForeignKeys)
        {
            if (string.Equals(foreignKey.ReferencedTable, ticket.TableName, StringComparison.OrdinalIgnoreCase))
                continue;

            if (!database.Schema.Tables.TryGetValue(foreignKey.ReferencedTable, out TableSchema? parent) || parent.Id is null)
                continue;

            if (!principal.HasPrivilege(Privilege.Select, database.Id, parent.Id))
                throw new CamusDBException(
                    CamusDBErrorCodes.InsufficientPrivilege,
                    $"Missing Select privilege on table '{database.Name}.{parent.Name}', which foreign key '{foreignKey.Name}' references");
        }
    }
}
