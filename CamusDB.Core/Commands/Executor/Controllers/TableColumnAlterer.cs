
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class TableColumnAlterer
{
    private readonly CatalogsManager catalogs;

    private readonly TableColumnAdder tableColumnAdder;

    private readonly TableColumnDropper tableColumnDropper;

    public TableColumnAlterer(CatalogsManager catalogsManager, ILogger<ICamusDB> logger)
    {
        catalogs = catalogsManager;

        tableColumnAdder = new(logger);
        tableColumnDropper = new(logger);
    }

    public async Task<bool> Alter(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterTableTicket ticket, KvTransaction tx)
    {
        RejectDropOfTtlColumn(table, ticket);

        return ticket.Operation switch
        {
            AlterTableOperation.AddColumn => await AddColumn(queryExecutor, database, table, ticket, tx).ConfigureAwait(false),
            AlterTableOperation.DropColumn => await DropColumn(queryExecutor, database, table, ticket, tx).ConfigureAwait(false),
            AlterTableOperation.RenameColumn => await RenameColumn(database, table, ticket, tx).ConfigureAwait(false),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid alter table operation"),
        };
    }

    /// <summary>
    /// Refuses to drop the column a table's row-level TTL expires on.
    ///
    /// <para>Dropping it would leave <c>ttl_expiration_expression</c> pointing at nothing. Nothing would
    /// fail at drop time — the failure would surface later, inside a background sweep, as a table that
    /// silently stops expiring anything. A configuration error must be reported where it is made, so the
    /// user is told to <c>RESET (ttl)</c> first and the intent stays explicit.</para>
    ///
    /// <para>Renaming, by contrast, is allowed: the setting is rewritten to follow the new name (see
    /// <c>CatalogsManager</c>'s rename path), because the user's intent there is unambiguous.</para>
    /// </summary>
    private static void RejectDropOfTtlColumn(TableDescriptor table, AlterTableTicket ticket)
    {
        if (ticket.Operation != AlterTableOperation.DropColumn)
            return;

        if (table.Schema.Settings is null ||
            !table.Schema.Settings.TryGetValue(Catalogs.Models.TableSettings.TtlExpirationExpressionKey, out string? ttlColumn))
            return;

        if (!string.Equals(ttlColumn, ticket.Column.Name, StringComparison.OrdinalIgnoreCase))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Column '{ticket.Column.Name}' cannot be dropped while it is the row-level TTL expiration " +
            $"column of table '{table.Name}'; run ALTER TABLE {table.Name} RESET (ttl) first");
    }

    private async Task<bool> AddColumn(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterTableTicket ticket, KvTransaction tx)
    {
        AlterColumnTicket alterColumnTicket = new(
            databaseName: database.Name,
            tableName: table.Name,
            column: ticket.Column,
            operation: ticket.Operation
        );

        await tableColumnAdder.AddColumn(catalogs, tx, queryExecutor, database, table, alterColumnTicket).ConfigureAwait(false);

        return true;
    }

    internal Task<int> BackfillColumnDefaultsAsync(
        QueryExecutor queryExecutor,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterColumnTicket ticket,
        KvTransaction tx
    ) => tableColumnAdder.BackfillColumnDefaultsAsync(queryExecutor, database, table, ticket, tx);

    private async Task<bool> RenameColumn(DatabaseDescriptor database, TableDescriptor table, AlterTableTicket ticket, KvTransaction tx)
    {
        AlterColumnTicket alterColumnTicket = new(
            databaseName: database.Name,
            tableName: table.Name,
            column: ticket.Column,
            operation: AlterTableOperation.RenameColumn,
            newName: ticket.NewName
        );

        await catalogs.AlterTable(database, alterColumnTicket, tx).ConfigureAwait(false);

        return true;
    }

    private async Task<bool> DropColumn(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterTableTicket ticket, KvTransaction tx)
    {
        AlterColumnTicket alterColumnTicket = new(
            databaseName: database.Name,
            tableName: table.Name,
            column: ticket.Column,
            operation: ticket.Operation
        );

        await tableColumnDropper.DropColumn(catalogs, tx, queryExecutor, database, table, alterColumnTicket).ConfigureAwait(false);

        return true;
    }
}

