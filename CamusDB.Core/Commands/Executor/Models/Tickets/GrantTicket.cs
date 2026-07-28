/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Ticket for <c>GRANT priv_list ON object TO user</c> and, with <see cref="Revoke"/> set,
/// <c>REVOKE priv_list ON object FROM user</c>.
///
/// <para>The scope is carried as <b>names</b> here (<see cref="DatabaseName"/> / <see cref="TableName"/>);
/// the executor resolves them to the immutable database/table ids at dispatch time — because the auth
/// DDL runs before any database is opened, id resolution for a table scope means opening the target
/// database's catalog. Global scope carries no names.</para>
/// </summary>
public readonly struct GrantTicket
{
    public string UserName { get; }

    public Privilege Privileges { get; }

    public GrantScopeKind ScopeKind { get; }

    /// <summary>Database name for <see cref="GrantScopeKind.Database"/>/<see cref="GrantScopeKind.Table"/>; empty for global.</summary>
    public string DatabaseName { get; }

    /// <summary>Table name for <see cref="GrantScopeKind.Table"/>; empty otherwise.</summary>
    public string TableName { get; }

    /// <summary>True for <c>REVOKE</c> (subtract privileges); false for <c>GRANT</c> (add).</summary>
    public bool Revoke { get; }

    public GrantTicket(
        string userName,
        Privilege privileges,
        GrantScopeKind scopeKind,
        string databaseName,
        string tableName,
        bool revoke)
    {
        UserName = userName;
        Privileges = privileges;
        ScopeKind = scopeKind;
        DatabaseName = databaseName;
        TableName = tableName;
        Revoke = revoke;
    }
}
