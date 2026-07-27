/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Ticket for <c>COMMENT ON TABLE|COLUMN|INDEX|DATABASE … IS '…'|NULL</c>.
///
/// <para>The <see cref="Comment"/> null-vs-empty distinction is load-bearing and must survive every
/// hop (ticket → payload → schema → HTTP forwarding): <c>null</c> means <c>IS NULL</c>, i.e. remove
/// the comment, while <c>""</c> means <c>IS ''</c>, i.e. store a present-but-empty comment. Code that
/// coalesces one into the other silently changes user-visible behavior, because
/// <c>SHOW CREATE TABLE</c> omits the clause entirely for null and emits <c>COMMENT ''</c> for
/// empty.</para>
///
/// <para><see cref="CommentTarget.Database"/> takes a different execution path from the other three:
/// a database comment lives on the cross-database registry entry, so it is handled before any
/// database is opened and never enters the per-database schema log.</para>
/// </summary>
public readonly struct CommentTicket
{
    public CommentTarget Target { get; }

    public string DatabaseName { get; }

    /// <summary>Table that owns the commented element. Null for <see cref="CommentTarget.Database"/>.</summary>
    public string? TableName { get; }

    /// <summary>
    /// Column or index name. Null for <see cref="CommentTarget.Table"/> and
    /// <see cref="CommentTarget.Database"/>.
    /// </summary>
    public string? ElementName { get; }

    /// <summary>The comment text, or <c>null</c> to remove an existing comment.</summary>
    public string? Comment { get; }

    public CommentTicket(
        CommentTarget target,
        string databaseName,
        string? tableName,
        string? elementName,
        string? comment)
    {
        Target = target;
        DatabaseName = databaseName;
        TableName = tableName;
        ElementName = elementName;
        Comment = comment;
    }
}
