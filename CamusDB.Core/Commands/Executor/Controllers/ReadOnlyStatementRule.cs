/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// The statements that a read-only transaction refuses with
/// <see cref="CamusDBErrorCodes.ReadOnlyTransaction"/>. Both SQL statement dispatchers call
/// <see cref="Enforce"/> right after the parse, so a statement gets the same answer at either entry
/// point, before it is authorized, reads a row or takes a lock.
///
/// <para>The rule has two groups, each with its own test:</para>
/// <list type="bullet">
///   <item>A statement that writes rows through the caller's transaction (<c>INSERT</c>,
///   <c>UPSERT</c>, <c>INSERT … SELECT</c>, <c>UPDATE</c>, <c>DELETE</c>) is refused in every
///   read-only transaction (<see cref="KvTransaction.RefusesWrites"/>). This includes the snapshot that
///   the server picks for an autocommit <c>SELECT</c>: that snapshot cannot write, and the commit of a
///   Serializable read-only transaction would discard the rows.</item>
///   <item>A statement that changes state in transactions of its own (schema DDL, <c>COMMENT ON</c>,
///   <c>REFRESH MATERIALIZED VIEW</c>) is refused only when the client declared the transaction
///   read-only (<see cref="KvTransaction.IsDeclaredReadOnly"/>). An internal job, such as a restarted
///   materialized-view rebuild, runs <c>REFRESH</c> under a snapshot that the server picked.</item>
/// </list>
///
/// <para>Other statements are checked where more is known. <c>CREATE TABLE … AS</c> is checked by its
/// executor, which knows whether it loads rows. <c>nextval()</c> and <c>setval()</c> are checked by
/// the sequence binder. Database-scoped statements (<c>CREATE DATABASE</c>, <c>CREATE USER</c>,
/// <c>GRANT</c>, cluster settings) get no transaction on any transport, so no access mode applies to
/// them.</para>
/// </summary>
internal static class ReadOnlyStatementRule
{
    /// <summary>
    /// Refuses <paramref name="ast"/> when <paramref name="tx"/> is read-only and the statement is one
    /// of the two groups in the class summary. A null transaction (a database-scoped statement) and a
    /// writable one return at once.
    /// </summary>
    internal static void Enforce(KvTransaction? tx, NodeAst ast)
    {
        if (tx is null || !tx.RefusesWrites)
            return;

        if (WriteStatementName(ast) is { } write)
        {
            tx.RequireWritable(write);
            return;
        }

        if (ChangesStateInOwnTransactions(ast.nodeType))
            tx.RequireReadWriteMode(CommandName(ast.nodeType));
    }

    /// <summary>
    /// The SQL verb of a statement that writes rows through the caller's transaction; null for every
    /// other statement. An <c>UPSERT</c> parses to an INSERT whose conflict clause says "upsert", and
    /// is named as the client wrote it.
    /// </summary>
    internal static string? WriteStatementName(NodeAst ast) => ast.nodeType switch
    {
        NodeType.Insert or NodeType.InsertSelect
            when ast.extendedThree is { nodeType: NodeType.OnConflict, yytext: sqlParser.OnConflictUpsert } => "UPSERT",
        NodeType.Insert or NodeType.InsertSelect => "INSERT",
        NodeType.Update => "UPDATE",
        NodeType.Delete => "DELETE",
        _ => null,
    };

    /// <summary>
    /// True for a statement that changes state in transactions of its own, so a rollback of the
    /// caller's transaction cannot undo it: schema DDL (<see cref="StatementScope.IsSchemaDdl"/>), the
    /// <c>COMMENT ON</c> family on database objects, and <c>REFRESH MATERIALIZED VIEW</c>.
    /// </summary>
    internal static bool ChangesStateInOwnTransactions(NodeType nodeType) =>
        StatementScope.IsSchemaDdl(nodeType) ||
        nodeType is NodeType.CommentOnTable or NodeType.CommentOnColumn or NodeType.CommentOnIndex or
                    NodeType.CommentOnSequence or NodeType.CommentOnView or NodeType.CommentOnMaterializedView or
                    NodeType.RefreshMaterializedView;

    /// <summary>
    /// The command name that the refusal message uses, in the style of a PostgreSQL command tag: the
    /// verb and the object type, such as <c>ALTER TABLE</c> or <c>DROP MATERIALIZED VIEW</c>.
    /// <c>CREATE INDEX</c> and <c>ALTER TABLE … ADD INDEX</c> parse to one node type, so both are named
    /// <c>CREATE INDEX</c>. Called only on the refusal path.
    /// </summary>
    internal static string CommandName(NodeType nodeType) => nodeType switch
    {
        NodeType.AlterTableAddIndex or NodeType.AlterTableAddIndexIfNotExists or
        NodeType.AlterTableAddUniqueIndex or NodeType.AlterTableAddUniqueIndexIfNotExists => "CREATE INDEX",
        NodeType.AlterTableDropIndex or NodeType.AlterTableDropIndexIfExists => "DROP INDEX",
        NodeType.CommentOnTable or NodeType.CommentOnColumn or NodeType.CommentOnIndex or
        NodeType.CommentOnSequence or NodeType.CommentOnView or NodeType.CommentOnMaterializedView => "COMMENT",
        _ => FamilyName(nodeType),
    };

    /// <summary>
    /// The first words of the node type's name, in upper case: the verb and the object type. The words
    /// <c>Or Replace</c> are skipped, and <c>Materialized</c> takes the next word with it. For example,
    /// <c>CreateOrReplaceView</c> gives <c>CREATE VIEW</c>, <c>AlterTableAddColumn</c> gives
    /// <c>ALTER TABLE</c>, and <c>DropMaterializedViewIfExists</c> gives
    /// <c>DROP MATERIALIZED VIEW</c>.
    /// </summary>
    private static string FamilyName(NodeType nodeType)
    {
        string name = nodeType.ToString();
        List<string> words = new(4);
        int start = 0;

        for (int i = 1; i <= name.Length; i++)
        {
            if (i < name.Length && !char.IsUpper(name[i]))
                continue;

            string word = name[start..i];
            start = i;

            if (word is "Or" or "Replace")
                continue;

            words.Add(word);
        }

        int take = words.Count > 2 && words[1] == "Materialized" ? 3 : Math.Min(2, words.Count);
        StringBuilder builder = new();

        for (int i = 0; i < take; i++)
        {
            if (i > 0)
                builder.Append(' ');

            builder.Append(words[i].ToUpperInvariant());
        }

        return builder.ToString();
    }
}
