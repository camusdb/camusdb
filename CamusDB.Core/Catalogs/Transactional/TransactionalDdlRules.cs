
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;
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
///
/// <para>The staged steps read the shared schema. A statement with a staged form therefore checks its own
/// definition against the private schema first, so it is refused for the same reason as outside a
/// transaction, and then refuses here each object it names that the shared schema does not hold in the same
/// form.</para>
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
    /// The refusal of a foreign key staged from the shared table <paramref name="child"/> that closes a cycle
    /// in the shared schema and none in the private one: the transaction dropped a foreign key of the cycle,
    /// which other transactions still enforce. <paramref name="refused"/> names the cycle.
    /// </summary>
    internal static CamusDBException CycleOfSharedSchema(string statement, TableDescriptor child, CamusDBException refused) => new(
        CamusDBErrorCodes.StatementNotAllowedInTransaction,
        $"{statement} on table '{child.Name}' cannot run in this transaction: a foreign key that the transaction removed still " +
        $"holds for other transactions before the commit, and the new one closes a cycle with it ({refused.Message}). " +
        "Commit first, then run it");

    /// <summary>
    /// Refuses a column staged on the shared table <paramref name="table"/> whose default draws from
    /// <paramref name="sequence"/>, a sequence of the private schema, when the shared schema has no such
    /// sequence or another one under the name: the transaction created or renamed it. Other transactions
    /// fill the column of the rows they write from the statement on, so they must be able to draw from it.
    /// </summary>
    internal static void RequireSharedSequence(TransactionSchemaOverlay overlay, SequenceSchema sequence, TableDescriptor table, string statement)
    {
        if (overlay.Database.SharedSchema.Sequences.TryGetValue(sequence.Name!, out SequenceSchema? shared)
            && string.Equals(shared.Id, sequence.Id, StringComparison.Ordinal))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.StatementNotAllowedInTransaction,
            $"{statement} on table '{table.Name}' cannot run in this transaction: the column draws from sequence " +
            $"'{sequence.Name}', which the transaction created or renamed, and the statement runs on the shared table, where " +
            "other transactions fill the column before the commit. Commit first, then run it");
    }

    /// <summary>
    /// Refuses a CHECK staged on the shared table <paramref name="table"/> when its
    /// <paramref name="condition"/> calls a function that the change set created, replaced or moved to
    /// another extension version. The existing rows are checked, and other transactions are held to the
    /// constraint, with the function of the shared schema. A function of another meaning would then take
    /// its place at the commit with no check of the rows, which a replace outside a transaction refuses for
    /// a function that a CHECK calls.
    /// </summary>
    internal static void RequireSharedFunctions(TransactionSchemaOverlay overlay, NodeAst condition, TableDescriptor table, string statement)
    {
        NodeAstWalk.Any(condition, node =>
        {
            if (node.nodeType == NodeType.ExprFuncCall && node.leftAst?.yytext is { Length: > 0 } name)
                RequireSharedFunction(overlay, name, table, statement);

            return false;
        });
    }

    /// <summary>
    /// Refuses <paramref name="statement"/>, staged on the shared table <paramref name="table"/>, when it calls
    /// the function <paramref name="name"/> and the change set created or changed that function: the rule of
    /// <see cref="RequireSharedFunctions"/> for one name. Also used for a column default that is stored as a
    /// function name, which other transactions evaluate for the rows they write from the statement on.
    /// </summary>
    internal static void RequireSharedFunction(TransactionSchemaOverlay overlay, string name, TableDescriptor table, string statement)
    {
        if (IsSameFunction(overlay.Schema, overlay.Database.SharedSchema, name))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.StatementNotAllowedInTransaction,
            $"{statement} on table '{table.Name}' cannot run in this transaction: it calls function '{name}', which the " +
            "transaction created or changed, and the statement runs on the shared table, where other transactions do not see " +
            "that change before the commit. Commit first, then run it");
    }

    /// <summary>
    /// True when a call of <paramref name="name"/> means the same in both schemas: the same inline function,
    /// and the same build of the extension that defines it. A built-in function is in neither.
    /// </summary>
    private static bool IsSameFunction(Schema own, Schema shared, string name)
    {
        // Both maps are immutable snapshots, which the private schema shares until the change set replaces one.
        if (ReferenceEquals(own.Functions, shared.Functions) && ReferenceEquals(own.Extensions, shared.Extensions))
            return true;

        own.Functions.TryGetValue(name, out InlineFunctionDefinition? ownFunction);
        shared.Functions.TryGetValue(name, out InlineFunctionDefinition? sharedFunction);

        if (!ReferenceEquals(ownFunction, sharedFunction)
            && (ownFunction is null || sharedFunction is null || !ownFunction.HasSameMeaning(sharedFunction)))
            return false;

        ExtensionDefinition? ownExtension = ExtensionOf(own, name);
        ExtensionDefinition? sharedExtension = ExtensionOf(shared, name);

        return ReferenceEquals(ownExtension, sharedExtension)
            || (ownExtension is not null && sharedExtension is not null
                && string.Equals(ownExtension.Name, sharedExtension.Name, StringComparison.OrdinalIgnoreCase)
                && string.Equals(ownExtension.Version, sharedExtension.Version, StringComparison.Ordinal)
                && string.Equals(ownExtension.Sha256, sharedExtension.Sha256, StringComparison.Ordinal)
                && string.Equals(ownExtension.DependenciesSha256, sharedExtension.DependenciesSha256, StringComparison.Ordinal));
    }

    /// <summary>The extension of <paramref name="schema"/> that defines the function a call names, by its plain name or as <c>extension.member</c>.</summary>
    private static ExtensionDefinition? ExtensionOf(Schema schema, string name)
    {
        int dot = name.IndexOf('.');

        return dot < 0
            ? InlineFunctionDeltaApplier.ExtensionThatDefines(schema, name)
            : schema.Extensions.GetValueOrDefault(name[..dot]);
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
