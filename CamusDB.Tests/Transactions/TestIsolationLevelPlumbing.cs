
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;

using Kahuna;
using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// Isolation level + transaction mode plumbing.
///
/// Verifies:
///   - Default begin → ReadCommitted / ReadWrite (zero observable change).
///   - Explicit Serializable / ReadOnly round-trips through KvTransactionsManager.
///   - All four {level} × {mode} combinations are readable from KvTransaction.
///   - the configured default isolation level is honoured when no explicit level is passed.
///   - SET TRANSACTION ISOLATION LEVEL SERIALIZABLE parses to NodeType.SetTransaction with
///     correct yytext ("Serializable") and leftAst.yytext ("ReadWrite").
///   - SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY / READ WRITE parse correctly.
///   - Unknown level and unknown mode throw parse errors.
///   - Zero observable change: all behaviour is metadata-only (no locking change).
/// </summary>
[TestFixture]
public sealed class TestIsolationLevelPlumbing
{
    /// <summary>Baseline configuration — no engine is involved in these tests.</summary>
    private static CamusDBOptions TransactionOptions => CamusDBOptions.Default;

    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr)> CreateAsync(
        string tag, CamusDBOptions? options = null)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);
        return (node, new KvTransactionsManager(node.Kahuna, options ?? TransactionOptions));
    }

    // ------------------------------------------------------------------
    // 1. Default: Serializable + ReadWrite
    // ------------------------------------------------------------------

    [Test]
    public async Task BeginAsync_Default_IsSerializableReadWrite()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("iso-default");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();

        Assert.AreEqual(CamusIsolationLevel.Serializable, tx.IsolationLevel);
        Assert.AreEqual(CamusTransactionMode.ReadWrite, tx.TransactionMode);
        Assert.IsFalse(tx.IsReadOnly);

        await mgr.RollbackAsync(tx);
    }

    // ------------------------------------------------------------------
    // 2. All four {level} × {mode} combinations
    // ------------------------------------------------------------------

    [Test]
    public async Task BeginAsync_Serializable_ReadWrite()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("iso-ser-rw");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        Assert.AreEqual(CamusIsolationLevel.Serializable, tx.IsolationLevel);
        Assert.AreEqual(CamusTransactionMode.ReadWrite, tx.TransactionMode);

        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task BeginAsync_Serializable_ReadOnly()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("iso-ser-ro");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        Assert.AreEqual(CamusIsolationLevel.Serializable, tx.IsolationLevel);
        Assert.AreEqual(CamusTransactionMode.ReadOnly, tx.TransactionMode);

        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task BeginAsync_ReadCommitted_ReadOnly()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("iso-rc-ro");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly);

        Assert.AreEqual(CamusIsolationLevel.ReadCommitted, tx.IsolationLevel);
        Assert.AreEqual(CamusTransactionMode.ReadOnly, tx.TransactionMode);

        await mgr.RollbackAsync(tx);
    }

    // ------------------------------------------------------------------
    // 3. The configured default isolation level is honoured
    // ------------------------------------------------------------------

    [Test]
    public async Task BeginAsync_HonoursConfigDefault_WhenLevelNotSpecified()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync(
            "iso-config-default", TransactionOptions with { DefaultIsolationLevel = CamusIsolationLevel.Serializable });
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();

        Assert.AreEqual(CamusIsolationLevel.Serializable, tx.IsolationLevel,
            "BeginAsync with no explicit level must honour the manager's configured default");

        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task BeginAsync_ExplicitLevelOverridesConfigDefault()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync(
            "iso-config-override", TransactionOptions with { DefaultIsolationLevel = CamusIsolationLevel.Serializable });
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);

        Assert.AreEqual(CamusIsolationLevel.ReadCommitted, tx.IsolationLevel,
            "Explicit ReadCommitted must override the Serializable configured default");

        await mgr.RollbackAsync(tx);
    }

    // ------------------------------------------------------------------
    // 4. SQL parser: SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
    // ------------------------------------------------------------------

    [Test]
    public void SetTransaction_Parse_Serializable_DefaultsToReadWrite()
    {
        NodeAst ast = SQLParserProcessor.Parse("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");

        Assert.AreEqual(NodeType.SetTransaction, ast.nodeType);
        Assert.AreEqual("Serializable", ast.yytext);
        // leftAst is NodeType.String (not Identifier) so IdentifierNormalizer leaves it as-is
        Assert.IsNotNull(ast.leftAst);
        Assert.AreEqual(NodeType.String, ast.leftAst!.nodeType);
        Assert.AreEqual("ReadWrite", ast.leftAst.yytext);
    }

    [Test]
    public void SetTransaction_Parse_Serializable_ReadOnly()
    {
        NodeAst ast = SQLParserProcessor.Parse("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY");

        Assert.AreEqual(NodeType.SetTransaction, ast.nodeType);
        Assert.AreEqual("Serializable", ast.yytext);
        Assert.IsNotNull(ast.leftAst);
        Assert.AreEqual(NodeType.String, ast.leftAst!.nodeType);
        Assert.AreEqual("ReadOnly", ast.leftAst.yytext);
    }

    [Test]
    public void SetTransaction_Parse_Serializable_ReadWrite_Explicit()
    {
        NodeAst ast = SQLParserProcessor.Parse("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE");

        Assert.AreEqual(NodeType.SetTransaction, ast.nodeType);
        Assert.AreEqual("Serializable", ast.yytext);
        Assert.IsNotNull(ast.leftAst);
        Assert.AreEqual(NodeType.String, ast.leftAst!.nodeType);
        Assert.AreEqual("ReadWrite", ast.leftAst.yytext);
    }

    [Test]
    public void SetTransaction_Parse_CaseInsensitive()
    {
        NodeAst ast = SQLParserProcessor.Parse("set transaction isolation level serializable read only");

        Assert.AreEqual(NodeType.SetTransaction, ast.nodeType);
        Assert.AreEqual("Serializable", ast.yytext);
        Assert.AreEqual("ReadOnly", ast.leftAst!.yytext);
    }

    [Test]
    public void SetTransaction_Parse_MixedCase()
    {
        NodeAst ast = SQLParserProcessor.Parse("Set Transaction Isolation Level Serializable");

        Assert.AreEqual(NodeType.SetTransaction, ast.nodeType);
        Assert.AreEqual("Serializable", ast.yytext);
    }

    // ------------------------------------------------------------------
    // 5. SQL parser: invalid usage throws
    // ------------------------------------------------------------------

    [Test]
    public void SetTransaction_Parse_UnknownLevel_Throws()
    {
        Assert.Throws<CamusDBException>(() =>
            SQLParserProcessor.Parse("SET TRANSACTION ISOLATION LEVEL SNAPSHOT"));
    }

    [Test]
    public void SetTransaction_Parse_WrongKeyword_Throws()
    {
        Assert.Throws<CamusDBException>(() =>
            SQLParserProcessor.Parse("SET TRANSACTION LEVEL ISOLATION SERIALIZABLE"));
    }

    [Test]
    public void SetTransaction_Parse_UnknownMode_Throws()
    {
        Assert.Throws<CamusDBException>(() =>
            SQLParserProcessor.Parse("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ MAYBE"));
    }

    // ------------------------------------------------------------------
    // 6. SET TRANSACTION LOCKING parser tests (no Kahuna node required)
    // ------------------------------------------------------------------

    [Test]
    public void SetTransactionLocking_Parse_Pessimistic()
    {
        NodeAst ast = SQLParserProcessor.Parse("SET TRANSACTION LOCKING PESSIMISTIC");

        Assert.AreEqual(NodeType.SetTransactionLocking, ast.nodeType);
        Assert.AreEqual("Pessimistic", ast.yytext);
    }

    [Test]
    public void SetTransactionLocking_Parse_Optimistic()
    {
        NodeAst ast = SQLParserProcessor.Parse("SET TRANSACTION LOCKING OPTIMISTIC");

        Assert.AreEqual(NodeType.SetTransactionLocking, ast.nodeType);
        Assert.AreEqual("Optimistic", ast.yytext);
    }

    [Test]
    public void SetTransactionLocking_Parse_CaseInsensitive()
    {
        NodeAst ast = SQLParserProcessor.Parse("set transaction locking optimistic");

        Assert.AreEqual(NodeType.SetTransactionLocking, ast.nodeType);
        Assert.AreEqual("Optimistic", ast.yytext);
    }

    [Test]
    public void SetTransactionLocking_Parse_MixedCase()
    {
        NodeAst ast = SQLParserProcessor.Parse("Set Transaction Locking Pessimistic");

        Assert.AreEqual(NodeType.SetTransactionLocking, ast.nodeType);
        Assert.AreEqual("Pessimistic", ast.yytext);
    }

    [Test]
    public void SetTransactionLocking_Parse_UnknownMode_Throws()
    {
        Assert.Throws<CamusDBException>(() =>
            SQLParserProcessor.Parse("SET TRANSACTION LOCKING SNAPSHOT"));
    }

    [Test]
    public void SetTransactionLocking_Parse_DoesNotConflictWithIsolationLevel()
    {
        // Isolation level parsing must still work alongside the new locking production.
        NodeAst iso = SQLParserProcessor.Parse("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        Assert.AreEqual(NodeType.SetTransaction, iso.nodeType);
        Assert.AreEqual("Serializable", iso.yytext);

        NodeAst locking = SQLParserProcessor.Parse("SET TRANSACTION LOCKING PESSIMISTIC");
        Assert.AreEqual(NodeType.SetTransactionLocking, locking.nodeType);
    }

    // ------------------------------------------------------------------
    // 7. ApplyLocking on KvTransaction (unit-level, no Kahuna required)
    // ------------------------------------------------------------------

    [Test]
    public async Task ApplyLocking_BeforeStatement_ChangesLockingMode()
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("locking-apply/warmup", CancellationToken.None);
        await using EmbeddedKahuna __ = node;

        // Wire mintLocalT and request deferStart so BeginAsync uses the deferred-start path, leaving
        // TransactionId==Zero until the first write. This is the precondition under which ApplyLocking
        // is valid (locking can only change before the Kahuna session opens).
        Kommander.Time.HLCTimestamp mintLocalT(Kommander.Time.HLCTimestamp? _) =>
            node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId());

        KvTransactionsManager mgr = new(node.Kahuna, TransactionOptions, mintLocalT);

        KvTransaction tx = await mgr.BeginAsync(deferStart: true);
        Assert.AreEqual(KeyValueTransactionLocking.Pessimistic, tx.Locking);

        tx.ApplyLocking(KeyValueTransactionLocking.Optimistic);
        Assert.AreEqual(KeyValueTransactionLocking.Optimistic, tx.Locking);

        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task ApplyLocking_AfterStatement_Throws()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("locking-after-stmt");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();
        tx.MarkStatementExecuted();

        Assert.Throws<CamusDBException>(() => tx.ApplyLocking(KeyValueTransactionLocking.Optimistic));

        await mgr.RollbackAsync(tx);
    }

    // ------------------------------------------------------------------
    // 8. Zero observable change: Serializable tx commits/rolls back OK
    // ------------------------------------------------------------------

    [Test]
    public async Task SerializableTx_CommitAndRollback_WorkAsUsual()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("iso-commit-rollback");
        await using EmbeddedKahuna __ = node;

        KvTransaction txCommit = await mgr.BeginAsync(CamusIsolationLevel.Serializable);
        await mgr.CommitAsync(txCommit);
        Assert.AreEqual(KvTransactionStatus.Committed, txCommit.Status);

        KvTransaction txRollback = await mgr.BeginAsync(CamusIsolationLevel.Serializable);
        await mgr.RollbackAsync(txRollback);
        Assert.AreEqual(KvTransactionStatus.RolledBack, txRollback.Status);
    }
}
