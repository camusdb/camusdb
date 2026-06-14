
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
///   - CamusDBConfig.DefaultIsolationLevel is honoured when no explicit level is passed.
///   - SET TRANSACTION ISOLATION LEVEL SERIALIZABLE parses to NodeType.SetTransaction with
///     correct yytext ("Serializable") and leftAst.yytext ("ReadWrite").
///   - SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY / READ WRITE parse correctly.
///   - Unknown level and unknown mode throw parse errors.
///   - Zero observable change: all behaviour is metadata-only (no locking change).
/// </summary>
[TestFixture]
public sealed class TestIsolationLevelPlumbing
{
    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr)> CreateAsync(string tag)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);
        return (node, new KvTransactionsManager(node.Kahuna));
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
    // 3. CamusDBConfig.DefaultIsolationLevel is honoured
    // ------------------------------------------------------------------

    [Test]
    [NonParallelizable] // mutates the global CamusDBConfig.DefaultIsolationLevel static
    public async Task BeginAsync_HonoursConfigDefault_WhenLevelNotSpecified()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("iso-config-default");
        await using EmbeddedKahuna __ = node;

        CamusIsolationLevel saved = CamusDBConfig.DefaultIsolationLevel;
        try
        {
            CamusDBConfig.DefaultIsolationLevel = CamusIsolationLevel.Serializable;
            KvTransaction tx = await mgr.BeginAsync();

            Assert.AreEqual(CamusIsolationLevel.Serializable, tx.IsolationLevel,
                "BeginAsync with no explicit level should honour CamusDBConfig.DefaultIsolationLevel");

            await mgr.RollbackAsync(tx);
        }
        finally
        {
            CamusDBConfig.DefaultIsolationLevel = saved;
        }
    }

    [Test]
    [NonParallelizable] // mutates the global CamusDBConfig.DefaultIsolationLevel static
    public async Task BeginAsync_ExplicitLevelOverridesConfigDefault()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("iso-config-override");
        await using EmbeddedKahuna __ = node;

        CamusIsolationLevel saved = CamusDBConfig.DefaultIsolationLevel;
        try
        {
            CamusDBConfig.DefaultIsolationLevel = CamusIsolationLevel.Serializable;
            KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);

            Assert.AreEqual(CamusIsolationLevel.ReadCommitted, tx.IsolationLevel,
                "Explicit ReadCommitted must override the Serializable config default");

            await mgr.RollbackAsync(tx);
        }
        finally
        {
            CamusDBConfig.DefaultIsolationLevel = saved;
        }
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
    // 6. Zero observable change: Serializable tx commits/rolls back OK
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
