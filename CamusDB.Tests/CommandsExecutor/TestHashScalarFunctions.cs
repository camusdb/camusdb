/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for the digest functions <c>md5</c>, <c>sha1</c>, <c>sha256</c> and <c>sha512</c>.
/// Expected values are the standard test vectors and digests computed by an independent tool
/// (Python's hashlib) over the UTF-8 bytes of the input, so they also match PostgreSQL.
/// </summary>
public sealed class TestHashScalarFunctions : SharedNodeBaseTest
{
    private const string NonAscii = "héllo wörld ✓ 😀";

    private static async Task<List<QueryResultRow>> ExecQuery(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<string> ScalarString(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, sql, parameters);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.String, rows[0].Row["0"].Type);
        return rows[0].Row["0"].StrValue!;
    }

    [TestCase("md5", "", "d41d8cd98f00b204e9800998ecf8427e")]
    [TestCase("md5", "abc", "900150983cd24fb0d6963f7d28e17f72")]
    [TestCase("sha1", "", "da39a3ee5e6b4b0d3255bfef95601890afd80709")]
    [TestCase("sha1", "abc", "a9993e364706816aba3e25717850c26c9cd0d89d")]
    [TestCase("sha256", "", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")]
    [TestCase("sha256", "abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")]
    [TestCase("sha512", "", "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e")]
    [TestCase("sha512", "abc", "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f")]
    public async Task KnownDigests_OfStringLiterals(string function, string input, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Assert.AreEqual(expected, await ScalarString(executor, database, dbname, $"SELECT {function}('{input}')"));
    }

    [TestCase("md5", "89aa2b820ceff5e2668f906e050e431d")]
    [TestCase("sha1", "1e635c63b152e750b83ca43d21b32e4b562af9db")]
    [TestCase("sha256", "23e0a0c5e66366c058e29407717008da83f9004340e71ff074bb24bd4b80eb2a")]
    [TestCase("sha512", "c0b1bdb8929a300951a589be04f022a372733ab3ea48b0a904779397fbb8f4bb8255fbaaa9915e96fcdcb05188466cc2ecfede8ad8bd83acb50fab8efac153e7")]
    public async Task NonAsciiString_HashesUtf8Bytes(string function, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Assert.AreEqual(expected, await ScalarString(executor, database, dbname, $"SELECT {function}('{NonAscii}')"));
    }

    // 300 two-byte characters: longer than the stack buffer, so the rented-buffer path runs.
    [TestCase("md5", "4bf78d5bfde016534964234b60ced94a")]
    [TestCase("sha1", "b99897d558467365d897b9c9500d4494138b5eef")]
    [TestCase("sha256", "7250b66610f8b7dbd6f5e5426d2143bcba6d826cedb4bea8a358695da78db023")]
    [TestCase("sha512", "043c2a02c28607fb9331e4474d83f8823245350566acd9aff935a9e79297847c58898563e30869360d1b34b1e0f0d87764553cbfd78b487d5f1f2e67c085b35c")]
    public async Task LongString_FromParameter_HashesUtf8Bytes(string function, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@v"] = new ColumnValue(ColumnType.String, new string('é', 300))
        };

        Assert.AreEqual(expected, await ScalarString(executor, database, dbname, $"SELECT {function}(@v)", parameters));
    }

    [Test]
    public async Task BytesArgument_HashesRawBytes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // X'616263' is the bytes of "abc", so it must give the same digest as the text.
        Assert.AreEqual("900150983cd24fb0d6963f7d28e17f72", await ScalarString(executor, database, dbname, "SELECT md5(X'616263')"));

        // Bytes that are not valid UTF-8 are hashed as they are, not re-encoded.
        Assert.AreEqual("481e4551ec039aada760901cf52b1917", await ScalarString(executor, database, dbname, "SELECT md5(X'00FF10')"));

        Assert.AreEqual(
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            await ScalarString(executor, database, dbname, "SELECT sha256(X'616263')"));
    }

    [TestCase("md5")]
    [TestCase("sha1")]
    [TestCase("sha256")]
    [TestCase("sha512")]
    public async Task NullArgument_ReturnsNull(string function)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, $"SELECT {function}(NULL)");
        Assert.AreEqual(ColumnType.Null, rows[0].Row["0"].Type);
    }

    [TestCase("SELECT md5(1)")]
    [TestCase("SELECT sha256(1.5)")]
    [TestCase("SELECT sha1(true)")]
    public async Task NonStringArgument_Throws(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        })!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("of type String or Bytes", ex.Message);
    }

    [Test]
    public async Task NumberCastToText_CanBeHashed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Assert.AreEqual("a1d0c6e83f027327d8461063f4ac58a6", await ScalarString(executor, database, dbname, "SELECT md5(42::text)"));
    }

    [Test]
    public async Task Md5_WithSubstring_GivesRandomStringShape()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        string value = await ScalarString(executor, database, dbname, "SELECT substring(md5('x'), 1, 12)");

        Assert.AreEqual(12, value.Length);
        Assert.IsTrue(value.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f'), value);
        Assert.AreEqual("9dd4e461268c8034f5c8564e155c67a6"[..12], value);
    }

    [Test]
    public async Task HashFunctions_WorkInTableProjectionAndWhere()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, database, dbname, "CREATE TABLE items (id int64 PRIMARY KEY, name string)");
        await ExecNonQuery(executor, database, dbname, "INSERT INTO items (id, name) VALUES (1, 'abc'), (2, 'def'), (3, NULL)");

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT id, md5(name) AS h FROM items ORDER BY id");
        Assert.AreEqual(3, rows.Count);
        Assert.AreEqual("900150983cd24fb0d6963f7d28e17f72", rows[0].Row["h"].StrValue);
        Assert.AreEqual(ColumnType.Null, rows[2].Row["h"].Type);

        List<QueryResultRow> filtered = await ExecQuery(
            executor, database, dbname,
            "SELECT id FROM items WHERE sha1(name) = 'a9993e364706816aba3e25717850c26c9cd0d89d'");
        Assert.AreEqual(1, filtered.Count);
        Assert.AreEqual(1L, filtered[0].Row["id"].LongValue);
    }

    [Test]
    public async Task HashFunctions_WorkInCheckConstraint()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, database, dbname,
            "CREATE TABLE blobs (id int64 PRIMARY KEY, body string, digest string CHECK (digest = sha256(body)))");

        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO blobs (id, body, digest) VALUES (1, 'abc', 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad')");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(executor, database, dbname,
                "INSERT INTO blobs (id, body, digest) VALUES (2, 'abc', 'not-the-digest')"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT id FROM blobs");
        Assert.AreEqual(1, rows.Count);
    }

    [Test]
    public async Task HashFunctions_WorkInColumnDefault()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, database, dbname,
            "CREATE TABLE tokens (id int64 PRIMARY KEY, token string DEFAULT (md5('seed')))");
        await ExecNonQuery(executor, database, dbname, "INSERT INTO tokens (id) VALUES (1)");

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT token FROM tokens WHERE id = 1");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("fe4c0f30aa359c41d9f9a5f69c8c4192", rows[0].Row["token"].StrValue);
    }
}
