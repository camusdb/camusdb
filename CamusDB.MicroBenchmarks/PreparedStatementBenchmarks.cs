/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using BenchmarkDotNet.Attributes;
using Google.Protobuf;

using CamusDB.Core.SQLParser;
using CamusDB.Grpc;
using CamusDB.Grpc.Client.Batching;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Measures what a prepared execution removes from the per-request cost, versus the inline request
/// carrying the same statement.
///
/// <para><b>Scope, stated plainly.</b> This is not an end-to-end throughput benchmark. It measures
/// the server-side per-request prologue plus the client-side cache lookup — <em>not</em> the whole
/// request. Engine execution past the prologue is identical by construction (a prepared execution
/// builds the same ticket and takes the same path), but that does not make the numbers below an
/// end-to-end figure, and they should not be quoted as one.</para>
///
/// <para>The two server-side costs the feature eliminates, both paid before any engine work
/// begins:</para>
/// <list type="number">
/// <item>protobuf request parsing, where every inline request re-materializes the SQL text, the
/// database name, and one .NET string per parameter <em>key</em> (the <c>ReadRawString</c> /
/// <c>ReadMapEntry</c> hotspot that motivated the work);</item>
/// <item>the transport-layer SQL parse, which both transports perform on every inline request purely
/// to decide how to route the statement, and which does <b>not</b> go through the executor's parser
/// cache.</item>
/// </list>
///
/// <para>And the client-side cost the feature must not <em>add</em>: looking a statement up in its
/// per-slot registration cache happens on every execution, so a key built by concatenating the SQL
/// each time would hand back in client allocations much of what the server saves. That is measured
/// here too, precisely because the server-side numbers cannot see it.</para>
/// </summary>
[MemoryDiagnoser]
public class PreparedStatementBenchmarks
{
    // Deliberately NOT const: a const database/SQL would let the compiler fold the concatenated key
    // into a single literal, hiding exactly the per-execution string copy this benchmark exists to
    // expose. Real callers hand these in at runtime.
    private static readonly string Sql = new string(
        "INSERT INTO robots (id, name, year, model, factory) VALUES (gen_id(), @name, @year, @model, @factory)".ToCharArray());

    private static readonly string Database = new string("productiondb".ToCharArray());

    private byte[] inlineBytes = null!;
    private byte[] preparedBytes = null!;

    // Stand-ins for a slot's registration cache, populated so both lookups are hits — the hot path a
    // client takes once a statement is registered.
    private readonly Dictionary<PreparedStatementKey, int> preparedCache = new();

    // Built once, exactly as CamusPreparedStatement builds its key at construction and reuses it for
    // every execution. Rebuilding it here would measure a cost the real path does not pay.
    private PreparedStatementKey key;
    private readonly Dictionary<string, int> concatenatedCache = new(StringComparer.Ordinal);

    [GlobalSetup]
    public void Setup()
    {
        SqlRequest inline = new() { Database = Database, Sql = Sql };
        inline.Parameters.Add("@name", new Value { StringValue = "optimus" });
        inline.Parameters.Add("@year", new Value { Int64Value = 1984 });
        inline.Parameters.Add("@model", new Value { StringValue = "convoy" });
        inline.Parameters.Add("@factory", new Value { StringValue = "cybertron-3" });
        inlineBytes = inline.ToByteArray();

        SqlRequest prepared = new() { StatementId = 7 };
        prepared.PositionalParameters.Add(new Value { StringValue = "optimus" });
        prepared.PositionalParameters.Add(new Value { Int64Value = 1984 });
        prepared.PositionalParameters.Add(new Value { StringValue = "convoy" });
        prepared.PositionalParameters.Add(new Value { StringValue = "cybertron-3" });
        preparedBytes = prepared.ToByteArray();

        key = new PreparedStatementKey(Database, Sql);
        preparedCache[key] = 1;
        concatenatedCache[Database + "\u001f" + Sql] = 1;

        // Wire size per execution is a property of the encoding, not something to benchmark, so it is
        // reported once here — it is the third number the results table cites.
        Console.WriteLine($"[wire] inline={inlineBytes.Length} B, prepared={preparedBytes.Length} B");
    }

    /// <summary>Protobuf parse only: the map-entry keys and the SQL/database strings.</summary>
    [Benchmark(Baseline = true)]
    public SqlRequest InlineProtoParse() => SqlRequest.Parser.ParseFrom(inlineBytes);

    [Benchmark]
    public SqlRequest PreparedProtoParse() => SqlRequest.Parser.ParseFrom(preparedBytes);

    /// <summary>
    /// What the transport actually does per request: parse the message, then parse the SQL to route
    /// it. The prepared counterpart has no second step at all — the root node was recorded once, at
    /// registration.
    /// </summary>
    [Benchmark]
    public NodeAst InlineProtoParseAndRoute()
    {
        SqlRequest request = SqlRequest.Parser.ParseFrom(inlineBytes);
        return SQLParserProcessor.Parse(request.Sql);
    }

    [Benchmark]
    public SqlRequest PreparedProtoParseAndRoute() => SqlRequest.Parser.ParseFrom(preparedBytes);

    // ─── Client-side cache lookup ─────────────────────────────────────────────

    /// <summary>
    /// The lookup a client performs per execution: the key was built once with the statement, so this
    /// allocates nothing and does not re-hash the SQL.
    /// </summary>
    [Benchmark]
    public bool ClientCacheLookup_StructuredKey() => preparedCache.ContainsKey(key);

    /// <summary>
    /// The same lookup with the delimiter-joined key this used to build. It copies the entire SQL
    /// text on every execution — kept as a benchmark so the regression stays visible if anyone
    /// reintroduces string composition on this path.
    /// </summary>
    [Benchmark]
    public bool ClientCacheLookup_ConcatenatedKey()
        => concatenatedCache.ContainsKey(Database + "\u001f" + Sql);
}
