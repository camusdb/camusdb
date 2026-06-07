
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.Config;

/// <summary>
/// PC3 acceptance tests — SQL parser cache knobs wired through ConfigDefinition / ConfigReader.
/// </summary>
[TestFixture]
public class TestConfigReaderSqlParserCache
{
    // ── Parsing — defaults ────────────────────────────────────────────────────

    [Test]
    public void ReadsDefaults_AllThreeKnobsMatchCamusDBConfigDefaults()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: standalone");

        Assert.That(config.SqlParserCacheTtlSeconds,   Is.EqualTo(300));
        Assert.That(config.SqlParserCacheMaxEntries,    Is.EqualTo(2048));
        Assert.That(config.SqlParserCacheSweepSeconds,  Is.EqualTo(60));
    }

    // ── Parsing — explicit overrides ──────────────────────────────────────────

    [Test]
    public void ReadsOverrides_AllThreeKnobsFromYaml()
    {
        string yml =
            "sql_parser_cache_ttl_seconds: 120\n" +
            "sql_parser_cache_max_entries: 500\n" +
            "sql_parser_cache_sweep_seconds: 30\n";

        ConfigDefinition config = new ConfigReader().Read(yml);

        Assert.That(config.SqlParserCacheTtlSeconds,  Is.EqualTo(120));
        Assert.That(config.SqlParserCacheMaxEntries,   Is.EqualTo(500));
        Assert.That(config.SqlParserCacheSweepSeconds, Is.EqualTo(30));
    }

    [Test]
    public void ReadsZeroTtl_Accepted_DisablesCache()
    {
        ConfigDefinition config = new ConfigReader().Read("sql_parser_cache_ttl_seconds: 0");

        Assert.That(config.SqlParserCacheTtlSeconds, Is.EqualTo(0));
    }

    [Test]
    public void ReadsZeroMaxEntries_Accepted_UnboundedCache()
    {
        ConfigDefinition config = new ConfigReader().Read("sql_parser_cache_max_entries: 0");

        Assert.That(config.SqlParserCacheMaxEntries, Is.EqualTo(0));
    }

    // ── Validation — rejected values ──────────────────────────────────────────

    [Test]
    public void RejectsNegativeTtl()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("sql_parser_cache_ttl_seconds: -1"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("sql_parser_cache_ttl_seconds"));
    }

    [Test]
    public void RejectsNegativeMaxEntries()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("sql_parser_cache_max_entries: -1"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("sql_parser_cache_max_entries"));
    }

    [Test]
    public void RejectsZeroSweepSeconds()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("sql_parser_cache_sweep_seconds: 0"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("sql_parser_cache_sweep_seconds"));
    }

    [Test]
    public void RejectsNegativeSweepSeconds()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("sql_parser_cache_sweep_seconds: -5"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("sql_parser_cache_sweep_seconds"));
    }

    // ── Integration — TTL=0 in config disables cache ──────────────────────────

    [Test]
    public async Task ZeroTtl_CacheDisabled_ParseWorksButStoresNothing()
    {
        // Simulate what Program.cs does: read config → create cache → use it.
        ConfigDefinition config = new ConfigReader().Read("sql_parser_cache_ttl_seconds: 0");

        await using SqlParserCache cache = new(
            logger: null,
            ttlSeconds:  config.SqlParserCacheTtlSeconds,
            maxEntries:  config.SqlParserCacheMaxEntries,
            sweepSeconds: config.SqlParserCacheSweepSeconds);

        SQLParserProcessor.Parse("SELECT id FROM users WHERE active = true", cache);

        Assert.That(cache.Count, Is.EqualTo(0),
            "With TTL=0 the cache must store nothing");
        Assert.That(cache.IsEnabled, Is.False);
    }
}
