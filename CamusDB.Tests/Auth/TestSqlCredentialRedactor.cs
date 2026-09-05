
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core.Auth;

namespace CamusDB.Tests.Auth;

/// <summary>
/// The redactor scans string literals with its own pattern rather than the lexer, so it has to be
/// tested against the same literal forms the lexer accepts. A password shape that ends the match
/// early leaves the tail of the credential in the log, which is the one failure this class exists to
/// prevent — so the assertions check that the secret is <em>absent</em>, not merely that something
/// was replaced.
/// </summary>
[TestFixture]
internal sealed class TestSqlCredentialRedactor
{
    [TestCase("CREATE USER bob IDENTIFIED BY 'secret'", "secret")]
    [TestCase("ALTER USER bob IDENTIFIED BY 'secret'", "secret")]
    [TestCase("CREATE USER bob IDENTIFIED WITH plugin BY 'secret'", "secret")]
    [TestCase("create user bob identified by 'secret'", "secret")]
    [TestCase("CREATE USER bob IDENTIFIED BY 'a''b'", "a''b")]
    public void RedactsThePasswordLiteral(string sql, string secret)
    {
        string redacted = SqlCredentialRedactor.Redact(sql);

        Assert.That(redacted, Does.Not.Contain(secret));
        Assert.That(redacted, Does.Contain("'***'"));
    }

    /// <summary>
    /// A self-service password change names two credentials, and only the first follows
    /// <c>IDENTIFIED … BY</c>. The second — the password the account holds right now — is reached
    /// through <c>REPLACE</c>, which a scan looking only for the first keyword walks straight past.
    /// If anything, it is the more damaging of the two to leave in a log: it is the secret currently
    /// in force, while the other is about to be.
    /// </summary>
    [TestCase("ALTER USER bob IDENTIFIED BY 'brand-new' REPLACE 'in-force'")]
    [TestCase("alter user bob identified by 'brand-new' replace 'in-force'")]
    [TestCase("ALTER USER bob IDENTIFIED WITH sha256_password BY 'brand-new' REPLACE 'in-force'")]
    [TestCase("ALTER USER bob IDENTIFIED BY E'brand\\'new' REPLACE E'in\\'force'")]
    public void RedactsBothPasswordsOfAReplaceClause(string sql)
    {
        string redacted = SqlCredentialRedactor.Redact(sql);

        Assert.That(redacted, Does.Not.Contain("brand"), redacted);
        Assert.That(redacted, Does.Not.Contain("force"), redacted);

        // Two literals masked, not one: a single mask would mean the scan stopped after the first.
        Assert.AreEqual(2, CountOccurrences(redacted, "'***'"), redacted);

        // The statement is still readable as a statement — the point is to log it, minus the secrets.
        Assert.That(redacted, Does.Contain("ALTER USER").IgnoreCase);
    }

    /// <summary>
    /// A bare <c>REPLACE</c> with nothing that parses as a literal after it must leave the statement
    /// alone rather than masking to the end. The clause is optional, so a statement without it is the
    /// common case and must survive unchanged past the first mask.
    /// </summary>
    [Test]
    public void LeavesATrailingReplaceWithNoLiteralAlone()
    {
        string redacted = SqlCredentialRedactor.Redact("ALTER USER bob IDENTIFIED BY 'secret' REPLACE");

        Assert.That(redacted, Does.Not.Contain("secret"));
        Assert.AreEqual(1, CountOccurrences(redacted, "'***'"), redacted);
        Assert.That(redacted, Does.EndWith("REPLACE"));
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        int count = 0;
        int at = 0;

        while ((at = haystack.IndexOf(needle, at, System.StringComparison.Ordinal)) >= 0)
        {
            count++;
            at += needle.Length;
        }

        return count;
    }

    /// <summary>
    /// An <c>E'…'</c> password is the case that breaks a plain-literal pattern: the escaped quote
    /// ends the match early and everything after it — including the rest of the password — survives
    /// into the log.
    /// </summary>
    [TestCase(@"CREATE USER bob IDENTIFIED BY E'a\'tail'", "tail")]
    [TestCase(@"ALTER USER bob IDENTIFIED BY E'pre\'post\'end'", "post")]
    [TestCase(@"CREATE USER bob IDENTIFIED BY E'a\\b'", "a\\b")]
    [TestCase(@"CREATE USER bob IDENTIFIED BY e'lower\'case'", "case")]
    public void RedactsAnEscapeFormPassword(string sql, string mustNotLeak)
    {
        string redacted = SqlCredentialRedactor.Redact(sql);

        Assert.That(redacted, Does.Not.Contain(mustNotLeak), $"credential leaked: {redacted}");
        Assert.That(redacted, Does.Contain("'***'"));
    }

    /// <summary>
    /// In a plain literal a backslash is an ordinary character, so it must not be treated as an
    /// escape — consuming the following quote would run the match past the end of the literal and
    /// swallow the rest of the statement into the redaction.
    /// </summary>
    [Test]
    public void PlainLiteralBackslashIsNotAnEscape()
    {
        string redacted = SqlCredentialRedactor.Redact(@"CREATE USER bob IDENTIFIED BY 'p\ss'; SELECT 1");

        Assert.That(redacted, Does.Not.Contain(@"p\ss"));
        Assert.That(redacted, Does.Contain("'***'"));
        Assert.That(redacted, Does.Contain("SELECT 1"), "the statement after the literal must survive");
    }

    /// <summary>
    /// The grammar accepts any string literal as the secret (<c>auth_secret : string</c>) and any
    /// identifier form as the plugin, including a backtick-quoted one. A redactor that only knows the
    /// single-quoted forms leaks the password for every other shape — and a leak is silent, so these
    /// combinations are enumerated rather than sampled.
    /// </summary>
    [TestCase("CREATE USER u IDENTIFIED BY \"secret\"", "secret")]
    [TestCase("CREATE USER u IDENTIFIED BY E\"secret\"", "secret")]
    [TestCase("CREATE USER u IDENTIFIED BY E\"line\\nsecret\"", "secret")]
    [TestCase("CREATE USER u IDENTIFIED BY \"a\"\"b\"", "a\"\"b")]
    [TestCase("CREATE USER u IDENTIFIED WITH `sha256_password` BY 'secret'", "secret")]
    [TestCase("CREATE USER u IDENTIFIED WITH `sha256_password` BY \"secret\"", "secret")]
    [TestCase("ALTER USER u IDENTIFIED WITH `plug in` BY 'secret'", "secret")]
    [TestCase("CREATE USER u IDENTIFIED  WITH  plugin  BY  'secret'", "secret")]
    public void RedactsEveryLiteralAndPluginFormTheGrammarAccepts(string sql, string secret)
    {
        string redacted = SqlCredentialRedactor.Redact(sql);

        Assert.That(redacted, Does.Not.Contain(secret), $"credential leaked: {redacted}");
        Assert.That(redacted, Does.Contain("'***'"));
    }

    /// <summary>
    /// An unterminated literal must mask through the end of the input. Leaving the tail unmasked
    /// would log the credential of exactly the malformed statement that is most likely to be logged,
    /// since redaction runs before validation rejects it.
    /// </summary>
    [Test]
    public void MasksThroughTheEndOfAnUnterminatedLiteral()
    {
        string redacted = SqlCredentialRedactor.Redact("CREATE USER u IDENTIFIED BY 'unterminated");

        Assert.That(redacted, Does.Not.Contain("unterminated"));
    }

    /// <summary>A word merely starting with the keyword must not trigger a mask.</summary>
    [Test]
    public void DoesNotMatchAKeywordPrefix()
    {
        const string sql = "SELECT identifiedx FROM t WHERE v = 'keep'";
        Assert.AreEqual(sql, SqlCredentialRedactor.Redact(sql));
    }

    /// <summary>Two credential statements in one body must both be masked.</summary>
    [Test]
    public void RedactsEveryOccurrence()
    {
        string redacted = SqlCredentialRedactor.Redact(
            "CREATE USER a IDENTIFIED BY 'first'; CREATE USER b IDENTIFIED BY 'second'");

        Assert.That(redacted, Does.Not.Contain("first"));
        Assert.That(redacted, Does.Not.Contain("second"));
    }

    /// <summary>A parameterized password has no literal to redact and must be left alone.</summary>
    [Test]
    public void LeavesAParameterizedPasswordAlone()
    {
        const string sql = "CREATE USER bob IDENTIFIED BY @p";
        Assert.AreEqual(sql, SqlCredentialRedactor.Redact(sql));
    }

    /// <summary>Statements with no credential are returned untouched.</summary>
    [Test]
    public void LeavesUnrelatedStatementsAlone()
    {
        const string sql = "SELECT * FROM users WHERE name = 'identified'";
        Assert.AreEqual(sql, SqlCredentialRedactor.Redact(sql));
    }
}
