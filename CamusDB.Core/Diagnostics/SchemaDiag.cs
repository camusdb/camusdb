/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Opt-in <see cref="Console.Error"/> tracing for the distributed schema-replication convergence
/// path — the schema-ack gate, follower schema-apply (including ordering), and follower→leader ack
/// delivery. Off unless <c>CAMUS_SCHEMA_DIAG=1</c> (or <c>true</c>) is set, so it adds no overhead or
/// log noise in normal operation.
///
/// <para>
/// Exists to diagnose cluster schema-convergence flakes, where <c>CreateTable</c>'s
/// <see cref="Storage.Kv.EmbeddedKahuna.WaitForSchemaAcksAsync"/> gate times out because a follower
/// never reaches the target schema version in time. The traces distinguish the two observed modes:
/// (a) the follower applies the version but its ack to the leader is lost (look for a
/// <c>SCHEMA-DIAG APPLY</c> for the version with no matching <c>SCHEMA-DIAG REMOTE-ACK</c>), and
/// (b) the follower never applies the version (no <c>SCHEMA-DIAG APPLY</c> for it at all, or a
/// <c>SCHEMA-DIAG APPLY-OOO</c> out-of-order entry whose <c>localVer</c> != the delta's from-version).
/// </para>
///
/// <para>
/// Enable and grep, e.g.: <c>CAMUS_SCHEMA_DIAG=1 dotnet test … 2&gt;&amp;1 | grep SCHEMA-DIAG</c>.
/// All lines carry the emitting node endpoint so flows can be reconstructed across nodes.
/// </para>
/// </summary>
public static class SchemaDiag
{
    /// <summary>Whether schema-convergence tracing is enabled (<c>CAMUS_SCHEMA_DIAG=1|true</c>).</summary>
    public static readonly bool Enabled =
        string.Equals(Environment.GetEnvironmentVariable("CAMUS_SCHEMA_DIAG"), "1", StringComparison.Ordinal) ||
        string.Equals(Environment.GetEnvironmentVariable("CAMUS_SCHEMA_DIAG"), "true", StringComparison.OrdinalIgnoreCase);

    /// <summary>Writes a <c>SCHEMA-DIAG &lt;message&gt;</c> line to stderr when enabled; a no-op otherwise.</summary>
    public static void Log(string message)
    {
        if (Enabled)
            Console.Error.WriteLine($"SCHEMA-DIAG {message}");
    }
}
