
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.Catalogs;

/// <summary>
/// The per-database, per-node clock of the <b>write-shape fence</b>. It orders the write statements
/// of this node against the schema changes this node applied, so a commit can tell whether a table it
/// wrote gained an index or a constraint after the write was planned.
///
/// <para><b>Why it exists.</b> A staged schema change waits for every <em>node</em> to apply each
/// step, but a transaction can outlive any number of steps. A transaction that inserts a row, stays
/// open while <c>CREATE INDEX</c> runs its backfill, and then commits, leaves a row with no index
/// entry: the insert planned its writes before the index existed, and the backfill read committed
/// rows only. The same shape lets an orphan commit after a foreign-key validation pass. The clock is
/// how such a commit is recognized and refused.</para>
///
/// <para><b>The protocol has three parts, and each depends on an order.</b></para>
/// <list type="number">
/// <item><b>A write statement reads <see cref="Current"/> before it resolves its table</b>, and pins
/// the table with that epoch (<see cref="Transactions.KvTransaction.PinWriteShape"/>). Before, not
/// after: an epoch read after the table was resolved could be newer than the descriptor the statement
/// plans with, and the pin would then vouch for writes that lack the new index.</item>
/// <item><b>A schema change calls <c>Advance</c> after its in-memory effect is complete</b>:
/// the schema is mutated, the foreign-key graph is published, the stale table descriptor is evicted.
/// So a statement that reads the new epoch is certain to plan with the new shape.</item>
/// <item><b><c>Advance</c> stamps the table before it publishes the epoch.</b> A statement
/// that read the old epoch is then refused at commit, because the stamp is later than its pin. With
/// the opposite order, a commit could run between the two writes, find no stamp, and pass.</item>
/// </list>
///
/// <para>What the stamp cannot refuse is a commit that passed its gate before the stamp and has not
/// landed yet. <see cref="Transactions.KvTransactionsManager.WaitForCommitsPlannedBeforeAsync"/> waits
/// for those, and the caller of <c>Advance</c> must use it before any backfill or validation
/// reads the table.</para>
///
/// <para><b>Node-local, in memory.</b> Every node runs the same protocol against its own
/// transactions. A node tells the schema leader that it applied a version only after its own commits
/// in flight have ended (<c>DatabaseDescriptor.PublishSchemaApplied</c>), which is how the leader
/// learns that the whole cluster is past them.</para>
/// </summary>
internal sealed class WriteShapeClock
{
    private readonly Lock sync = new();

    private long epoch;

    /// <summary>
    /// The published epoch. A write statement captures it <b>before</b> it opens its table; see the
    /// class summary for why the order matters.
    /// </summary>
    public long Current => Volatile.Read(ref epoch);

    /// <summary>
    /// Records that every table of <paramref name="tables"/> gained a write obligation, and returns
    /// the new epoch. Call it only after the change is fully visible to new statements on this node.
    /// Several tables are stamped together for a foreign key, which adds an obligation to the child
    /// and to the parent in one step.
    /// </summary>
    public long Advance(IReadOnlyList<TableSchema> tables)
    {
        ArgumentNullException.ThrowIfNull(tables);

        lock (sync)
        {
            long next = epoch + 1;

            // Stamp first, publish second. See the class summary, part 3.
            for (int i = 0; i < tables.Count; i++)
                tables[i].StampWriteShapeChange(next);

            Volatile.Write(ref epoch, next);

            return next;
        }
    }

    /// <summary><see cref="Advance(IReadOnlyList{TableSchema})"/> for one table.</summary>
    public long Advance(TableSchema table)
    {
        ArgumentNullException.ThrowIfNull(table);

        lock (sync)
        {
            long next = epoch + 1;

            table.StampWriteShapeChange(next);
            Volatile.Write(ref epoch, next);

            return next;
        }
    }

    /// <summary>
    /// Marks <paramref name="table"/> as an instance this node no longer uses, because it loaded a
    /// fresh one in its place. Every pin on the old instance fails from now on: the transaction
    /// planned against a schema this node gave up, and nothing can tell what it missed.
    /// </summary>
    public void Retire(TableSchema table)
    {
        ArgumentNullException.ThrowIfNull(table);

        lock (sync)
            table.StampWriteShapeChange(long.MaxValue);
    }
}
