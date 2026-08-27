/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Workload;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// The multi-table dataset mapping. A table that ends up empty, or a row index that two tables both
/// claim, would not fail loudly at run time: the seeder would still write every row, and the run would
/// still report throughput — it would just be testing a different placement than the one asked for.
/// These assertions pin the mapping instead.
/// </summary>
[TestFixture]
public sealed class DatasetTablesTests
{
    [Test]
    public void SingleTableKeepsTheHistoricalNameAndFingerprint()
    {
        Dataset implicitSingle = new(1847, 1_000, 256);
        Dataset explicitSingle = new(1847, 1_000, 256, tables: 1);

        Assert.That(implicitSingle.Tables, Is.EqualTo(1));
        Assert.That(implicitSingle.TableNames, Is.EqualTo(new[] { Dataset.TableName }));
        Assert.That(explicitSingle.Fingerprint(), Is.EqualTo(implicitSingle.Fingerprint()));
        Assert.That(implicitSingle.TableOf(0), Is.EqualTo(Dataset.TableName));
        Assert.That(implicitSingle.TableOf(999), Is.EqualTo(Dataset.TableName));
    }

    [Test]
    public void TableCountChangesTheFingerprint()
    {
        Dataset one = new(1847, 1_000, 256);
        Dataset many = new(1847, 1_000, 256, tables: 8);

        Assert.That(many.Fingerprint(), Is.Not.EqualTo(one.Fingerprint()),
            "a run must not accept a dataset seeded with a different table count");
        Assert.That(many.TableNames, Has.Count.EqualTo(8));
        Assert.That(many.TableNames[0], Is.EqualTo("workload_accounts_00"));
        Assert.That(many.TableNames[7], Is.EqualTo("workload_accounts_07"));
    }

    [TestCase(1_000L, 8)]
    [TestCase(1_000L, 3)]
    [TestCase(5L, 4)]
    [TestCase(64L, 64)]
    public void EveryRowLandsInExactlyOneTableAndNoTableIsEmpty(long rows, int tables)
    {
        Dataset dataset = new(1847, rows, 64, tables);
        long[] counted = new long[dataset.Tables];

        for (long index = 0; index < rows; index++)
        {
            int table = dataset.TableIndexOf(index);
            Assert.That(table, Is.InRange(0, dataset.Tables - 1));
            counted[table]++;
        }

        Assert.That(counted.Sum(), Is.EqualTo(rows));
        for (int table = 0; table < dataset.Tables; table++)
        {
            Assert.That(counted[table], Is.EqualTo(dataset.TableRowCount(table)),
                $"declared and observed row counts disagree for table {table}");
            Assert.That(counted[table], Is.GreaterThan(0), $"table {table} would be created and never used");
        }
    }

    [Test]
    public void TableBlocksAreContiguousAndStartWhereTheyClaim()
    {
        Dataset dataset = new(1847, 1_000, 64, tables: 7);

        Assert.That(dataset.TableRowStart(0), Is.EqualTo(0));
        for (int table = 0; table < dataset.Tables; table++)
        {
            long start = dataset.TableRowStart(table);
            long end = dataset.TableRowStart(table + 1);
            Assert.That(dataset.TableIndexOf(start), Is.EqualTo(table));
            Assert.That(dataset.TableIndexOf(end - 1), Is.EqualTo(table));
        }
        Assert.That(dataset.TableRowStart(dataset.Tables), Is.EqualTo(1_000));
    }

    [Test]
    public void RowIndexInTableStaysInsideTheRequestedTable()
    {
        Dataset dataset = new(1847, 1_000, 64, tables: 8);

        for (int table = 0; table < dataset.Tables; table++)
        {
            for (ulong offset = 0; offset < 300; offset += 7)
            {
                long index = dataset.RowIndexInTable(table, offset);
                Assert.That(dataset.TableIndexOf(index), Is.EqualTo(table));
                Assert.That(index, Is.InRange(0L, dataset.Rows - 1));
            }
        }
    }

    [Test]
    public void MoreTablesThanRowsIsClampedRatherThanLeavingEmptyTables()
    {
        Dataset dataset = new(1847, 3, 64, tables: 16);

        Assert.That(dataset.Tables, Is.EqualTo(3));
        Assert.That(dataset.TableNames, Has.Count.EqualTo(3));
    }

    [Test]
    public void RowIdentityDoesNotDependOnTheTableCount()
    {
        Dataset one = new(1847, 1_000, 64);
        Dataset many = new(1847, 1_000, 64, tables: 8);

        // Ids must stay a pure function of (seed, index): the transfer ledger and the row baseline are
        // written by index, and both are read back against whichever table now holds that index.
        for (long index = 0; index < 1_000; index += 97)
            Assert.That(many.RowFor(index), Is.EqualTo(one.RowFor(index)));
    }
}
