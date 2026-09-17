/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// How the writer may store a large value of one variable-length column (<c>string</c>,
/// <c>bytes</c>, array). The names and the behaviour follow PostgreSQL's column storage modes,
/// because an operator who knows those already knows these.
///
/// <para>The strategy is a <b>write-time decision only</b>. Every stored row records, per cell,
/// whether that cell is compressed and whether it holds an out-of-line pointer, and a reader follows
/// those per-cell marks, never the column's current strategy. So an <c>ALTER ... SET STORAGE</c>
/// changes the form of future writes and cannot break a read of a row written under an earlier
/// strategy.</para>
///
/// <para>A vector column (a <c>bytes</c> value of packed float32) should take <see cref="External"/>
/// or <see cref="Plain"/>: float32 data does not compress, so <see cref="Extended"/> only spends
/// CPU on every write. The compression payback rule refuses to store such a value compressed, but the
/// attempt still costs time.</para>
///
/// <para>Numeric values are persisted in schema JSON and in the schema log, so they must stay
/// stable. A column with no stored strategy is <see cref="Extended"/>.</para>
/// </summary>
public enum ColumnStorageStrategy
{
    /// <summary>
    /// The default. Compress the value when compression pays, then move it out of the row when the
    /// stored form is still at or above the threshold.
    /// </summary>
    Extended = 0,

    /// <summary>Never compress, never move out of the row.</summary>
    Plain = 1,

    /// <summary>Compress the value when compression pays, and always keep it inside the row.</summary>
    Main = 2,

    /// <summary>Never compress. Move the value out of the row when it is at or above the threshold.</summary>
    External = 3,
}

/// <summary>SQL spelling of <see cref="ColumnStorageStrategy"/>, shared by the parser and SHOW CREATE TABLE.</summary>
public static class ColumnStorageStrategies
{
    /// <summary>
    /// Parses a strategy name case-insensitively. An unknown name raises
    /// <see cref="CamusDBErrorCodes.InvalidInput"/> that lists the accepted names.
    /// </summary>
    public static ColumnStorageStrategy Parse(string name) => name.ToUpperInvariant() switch
    {
        "PLAIN" => ColumnStorageStrategy.Plain,
        "MAIN" => ColumnStorageStrategy.Main,
        "EXTERNAL" => ColumnStorageStrategy.External,
        "EXTENDED" => ColumnStorageStrategy.Extended,
        _ => throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Unknown storage strategy '{name}'. Expected PLAIN, MAIN, EXTERNAL or EXTENDED"),
    };

    /// <summary>The SQL keyword for <paramref name="strategy"/>.</summary>
    public static string ToSql(ColumnStorageStrategy strategy) => strategy switch
    {
        ColumnStorageStrategy.Plain => "PLAIN",
        ColumnStorageStrategy.Main => "MAIN",
        ColumnStorageStrategy.External => "EXTERNAL",
        _ => "EXTENDED",
    };
}
