/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core;

/// <summary>
/// Values that are fixed by the storage/wire formats rather than tunable by an operator: changing one
/// is a format change, not a configuration change, so they are compile-time constants and deliberately
/// absent from <see cref="CamusDBOptions"/>. Anything an operator may legitimately set belongs there
/// instead.
/// </summary>
public static class CamusDBConstants
{
    /// <summary>
    /// The internal name used to identify primary key indices.
    /// This name should only be changed in a new installation. Changing it after
    /// having databases with tables and data can cause unexpected problems.
    /// </summary>
    public const string PrimaryKeyInternalName = "~pk";

    /// <summary>
    /// Default maximum length (in UTF-16 <c>string.Length</c> characters) for a <c>String</c>
    /// column declared without an explicit <c>string(N)</c> bound.
    /// Enforced when a row value is validated on write; stored as <c>null</c> in the schema metadata.
    /// Value: 2 621 440 characters (~5 MB in the worst-case UTF-16 encoding).
    /// </summary>
    public const int DefaultStringMaxLength = 2_621_440;

    /// <summary>
    /// Default maximum payload length (in bytes) for a <c>Bytes</c> column declared without
    /// an explicit bound.
    /// Enforced when a row value is validated on write; stored as <c>null</c> in the schema metadata.
    /// Value: 10 485 760 bytes (10 MB).
    /// </summary>
    public const int DefaultBytesMaxLength = 10_485_760;

    /// <summary>
    /// Maximum length (in UTF-16 <c>string.Length</c> characters) of a comment attached to a table,
    /// column, index, or database. Comments ride the replicated per-table metadata blob and the
    /// registry entry, so an unbounded comment would inflate every schema checkpoint and every
    /// schema-log entry. Enforced at ticket validation; exceeding it raises
    /// <see cref="CamusDBErrorCodes.CommentTooLong"/>.
    /// </summary>
    public const int MaxCommentLength = 65_535;

    /// <summary>
    /// Upper bound (in UTF-8 bytes) on a supplied password before it is fed to the key-derivation
    /// function. Caps the work an attacker can force per hash attempt (a multi-megabyte password would
    /// otherwise turn each verification into a denial-of-service lever). Exceeding it is rejected at
    /// ticket validation.
    /// </summary>
    public const int MaxPasswordBytes = 1024;
}
