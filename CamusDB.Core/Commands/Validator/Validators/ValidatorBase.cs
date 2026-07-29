
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsValidator.Validators;

internal abstract class ValidatorBase
{
    /// <summary>
    /// Validates a user-facing identifier (database, table, column, or index name):
    /// must be non-empty, within <see cref="CamusDB.Core.CamusDBConfig.MaxIdentifierLength"/>,
    /// and composed only of alphanumeric characters and underscores.
    /// </summary>
    protected static void ValidateIdentifier(string name, string kind)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"{kind} name is required");

        int maxLen = CamusDB.Core.CamusDBConfig.MaxIdentifierLength;
        if (maxLen > 0 && name.Length > maxLen)
            throw new CamusDBException(
                CamusDBErrorCodes.SchemaLimitExceeded,
                $"{kind} name '{name}' is too long ({name.Length} > {maxLen})");

        if (!HasValidCharacters(name))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"{kind} name has invalid characters");
    }

    /// <summary>
    /// Enforces <see cref="CamusDB.Core.CamusDBConfig.MaxCommentLength"/> on one comment.
    ///
    /// <para>Shared rather than inlined because comments arrive through several unrelated tickets —
    /// <c>COMMENT ON</c>, inline <c>CREATE TABLE</c> (table, column, and index positions), and
    /// <c>ALTER TABLE … ADD COLUMN</c> — and they all end up in the same replicated schema-log entry
    /// and KV checkpoint. A bound enforced on only one of those entry points does not bound anything;
    /// every comment-bearing field on every comment-bearing ticket must call this.</para>
    ///
    /// <para><paramref name="comment"/> may be null (no comment, or a removal); only a present value
    /// is measured.</para>
    /// </summary>
    protected static void ValidateCommentLength(string? comment, string subject)
    {
        if (comment is null)
            return;

        int maxLen = CamusDB.Core.CamusDBConfig.MaxCommentLength;
        if (comment.Length > maxLen)
            throw new CamusDBException(
                CamusDBErrorCodes.CommentTooLong,
                $"{subject} comment is {comment.Length} characters, exceeding the maximum of {maxLen}");
    }


    /// <summary>
    /// Validates a column's constant <c>DEFAULT</c> against the column it belongs to: the value must
    /// be convertible to the column's type, and a String/Bytes default must respect the same length
    /// bound a row value would.
    ///
    /// <para>This runs at the <em>ticket</em> layer on purpose. The SQL path coerces the default while
    /// building the ticket, but the HTTP/gRPC path copies <c>DefaultValue</c> straight into
    /// <see cref="ColumnInfo"/>, so nothing checked its type or size before it was persisted into the
    /// table schema, replicated through the schema log, and written into every checkpoint. A bound
    /// enforced only on the SQL path bounds nothing; validating the ticket covers both entry
    /// points.</para>
    ///
    /// <para>The length bound deliberately matches <c>RowInserter.EnforceLengthBound</c> — a default
    /// that a plain <c>INSERT</c> of the same value would reject must not become storable by being
    /// declared as a default instead.</para>
    /// </summary>
    protected static void ValidateColumnDefault(ColumnInfo column, string subject)
    {
        ColumnValue? defaultValue = column.Default;

        if (defaultValue is null || defaultValue.Type == ColumnType.Null)
            return;

        ColumnValue coerced;

        try
        {
            coerced = CastScalarFunctions.CoerceToColumnType(defaultValue, column.Type);
        }
        catch (CamusDBException)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"{subject} has a DEFAULT of type {defaultValue.Type}, which cannot be converted to the column type {column.Type}");
        }

        if (column.Type == ColumnType.String)
        {
            int max = column.MaxLength ?? CamusDBConfig.DefaultStringMaxLength;
            int length = (coerced.StrValue ?? "").Length;

            if (length > max)
                throw new CamusDBException(
                    CamusDBErrorCodes.ValueTooLong,
                    $"{subject} has a DEFAULT that is too long (max {max}, got {length})");
        }
        else if (column.Type == ColumnType.Bytes)
        {
            int max = column.MaxLength ?? CamusDBConfig.DefaultBytesMaxLength;
            int length = (coerced.BytesValue ?? []).Length;

            if (length > max)
                throw new CamusDBException(
                    CamusDBErrorCodes.ValueTooLong,
                    $"{subject} has a DEFAULT that is too long (max {max}, got {length})");
        }
    }

    protected static bool HasValidCharacters(string name)
    {
        for (int i = 0; i < name.Length; i++)
        {
            char ch = name[i];

            if (ch >= 'a' && ch <= 'z')
                continue;

            if (ch >= 'A' && ch <= 'Z')
                continue;

            if (ch >= '0' && ch <= '9')
                continue;

            if (ch == '_')
                continue;

            return false;
        }

        return true;
    }

    protected static bool IsReservedName(string name)
    {
        return name == "_id";            
    }
}

