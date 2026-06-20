
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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

