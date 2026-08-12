
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsValidator.Validators;

/// <summary>
/// Input-level validation for rename payloads. Schema-aware checks (target existence,
/// name uniqueness, active coordinator jobs) are enforced by <c>ApplySchemaDelta</c>.
/// </summary>
internal sealed class RenameValidator : ValidatorBase
{
    public RenameValidator(CamusDBOptions options) : base(options) { }

    /// <summary>
    /// Validates a rename against <paramref name="options"/>. A shared singleton is no longer possible:
    /// the identifier limit it enforces is per-engine configuration.
    /// </summary>
    public static void Validate(SchemaRenamePayload payload, CamusDBOptions options)
    {
        new RenameValidator(options).ValidateInternal(payload);
    }

    private void ValidateInternal(SchemaRenamePayload payload)
    {
        ValidateIdentifier(payload.TableName, "Table");
        ValidateIdentifier(payload.NewName, "New table");

        if (payload.NewName == payload.TableName)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"New table name '{payload.NewName}' is the same as the current name");

        if (IsReservedName(payload.NewName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"'{payload.NewName}' is a reserved name and cannot be used as a table name");

        ValidateNotReservedRelationName(payload.NewName, "New table");
    }
}
