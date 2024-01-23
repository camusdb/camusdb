
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core;

public static class CamusDBErrorCodes
{
    public const string DatabaseDoesntExist = "CADB0010";
    public const string TableDoesntExist = "CADB0011";
    public const string DatabaseAlreadyExists = "CADB0012";
    public const string TableAlreadyExists = "CADB0013";
    public const string SystemSpaceCorrupt = "CADB0014";
    public const string TableCorrupt = "CADB0015";
    public const string IndexDoesntExist = "CADB0016";
    public const string InvalidIndexLayout = "CADB0017";

    public const string InvalidPageOffset = "CADB00297";

    public const string InvalidInternalOperation = "CADB0099";
    public const string InvalidPageChecksum = "CADB0098";
    public const string InvalidPageLength = "CADB0097";
    public const string InvalidInformationSchema = "CADB0096";

    public const string InvalidInput = "CADB0400";
    public const string UnknownType = "CADB0401";
    public const string DuplicatePrimaryKey = "CADB0402";
    public const string DuplicateColumn = "CADB0403";
    public const string UnknownColumn = "CADB0404";
    public const string UnknownKey = "CADB0405";
    public const string SqlSyntaxError = "CADB0406";
    public const string InvalidAstStmt = "CADB0407";

    public const string DuplicateUniqueKeyValue = "CADB0300";
    public const string NotNullViolation = "CADB0301";
}
