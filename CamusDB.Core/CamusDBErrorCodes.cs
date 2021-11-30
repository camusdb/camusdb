
namespace CamusDB.Core
{
    public static class CamusDBErrorCodes
    {        
        public const string DatabaseDoesntExist = "CADB0010";
        public const string TableDoesntExist = "CADB0011";
        public const string DatabaseAlreadyExists = "CADB0012";
        public const string TableAlreadyExists = "CADB0013";
        public const string SystemSpaceCorrupt = "CADB0014";

        public const string InvalidInternalOperation = "CADB0099";
        public const string InvalidPageChecksum = "CADB0098";

        public const string InvalidInput = "CADB0200";
        public const string UnknownType = "CADB0201";
        public const string DuplicatePrimaryKey = "CADB0202";
        public const string DuplicateColumn = "CADB0203";

        public const string DuplicatePrimaryKeyValue = "CADB0300";
    }
}

