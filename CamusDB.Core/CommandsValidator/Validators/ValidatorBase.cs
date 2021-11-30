
namespace CamusDB.Core.CommandsValidator.Validators;

internal abstract class ValidatorBase
{
    protected bool HasValidCharacters(string name)
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
}

