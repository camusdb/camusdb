

namespace CamusDB.Core.CommandsExecutor.Controllers;

public abstract class CommandBase
{
    public void JournalForceFailure()
    {
        throw new CamusDBException(
            CamusDBErrorCodes.JournalForcedFailure,
            "Journal forced failure"
        );
    }
}
