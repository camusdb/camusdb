
namespace CamusDB.Core.Support;

public class Guard
{
    private const int NOTCALLED = 0;

    private const int CALLED = 1;

    private int _state = NOTCALLED;
        
    public bool CheckAndSetFirstCall
    {
        get { return Interlocked.Exchange(ref _state, CALLED) == NOTCALLED; }
    }
}
