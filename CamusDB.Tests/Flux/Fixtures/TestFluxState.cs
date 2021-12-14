
using System.Threading.Tasks;

namespace CamusDB.Tests.Flux.Fixtures;

public class TestFluxState
{
    public int Number { get; private set; } = 0;

    public void Increase()
    {
        Number++;
    }

    public async Task IncreaseAsync()
    {
        await Task.Yield();
        Number++;
    }
}
