
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Flux;

public enum FluxAction
{
    Abort = 0,
    Continue = 1,
    Completed = 2,
}

public class FluxMachine<TSteps, TState> where TSteps : Enum
{
    private int currentStep = -1;

    private readonly TState state;

    private readonly Dictionary<TSteps, Func<TState, FluxAction>> handlers = new();

    private readonly Dictionary<TSteps, Func<TState, Task<FluxAction>>> asyncHandlers = new();

    public bool IsAborted { get; private set; }

    public FluxMachine(TState state)
    {
        this.state = state;
    }

    public void When(TSteps status, Func<TState, FluxAction> handler)
    {
        handlers.Add(status, handler);
    }

    public void When(TSteps status, Func<TState, Task<FluxAction>> handler)
    {
        asyncHandlers.Add(status, handler);
    }

    public async Task Feed(TSteps status)
    {
        TryExecuteHandler(status);
        await TryExecuteAsyncHandler(status);
    }

    private void TryExecuteHandler(TSteps status)
    {
        if (IsAborted)
            return;

        if (!handlers.TryGetValue(status, out Func<TState, FluxAction>? handler))
            return;

        FluxAction action = handler(state);

        if (action == FluxAction.Abort || action == FluxAction.Completed)
        {
            IsAborted = true;
            return;
        }
    }

    private async Task TryExecuteAsyncHandler(TSteps status)
    {
        if (IsAborted)
            return;

        if (!asyncHandlers.TryGetValue(status, out Func<TState, Task<FluxAction>>? handler))
            return;

        FluxAction action = await handler(state);

        if (action == FluxAction.Abort || action == FluxAction.Completed)
        {
            IsAborted = true;
            return;
        }
    }

    public TSteps NextStep()
    {
        currentStep++;
        Type type = typeof(TSteps);

        if (!Enum.IsDefined(type, currentStep))
        {
            currentStep = 0;
            IsAborted = true;
        }

        Console.WriteLine(currentStep);

        return (TSteps)Enum.ToObject(type, currentStep);
    }
}
