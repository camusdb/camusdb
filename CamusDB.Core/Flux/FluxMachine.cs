
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Flux.Models;

namespace CamusDB.Core.Flux;

/// <summary>
/// Cheap state machine
/// </summary>
/// <typeparam name="TSteps"></typeparam>
/// <typeparam name="TState"></typeparam>
public sealed class FluxMachine<TSteps, TState> where TSteps : Enum
{
    private int currentStep = -1;

    private readonly TState state;

    private Func<TState, FluxAction>? abortHandler;

    private Func<TState, Task<FluxAction>>? abortAsyncHandler;

    private readonly Dictionary<TSteps, Func<TState, FluxAction>> handlers = new();

    private readonly Dictionary<TSteps, Func<TState, Task<FluxAction>>> asyncHandlers = new();

    public bool IsAborted { get; private set; }

    public FluxAction LastAction { get; private set; }

    public FluxMachine(TState state)
    {
        this.state = state;
    }

    public FluxMachine(TState state, TSteps step)
    {
        this.state = state;
        this.currentStep = Convert.ToInt32(step) - 1;
    }

    public void When(TSteps status, Func<TState, FluxAction> handler)
    {
        handlers.Add(status, handler);
    }

    public void When(TSteps status, Func<TState, Task<FluxAction>> handler)
    {
        asyncHandlers.Add(status, handler);
    }

    public void WhenAbort(Func<TState, FluxAction> handler)
    {
        abortHandler = handler;
    }

    public void WhenAbort(Func<TState, Task<FluxAction>> handler)
    {
        abortAsyncHandler = handler;
    }

    public async Task RunStep(TSteps status)
    {
        //Console.WriteLine(status);

        await TryExecuteHandler(status);
        await TryExecuteAsyncHandler(status);
    }

    private async Task TryExecuteHandler(TSteps status)
    {
        if (IsAborted)
            return;

        if (!handlers.TryGetValue(status, out Func<TState, FluxAction>? handler))
            return;

        try
        {
            LastAction = handler(state);

            if (LastAction == FluxAction.Completed)
                IsAborted = true;

            if (LastAction == FluxAction.Abort)
            {
                IsAborted = true;
                await RunAbortHandlers();
            }
        }
        catch (CamusDBException)
        {
            await RunAbortHandlers();
            throw;
        }
        catch (Exception)
        {
            await RunAbortHandlers();
            throw;
        }
    }

    private async Task TryExecuteAsyncHandler(TSteps status)
    {
        if (IsAborted)
            return;

        if (!asyncHandlers.TryGetValue(status, out Func<TState, Task<FluxAction>>? handler))
            return;

        try
        {
            LastAction = await handler(state);

            if (LastAction == FluxAction.Completed)
                IsAborted = true;

            if (LastAction == FluxAction.Abort)
            {
                IsAborted = true;
                await RunAbortHandlers();
            }
        }
        catch (CamusDBException)
        {
            await RunAbortHandlers();
            throw;
        }
        catch (Exception)
        {
            await RunAbortHandlers();
            throw;
        }
    }

    private async Task RunAbortHandlers()
    {
        abortHandler?.Invoke(state);

        if (abortAsyncHandler != null)
            await abortAsyncHandler(state);
    }

    public TSteps NextStep()
    {
        currentStep++;
        Type type = typeof(TSteps);

        if (!Enum.IsDefined(type, currentStep))
        {
            currentStep = 0;
            IsAborted = true;
            LastAction = FluxAction.Completed;
        }

        //Console.WriteLine(currentStep);

        return (TSteps)Enum.ToObject(type, currentStep);
    }
}
