
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */


using System.IO;
using NUnit.Framework;
using CamusDB.Core.Flux;
using System.Threading.Tasks;
using CamusDB.Tests.Flux.Fixtures;

namespace CamusDB.Tests.Flux;

internal sealed class TestFluxAsync
{
    private async Task<FluxAction> CallStepAsync(TestFluxState state)
    {
        await state.IncreaseAsync();
        return FluxAction.Continue;
    }

    private async Task<FluxAction> AbortStepAsync(TestFluxState state)
    {
        await state.IncreaseAsync();
        return FluxAction.Abort;
    }

    private async Task<FluxAction> CompleteStepAsync(TestFluxState state)
    {
        await state.IncreaseAsync();
        return FluxAction.Completed;
    }

    [Test]
    public async Task TestSimpleMachineOneStep()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, CallStepAsync);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        Assert.AreEqual(1, state.Number);
    }

    [Test]
    public async Task TestSimpleMachineTwoSteps()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, CallStepAsync);
        machine.When(TestFluxEnum.Step1, CallStepAsync);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        Assert.AreEqual(2, state.Number);
    }

    [Test]
    public async Task TestSimpleMachineThreeSteps()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, CallStepAsync);
        machine.When(TestFluxEnum.Step1, CallStepAsync);
        machine.When(TestFluxEnum.Step2, CallStepAsync);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        Assert.AreEqual(3, state.Number);
    }

    [Test]
    public async Task TestSimpleMachineMissingSteps()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, CallStepAsync);
        machine.When(TestFluxEnum.Step2, CallStepAsync);
        machine.When(TestFluxEnum.Step4, CallStepAsync);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        Assert.AreEqual(3, state.Number);
    }

    [Test]
    public async Task TestSimpleMachineAbortSteps()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, CallStepAsync);
        machine.When(TestFluxEnum.Step2, AbortStepAsync);
        machine.When(TestFluxEnum.Step4, CallStepAsync);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        Assert.AreEqual(2, state.Number);
    }

    [Test]
    public async Task TestSimpleMachineCompleteSteps()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, CallStepAsync);
        machine.When(TestFluxEnum.Step2, CompleteStepAsync);
        machine.When(TestFluxEnum.Step4, CallStepAsync);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        Assert.AreEqual(2, state.Number);
    }
}
