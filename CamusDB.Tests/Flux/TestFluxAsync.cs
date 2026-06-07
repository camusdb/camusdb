
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
using CamusDB.Core.Flux.Models;

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

    private async Task<FluxAction> ExceptionStepAsync(TestFluxState state)
    {
        await Task.Yield();
        throw new System.Exception("error");
    }

    private async Task<FluxAction> OnExceptionAsync(TestFluxState state)
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

    [Test]
    public async Task TestSimpleMachineAutoCompleteSteps()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, CallStepAsync);
        machine.When(TestFluxEnum.Step1, CallStepAsync);
        machine.When(TestFluxEnum.Step2, CallStepAsync);
        machine.When(TestFluxEnum.Step3, CallStepAsync);
        machine.When(TestFluxEnum.Step4, CallStepAsync);
        machine.When(TestFluxEnum.Step5, CallStepAsync);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        Assert.AreEqual(6, state.Number);
    }

    [Test]
    public void TestSimpleMachineForceAbortException()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, ExceptionStepAsync);
        machine.When(TestFluxEnum.Step1, CallStepAsync);
        machine.When(TestFluxEnum.Step2, CallStepAsync);

        machine.WhenAbort(OnExceptionAsync);

        var ex = Assert.CatchAsync<System.Exception>(async () =>
        {
            while (!machine.IsAborted)
                await machine.RunStep(machine.NextStep());
        });

        Assert.IsInstanceOf<System.Exception>(ex);
        Assert.AreEqual("error", ex!.Message);
        Assert.AreEqual(1, state.Number);
    }
}
