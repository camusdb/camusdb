
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using NUnit.Framework;
using System.Threading.Tasks;
using CamusDB.Core.Flux;
using CamusDB.Tests.Flux.Fixtures;
using CamusDB.Core.Flux.Models;

namespace CamusDB.Tests.Flux;

internal sealed class TestFlux
{
    private FluxAction CallStep(TestFluxState state)
    {
        state.Increase();
        return FluxAction.Continue;
    }

    private FluxAction AbortStep(TestFluxState state)
    {
        state.Increase();
        return FluxAction.Abort;
    }

    private FluxAction CompleteStep(TestFluxState state)
    {
        state.Increase();
        return FluxAction.Completed;
    }

    private FluxAction ExceptionStep(TestFluxState state)
    {        
        throw new System.Exception("error");
    }

    private FluxAction OnException(TestFluxState state)
    {
        state.Increase();
        return FluxAction.Completed;
    }

    [Test]
    public async Task TestSimpleMachineOneStep()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, CallStep);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        Assert.AreEqual(1, state.Number);
    }

    [Test]
    public async Task TestSimpleMachineTwoSteps()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, CallStep);
        machine.When(TestFluxEnum.Step1, CallStep);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        Assert.AreEqual(2, state.Number);
    }

    [Test]
    public async Task TestSimpleMachineThreeSteps()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, CallStep);
        machine.When(TestFluxEnum.Step1, CallStep);
        machine.When(TestFluxEnum.Step2, CallStep);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        Assert.AreEqual(3, state.Number);
    }

    [Test]
    public async Task TestSimpleMachineMissingSteps()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, CallStep);
        machine.When(TestFluxEnum.Step2, CallStep);
        machine.When(TestFluxEnum.Step4, CallStep);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        Assert.AreEqual(3, state.Number);
    }

    [Test]
    public async Task TestSimpleMachineAbortSteps()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, CallStep);
        machine.When(TestFluxEnum.Step2, AbortStep);
        machine.When(TestFluxEnum.Step4, CallStep);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        Assert.AreEqual(2, state.Number);
    }

    [Test]
    public async Task TestSimpleMachineCompleteSteps()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, CallStep);
        machine.When(TestFluxEnum.Step2, CompleteStep);
        machine.When(TestFluxEnum.Step4, CallStep);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        Assert.AreEqual(2, state.Number);
    }

    [Test]
    public async Task TestSimpleMachineAutoCompleteSteps()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, CallStep);
        machine.When(TestFluxEnum.Step1, CallStep);
        machine.When(TestFluxEnum.Step2, CallStep);
        machine.When(TestFluxEnum.Step3, CallStep);
        machine.When(TestFluxEnum.Step4, CallStep);
        machine.When(TestFluxEnum.Step5, CallStep);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        Assert.AreEqual(6, state.Number);
    }

    [Test]
    public void TestSimpleMachineForceAbortException()
    {
        TestFluxState state = new();
        FluxMachine<TestFluxEnum, TestFluxState> machine = new(state);

        machine.When(TestFluxEnum.Step0, ExceptionStep);
        machine.When(TestFluxEnum.Step1, CallStep);
        machine.When(TestFluxEnum.Step2, CallStep);

        machine.WhenAbort(OnException);

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
