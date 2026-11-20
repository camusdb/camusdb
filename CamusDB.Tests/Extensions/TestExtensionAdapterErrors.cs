/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Extensions;
using CamusDB.Extensions;

namespace CamusDB.Tests.Extensions;

/// <summary>
/// What the compiled adapter of an extension function does with an exception of the user method:
/// it wraps it, lets a cancellation pass as it is, and throws the wrapped error from its own frame.
/// </summary>
[TestFixture]
public sealed class TestExtensionAdapterErrors
{
    private static readonly OperationCanceledException Cancelled = new("stopped by the test");

    public static class Throwing
    {
        [CamusFunction("t_fail")]
        public static long Fail(long n) => throw new InvalidOperationException("boom " + n);

        [CamusFunction("t_cancel")]
        public static long Cancel(long n) => throw Cancelled;

        /// <summary>Calls itself until the runtime says the stack is no longer sufficient, then throws.</summary>
        [CamusFunction("t_deep")]
        public static long Deep(long n) =>
            RuntimeHelpers.TryEnsureSufficientExecutionStack() ? Deep(n + 1) + 1 : throw new InvalidOperationException("deep");

        [CamusFunction("t_ok")]
        public static long Ok(long n) => n + 1;
    }

    private static ScalarFunctionEvaluatorDelegate Evaluator(string methodName)
    {
        MethodInfo method = typeof(Throwing).GetMethod(methodName)!;
        return ExtensionFunctionBinder.Bind(method, method.GetCustomAttribute<CamusFunctionAttribute>()!).Evaluator;
    }

    private static ColumnValue[] Arguments(long n) => [new ColumnValue(ColumnType.Integer64, n)];

    [Test]
    public void AnExceptionOfTheMethod_IsWrappedWithTheNameAndTheMessage()
    {
        ScalarFunctionEvaluatorDelegate evaluator = Evaluator(nameof(Throwing.Fail));

        CamusDBException failed = Assert.Throws<CamusDBException>(() => evaluator("t_fail", Arguments(7)))!;

        Assert.AreEqual(CamusDBErrorCodes.ExtensionFunctionFailed, failed.Code);
        Assert.AreEqual("Function 't_fail' failed: boom 7", failed.Message);
        Assert.IsInstanceOf<InvalidOperationException>(failed.InnerException);
    }

    [Test]
    public void AFailedCall_DoesNotFailTheNextCall()
    {
        ScalarFunctionEvaluatorDelegate fail = Evaluator(nameof(Throwing.Fail));
        ScalarFunctionEvaluatorDelegate ok = Evaluator(nameof(Throwing.Ok));

        Assert.Throws<CamusDBException>(() => fail("t_fail", Arguments(1)));
        Assert.Throws<CamusDBException>(() => fail("t_fail", Arguments(2)));
        Assert.AreEqual(6, ok("t_ok", Arguments(5)).LongValue);
    }

    [Test]
    public void ACancellation_PassesAsItIs()
    {
        ScalarFunctionEvaluatorDelegate evaluator = Evaluator(nameof(Throwing.Cancel));

        OperationCanceledException passed = Assert.Throws<OperationCanceledException>(() => evaluator("t_cancel", Arguments(1)))!;

        Assert.AreSame(Cancelled, passed);
    }

    /// <summary>
    /// The runtime runs a <c>catch</c> block on top of the stack of the frame that threw. The filter
    /// below runs while the runtime looks for the handler of the wrapped error, on top of the frame
    /// that threw it. Thrown from inside the adapter's <c>catch</c> block, that frame would be at the
    /// bottom of the recursion, where the stack is not sufficient; an inline function that is stopped
    /// for its depth would then handle one more exception there.
    /// </summary>
    [Test]
    public void TheWrappedError_IsThrownAfterTheStackOfTheMethodIsGivenBack()
    {
        ScalarFunctionEvaluatorDelegate evaluator = Evaluator(nameof(Throwing.Deep));
        bool? roomWhenThrown = null;
        CamusDBException? failed = null;

        bool Observe()
        {
            roomWhenThrown = RuntimeHelpers.TryEnsureSufficientExecutionStack();
            return true;
        }

        Thread thread = new(
            () =>
            {
                try
                {
                    evaluator("t_deep", Arguments(0));
                }
                catch (CamusDBException failure) when (Observe())
                {
                    failed = failure;
                }
            },
            maxStackSize: 1_572_864);

        thread.Start();
        thread.Join();

        Assert.IsNotNull(failed);
        Assert.AreEqual("Function 't_deep' failed: deep", failed!.Message);
        Assert.AreEqual(true, roomWhenThrown, "the wrapped error was thrown while the stack of the method was still in use");
    }
}
