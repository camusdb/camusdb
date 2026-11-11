/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Buffers;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using CamusDB.Extensions;

namespace CamusDB.Fixtures.HostAccess;

/// <summary>
/// Test-only: one use of each API that the SAFE check bans for host disclosure, reflection, raw memory
/// and code that outlives the call. The SAFE verifier must list every one of them in one error; none of
/// these functions is ever called.
/// </summary>
public static class Functions
{
    [CamusFunction("fx_env_secret")]
    public static string EnvironmentSecret(string name) => Environment.GetEnvironmentVariable(name) ?? "";

    [CamusFunction("fx_env_host")]
    public static string Host(string prefix) =>
        prefix + Environment.MachineName + Environment.UserName + Environment.CommandLine + Environment.ProcessId;

    // The reflection path that reads a file with no reference to System.IO.File.
    [CamusFunction("fx_reflection_file")]
    public static string ReflectionFile(string path)
    {
        Type file = typeof(string).Module.GetType("System.IO.File")!;
        MethodInfo read = file.GetMethod("ReadAllText", new[] { typeof(string) })!;
        return ((Func<string, string>)read.CreateDelegate(typeof(Func<string, string>)))(path);
    }

    [CamusFunction("fx_reflection_delegate")]
    public static string ReflectionDelegate(string text)
    {
        Delegate trim = Delegate.CreateDelegate(typeof(Func<string>), text, "Trim");
        return (string)trim.DynamicInvoke()!;
    }

    [CamusFunction("fx_raw_memory")]
    public static long RawMemory(long value) => Unsafe.As<long, long>(ref value);

    [CamusFunction("fx_shared_pool")]
    public static long SharedPool(long size)
    {
        ArrayPool<byte>.Shared.Return(new byte[size]);
        return size;
    }

    [CamusFunction("fx_background")]
    public static long Background(long value)
    {
        new Thread(() => { }).Start();
        ThreadPool.QueueUserWorkItem(_ => { });
        Task.Run(() => { });
        using Timer timer = new(_ => { }, null, 0, 1000);
        return value;
    }
}
