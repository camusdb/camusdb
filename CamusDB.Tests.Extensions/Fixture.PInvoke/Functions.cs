/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.InteropServices;
using CamusDB.Extensions;

namespace CamusDB.Fixtures.PInvoke;

/// <summary>Test-only: a P/Invoke declaration, which the SAFE verifier must refuse.</summary>
public static class Functions
{
    [DllImport("libc", EntryPoint = "getpid")]
    private static extern int GetPid();

    [CamusFunction("fx_pid")]
    public static long Pid() => GetPid();
}
