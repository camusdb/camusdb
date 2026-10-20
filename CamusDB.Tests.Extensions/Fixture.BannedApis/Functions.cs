/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using CamusDB.Extensions;

namespace CamusDB.Fixtures.BannedApis;

/// <summary>Test-only: one use of a banned API at each rule level (namespace, type, member), so
/// the SAFE verifier must list all of them in one error.</summary>
public static class Functions
{
    [CamusFunction("fx_banned_file")]
    public static string ReadFile(string path) => System.IO.File.ReadAllText(path);

    [CamusFunction("fx_banned_exit")]
    public static long Exit(long code)
    {
        Environment.Exit((int)code);
        return code;
    }

    [CamusFunction("fx_banned_http")]
    public static string Http(string url)
    {
        using System.Net.Http.HttpClient client = new();
        return client.BaseAddress?.ToString() ?? url;
    }
}
