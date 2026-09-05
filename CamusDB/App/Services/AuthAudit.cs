/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;

namespace CamusDB.App.Services;

/// <summary>
/// The authentication events every transport records: a login that succeeded, one that failed, and a
/// logout. One place, so the REST and dashboard sign-in paths cannot drift into logging different
/// things about the same event.
///
/// <para><b>What changed and why.</b> A failed login used to log its error code and nothing else. That
/// was the right instinct about the password and the wrong conclusion about everything around it: with
/// no account and no source in the record, an operator cannot answer "has someone been trying
/// <c>admin</c> from 10.0.0.9 all night". The rate limiter reacts in the moment and then forgets, so
/// there was no way to detect a brute-force attempt from the server's own logs at all.</para>
///
/// <para><b>What is safe to record.</b> The account name and the source address are not credentials.
/// The password, and anything derived from it, is never passed to these methods — the signatures give
/// it nowhere to go.</para>
///
/// <para><b>Every value is a structured log parameter.</b> An account name is text the caller chose,
/// so building the message by concatenation would let a name containing newlines forge a second log
/// line and describe an event that never happened.</para>
/// </summary>
public static class AuthAudit
{
    /// <summary>
    /// Records a rejected login at Warning, with the account that was attempted and where from.
    ///
    /// <para>Warning rather than Information because this is the line an operator greps for, and it is
    /// the one signal that distinguishes a user who mistyped a password from a sustained attempt. A
    /// flood does produce a flood of these — the rate limiter caps how fast they can arrive, and the
    /// alternative of staying quiet is what made the attack invisible.</para>
    ///
    /// <para>The account is recorded as it arrived, not normalized, because the operator reading this
    /// is matching it against what a client sent.</para>
    /// </summary>
    public static void LoginFailed(ILogger<ICamusDB> logger, string account, string source, string code)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            logger.LogWarning(
                "Login failed for {Account} from {Source}: {Code}", account, source, code);
    }

    /// <summary>
    /// Records an accepted login at Information. It is the counterpart that makes the failures
    /// readable: a run of failures ending in a success tells a different story from one that does not.
    /// </summary>
    public static void LoginSucceeded(ILogger<ICamusDB> logger, string account, string source)
    {
        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Login succeeded for {Account} from {Source}", account, source);
    }

    /// <summary>
    /// Records a logout at Information. The account is deliberately absent: logout authenticates by
    /// presenting a token, and resolving it back to a name purely to log it would add a catalog read to
    /// a path that otherwise needs none.
    /// </summary>
    public static void LoggedOut(ILogger<ICamusDB> logger, string source)
    {
        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Logout from {Source}", source);
    }
}
