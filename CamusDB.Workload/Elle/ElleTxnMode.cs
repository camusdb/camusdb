/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Elle;

/// <summary>
/// How an Elle transaction reaches the server. The history Elle reads is the same for all three; what
/// differs is the server path the statements take, and each path is a separate arm of the no-fault
/// campaign because each has had its own class of defect elsewhere.
/// </summary>
public enum ElleTxnMode
{
    /// <summary><c>BEGIN</c>, one round trip per statement, <c>COMMIT</c>: the interactive path.</summary>
    Explicit,

    /// <summary>No explicit transaction: every statement commits on its own, so every Elle transaction
    /// is one statement long. This is the per-statement path, where a point read or a point write is
    /// its own transaction on the server.</summary>
    Autocommit,

    /// <summary>The statements of a transaction are sent as pipelined frames with <c>BEGIN</c> in the
    /// first frame and the commit in the last, so the server runs a whole transaction from one or two
    /// exchanges: the batch path.</summary>
    Pipeline,
}

/// <summary>Parses the <c>--txn-mode</c> option.</summary>
public static class ElleTxnModes
{
    public static bool TryParse(string value, out ElleTxnMode mode)
    {
        switch (value.Trim().ToLowerInvariant())
        {
            case "explicit": mode = ElleTxnMode.Explicit; return true;
            case "autocommit": mode = ElleTxnMode.Autocommit; return true;
            case "pipeline": mode = ElleTxnMode.Pipeline; return true;
            default: mode = ElleTxnMode.Explicit; return false;
        }
    }

    public static string Name(ElleTxnMode mode) => mode switch
    {
        ElleTxnMode.Autocommit => "autocommit",
        ElleTxnMode.Pipeline => "pipeline",
        _ => "explicit",
    };
}
