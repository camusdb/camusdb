/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Extensions;

/// <summary>
/// The version of an extension as its manifest declares it: <c>major.minor.patch</c> with an optional
/// <c>-prerelease</c> suffix, ordered by the SemVer 2.0 precedence rules. Build metadata
/// (<c>+build</c>) is refused rather than ignored: two builds that differ only in metadata would
/// compare equal, and a node could then not tell which of two loaded files a database means.
///
/// <para>The ordering decides one thing only: which version <c>CREATE EXTENSION</c> and
/// <c>ALTER EXTENSION ... UPDATE</c> pick when the statement names none (the highest version the
/// node loads). A database records the exact text, and binding compares text, never order.</para>
/// </summary>
internal readonly struct ExtensionVersion : IComparable<ExtensionVersion>
{
    private readonly string[] prerelease;

    private ExtensionVersion(int major, int minor, int patch, string[] prerelease, string text)
    {
        Major = major;
        Minor = minor;
        Patch = patch;
        this.prerelease = prerelease;
        Text = text;
    }

    public int Major { get; }

    public int Minor { get; }

    public int Patch { get; }

    /// <summary>The version as written in the manifest.</summary>
    public string Text { get; }

    /// <summary>Parses <paramref name="text"/>, or returns false when it is not a SemVer version without build metadata.</summary>
    public static bool TryParse(string? text, out ExtensionVersion version)
    {
        version = default;

        if (string.IsNullOrEmpty(text) || text.Contains('+'))
            return false;

        int dash = text.IndexOf('-');
        string core = dash < 0 ? text : text[..dash];
        string[] parts = core.Split('.');

        if (parts.Length != 3)
            return false;

        if (!TryParseNumber(parts[0], out int major) || !TryParseNumber(parts[1], out int minor) || !TryParseNumber(parts[2], out int patch))
            return false;

        string[] pre = [];

        if (dash >= 0)
        {
            pre = text[(dash + 1)..].Split('.');

            foreach (string identifier in pre)
            {
                if (identifier.Length == 0)
                    return false;

                foreach (char c in identifier)
                {
                    if (!char.IsAsciiLetterOrDigit(c) && c != '-')
                        return false;
                }

                if (IsNumeric(identifier) && identifier.Length > 1 && identifier[0] == '0')
                    return false;
            }
        }

        version = new ExtensionVersion(major, minor, patch, pre, text);
        return true;
    }

    public int CompareTo(ExtensionVersion other)
    {
        int result = Major.CompareTo(other.Major);
        if (result != 0)
            return result;

        result = Minor.CompareTo(other.Minor);
        if (result != 0)
            return result;

        result = Patch.CompareTo(other.Patch);
        if (result != 0)
            return result;

        string[] mine = prerelease ?? [];
        string[] theirs = other.prerelease ?? [];

        // A release outranks every prerelease of the same core version.
        if (mine.Length == 0 || theirs.Length == 0)
            return theirs.Length.CompareTo(mine.Length);

        for (int i = 0; i < Math.Min(mine.Length, theirs.Length); i++)
        {
            bool mineNumeric = IsNumeric(mine[i]);
            bool theirsNumeric = IsNumeric(theirs[i]);

            if (mineNumeric && theirsNumeric)
                result = long.Parse(mine[i]).CompareTo(long.Parse(theirs[i]));
            else if (mineNumeric != theirsNumeric)
                result = mineNumeric ? -1 : 1;
            else
                result = string.CompareOrdinal(mine[i], theirs[i]);

            if (result != 0)
                return result;
        }

        return mine.Length.CompareTo(theirs.Length);
    }

    public override string ToString() => Text;

    private static bool TryParseNumber(string part, out int value)
    {
        value = 0;

        if (part.Length == 0 || !IsNumeric(part) || (part.Length > 1 && part[0] == '0'))
            return false;

        return int.TryParse(part, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out value);
    }

    private static bool IsNumeric(string part)
    {
        if (part.Length == 0)
            return false;

        foreach (char c in part)
        {
            if (!char.IsAsciiDigit(c))
                return false;
        }

        return part.Length <= 18;
    }
}
