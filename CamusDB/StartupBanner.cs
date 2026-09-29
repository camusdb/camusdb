/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Reflection;
using System.Text;

namespace CamusDB;

/// <summary>
/// Prints the startup banner. On a terminal that renders ANSI colors and UTF-8 the banner is a
/// pixel-font "CamusDB" drawn with half-block characters and a sky-blue to dark-blue gradient;
/// anywhere else (redirected output, NO_COLOR, TERM=dumb, a non-UTF-8 console) it falls back to
/// the plain ASCII banner, so log files and pipes never receive escape sequences.
/// </summary>
internal static class StartupBanner
{
    // Each glyph is 12 pixel rows high. Two pixel rows are packed into one text line with the
    // upper/lower half-block characters, which keeps pixels roughly square on a terminal. Glyph
    // widths may differ ('m' is wider); the renderer reads each glyph's own width.
    private const int GlyphHeight = 12;

    private const int GlyphSpacing = 2;

    private const int ColoredIndent = 2;

    // Width of the widest line of the plain ASCII banner.
    private const int PlainWidth = 45;

    private static readonly string[] UpperC =
    [
        "..#####.",
        ".#######",
        "###...##",
        "##......",
        "##......",
        "##......",
        "##......",
        "##......",
        "##......",
        "###...##",
        ".#######",
        "..#####.",
    ];

    // Lowercase glyphs sit on the same baseline; the x-height is the bottom eight pixel rows.
    private static readonly string[] LowerA =
    [
        "........",
        "........",
        "........",
        "........",
        ".######.",
        "......##",
        ".#######",
        "########",
        "##....##",
        "##....##",
        "########",
        ".#######",
    ];

    private static readonly string[] LowerM =
    [
        "..........",
        "..........",
        "..........",
        "..........",
        "#########.",
        "##########",
        "##..##..##",
        "##..##..##",
        "##..##..##",
        "##..##..##",
        "##..##..##",
        "##..##..##",
    ];

    private static readonly string[] LowerU =
    [
        "........",
        "........",
        "........",
        "........",
        "##....##",
        "##....##",
        "##....##",
        "##....##",
        "##....##",
        "###..###",
        ".######.",
        "..####..",
    ];

    private static readonly string[] LowerS =
    [
        "........",
        "........",
        "........",
        "........",
        ".#######",
        "########",
        "##......",
        "#######.",
        ".#######",
        "......##",
        "########",
        "#######.",
    ];

    private static readonly string[] UpperD =
    [
        "######..",
        "#######.",
        "##...###",
        "##....##",
        "##....##",
        "##....##",
        "##....##",
        "##....##",
        "##....##",
        "##...###",
        "#######.",
        "######..",
    ];

    private static readonly string[] UpperB =
    [
        "######..",
        "#######.",
        "##...###",
        "##....##",
        "##...###",
        "#######.",
        "#######.",
        "##...###",
        "##....##",
        "##...###",
        "#######.",
        "######..",
    ];

    private static readonly string[][] Word = [UpperC, LowerA, LowerM, LowerU, LowerS, UpperD, UpperB];

    // One color per pixel row: a neutral mid blue at the top fading to a dark blue that is almost
    // black at the bottom.
    private static readonly (byte R, byte G, byte B)[] Gradient =
    [
        (72, 118, 214),
        (66, 109, 199),
        (61, 99, 183),
        (55, 90, 168),
        (49, 81, 152),
        (44, 72, 137),
        (38, 62, 121),
        (33, 53, 106),
        (27, 44, 90),
        (21, 35, 75),
        (16, 25, 59),
        (10, 16, 44),
    ];

    // The version text uses a shade from the upper half of the gradient, so it stays readable on a
    // dark terminal background.
    private static readonly (byte R, byte G, byte B) VersionColor = (66, 109, 199);

    public static void Print()
    {
        string version = "v" + GetVersion();

        if (TryGetColorMode(out bool trueColor))
        {
            Console.Write(RenderColored(trueColor));

            StringBuilder sb = new(64);
            sb.Append(' ', Math.Max(0, ColoredIndent + ColoredWidth() - version.Length));
            AppendColor(sb, VersionColor, foreground: true, trueColor);
            sb.Append(version).Append("\e[0m");
            Console.WriteLine(sb.ToString());
        }
        else
        {
            PrintPlain();
            Console.WriteLine(version.PadLeft(PlainWidth));
        }

        Console.WriteLine();
    }

    /// <summary>
    /// The product version from the assembly's informational version (the csproj
    /// <c>&lt;Version&gt;</c>), without the source-revision suffix the SDK appends after '+'.
    /// </summary>
    private static string GetVersion()
    {
        Assembly assembly = typeof(StartupBanner).Assembly;

        string? informational = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (!string.IsNullOrEmpty(informational))
        {
            int plus = informational.IndexOf('+');
            return plus >= 0 ? informational[..plus] : informational;
        }

        return assembly.GetName().Version?.ToString(3) ?? "unknown";
    }

    private static int ColoredWidth()
    {
        int width = GlyphSpacing * (Word.Length - 1);
        for (int g = 0; g < Word.Length; g++)
            width += Word[g][0].Length;

        return width;
    }

    private static void PrintPlain()
    {
        Console.WriteLine("   ____                          ____  ____  ");
        Console.WriteLine("  / ___|__ _ _ __ ___  _   _ ___|  _ \\| __ ) ");
        Console.WriteLine(" | |   / _` | '_ ` _ \\| | | / __| | | |  _ \\ ");
        Console.WriteLine(" | |__| (_| | | | | | | |_| \\__ \\ |_| | |_) |");
        Console.WriteLine("  \\____\\__,_|_| |_| |_|\\__,_|___/____/|____/ ");
    }

    private static string RenderColored(bool trueColor)
    {
        StringBuilder sb = new(4096);
        sb.AppendLine();

        for (int row = 0; row < GlyphHeight; row += 2)
        {
            (byte R, byte G, byte B) top = Gradient[row];
            (byte R, byte G, byte B) bottom = Gradient[row + 1];

            sb.Append(' ', ColoredIndent);

            for (int g = 0; g < Word.Length; g++)
            {
                string upper = Word[g][row];
                string lower = Word[g][row + 1];

                for (int x = 0; x < upper.Length; x++)
                {
                    bool hasTop = upper[x] == '#';
                    bool hasBottom = lower[x] == '#';

                    if (hasTop && hasBottom)
                    {
                        AppendColor(sb, top, foreground: true, trueColor);
                        AppendColor(sb, bottom, foreground: false, trueColor);
                        sb.Append('▀');
                    }
                    else if (hasTop)
                    {
                        AppendColor(sb, top, foreground: true, trueColor);
                        sb.Append("\e[49m▀");
                    }
                    else if (hasBottom)
                    {
                        AppendColor(sb, bottom, foreground: true, trueColor);
                        sb.Append("\e[49m▄");
                    }
                    else
                    {
                        sb.Append("\e[49m ");
                    }
                }

                if (g < Word.Length - 1)
                    sb.Append("\e[49m").Append(' ', GlyphSpacing);
            }

            sb.Append("\e[0m").AppendLine();
        }

        return sb.ToString();
    }

    private static void AppendColor(StringBuilder sb, (byte R, byte G, byte B) color, bool foreground, bool trueColor)
    {
        sb.Append("\e[").Append(foreground ? "38" : "48");

        if (trueColor)
        {
            sb.Append(";2;").Append(color.R).Append(';').Append(color.G).Append(';').Append(color.B).Append('m');
            return;
        }

        // Nearest entry in the 6x6x6 cube of the xterm 256-color palette.
        int r = (color.R * 5 + 127) / 255;
        int g = (color.G * 5 + 127) / 255;
        int b = (color.B * 5 + 127) / 255;
        sb.Append(";5;").Append(16 + 36 * r + 6 * g + b).Append('m');
    }

    /// <summary>
    /// Decides whether stdout is a terminal that renders ANSI colors and the half-block characters,
    /// and whether it accepts 24-bit color or only the 256-color palette.
    /// </summary>
    private static bool TryGetColorMode(out bool trueColor)
    {
        trueColor = false;

        if (Console.IsOutputRedirected)
            return false;

        // https://no-color.org: any non-empty value disables color.
        if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("NO_COLOR")))
            return false;

        if (Console.OutputEncoding.CodePage != Encoding.UTF8.CodePage)
            return false;

        string? term = Environment.GetEnvironmentVariable("TERM");
        if (string.Equals(term, "dumb", StringComparison.Ordinal))
            return false;

        bool windowsTerminal = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("WT_SESSION"));

        // The legacy Windows console only interprets escape sequences when virtual-terminal
        // processing is enabled, so on Windows require a host that is known to handle them.
        if (OperatingSystem.IsWindows() && !windowsTerminal && string.IsNullOrEmpty(term))
            return false;

        if (!OperatingSystem.IsWindows() && string.IsNullOrEmpty(term))
            return false;

        string? colorTerm = Environment.GetEnvironmentVariable("COLORTERM");
        trueColor = windowsTerminal
                    || string.Equals(colorTerm, "truecolor", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(colorTerm, "24bit", StringComparison.OrdinalIgnoreCase);

        return true;
    }
}
