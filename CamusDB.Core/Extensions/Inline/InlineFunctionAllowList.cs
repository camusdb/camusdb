/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Frozen;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>
/// The types and members that the body of an inline SQL function may use. This list is the security
/// boundary of the feature: a body compiles only when every symbol it binds is here. It is a positive
/// list, so a type or member that a new framework version adds is refused until it is added here.
///
/// <para><b>The rule for an entry.</b> The time and memory a member uses are bounded by a small
/// polynomial of the sizes of its inputs, its result depends on its inputs only (not on a clock, a
/// random source, the process, the machine's time zone or a hash that changes between processes), and
/// it gives the body no object that leads outside the list. Each addition needs a review against that
/// rule and a test with a large input under the call deadline.</para>
///
/// <para><b>Two kinds of entry.</b> A type listed with <see cref="TypeRule.AllMembers"/> allows every
/// member except the denied ones; that form is used only for types whose whole surface is pure
/// computation (the numeric types, <c>Math</c>, <c>TimeSpan</c>, enums, tuples). Every other type
/// lists its allowed member names. A name allows all overloads of the member; a single overload is
/// taken out with a signature such as <c>.ctor(int)</c>. In a signature a type is named by its short
/// name, and a type parameter by its position: <c>!0</c> is the first type parameter of the type and
/// <c>!!0</c> of the method, as metadata writes them, so the image check can form the same text.</para>

///
/// <para><b>Member keys.</b> A method is named by its metadata name (<c>Substring</c>, <c>.ctor</c>,
/// <c>op_Equality</c>). A property is named by its name for a read and by its name followed by
/// <c>=</c> for a write, so <c>StringBuilder.Length</c> can be read but not set. A signature is the
/// name followed by the parameter types of the member's definition, as
/// <c>InlineFunctionSymbolChecker</c> spells them.</para>
///
/// <para><b>Rules that no entry can lift</b> are in the symbol checker: no <c>GetHashCode</c> or
/// <c>GetType</c> on any type, every type in a member's signature must be listed, and the limits on
/// where <c>object</c>, a lazy sequence and an exception type may appear.</para>
///
/// <para><see cref="Revision"/> is stored on each function record. Raise it whenever an entry is
/// removed or a rule becomes stricter, so a stored function that a node now refuses is reported with
/// the revision it was accepted under.</para>
/// </summary>
internal static class InlineFunctionAllowList
{
    /// <summary>The revision of this list and of the rules of the symbol checker.</summary>
    public const int Revision = 1;

    /// <summary>What one listed type allows.</summary>
    internal sealed class TypeRule
    {
        /// <summary>True when every member is allowed except <see cref="Denied"/>.</summary>
        public bool AllMembers { get; init; }

        /// <summary>The allowed member keys of a type that is not <see cref="AllMembers"/>.</summary>
        public FrozenSet<string> Allowed { get; init; } = FrozenSet<string>.Empty;

        /// <summary>Member keys and signatures that are refused, whatever <see cref="Allowed"/> holds.</summary>
        public FrozenSet<string> Denied { get; init; } = FrozenSet<string>.Empty;

        /// <summary>True for an exception type: the body can throw and catch it, and read its message.</summary>
        public bool IsException { get; init; }

        /// <summary>True for a type whose constructors and static methods must take a match timeout.</summary>
        public bool RequiresMatchTimeout { get; init; }
    }

    /// <summary>Member names refused on every type, listed or not.</summary>
    public static readonly FrozenSet<string> DeniedEverywhere = new[]
    {
        // The string hash differs between processes, so a hash the body can see differs between nodes.
        "GetHashCode",
        // The door to reflection.
        "GetType",
        "MemberwiseClone",
        "Finalize",
    }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>
    /// Types that hold a sequence that is computed when it is read. The body may use a value of such a
    /// type inside one expression, but may not keep it: see the symbol checker.
    /// </summary>
    public static readonly FrozenSet<string> LazySequenceTypes = new[]
    {
        "System.Collections.Generic.IEnumerable`1",
        "System.Collections.IEnumerable",
        "System.Linq.IOrderedEnumerable`1",
    }.ToFrozenSet(StringComparer.Ordinal);

    private static readonly FrozenDictionary<string, TypeRule> Types = Build();

    private static readonly FrozenDictionary<string, TypeRule> TriggerTypes = BuildTrigger();

    /// <summary>The rule of a listed type, by its full metadata name (<c>System.Collections.Generic.List`1</c>).</summary>
    public static bool TryGetRule(string typeName, out TypeRule rule) => Types.TryGetValue(typeName, out rule!);

    /// <summary>The rule of a type for a body of the given kind: a trigger body sees a few more types and members.</summary>
    public static bool TryGetRule(string typeName, InlineUnitKind kind, out TypeRule rule) =>
        (kind == InlineUnitKind.Trigger ? TriggerTypes : Types).TryGetValue(typeName, out rule!);

    /// <summary>True when the type is listed.</summary>
    public static bool IsListed(string typeName) => Types.ContainsKey(typeName);

    public static bool IsListed(string typeName, InlineUnitKind kind) =>
        (kind == InlineUnitKind.Trigger ? TriggerTypes : Types).ContainsKey(typeName);

    /// <summary>Every listed type name, for the image check and the tests.</summary>
    public static IEnumerable<string> ListedTypes => Types.Keys;

    /// <summary>
    /// True when <paramref name="rule"/> allows the member. <paramref name="key"/> is the member key
    /// and <paramref name="signature"/> the key followed by the parameter types.
    /// </summary>
    public static bool Allows(TypeRule rule, string key, string signature)
    {
        if (rule.Denied.Contains(key) || rule.Denied.Contains(signature))
            return false;

        return rule.AllMembers || rule.Allowed.Contains(key);
    }

    private static FrozenDictionary<string, TypeRule> Build()
    {
        Dictionary<string, TypeRule> types = new(StringComparer.Ordinal);

        void All(string names, string denied = "")
        {
            foreach (string name in Split(names))
                types.Add(name, new TypeRule { AllMembers = true, Denied = Set(denied) });
        }

        void Only(string name, string allowed, string denied = "", bool matchTimeout = false) =>
            types.Add(name, new TypeRule { Allowed = Set(allowed), Denied = Set(denied), RequiresMatchTimeout = matchTimeout });

        // ── Values ───────────────────────────────────────────────────────────────────────────────

        // Pure computation. The format argument of ToString is checked by the symbol checker.
        All("System.Boolean System.Byte System.SByte System.Int16 System.UInt16 System.Int32 System.UInt32 " +
            "System.Int64 System.UInt64 System.Char System.Single System.Double System.Decimal");

        All("System.Math System.MathF System.TimeSpan System.DateOnly System.TimeOnly System.Nullable`1");

        All("System.ValueTuple System.ValueTuple`1 System.ValueTuple`2 System.ValueTuple`3 System.ValueTuple`4 " +
            "System.ValueTuple`5 System.ValueTuple`6 System.ValueTuple`7 System.ValueTuple`8 " +
            "System.Collections.Generic.KeyValuePair`2 System.Collections.Generic.KeyValuePair");

        // Enums. A cast from a number can make any value of an enum, so an enum is listed only when
        // every value of it is harmless.
        All("System.DayOfWeek System.StringComparison System.StringSplitOptions System.MidpointRounding " +
            "System.Globalization.NumberStyles System.Globalization.DateTimeStyles");

        // Compiled builds code with Reflection.Emit; the other options only change the match.
        All("System.Text.RegularExpressions.RegexOptions", denied: "Compiled");

        types.Add("System.Void", new TypeRule());

        Only("System.Object", ".ctor Equals ReferenceEquals ToString");

        // Left out on purpose: the amplifiers PadLeft, PadRight and new string(char, int); Format, whose
        // alignment can arrive at run time; Intern, which keeps memory after the call; Normalize, whose
        // result depends on the host's globalization data.
        Only("System.String",
            "Length Chars Empty .ctor Substring IndexOf LastIndexOf IndexOfAny LastIndexOfAny Contains StartsWith EndsWith " +
            "Trim TrimStart TrimEnd ToUpper ToLower ToUpperInvariant ToLowerInvariant Replace ReplaceLineEndings Split Join " +
            "Concat Insert Remove Equals Compare CompareOrdinal CompareTo IsNullOrEmpty IsNullOrWhiteSpace ToCharArray " +
            "ToString GetEnumerator CopyTo op_Equality op_Inequality",
            denied: ".ctor(char,int) Concat(object[]) Join(string,object[]) Join(char,object[])");

        Only("System.CharEnumerator", "MoveNext Current Dispose");

        // No capacity constructors, no Append(char, count), no AppendFormat, and no write of Length:
        // each makes memory that the inputs do not bound.
        Only("System.Text.StringBuilder",
            ".ctor Append AppendLine AppendJoin Insert Remove Replace Clear ToString Length Chars Chars= Equals",
            denied: ".ctor(int) .ctor(int,int) .ctor(string,int) .ctor(string,int,int,int) Append(char,int) " +
                    "Insert(int,string,int) AppendJoin(string,object[]) AppendJoin(char,object[])");

        Only("System.Guid",
            ".ctor Parse TryParse ParseExact TryParseExact ToString ToByteArray Equals CompareTo Empty AllBitsSet Version Variant " +
            "op_Equality op_Inequality op_LessThan op_GreaterThan op_LessThanOrEqual op_GreaterThanOrEqual");

        // No Now, UtcNow or Today, and nothing that reads the machine's time zone: Parse, ToLocalTime,
        // ToUniversalTime, the file-time conversions and FromBinary. DateTimeKind is not listed, so
        // the body cannot make a local time.
        Only("System.DateTime",
            ".ctor Year Month Day Hour Minute Second Millisecond Microsecond Nanosecond Ticks DayOfWeek DayOfYear Date TimeOfDay " +
            "AddDays AddHours AddMinutes AddSeconds AddMilliseconds AddMicroseconds AddMonths AddYears AddTicks Add Subtract " +
            "Compare CompareTo Equals DaysInMonth IsLeapYear MinValue MaxValue UnixEpoch ToString ToOADate FromOADate Deconstruct " +
            "op_Addition op_Subtraction op_Equality op_Inequality op_LessThan op_GreaterThan op_LessThanOrEqual op_GreaterThanOrEqual");

        Only("System.Convert",
            "ToBoolean ToByte ToSByte ToChar ToInt16 ToUInt16 ToInt32 ToUInt32 ToInt64 ToUInt64 ToSingle ToDouble ToDecimal " +
            "ToString ToBase64String FromBase64String ToHexString ToHexStringLower FromHexString");

        Only("System.BitConverter",
            "GetBytes ToInt16 ToInt32 ToInt64 ToUInt16 ToUInt32 ToUInt64 ToSingle ToDouble ToBoolean ToChar ToString " +
            "DoubleToInt64Bits Int64BitsToDouble SingleToInt32Bits Int32BitsToSingle DoubleToUInt64Bits UInt64BitsToDouble " +
            "SingleToUInt32Bits UInt32BitsToSingle IsLittleEndian");

        Only("System.Enum", "ToString HasFlag CompareTo Equals Parse TryParse IsDefined GetName GetNames GetValues");

        Only("System.Globalization.CultureInfo", "InvariantCulture");
        Only("System.IFormatProvider", "");

        // ── Collections ──────────────────────────────────────────────────────────────────────────

        // No Resize and no CreateInstance. A sized array is made by "new T[n]", which the server
        // compiles to a checked call.
        Only("System.Array",
            "Length Rank GetLength IndexOf LastIndexOf Sort Reverse Copy Empty Fill Clear BinarySearch Exists Find FindAll " +
            "FindIndex FindLast FindLastIndex TrueForAll ForEach Clone GetEnumerator");

        const string enumerator = "MoveNext Current Dispose";
        const string view = "Count GetEnumerator Contains CopyTo";

        // No capacity constructors, Capacity, EnsureCapacity or TrimExcess on any collection.
        Only("System.Collections.Generic.List`1",
            ".ctor Add AddRange Insert InsertRange Remove RemoveAt RemoveAll RemoveRange Clear Contains IndexOf LastIndexOf Count " +
            "Item Item= Sort Reverse ToArray Find FindAll FindIndex FindLast FindLastIndex Exists TrueForAll ForEach BinarySearch " +
            "GetRange GetEnumerator CopyTo",
            denied: ".ctor(int)");
        Only("System.Collections.Generic.List`1+Enumerator", enumerator);

        Only("System.Collections.Generic.Dictionary`2",
            ".ctor Add TryAdd Remove Clear ContainsKey ContainsValue TryGetValue Item Item= Count Keys Values GetEnumerator",
            denied: ".ctor(int) .ctor(int,IEqualityComparer<!0>)");
        Only("System.Collections.Generic.Dictionary`2+Enumerator", enumerator);
        Only("System.Collections.Generic.Dictionary`2+KeyCollection", view);
        Only("System.Collections.Generic.Dictionary`2+KeyCollection+Enumerator", enumerator);
        Only("System.Collections.Generic.Dictionary`2+ValueCollection", view);
        Only("System.Collections.Generic.Dictionary`2+ValueCollection+Enumerator", enumerator);

        const string set =
            ".ctor Add Remove Contains Clear Count UnionWith IntersectWith ExceptWith SymmetricExceptWith IsSubsetOf IsSupersetOf " +
            "IsProperSubsetOf IsProperSupersetOf Overlaps SetEquals RemoveWhere TryGetValue GetEnumerator CopyTo";

        Only("System.Collections.Generic.HashSet`1", set, denied: ".ctor(int) .ctor(int,IEqualityComparer<!0>)");
        Only("System.Collections.Generic.HashSet`1+Enumerator", enumerator);

        Only("System.Collections.Generic.SortedDictionary`2",
            ".ctor Add Remove Clear ContainsKey ContainsValue TryGetValue Item Item= Count Keys Values GetEnumerator");
        Only("System.Collections.Generic.SortedDictionary`2+Enumerator", enumerator);
        Only("System.Collections.Generic.SortedDictionary`2+KeyCollection", view);
        Only("System.Collections.Generic.SortedDictionary`2+KeyCollection+Enumerator", enumerator);
        Only("System.Collections.Generic.SortedDictionary`2+ValueCollection", view);
        Only("System.Collections.Generic.SortedDictionary`2+ValueCollection+Enumerator", enumerator);

        // No GetViewBetween: a view of a view of a view calls through every level, and a loop can
        // make the chain deeper than the stack.
        Only("System.Collections.Generic.SortedSet`1", set + " Min Max Reverse");
        Only("System.Collections.Generic.SortedSet`1+Enumerator", enumerator);

        Only("System.Collections.Generic.Queue`1",
            ".ctor Enqueue Dequeue Peek TryDequeue TryPeek Count Clear Contains ToArray GetEnumerator", denied: ".ctor(int)");
        Only("System.Collections.Generic.Queue`1+Enumerator", enumerator);

        Only("System.Collections.Generic.Stack`1",
            ".ctor Push Pop Peek TryPop TryPeek Count Clear Contains ToArray GetEnumerator", denied: ".ctor(int)");
        Only("System.Collections.Generic.Stack`1+Enumerator", enumerator);

        Only("System.Collections.Generic.CollectionExtensions", "GetValueOrDefault TryAdd Remove");

        Only("System.Collections.Generic.IEnumerable`1", "GetEnumerator");
        Only("System.Collections.Generic.IEnumerator`1", "Current");
        Only("System.Collections.IEnumerable", "GetEnumerator");
        Only("System.Collections.IEnumerator", "MoveNext Current");
        Only("System.IDisposable", "Dispose");
        Only("System.Collections.Generic.IReadOnlyCollection`1", "Count");
        Only("System.Collections.Generic.IReadOnlyList`1", "Item");
        Only("System.Collections.Generic.ICollection`1", "Count Add Remove Contains Clear CopyTo IsReadOnly");
        Only("System.Collections.Generic.IList`1", "Item Item= IndexOf Insert RemoveAt");
        Only("System.Collections.Generic.IDictionary`2", "Item Item= Keys Values ContainsKey Add Remove TryGetValue");
        Only("System.Collections.Generic.IReadOnlyDictionary`2", "Item Keys Values ContainsKey TryGetValue");
        Only("System.Collections.Generic.IComparer`1", "Compare");
        Only("System.Collections.Generic.IEqualityComparer`1", "Equals");
        Only("System.IComparable`1", "CompareTo");
        Only("System.IEquatable`1", "Equals");

        Only("System.Collections.Generic.Comparer`1", "Default Compare Create");
        Only("System.Collections.Generic.EqualityComparer`1", "Default Equals");

        // The culture comparers are left out: their order depends on the host's globalization data.
        Only("System.StringComparer", "Ordinal OrdinalIgnoreCase Compare Equals");

        // ── LINQ ─────────────────────────────────────────────────────────────────────────────────

        // Left out on purpose: Range, Repeat, Sequence and InfiniteSequence make a sequence that no
        // input bounds, and Shuffle is random.
        Only("System.Linq.Enumerable",
            "Where Select SelectMany OrderBy OrderByDescending ThenBy ThenByDescending Order OrderDescending GroupBy Join GroupJoin " +
            "Distinct DistinctBy Union UnionBy Intersect IntersectBy Except ExceptBy Concat Zip Take TakeWhile TakeLast Skip " +
            "SkipWhile SkipLast First FirstOrDefault Last LastOrDefault Single SingleOrDefault ElementAt ElementAtOrDefault Any All " +
            "Count LongCount Contains Sum Min Max MinBy MaxBy Average Aggregate ToList ToArray ToDictionary ToHashSet ToLookup " +
            "Reverse SequenceEqual DefaultIfEmpty Empty Append Prepend Chunk AsEnumerable Cast OfType Index CountBy AggregateBy " +
            "TryGetNonEnumeratedCount");

        Only("System.Linq.IOrderedEnumerable`1", "");
        Only("System.Linq.IGrouping`2", "Key");
        Only("System.Linq.ILookup`2", "Item Contains Count");

        // ── Delegates ────────────────────────────────────────────────────────────────────────────

        // Creation and Invoke only. Method, Target, DynamicInvoke and the static members of Delegate
        // lead to reflection.
        const string callable = ".ctor Invoke";

        Only("System.Action", callable);
        Only("System.Predicate`1", callable);
        Only("System.Comparison`1", callable);
        Only("System.Text.RegularExpressions.MatchEvaluator", callable);

        for (int arity = 1; arity <= 16; arity++)
        {
            Only("System.Action`" + arity.ToString(System.Globalization.CultureInfo.InvariantCulture), callable);
            Only("System.Func`" + arity.ToString(System.Globalization.CultureInfo.InvariantCulture), callable);
        }

        Only("System.Func`17", callable);

        // ── Regular expressions ──────────────────────────────────────────────────────────────────

        // A constructor or a static method must take a match timeout, which the server limits to the
        // time the call has left. InfiniteMatchTimeout and the static CacheSize are left out.
        Only("System.Text.RegularExpressions.Regex",
            ".ctor IsMatch Match Matches Replace Split Count Escape Unescape GetGroupNames GetGroupNumbers GroupNameFromNumber " +
            "GroupNumberFromName ToString Options MatchTimeout RightToLeft",
            matchTimeout: true);

        Only("System.Text.RegularExpressions.Match", "Value Success Index Length Groups Captures NextMatch Result Name Empty ToString");
        Only("System.Text.RegularExpressions.Group", "Value Success Index Length Captures Name ToString");
        Only("System.Text.RegularExpressions.Capture", "Value Index Length ToString");
        // GetEnumerator only, and the symbol checker lets it be read by foreach only: each element is
        // one more match, and Count or a LINQ call would run them all with no budget check in between.
        Only("System.Text.RegularExpressions.MatchCollection", "GetEnumerator");

        Only("System.Text.RegularExpressions.GroupCollection", "Count Item GetEnumerator Keys Values ContainsKey TryGetValue");
        Only("System.Text.RegularExpressions.CaptureCollection", "Count Item GetEnumerator");

        // ── Exceptions ───────────────────────────────────────────────────────────────────────────

        // Construction, the message and the throw helpers. No StackTrace, no TargetSite, no ToString:
        // each shows server internals or leads to reflection.
        const string exception =
            ".ctor Message ParamName ThrowIfNull ThrowIfNullOrEmpty ThrowIfNullOrWhiteSpace ThrowIfNegative ThrowIfNegativeOrZero " +
            "ThrowIfZero ThrowIfGreaterThan ThrowIfGreaterThanOrEqual ThrowIfLessThan ThrowIfLessThanOrEqual ThrowIfEqual ThrowIfNotEqual";

        foreach (string name in Split(
            "System.Exception System.ArgumentException System.ArgumentNullException System.ArgumentOutOfRangeException " +
            "System.InvalidOperationException System.FormatException System.OverflowException System.DivideByZeroException " +
            "System.Collections.Generic.KeyNotFoundException System.IndexOutOfRangeException System.NullReferenceException " +
            "System.Text.RegularExpressions.RegexMatchTimeoutException"))
        {
            types.Add(name, new TypeRule { Allowed = Set(exception), IsException = true });
        }

        return types.ToFrozenDictionary(StringComparer.Ordinal);
    }

    /// <summary>
    /// The list of a trigger body: the function list, plus the types a trigger works with, plus two
    /// members that a function cannot have. A trigger is not cached and has side effects by nature, so
    /// the clock and a fresh id are no risk to it; <c>UtcNow</c> and not <c>Now</c>, because the local
    /// time of a node is still a value another node does not share. <c>Task</c> is listed with no
    /// member: the body can await a task the session gives it and nothing else.
    /// </summary>
    private static FrozenDictionary<string, TypeRule> BuildTrigger()
    {
        Dictionary<string, TypeRule> types = new(Types, StringComparer.Ordinal);

        foreach (string name in Split(
            "CamusDB.Extensions.ITriggerContext CamusDB.Extensions.IRow CamusDB.Extensions.IWritableRow " +
            "CamusDB.Extensions.TriggerTiming CamusDB.Extensions.TriggerOperation CamusDB.Extensions.Inline.InlineSqlSession"))
            types.Add(name, new TypeRule { AllMembers = true });

        types.Add("System.Threading.Tasks.Task", new TypeRule());
        types.Add("System.Threading.Tasks.Task`1", new TypeRule());

        types["System.DateTime"] = WithMembers(types["System.DateTime"], "UtcNow");
        types["System.Guid"] = WithMembers(types["System.Guid"], "NewGuid");

        return types.ToFrozenDictionary(StringComparer.Ordinal);
    }

    private static TypeRule WithMembers(TypeRule rule, string names)
    {
        HashSet<string> allowed = new(rule.Allowed, StringComparer.Ordinal);
        allowed.UnionWith(Split(names));

        return new TypeRule
        {
            AllMembers = rule.AllMembers,
            Allowed = allowed.ToFrozenSet(StringComparer.Ordinal),
            Denied = rule.Denied,
            IsException = rule.IsException,
            RequiresMatchTimeout = rule.RequiresMatchTimeout,
        };
    }

    private static string[] Split(string names) => names.Split(' ', StringSplitOptions.RemoveEmptyEntries);


    private static FrozenSet<string> Set(string names) =>
        names.Length == 0 ? FrozenSet<string>.Empty : Split(names).ToFrozenSet(StringComparer.Ordinal);
}
