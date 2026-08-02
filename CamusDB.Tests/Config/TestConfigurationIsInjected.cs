
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using NUnit.Framework;

using CamusDB.Core;

namespace CamusDB.Tests.Config;

/// <summary>
/// Guards the property the configuration migration bought: a component's settings come from the
/// <see cref="CamusDBOptions"/> it was constructed with, not from a value any other code can change.
///
/// <para>These are structural assertions rather than behavioural ones, and that is deliberate. The
/// failure they prevent is not a broken feature but a slow slide back: one convenient
/// <c>public static bool SomethingEnabled</c> reintroduces the very coupling that made tests
/// order-dependent and made "configure the engine, then run" and "run, then configure the engine"
/// indistinguishable at the call site. A behavioural test cannot catch the first such addition —
/// nothing is broken yet — so the shape is asserted instead.</para>
/// </summary>
[TestFixture]
public sealed class TestConfigurationIsInjected
{
    /// <summary>
    /// The remaining process-wide surface, and the only members allowed on it. <c>Ambient</c> and
    /// <c>SetAmbient</c> exist for the regex helpers reached from the static expression evaluator,
    /// which has nowhere yet to carry per-query options; <c>DataDirectory</c> is an async-local
    /// override that gives concurrently running tests isolated directories.
    ///
    /// <para>Adding a name here is a deliberate act: it means accepting a setting that no component
    /// owns. Prefer a property on <see cref="CamusDBOptions"/> passed to whatever needs it.</para>
    /// </summary>
    private static readonly HashSet<string> AllowedAmbientMembers = new(StringComparer.Ordinal)
    {
        "Ambient",
        "SetAmbient",
        "DataDirectory",
    };

    [Test]
    public void TheProcessWideFacadeExposesNothingBeyondTheAgreedRemnants()
    {
        IEnumerable<string> members = typeof(CamusDBConfig)
            .GetMembers(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Where(m => m.MemberType is MemberTypes.Property or MemberTypes.Field or MemberTypes.Method)
            // Property accessors surface as methods too; the property itself is already covered.
            .Where(m => m is not MethodInfo { IsSpecialName: true })
            // Compile-time constants are values, not configuration — they cannot be changed at runtime.
            .Where(m => m is not FieldInfo { IsLiteral: true })
            .Select(m => m.Name);

        IEnumerable<string> unexpected = members.Where(name => !AllowedAmbientMembers.Contains(name));

        Assert.That(unexpected, Is.Empty,
            "A new process-wide setting appeared. Configuration belongs on CamusDBOptions, passed to the " +
            "component that needs it: a static is visible to every engine in the process, and a component " +
            "fixes its configuration when it is constructed, so assigning one after the fact silently does " +
            "nothing. If a setting genuinely has no owner yet, add it to AllowedAmbientMembers with the reason.");
    }

    /// <summary>
    /// No per-knob setters may return. Their absence is what makes "configure, then construct" the only
    /// expressible order — with a setter, a test can write configuration an already-built engine will
    /// never read, and it will pass or fail depending on construction order rather than on behaviour.
    /// </summary>
    [Test]
    public void TheProcessWideFacadeHasNoWritableKnobs()
    {
        IEnumerable<string> writable = typeof(CamusDBConfig)
            .GetProperties(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Where(p => p.CanWrite && p.Name != "DataDirectory")
            .Select(p => p.Name);

        Assert.That(writable, Is.Empty,
            "A writable process-wide knob reappeared. Build the component with the options it should " +
            "have — CamusDBOptions is a record, so `defaults with { Knob = value }` is the replacement.");
    }

    /// <summary>
    /// Every setting on the options record must be immutable after construction. A settable property
    /// would let one holder of the instance change what another holder already read, which is the
    /// shared-mutable-state problem again with extra steps.
    /// </summary>
    [Test]
    public void EverySettingOnTheOptionsRecordIsInitOnly()
    {
        IEnumerable<string> mutable = typeof(CamusDBOptions)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.SetMethod is not null)
            .Where(p => !p.SetMethod!.ReturnParameter
                          .GetRequiredCustomModifiers()
                          .Any(m => m.FullName == "System.Runtime.CompilerServices.IsExternalInit"))
            .Select(p => p.Name);

        Assert.That(mutable, Is.Empty,
            "A CamusDBOptions property is settable after construction. Make it `init` — an engine reads " +
            "its configuration once, so a later write is either invisible or a race.");
    }
}
