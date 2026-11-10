/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

#if !CAMUSDB_BROWSER

using System.Collections.Frozen;
using System.Collections.Immutable;
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;
using System.Reflection.PortableExecutable;
using System.Text;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>
/// The last check before the server loads the code of an inline function: it reads the metadata of
/// the compiled image and requires that <b>every</b> assembly, type and member the image refers to is
/// allowed. It is the second line behind <see cref="InlineFunctionSymbolChecker"/>, and it does not
/// share that check's view: it reads what the compiler wrote, not what the source said. An error in
/// the first check, or in the rewrite, that let a forbidden call through is stopped here.
///
/// <para><b>Positive, like the first check.</b> The check for an operator's extension assembly asks
/// "is anything banned"; this one asks "is everything allowed". A banned list cannot do the job: the
/// compiler itself emits helpers from namespaces a banned list would name
/// (<c>CollectionsMarshal</c>, <c>RuntimeHelpers</c>), so a banned list would need exemptions, and an
/// exemption is a guess about the compiler. Here the helpers are listed exactly
/// (<see cref="HelperMembers"/>), and a test compiles one body for each language form and pins the
/// list: a compiler that starts to emit another helper fails that test, and fails the function with
/// a clear error, instead of loading a call that nobody reviewed.</para>
///
/// <para><b>What it accepts.</b></para>
/// <list type="bullet">
/// <item>An assembly reference on <see cref="AllowedAssemblies"/>.</item>
/// <item>A type reference that is on <see cref="InlineFunctionAllowList"/>, a budget type, or a
/// helper type.</item>
/// <item>A member reference that the allowed list accepts by name and signature, a budget member, or
/// one exact helper member. A helper type gives nothing by itself: only its listed members
/// pass.</item>
/// <item>No native import, no module reference, and no method without managed code.</item>
/// </list>
///
/// <para>It is coarser than the first check in one way: the rules about where <c>object</c>, a lazy
/// sequence or an interface may appear are rules about the source, and metadata does not show them.
/// Those rules protect the stack, not the boundary.</para>
/// </summary>
internal static class InlineFunctionImageVerifier
{
    /// <summary>The simple name of the assembly that holds the budget checks.</summary>
    public const string BudgetAssemblyName = "CamusDB.Extensions.Inline";

    /// <summary>
    /// The assemblies a compiled function may refer to: the framework assemblies that hold the listed
    /// types, and the budget assembly. The load context of a compiled function resolves the same set.
    /// </summary>
    public static readonly FrozenSet<string> AllowedAssemblies = new[]
    {
        "System.Private.CoreLib",
        "System.Runtime",
        "System.Collections",
        "System.Linq",
        "System.Text.RegularExpressions",
        BudgetAssemblyName,
    }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    /// <summary>The members of the budget assembly that the rewrite inserts. <c>Enter</c> and <c>Exit</c> are the server's, and are not here.</summary>
    private static readonly FrozenSet<string> BudgetMembers = new[]
    {
        "CamusDB.Extensions.Inline.CallBudget.Tick",
        "CamusDB.Extensions.Inline.CallBudget.NewArray",
        "CamusDB.Extensions.Inline.CallBudget.IsBudgetException",
        "CamusDB.Extensions.Inline.CallBudget.RegexTimeout",
        "CamusDB.Extensions.Inline.InlineValues.Utc",
    }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>
    /// Types that the compiler refers to for ordinary code and that the body cannot name. A type here
    /// allows no member: see <see cref="HelperMembers"/>.
    /// </summary>
    internal static readonly FrozenSet<string> HelperTypes = new[]
    {
        // Base types of what the compiler generates: closures, delegates, value types, attributes.
        "System.ValueType",
        "System.MulticastDelegate",
        "System.Delegate",
        "System.Attribute",
        "System.IntPtr",
        "System.AsyncCallback",
        "System.IAsyncResult",

        // Attributes on the module, the assembly and generated members.
        "System.Runtime.CompilerServices.CompilationRelaxationsAttribute",
        "System.Runtime.CompilerServices.RuntimeCompatibilityAttribute",
        "System.Runtime.CompilerServices.CompilerGeneratedAttribute",
        "System.Runtime.CompilerServices.NullableAttribute",
        "System.Runtime.CompilerServices.NullableContextAttribute",
        "System.Runtime.CompilerServices.RefSafetyRulesAttribute",
        "System.Runtime.CompilerServices.IsReadOnlyAttribute",
        "System.Runtime.CompilerServices.TupleElementNamesAttribute",
        "System.Runtime.CompilerServices.IsExternalInit",
        "System.Runtime.InteropServices.InAttribute",
        "System.Diagnostics.DebuggableAttribute",
        "System.Diagnostics.DebuggableAttribute+DebuggingModes",
        "System.ParamArrayAttribute",

        // String interpolation.
        "System.Runtime.CompilerServices.DefaultInterpolatedStringHandler",

        // Array initializers, collection expressions and string switches.
        "System.Runtime.CompilerServices.RuntimeHelpers",
        "System.RuntimeFieldHandle",
        "System.Runtime.InteropServices.CollectionsMarshal",
        "System.Span`1",
        "System.ReadOnlySpan`1",

        // A collection expression whose target is an interface: the compiler writes a read-only list
        // type into the image, and that type implements these.
        "System.Collections.ICollection",
        "System.Collections.IList",
        "System.NotSupportedException",
    }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>
    /// The exact members the compiler calls for ordinary code, as <c>Type.Member</c>, or as
    /// <c>Type.Member(parameter types)</c> when only one overload is meant. Each was read against the
    /// entry rule of the allowed list: none gives the body an object it can keep. An entry here can be
    /// a member that the source check refuses when the body writes it (the capacity constructor of a
    /// list): the compiler calls it with a number it took from the source.
    /// </summary>
    internal static readonly FrozenSet<string> HelperMembers = new[]
    {
        "System.Runtime.CompilerServices.CompilationRelaxationsAttribute..ctor",
        "System.Runtime.CompilerServices.RuntimeCompatibilityAttribute..ctor",
        "System.Runtime.CompilerServices.RuntimeCompatibilityAttribute.WrapNonExceptionThrows",
        "System.Runtime.CompilerServices.CompilerGeneratedAttribute..ctor",
        "System.Runtime.CompilerServices.NullableAttribute..ctor",
        "System.Runtime.CompilerServices.NullableContextAttribute..ctor",
        "System.Runtime.CompilerServices.RefSafetyRulesAttribute..ctor",
        "System.Runtime.CompilerServices.IsReadOnlyAttribute..ctor",
        "System.Runtime.CompilerServices.TupleElementNamesAttribute..ctor",
        "System.Diagnostics.DebuggableAttribute..ctor",
        "System.ParamArrayAttribute..ctor",

        "System.Runtime.CompilerServices.DefaultInterpolatedStringHandler..ctor",
        "System.Runtime.CompilerServices.DefaultInterpolatedStringHandler.AppendLiteral",
        "System.Runtime.CompilerServices.DefaultInterpolatedStringHandler.AppendFormatted",
        "System.Runtime.CompilerServices.DefaultInterpolatedStringHandler.ToStringAndClear",

        "System.Runtime.CompilerServices.RuntimeHelpers.InitializeArray",
        "System.Runtime.InteropServices.CollectionsMarshal.SetCount",
        "System.Runtime.InteropServices.CollectionsMarshal.AsSpan",
        "System.Span`1.get_Item",
        "System.Span`1.get_Length",

        "System.NotSupportedException..ctor",

        // A collection expression makes its list with the number of elements the source wrote.
        "System.Collections.Generic.List`1..ctor(int)",

        // "text + character": the compiler joins the two as spans, and makes the span of the
        // character from a reference to it.
        "System.String.op_Implicit(string)",
        "System.ReadOnlySpan`1..ctor(!0)",

        // A spread in a collection expression ("[..a, x, ..b]"): the compiler copies each source as a
        // span into the new list.
        "System.ReadOnlySpan`1..ctor(!0[])",
        "System.ReadOnlySpan`1.get_Length",
        "System.ReadOnlySpan`1.CopyTo",
        "System.Span`1.Slice(int,int)",

        "System.Array.Get",
        "System.Array.Set",
        "System.Array.Address",
        "System.Array..ctor",
    }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>
    /// Helper types whose every member passes. Only the two interfaces that the compiler's own
    /// read-only list type implements: that type must name each interface member to implement it, and
    /// the body cannot name the interfaces at all.
    /// </summary>
    internal static readonly FrozenSet<string> HelperInterfaces = new[]
    {
        "System.Collections.ICollection",
        "System.Collections.IList",
    }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>
    /// Returns every reference of <paramref name="image"/> that is not allowed, in metadata order. An
    /// empty list means the image passes.
    /// </summary>
    public static IReadOnlyList<string> Verify(byte[] image)
    {
        using PEReader pe = new(new MemoryStream(image, writable: false));
        MetadataReader reader = pe.GetMetadataReader();

        List<string> findings = [];
        HashSet<string> seen = new(StringComparer.Ordinal);

        void Add(string finding)
        {
            if (seen.Add(finding))
                findings.Add(finding);
        }

        foreach (AssemblyReferenceHandle handle in reader.AssemblyReferences)
        {
            string name = reader.GetString(reader.GetAssemblyReference(handle).Name);

            if (!AllowedAssemblies.Contains(name))
                Add($"assembly reference {name}");
        }

        foreach (TypeReferenceHandle handle in reader.TypeReferences)
        {
            string name = TypeName(reader, handle);

            if (!IsAllowedType(name))
                Add($"type {name}");
        }

        SignatureNames names = SignatureNames.Instance;

        foreach (MemberReferenceHandle handle in reader.MemberReferences)
        {
            MemberReference member = reader.GetMemberReference(handle);
            string memberName = reader.GetString(member.Name);
            string? owner = OwnerOf(reader, member.Parent, names);

            // A member of a type the image defines itself: a closure, a generated list type.
            if (owner is null)
            {
                if (member.Parent.Kind is not (HandleKind.TypeDefinition or HandleKind.TypeSpecification or HandleKind.MethodDefinition))
                    Add($"member {memberName} of an unknown owner ({member.Parent.Kind})");

                continue;
            }

            string full = owner + "." + memberName;
            string key = MemberKey(memberName, member.GetKind());
            string parameters = member.GetKind() == MemberReferenceKind.Method
                ? "(" + string.Join(",", member.DecodeMethodSignature(names, null).ParameterTypes) + ")"
                : "";

            if (BudgetMembers.Contains(full) || HelperMembers.Contains(full) || HelperMembers.Contains(full + parameters) ||
                HelperInterfaces.Contains(owner))
                continue;


            if (InlineFunctionAllowList.DeniedEverywhere.Contains(memberName) ||
                !InlineFunctionAllowList.TryGetRule(owner, out InlineFunctionAllowList.TypeRule? rule) ||
                !InlineFunctionAllowList.Allows(rule, key, key + parameters))
                Add($"member {full}{parameters}");

        }

        if (reader.GetTableRowCount(TableIndex.ImplMap) > 0)
            Add("a native import (DllImport)");

        if (reader.GetTableRowCount(TableIndex.ModuleRef) > 0)
            Add("a module reference");

        foreach (MethodDefinitionHandle handle in reader.MethodDefinitions)
        {
            MethodDefinition method = reader.GetMethodDefinition(handle);

            // A delegate's methods are implemented by the runtime; nothing else in a body is.
            const System.Reflection.MethodImplAttributes unmanaged =
                System.Reflection.MethodImplAttributes.Native | System.Reflection.MethodImplAttributes.Unmanaged |
                System.Reflection.MethodImplAttributes.InternalCall;

            if ((method.ImplAttributes & unmanaged) != 0 || (method.Attributes & System.Reflection.MethodAttributes.PinvokeImpl) != 0)
                Add($"method {reader.GetString(method.Name)} without managed code");
        }

        return findings;
    }

    private static bool IsAllowedType(string name) =>
        InlineFunctionAllowList.IsListed(name) || HelperTypes.Contains(name) ||
        name is "CamusDB.Extensions.Inline.CallBudget" or "CamusDB.Extensions.Inline.InlineValues";

    /// <summary>
    /// The allowed-list key of a referenced member: a property accessor is its property, read
    /// (<c>get_Length</c> is <c>Length</c>) or written (<c>set_Item</c> is <c>Item=</c>).
    /// </summary>
    private static string MemberKey(string name, MemberReferenceKind kind)
    {
        if (kind != MemberReferenceKind.Method)
            return name;

        if (name.StartsWith("get_", StringComparison.Ordinal))
            return name[4..];

        if (name.StartsWith("set_", StringComparison.Ordinal))
            return name[4..] + "=";

        return name;
    }

    /// <summary>
    /// The full name of the type that owns a referenced member when another assembly defines it, and
    /// null when the image defines it. A member of a constructed type (<c>List&lt;int&gt;</c>) and of
    /// an array is reached through a type specification.
    /// </summary>
    private static string? OwnerOf(MetadataReader reader, EntityHandle parent, SignatureNames names)
    {
        switch (parent.Kind)
        {
            case HandleKind.TypeReference:
                return TypeName(reader, (TypeReferenceHandle)parent);

            case HandleKind.TypeSpecification:
                return reader.GetTypeSpecification((TypeSpecificationHandle)parent).DecodeSignature(OwnerNames.Instance, null);

            default:
                return null;
        }
    }

    internal static string TypeName(MetadataReader reader, TypeReferenceHandle handle)
    {
        TypeReference type = reader.GetTypeReference(handle);
        string name = reader.GetString(type.Name);

        if (type.ResolutionScope.Kind == HandleKind.TypeReference)
            return TypeName(reader, (TypeReferenceHandle)type.ResolutionScope) + "+" + name;

        string space = reader.GetString(type.Namespace);
        return space.Length == 0 ? name : space + "." + name;
    }

    /// <summary>
    /// Reads the owner of a member from a type specification: the definition of a constructed generic
    /// type, <c>System.Array</c> for an array, and null for a type the image defines.
    /// </summary>
    private sealed class OwnerNames : ISignatureTypeProvider<string?, object?>
    {
        public static readonly OwnerNames Instance = new();

        public string? GetArrayType(string? elementType, ArrayShape shape) => "System.Array";

        public string? GetSZArrayType(string? elementType) => "System.Array";

        public string? GetGenericInstantiation(string? genericType, ImmutableArray<string?> typeArguments) => genericType;

        public string? GetTypeFromReference(MetadataReader reader, TypeReferenceHandle handle, byte rawTypeKind) => TypeName(reader, handle);

        public string? GetTypeFromDefinition(MetadataReader reader, TypeDefinitionHandle handle, byte rawTypeKind) => null;

        public string? GetTypeFromSpecification(MetadataReader reader, object? genericContext, TypeSpecificationHandle handle, byte rawTypeKind) =>
            reader.GetTypeSpecification(handle).DecodeSignature(this, genericContext);

        public string? GetByReferenceType(string? elementType) => elementType;

        public string? GetPointerType(string? elementType) => "pointer";

        public string? GetFunctionPointerType(MethodSignature<string?> signature) => "pointer";

        public string? GetGenericMethodParameter(object? genericContext, int index) => null;

        public string? GetGenericTypeParameter(object? genericContext, int index) => null;

        public string? GetModifiedType(string? modifier, string? unmodifiedType, bool isRequired) => unmodifiedType;

        public string? GetPinnedType(string? elementType) => elementType;

        public string? GetPrimitiveType(PrimitiveTypeCode typeCode) => "System." + typeCode;
    }

    /// <summary>
    /// Spells the parameter types of a referenced method the way
    /// <see cref="InlineFunctionSymbolChecker.Signature"/> does, so a signature on the allowed list
    /// means the same in both checks: short type names, keywords for the built-in types, and
    /// <c>!0</c> / <c>!!0</c> for type parameters.
    /// </summary>
    private sealed class SignatureNames : ISignatureTypeProvider<string, object?>
    {
        public static readonly SignatureNames Instance = new();

        public string GetArrayType(string elementType, ArrayShape shape) => elementType + "[" + new string(',', shape.Rank - 1) + "]";

        public string GetSZArrayType(string elementType) => elementType + "[]";

        public string GetGenericInstantiation(string genericType, ImmutableArray<string> typeArguments) =>
            genericType + "<" + string.Join(",", typeArguments) + ">";

        public string GetTypeFromReference(MetadataReader reader, TypeReferenceHandle handle, byte rawTypeKind) =>
            Short(reader.GetString(reader.GetTypeReference(handle).Name));

        public string GetTypeFromDefinition(MetadataReader reader, TypeDefinitionHandle handle, byte rawTypeKind) =>
            Short(reader.GetString(reader.GetTypeDefinition(handle).Name));

        public string GetTypeFromSpecification(MetadataReader reader, object? genericContext, TypeSpecificationHandle handle, byte rawTypeKind) =>
            reader.GetTypeSpecification(handle).DecodeSignature(this, genericContext);

        public string GetByReferenceType(string elementType) => elementType;

        public string GetPointerType(string elementType) => elementType + "*";

        public string GetFunctionPointerType(MethodSignature<string> signature) => "delegate*";

        public string GetGenericMethodParameter(object? genericContext, int index) => "!!" + index.ToString(System.Globalization.CultureInfo.InvariantCulture);

        public string GetGenericTypeParameter(object? genericContext, int index) => "!" + index.ToString(System.Globalization.CultureInfo.InvariantCulture);

        public string GetModifiedType(string modifier, string unmodifiedType, bool isRequired) => unmodifiedType;

        public string GetPinnedType(string elementType) => elementType;

        public string GetPrimitiveType(PrimitiveTypeCode typeCode) => typeCode switch
        {
            PrimitiveTypeCode.Object => "object",
            PrimitiveTypeCode.String => "string",
            PrimitiveTypeCode.Boolean => "bool",
            PrimitiveTypeCode.Char => "char",
            PrimitiveTypeCode.Byte => "byte",
            PrimitiveTypeCode.SByte => "sbyte",
            PrimitiveTypeCode.Int16 => "short",
            PrimitiveTypeCode.UInt16 => "ushort",
            PrimitiveTypeCode.Int32 => "int",
            PrimitiveTypeCode.UInt32 => "uint",
            PrimitiveTypeCode.Int64 => "long",
            PrimitiveTypeCode.UInt64 => "ulong",
            PrimitiveTypeCode.Single => "float",
            PrimitiveTypeCode.Double => "double",
            _ => typeCode.ToString(),
        };

        /// <summary>
        /// A metadata type name without its arity: <c>IEqualityComparer`1</c> is
        /// <c>IEqualityComparer</c>. <c>Decimal</c> is a type reference in metadata and a keyword in
        /// the source, so it is spelled as the keyword here too.
        /// </summary>
        private static string Short(string name)
        {
            if (name == "Decimal")
                return "decimal";

            int tick = name.IndexOf('`');
            return tick < 0 ? name : name[..tick];
        }
    }

}

#endif
