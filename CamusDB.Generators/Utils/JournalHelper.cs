
using System;
using Microsoft.CodeAnalysis;
using System.Collections.Generic;

namespace CamusDB.Generators.Utils
{
    public static class JournalHelper
    {
        private const string ColumnAttribute = @"CamusDB.Core.Journal.Attributes.JournalField";

        public static IEnumerable<IPropertySymbol> GetProperties(ITypeSymbol symbol)
        {
            foreach (var property in symbol.GetMembers().OfType<IPropertySymbol>())
                yield return property;
        }

        public static (ITypeSymbol type, string fullName, string name) GetGenericArgumentType(IPropertySymbol symbol, int number)
        {
            if (symbol.Type is INamedTypeSymbol namedType && namedType.IsGenericType)
            {
                var type = namedType.TypeArguments[number];
                return (type, type.ContainingNamespace + "." + type.Name, type.Name);
            }

            throw new Exception("Unknown List<T> type");
        }

        public static bool IsJournalField(IPropertySymbol symbol)
        {
            foreach (var attribute in symbol.GetAttributes())
            {
                if (ColumnAttribute == attribute.AttributeClass.ToString())
                    return true;
            }

            return false;
        }

        public static string Uncamelize(string str)
        {
            return str.Substring(0, 1).ToLower() + str.Substring(1);
        }
    }
}

