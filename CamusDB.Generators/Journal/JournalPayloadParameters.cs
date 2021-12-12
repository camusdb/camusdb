
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using Microsoft.CodeAnalysis;
using System.Collections.Generic;
using CamusDB.Generators.Utils;

namespace CamusDB.Generators.Journal
{
    internal static class JournalPayloadParameters
    {
        public static void GetMethodParameters(IPropertySymbol symbol, List<string> parameters)
        {
            if (!JournalHelper.IsJournalField(symbol))
                return;

            string type = symbol.Type.Name.ToString();

            switch (type)
            {
                case "String":
                    parameters.Add("string " + JournalHelper.Uncamelize(symbol.Name));
                    break;

                case "Int32":
                    parameters.Add("int " + JournalHelper.Uncamelize(symbol.Name));
                    break;

                case "Boolean":
                    parameters.Add("bool " + JournalHelper.Uncamelize(symbol.Name));
                    break;

                case "List":
                    {
                        (ITypeSymbol type, string fullName, string name) typeOne = JournalHelper.GetGenericArgumentType(symbol, 0);
                        parameters.Add("List<" + typeOne.fullName + "> " + JournalHelper.Uncamelize(symbol.Name));
                    }
                    break;

                case "Dictionary":
                    {
                        (ITypeSymbol type, string fullName, string name) typeOne = JournalHelper.GetGenericArgumentType(symbol, 0);
                        (ITypeSymbol type, string fullName, string name) typeTwo = JournalHelper.GetGenericArgumentType(symbol, 1);
                        parameters.Add($"Dictionary<{typeOne.fullName},{typeTwo.fullName}> {JournalHelper.Uncamelize(symbol.Name)}");
                    }
                    break;

                default:
                    string fullName = symbol.Type.ContainingNamespace + "." + symbol.Type.Name;
                    break;
            }
        }

        public static void GetCallParameters(IPropertySymbol symbol, List<string> parameters)
        {
            if (!JournalHelper.IsJournalField(symbol))
                return;

            parameters.Add(JournalHelper.Uncamelize(symbol.Name));
        }

        public static void GetDataCallParameters(IPropertySymbol symbol, List<string> parameters)
        {
            if (!JournalHelper.IsJournalField(symbol))
                return;
            
            parameters.Add($"data.{symbol.Name}");
        }
    }
}

